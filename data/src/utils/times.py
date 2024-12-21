import argparse
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import requests as r

from utils.constants import (
    DOCKER_ENDPOINT_FIRST_PASS,
    DOCKER_ENDPOINT_SECOND_PASS,
)
from utils.utils import (
    create_empty_df,
    format_time,
    group_by_column_sets,
    merge_overlapping_df_list,
)


class TravelTimeArgs:
    """
    Class to hold and validate arguments for travel time calculations.

    Arguments are passed at runtime via the command line and validated
    against the parameters file ('params.yaml').
    """

    def __init__(self, args: argparse.Namespace, params: dict) -> None:
        self.mode: str
        self.year: str
        self.geography: str
        self.state: str
        self.centroid_type: str
        self.chunk: str | None
        self.write_to_s3: bool

        self._args_to_attr(args)
        self._validate_mode(params, self.mode)
        self._validate_centroid_type(self.centroid_type)
        self._validate_chunk(self.chunk)

    def _args_to_attr(self, args: argparse.Namespace) -> None:
        for k, v in vars(args).items():
            setattr(self, k.replace("-", "_"), v)

    def _validate_mode(self, params: dict, mode: str) -> None:
        valid_modes = params["times"]["mode"]
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode, must be one of: {valid_modes}")

    def _validate_centroid_type(self, centroid_type: str) -> None:
        valid_centroid_types = ["weighted", "unweighted"]
        if centroid_type not in valid_centroid_types:
            raise ValueError(
                "Invalid centroid_type, must be one "
                f"of: {valid_centroid_types}"
            )

    def _validate_chunk(self, chunk: str | None) -> None:
        if chunk and not re.match(r"^\d+-\d+_\d+-\d+$", chunk):
            raise ValueError(
                "Invalid chunk argument. Must be four numbers "
                "separated by dashes and an underscore (e.g., '01-02_00-10')."
            )
        if chunk:
            for part in chunk.split("_"):
                sub = part.split("-")
                if len(sub[0]) != len(sub[1]):
                    raise ValueError(
                        "Invalid chunk argument. Both numbers must have "
                        "the same number of digits (including zero-padding)."
                    )


class TravelTimePaths:
    """
    Class to manage all input and output paths for travel time calculations.

    Paths are generated based on input arguments. Also holds remote (R2)
    paths and write settings for Parquet files.
    """

    def __init__(
        self,
        args: TravelTimeArgs,
        version: str,
        s3_bucket: str,
        compression_type: Literal["snappy", "gzip", "brotli", "lz4", "zstd"],
        compression_level: int = 3,
        endpoint_url: str | None = None,
    ) -> None:
        self.args: TravelTimeArgs = args
        self.version: str = version
        self.s3_bucket: str = s3_bucket
        self.compression_type: Literal[
            "snappy", "gzip", "brotli", "lz4", "zstd"
        ] = compression_type
        self.compression_level: int = compression_level
        self.endpoint_url: str | None = endpoint_url
        self.storage_options = {
            "s3": {"client_kwargs": {"endpoint_url": endpoint_url}},
            "local": {},
        }

        self.input: dict[str, dict] = {}
        self.output: dict[str, dict] = {}
        self._setup_paths()

    @property
    def _main_path(self) -> Path:
        """Base path for all data."""
        return Path(
            f"year={self.args.year}/geography={self.args.geography}/",
            f"state={self.args.state}",
        )

    @property
    def _output_path(self) -> Path:
        """Base path for output data."""
        return Path(
            f"version={self.version}/mode={self.args.mode}/",
            self._main_path,
            f"centroid_type={self.args.centroid_type}",
        )

    @property
    def _file_name(self) -> str:
        """Generates file name based on chunk."""
        return (
            f"part-{self.args.chunk}.parquet"
            if self.args.chunk
            else "part-0.parquet"
        )

    def _setup_paths(self) -> None:
        """Sets up all input and output paths."""
        self.input = self._create_input_paths()
        self.output = self._create_output_paths()
        self._create_output_directories()

    def _create_input_paths(self) -> dict[str, dict[str, Path]]:
        """Creates all input paths and stores them in a dictionary."""
        return {
            "main": {"path": self._main_path},
            "dirs": {
                "valhalla_tiles": Path(
                    Path.cwd(),
                    f"intermediate/valhalla_tiles/year={self.args.year}",
                    f"geography=state/state={self.args.state}",
                )
            },
            "files": {
                "valhalla_tiles_file": Path(
                    Path.cwd(),
                    f"intermediate/valhalla_tiles/year={self.args.year}",
                    f"geography=state/state={self.args.state}/",
                    "valhalla_tiles.tar.zst",
                ),
                "origins_file": Path(
                    Path.cwd(),
                    "intermediate/cenloc",
                    self._main_path,
                    f"{self.args.state}.parquet",
                ),
                "destinations_file": Path(
                    Path.cwd(),
                    "intermediate/destpoint",
                    self._main_path,
                    f"{self.args.state}.parquet",
                ),
            },
        }

    def _create_output_paths(self) -> dict[str, dict[str, Any]]:
        """Creates all input paths and stores them in a dictionary."""
        output_dirs = {
            "times": Path("times", self._output_path),
            "origins": Path("points", self._output_path, "point_type=origin"),
            "destinations": Path(
                "points", self._output_path, "point_type=destination"
            ),
            "missing_pairs": Path("missing_pairs", self._output_path),
            "metadata": Path("metadata", self._output_path),
        }

        prefix = {
            "local": Path(Path.cwd(), "output"),
            "s3": Path(self.s3_bucket),
        }

        output_files = {}
        for loc in ["local", "s3"]:
            output_files[loc] = {
                f"{key}_file": f"s3://{Path(prefix[loc], path, self._file_name)}"
                if loc == "s3"
                else Path(prefix[loc], path, self._file_name)
                for key, path in output_dirs.items()
            }

        return {"prefix": prefix, "dirs": output_dirs, **output_files}

    def _create_output_directories(self) -> None:
        """Creates local output directories if they don't exist."""
        for path in self.output["dirs"].values():
            full_path = self.output["prefix"]["local"] / path
            full_path.mkdir(parents=True, exist_ok=True)

    def get_path(
        self, dataset: str, path_type: str = "output", location: str = "local"
    ) -> str | Path:
        """
        Get a specific path by dataset name, type, and location.

        Args:
            dataset: The dataset name (e.g., 'times', 'origins', 'metadata').
            path_type: Either 'input' or 'output'.
            location: Either 'local' or 's3'.
        """
        if path_type == "output":
            path = self.output[location][f"{dataset}_file"]
        else:
            path = self.input["files"][f"{dataset}_file"]
        return str(path) if location == "s3" else path

    def write_to_parquet(
        self, df: pd.DataFrame, dataset: str, location: str = "local"
    ) -> None:
        """
        Write a DataFrame to an output Parquet file.

        Args:
            dataset: The dataset name (e.g., 'times', 'origins', 'metadata').
            location: Either 'local' or 's3'.
        """
        df.to_parquet(
            self.get_path(dataset, path_type="output", location=location),
            engine="pyarrow",
            compression=self.compression_type,
            compression_level=self.compression_level,
            index=False,
            storage_options=self.storage_options[location],
        )


class TravelTimeInputs:
    """
    Class to hold input data and chunk settings for travel time calculations.
    """

    def __init__(
        self,
        origins: pd.DataFrame,
        destinations: pd.DataFrame,
        chunk: str | None,
        max_split_size_origins: int,
        max_split_size_destinations: int,
    ) -> None:
        self.origins = origins
        self.destinations = destinations

        # "full" is the original (before chunk subsetting) number of origins
        self.n_origins_full: int = len(self.origins)

        self.chunk = chunk
        self.o_chunk_start_idx: int
        self.o_chunk_end_idx: int
        self.d_chunk_start_idx: int
        self.d_chunk_end_idx: int
        self.o_chunk_size: int = int(10e7)
        self.d_chunk_size: int = int(10e7)
        self._set_chunk_attributes()
        self._subset_origins()
        self._subset_destinations()

        self.n_origins: int = len(self.origins)
        self.n_destinations: int = len(self.destinations)
        self.max_split_size_origins = min(
            max_split_size_origins, self.o_chunk_size
        )
        self.max_split_size_destinations = min(
            max_split_size_destinations, self.d_chunk_size
        )

    def _set_chunk_attributes(self) -> None:
        """Sets the origin chunk indices given the input chunk string."""
        if self.chunk:
            o_chunk, d_chunk = self.chunk.split("_")
            o_chunk_start_idx, o_chunk_end_idx = o_chunk.split("-")
            d_chunk_start_idx, d_chunk_end_idx = d_chunk.split("-")
            self.o_chunk_start_idx = int(o_chunk_start_idx)
            self.o_chunk_end_idx = int(o_chunk_end_idx)
            self.o_chunk_size = int(o_chunk_end_idx) - int(o_chunk_start_idx)
            self.d_chunk_start_idx = int(d_chunk_start_idx)
            self.d_chunk_end_idx = int(d_chunk_end_idx)
            self.d_chunk_size = int(d_chunk_end_idx) - int(d_chunk_start_idx)

    def _subset_origins(self) -> None:
        """Sets the origins chunk (if chunk is specified)."""
        if self.chunk:
            self.origins = self.origins.iloc[
                self.o_chunk_start_idx : self.o_chunk_end_idx
            ]

    def _subset_destinations(self) -> None:
        """Sets the destinations chunk (if chunk is specified)."""
        if self.chunk:
            self.destinations = self.destinations.iloc[
                self.d_chunk_start_idx : self.d_chunk_end_idx
            ]


class TravelTimeConfig:
    """
    Utility class to hold all configuration settings for travel time
    calculations. Also includes loaders for the default input data.
    """

    OD_COLS = {
        "weighted": {"geoid": "id", "x_4326_wt": "lon", "y_4326_wt": "lat"},
        "unweighted": {"geoid": "id", "x_4326": "lon", "y_4326": "lat"},
    }

    def __init__(
        self,
        args: argparse.Namespace,
        params: dict,
        logger: logging.Logger,
        verbose: bool = False,
    ) -> None:
        self.args = TravelTimeArgs(args, params)
        self.params = params
        self.paths = TravelTimePaths(
            args=self.args,
            version=self.params["times"]["version"],
            s3_bucket=self.params["s3"]["data_bucket"],
            compression_type=self.params["output"]["compression"]["type"],
            compression_level=self.params["output"]["compression"]["level"],
            endpoint_url=self.params["s3"]["endpoint_url"],
        )
        self.logger = logger
        self.verbose = verbose

    def _load_od_file(self, path: str) -> pd.DataFrame:
        """Load an origins or destinations file and prep for Valhalla."""
        df = (
            pd.read_parquet(self.paths.get_path(path, path_type="input"))
            .loc[:, list(self.OD_COLS[self.args.centroid_type].keys())]
            .rename(columns=self.OD_COLS[self.args.centroid_type])
            .sort_values(by="id")
        )
        return df

    def load_default_inputs(self) -> TravelTimeInputs:
        """Load default origins and destinations files."""
        origins = self._load_od_file("origins")
        destinations = self._load_od_file("destinations")
        return TravelTimeInputs(
            origins=origins,
            destinations=destinations,
            chunk=self.args.chunk,
            max_split_size_origins=self.params["times"]["max_split_size"],
            max_split_size_destinations=self.params["times"]["max_split_size"],
        )


class TravelTimeCalculator:
    """
    Class to calculate travel times between origins and destinations.
    Uses chunked requests to the Valhalla Matrix API for calculation.
    """

    def __init__(
        self,
        config: TravelTimeConfig,
        inputs: TravelTimeInputs,
    ) -> None:
        self.config = config
        self.inputs = inputs

    def _calculate_times(
        self,
        origins: pd.DataFrame,
        destinations: pd.DataFrame,
        endpoint: str,
    ) -> pd.DataFrame:
        """
        Sends the travel time calculation request to the Valhalla Matrix API.
        Responsible for taking an origin/destination input, transforming it to
        the required JSON format, and parsing the response.

        Returns:
            DataFrame containing origin IDs, destination IDs, travel durations,
            and distances.
        """

        # Helper to use the snapped lat/lon columns (if specified via parameter)
        def _col_dict(x, snapped=self.config.params["times"]["use_snapped"]):
            col_suffix = "_snapped" if snapped else ""
            return {"lat": x[f"lat{col_suffix}"], "lon": x[f"lon{col_suffix}"]}

        # Convert the origin and destination points to lists, then squash them
        # into JSON: https://valhalla.github.io/valhalla/api/matrix/api-reference/
        origins_list = origins.apply(_col_dict, axis=1).tolist()
        destinations_list = destinations.apply(_col_dict, axis=1).tolist()
        request_json = json.dumps(
            {
                "sources": origins_list,
                "targets": destinations_list,
                "costing": self.config.args.mode,
                "verbose": False,
            }
        )

        # Get the actual JSON response from the API
        response = r.post(endpoint + "/sources_to_targets", data=request_json)
        response.raise_for_status()
        response_data = response.json()

        # Parse the response data and convert it to a DataFrame. Recover the
        # origin and destination indices and append them to the DataFrame
        durations = response_data["sources_to_targets"]["durations"]
        distances = response_data["sources_to_targets"]["distances"]
        origin_ids = origins["id"].repeat(len(destinations)).tolist()
        destination_ids = destinations["id"].tolist() * (len(origins))

        df = pd.DataFrame(
            {
                "origin_id": origin_ids,
                "destination_id": destination_ids,
                "duration_sec": [i for sl in durations for i in sl],
                "distance_km": [i for sl in distances for i in sl],
            }
        )

        return df

    def _binary_search(
        self,
        o_start_idx: int,
        d_start_idx: int,
        o_end_idx: int,
        d_end_idx: int,
        print_log: bool,
        cur_depth: int,
        origins: pd.DataFrame,
        destinations: pd.DataFrame,
        endpoint: str,
    ) -> list[pd.DataFrame]:
        """
        Recursively split the origins and destinations into smaller chunks.

        Necessary because Valhalla will terminate certain unroutable requests.
        Binary searching all origins and destinations will return the routable
        values around the unroutable ones.

        Higher depth levels will use a fallback router to ensure we're not
        simply querying the same unroutable values over and over.
        """
        start_time = time.time()
        if print_log or self.config.verbose:
            self.config.logger.info(
                "Routing origin indices %s-%s to destination indices %s-%s",
                o_start_idx,
                o_end_idx,
                d_start_idx,
                d_end_idx,
            )

        # If indices are out-of-bounds return an empty list
        if o_start_idx >= o_end_idx or d_start_idx >= d_end_idx:
            return []

        # Stop recursion if the chunks are too small (i.e. equal to 1)
        if (o_end_idx - o_start_idx <= 1) and (d_end_idx - d_start_idx <= 1):
            try:
                df = self._calculate_times(
                    origins=origins.iloc[o_start_idx:o_end_idx],
                    destinations=destinations.iloc[d_start_idx:d_end_idx],
                    endpoint=endpoint,
                )
            except Exception as e:
                df = create_empty_df(
                    o_start_idx,
                    d_start_idx,
                    o_end_idx,
                    d_end_idx,
                    origins["id"],
                    destinations["id"],
                )
                if print_log or self.config.verbose:
                    self.config.logger.warning(
                        f"{e}. Returning empty DataFrame"
                    )

            return [df]

        max_depth = self.config.params["times"]["max_recursion_depth"]
        if cur_depth >= max_depth:
            if print_log or self.config.verbose:
                self.config.logger.warning(
                    f"Max recursion depth {max_depth} reached. "
                    "Returning empty DataFrame"
                )
            empty_df = create_empty_df(
                o_start_idx,
                d_start_idx,
                o_end_idx,
                d_end_idx,
                origins["id"],
                destinations["id"],
            )
            return [empty_df]

        try:
            # Do time calculation if none of the minimal conditioners were met
            times = self._calculate_times(
                origins=origins.iloc[o_start_idx:o_end_idx],
                destinations=destinations.iloc[d_start_idx:d_end_idx],
                endpoint=endpoint,
            )

            if print_log or self.config.verbose:
                elapsed_time = time.time() - start_time
                self.config.logger.info(
                    "Routed %s pairs (%s missing) in %s",
                    (o_end_idx - o_start_idx) * (d_end_idx - d_start_idx),
                    len(times[times["duration_sec"].isnull()]),
                    format_time(elapsed_time),
                )

            return [times]

        # If the request fails, split the origins and destinations into
        # quadrants and start a binary search
        except Exception as e:
            if print_log or self.config.verbose:
                self.config.logger.warning(f"{e}. Starting binary search...")
            osi, oei, dsi, dei = o_start_idx, o_end_idx, d_start_idx, d_end_idx
            mo, md = (osi + oei) // 2, (dsi + dei) // 2
            # fmt: off
            return (
                self._binary_search(osi, dsi, mo, md, False, cur_depth + 1, origins, destinations, endpoint)
                + self._binary_search(mo, dsi, oei, md, False, cur_depth + 1, origins, destinations, endpoint)
                + self._binary_search(osi, md, mo, dei, False, cur_depth + 1, origins, destinations, endpoint)
                + self._binary_search(mo, md, oei, dei, False, cur_depth + 1, origins, destinations, endpoint)
            )
            # fmt: on

    def many_to_many(self, second_pass: bool = True) -> pd.DataFrame:
        """
        Entrypoint to calculate times for all combinations of origins and
        destinations in inputs. Includes an optional second pass which performs
        a more intensive (time-consuming) search for missing pairs from the
        first pass.

        Returns:
            DataFrame containing origin IDs, destination IDs, travel durations,
            and distances for all inputs.
        """
        results = []
        max_spl_o = self.inputs.max_split_size_origins
        n_oc = self.inputs.n_origins
        m_spl_d = self.inputs.max_split_size_destinations
        n_dc = self.inputs.n_destinations

        with ThreadPoolExecutor() as executor:
            futures = []
            for o in range(0, n_oc, max_spl_o):
                for d in range(0, n_dc, m_spl_d):
                    futures.append(
                        executor.submit(
                            self._binary_search,
                            o_start_idx=o,
                            d_start_idx=d,
                            o_end_idx=min(o + max_spl_o, n_oc),
                            d_end_idx=min(d + m_spl_d, n_dc),
                            print_log=True,
                            cur_depth=0,
                            origins=self.inputs.origins,
                            destinations=self.inputs.destinations,
                            endpoint=DOCKER_ENDPOINT_FIRST_PASS,
                        )
                    )
            for future in futures:
                results.extend(future.result())

        # Return empty result set if nothing is routable
        if len(results) == 0:
            return pd.DataFrame(
                columns=[
                    "origin_id",
                    "destination_id",
                    "duration_sec",
                    "distance_km",
                ]
            )
        else:
            results_df = (
                pd.concat(results, ignore_index=True)
                .set_index(["origin_id", "destination_id"])
                .sort_index()
            )
            del results

        # Check for completeness in the output. If any times are missing
        # after the first pass, run a second pass with the fallback router
        if results_df.isnull().values.any() and second_pass:
            missing = results_df[results_df["duration_sec"].isnull()]
            self.config.logger.info(
                "Starting second pass for %s missing pairs (%s total)",
                len(missing),
                len(results_df),
            )

            # Find the unique set of destinations, then use it to create groups
            # of origins, such that all origins in a DataFrame share the same
            # set of destinations. This greatly increases second pass speed
            results_sp = []
            missing_sets = group_by_column_sets(
                missing.reset_index(), "origin_id", "destination_id"
            )

            # Merge missing sets of OD pairs that overlap significantly (think
            # two origins that share 1000 destinations but not the 1001st)
            missing_sets = [
                df[["origin_id", "destination_id"]] for df in missing_sets
            ]
            merged_sets = merge_overlapping_df_list(missing_sets, 0.8)

            # Gut check that both sets contain the same number of rows
            try:
                assert sum(len(df) for df in missing_sets) == sum(
                    len(df) for df in merged_sets
                )
            except AssertionError:
                raise ValueError(
                    "The total number of rows in missing_sets does not"
                    "match the total number of rows in merged_sets"
                )

            self.config.logger.info(
                "Found %s unique missing sets. Merged to %s sets",
                len(missing_sets),
                len(merged_sets),
            )
            for idx, missing_set in enumerate(merged_sets):
                self.config.logger.info("Routing missing set number %s", idx)
                o_ids = missing_set["origin_id"].unique()
                d_ids = missing_set["destination_id"].unique()
                with ThreadPoolExecutor() as executor:
                    futures = []
                    for o in range(0, len(o_ids), max_spl_o):
                        for d in range(0, len(d_ids), m_spl_d):
                            futures.append(
                                executor.submit(
                                    self._binary_search,
                                    o_start_idx=o,
                                    d_start_idx=d,
                                    o_end_idx=min(o + max_spl_o, len(o_ids)),
                                    d_end_idx=min(d + m_spl_d, len(d_ids)),
                                    print_log=True,
                                    cur_depth=0,
                                    origins=self.inputs.origins[
                                        self.inputs.origins["id"].isin(o_ids)
                                    ],
                                    destinations=self.inputs.destinations[
                                        self.inputs.destinations["id"].isin(
                                            d_ids
                                        )
                                    ],
                                    endpoint=DOCKER_ENDPOINT_SECOND_PASS,
                                )
                            )

                    for future in futures:
                        results_sp.extend(future.result())

            # Merge the results from the second pass with the first pass
            results_sp_df = (
                pd.concat(results_sp, ignore_index=True)
                .set_index(["origin_id", "destination_id"])
                .sort_index()
            )
            del results_sp
            results_df.update(results_sp_df)

        return results_df


def snap_df_to_osm(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    """
    Snap a DataFrame of lat/lon points to the OpenStreetMap network using
    the Valhalla Locate API.

    Args:
        df: DataFrame containing the columns 'id', 'lat', and 'lon'.
        mode: Travel mode to use for snapping.
    """
    df_list = df.apply(
        lambda x: {"lat": x["lat"], "lon": x["lon"]}, axis=1
    ).tolist()
    request_json = json.dumps(
        {
            "locations": df_list,
            "costing": mode,
            "verbose": False,
        }
    )

    response_data = r.post(
        "http://127.0.0.1:8002/locate", data=request_json
    ).json()

    # Use the first element of nodes to populate the snapped lat/lon, otherwise
    # fallback to the correlated lat/lon from edges
    def _get_col(x: dict, col: str):
        return (
            x["nodes"][0][col]
            if x["nodes"]
            else (x["edges"][0][f"correlated_{col}"] if x["edges"] else None)
        )

    response_df = pd.DataFrame(
        [
            {
                "lon_snapped": _get_col(item, "lon"),
                "lat_snapped": _get_col(item, "lat"),
            }
            for item in response_data
        ]
    )

    df = pd.concat(
        [df.reset_index(drop=True), response_df.reset_index(drop=True)],
        axis=1,
    )
    df.fillna({"lon_snapped": df["lon"]}, inplace=True)
    df.fillna({"lat_snapped": df["lat"]}, inplace=True)
    df["is_snapped"] = df["lon"] != df["lon_snapped"]
    return df
