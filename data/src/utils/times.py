import argparse
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import valhalla  # type: ignore

from utils.constants import DOCKER_INTERNAL_PATH
from utils.utils import format_time, suppress_stdout


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
        if chunk and not re.match(r"^\d+-\d+$", chunk):
            raise ValueError(
                "Invalid chunk argument. Must be two numbers "
                "separated by a dash (e.g., '1-2')."
            )
        if chunk:
            parts = chunk.split("-")
            if len(parts[0]) != len(parts[1]):
                raise ValueError(
                    "Invalid chunk argument. Both numbers must have"
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
        docker_path: Path,
        s3_bucket: str,
        compression_type: Literal["snappy", "gzip", "brotli", "lz4", "zstd"],
        compression_level: int = 3,
        endpoint_url: str | None = None,
    ) -> None:
        self.args: TravelTimeArgs = args
        self.version: str = version
        self.docker_path: Path = docker_path
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
                    self.docker_path,
                    f"intermediate/valhalla_tiles/year={self.args.year}",
                    f"geography=state/state={self.args.state}",
                )
            },
            "files": {
                "valhalla_tiles_file": Path(
                    self.docker_path,
                    f"intermediate/valhalla_tiles/year={self.args.year}",
                    f"geography=state/state={self.args.state}/",
                    "valhalla_tiles.tar.zst",
                ),
                "origins_file": Path(
                    self.docker_path,
                    "intermediate/cenloc",
                    self._main_path,
                    f"{self.args.state}.parquet",
                ),
                "destinations_file": Path(
                    self.docker_path,
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
            "local": Path(self.docker_path, "output"),
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
        self.origins_chunk: pd.DataFrame

        self.chunk = chunk
        self.chunk_start_idx: int
        self.chunk_end_idx: int
        self.chunk_size: int = int(10e7)
        self._set_chunk_attributes()
        self._set_origins_chunk()

        self.n_origins: int = len(self.origins)
        self.n_destinations: int = len(self.destinations)
        self.n_origins_chunk: int = len(self.origins_chunk)
        self.n_destinations_chunk: int = len(self.destinations)

        self.max_split_size_origins = min(
            max_split_size_origins, self.chunk_size
        )
        self.max_split_size_destinations = min(
            max_split_size_destinations, self.n_destinations
        )

    def _set_chunk_attributes(self) -> None:
        """Sets the origin chunk indices given the input chunk string."""
        if self.chunk:
            self.chunk_start_idx, self.chunk_end_idx = map(
                int, self.chunk.split("-")
            )
            self.chunk_size = self.chunk_end_idx - self.chunk_start_idx

    def _set_origins_chunk(self) -> None:
        """Sets the origins chunk (if chunk is specified)."""
        df = self.origins
        if self.chunk:
            df = df.iloc[self.chunk_start_idx : self.chunk_end_idx]
        self.origins_chunk = df


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
    ) -> None:
        self.args = TravelTimeArgs(args, params)
        self.params = params
        self.paths = TravelTimePaths(
            args=self.args,
            version=self.params["times"]["version"],
            docker_path=DOCKER_INTERNAL_PATH,
            s3_bucket=self.params["s3"]["data_bucket"],
            compression_type=self.params["output"]["compression"]["type"],
            compression_level=self.params["output"]["compression"]["level"],
            endpoint_url=self.params["s3"]["endpoint_url"],
        )
        self.logger = logger

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
        actor: valhalla.Actor,
        config: TravelTimeConfig,
        inputs: TravelTimeInputs,
    ) -> None:
        self.actor = actor
        self.config = config
        self.inputs = inputs

    def _calculate_times(
        self,
        o_start_idx: int,
        d_start_idx: int,
        o_end_idx: int,
        d_end_idx: int,
    ) -> pd.DataFrame:
        """
        Calculates travel times and distances between origins and destinations.

        Args:
            o_start_idx: Starting index for the origins DataFrame.
            d_start_idx: Starting index for the destinations DataFrame.
            o_end_idx: Ending index for the origins DataFrame.
            d_end_idx: Ending index for the destinations DataFrame.

        Returns:
            DataFrame containing origin IDs, destination IDs, travel durations,
            and distances.
        """

        # Get the subset of origin and destination points and convert them to
        # lists then squash them into the request body
        origins_list = (
            self.inputs.origins.iloc[o_start_idx:o_end_idx]
            .apply(lambda row: {"lat": row["lat"], "lon": row["lon"]}, axis=1)
            .tolist()
        )
        destinations_list = (
            self.inputs.destinations.iloc[d_start_idx:d_end_idx]
            .apply(lambda row: {"lat": row["lat"], "lon": row["lon"]}, axis=1)
            .tolist()
        )
        request_json = json.dumps(
            {
                "sources": origins_list,
                "targets": destinations_list,
                "costing": self.config.args.mode,
                "verbose": False,
            }
        )

        # Make the actual JSON request to the matrix API
        with suppress_stdout():
            response = self.actor.matrix(request_json)
            response_data = json.loads(response)

        # Parse the response data and convert it to a DataFrame. Recover the
        # origin and destination indices and append them to the DataFrame
        durations = response_data["sources_to_targets"]["durations"]
        distances = response_data["sources_to_targets"]["distances"]
        origin_ids = (
            self.inputs.origins.iloc[o_start_idx:o_end_idx]["id"]
            .repeat(d_end_idx - d_start_idx)
            .tolist()
        )
        destination_ids = self.inputs.destinations.iloc[d_start_idx:d_end_idx][
            "id"
        ].tolist() * (o_end_idx - o_start_idx)

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
        print_log: bool = True,
    ) -> list[pd.DataFrame]:
        """
        Recursively split the origins and destinations into smaller chunks.

        Necessary because Valhalla will terminate certain unroutable requests.
        Binary searching all origins and destinations will return all routable
        values AROUND the unroutable ones.
        """
        start_time = time.time()
        if print_log:
            self.config.logger.info(
                "Routing origin indices %s-%s to destination indices %s-%s",
                o_start_idx,
                max(o_end_idx - 1, 0),
                d_start_idx,
                max(d_end_idx - 1, 0),
            )

        if o_start_idx + 1 >= o_end_idx and d_start_idx + 1 >= d_end_idx:
            df = pd.merge(
                pd.DataFrame(
                    self.inputs.origins[o_start_idx:o_end_idx],
                    columns=["origin_id"],
                ),
                pd.DataFrame(
                    self.inputs.destinations[d_start_idx:d_end_idx],
                    columns=["destination_id"],
                ),
                how="cross",
            )
            df["distance_km"] = pd.Series([], dtype=float)
            df["duration_sec"] = pd.Series([], dtype=float)

            return [df]
        try:
            times = self._calculate_times(
                o_start_idx=o_start_idx,
                d_start_idx=d_start_idx,
                o_end_idx=o_end_idx,
                d_end_idx=d_end_idx,
            )

            if print_log:
                elapsed_time = time.time() - start_time
                self.config.logger.info(
                    "Routed %s pairs in %s",
                    (o_end_idx - o_start_idx) * (d_end_idx - d_start_idx),
                    format_time(elapsed_time),
                )

            return [times]

        except Exception as e:
            if print_log:
                self.config.logger.error(f"{e}. Backing off and retrying...")
            mo = (o_start_idx + o_end_idx) // 2
            md = (d_start_idx + d_end_idx) // 2
            return (
                self._binary_search(o_start_idx, d_start_idx, mo, md, False)
                + self._binary_search(mo, d_start_idx, o_end_idx, md, False)
                + self._binary_search(o_start_idx, md, mo, d_end_idx, False)
                + self._binary_search(mo, md, o_end_idx, d_end_idx, False)
            )

    def get_times(self) -> pd.DataFrame:
        """
        Entrypoint to calculate times for all combinations of origins and
        destinations in inputs.

        Returns:
            DataFrame containing origin IDs, destination IDs, travel durations,
            and distances for all inputs.
        """
        results = []
        msso = self.inputs.max_split_size_origins
        noc = self.inputs.n_origins_chunk
        mssd = self.inputs.max_split_size_destinations
        ndc = self.inputs.n_destinations_chunk

        for o in range(0, noc, msso):
            for d in range(0, ndc, mssd):
                results.extend(
                    self._binary_search(
                        o, d, min(o + msso, noc), min(d + mssd, ndc), True
                    )
                )

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
            results_df = pd.concat(results, ignore_index=True).set_index(
                ["origin_id", "destination_id"]
            )
            del results
            return results_df
