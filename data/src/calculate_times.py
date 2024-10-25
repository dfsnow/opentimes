import argparse
import json
import os
import re
import time
import uuid
from pathlib import Path

import pandas as pd
import valhalla
import yaml
from utils.utils import format_time, get_md5_hash

# Path within the valhalla-run Docker container that input/output directories
# are mounted to
DOCKER_PATH = Path("/data")

# Load params, env vars, and JSON used for config
params = yaml.safe_load(open(DOCKER_PATH / "params.yaml"))
os.environ["AWS_PROFILE"] = params["s3"]["profile"]
with open(DOCKER_PATH / "valhalla.json", "r") as f:
    valhalla_data = json.load(f)


def calculate_times(
    actor,
    o_start_idx: int,
    d_start_idx: int,
    origins: pd.DataFrame,
    destinations: pd.DataFrame,
    max_split_size: int,
    mode: str,
) -> pd.DataFrame:
    """Calculates travel times and distances between origins and destinations.

    Args:
        actor: Valhalla actor instance for making matrix API requests.
        o_start_idx: Starting index for the origins DataFrame.
        d_start_idx: Starting index for the destinations DataFrame.
        origins: DataFrame containing origin points with 'lat' and 'lon' columns.
        destinations: DataFrame containing destination points with 'lat' and 'lon' columns.
        max_split_size: Maximum number of points to process in one iteration.
        mode: Travel mode for the Valhalla API (e.g., 'auto', 'bicycle').

    Returns:
        DataFrame containing origin IDs, destination IDs, travel durations, and distances.
    """
    start_time = time.time()
    o_end_idx = min(o_start_idx + max_split_size, len(origins))
    d_end_idx = min(d_start_idx + max_split_size, len(destinations))
    job_string = (
        f"Starting origins {o_start_idx}-{o_end_idx} and "
        f"destinations {d_start_idx}-{d_end_idx}"
    )
    print(job_string)

    # Get the subset of origin and destination points and convert them to lists
    # then squash them into the request body
    origins_list = (
        origins.iloc[o_start_idx:o_end_idx]
        .apply(lambda row: {"lat": row["lat"], "lon": row["lon"]}, axis=1)
        .tolist()
    )
    destinations_list = (
        destinations.iloc[d_start_idx:d_end_idx]
        .apply(lambda row: {"lat": row["lat"], "lon": row["lon"]}, axis=1)
        .tolist()
    )
    request_json = json.dumps(
        {
            "sources": origins_list,
            "targets": destinations_list,
            "costing": mode,
            "verbose": False,
        }
    )

    # Make the actual request to the matrix API
    response = actor.matrix(request_json)
    response_data = json.loads(response)

    # Parse the response data and convert it to a dataframe. Recover the
    # origin and destination indices and append them to the dataframe
    durations = response_data["sources_to_targets"]["durations"]
    distances = response_data["sources_to_targets"]["distances"]
    origin_ids = (
        origins.iloc[o_start_idx:o_end_idx]["id"]
        .repeat(d_end_idx - d_start_idx)
        .tolist()
    )
    destination_ids = destinations.iloc[d_start_idx:d_end_idx][
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

    elapsed_time = time.time() - start_time
    print(job_string, f": {format_time(elapsed_time)}")
    return df


def create_write_path(key: str, out_type: str, output_dict: dict) -> str:
    """Tiny helper to create Parquet output write paths."""
    return (
        "s3://" + output_dict[out_type][key].as_posix()
        if out_type == "s3"
        else output_dict[out_type][key].as_posix()
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, type=str)
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=True, type=str)
    parser.add_argument("--state", required=True, type=str)
    parser.add_argument("--centroid-type", required=True, type=str)
    parser.add_argument("--chunk", required=False, type=str)
    parser.add_argument("--write-to-s3", action="store_true", default=False)
    args = parser.parse_args()
    script_start_time = time.time()

    if args.mode not in params["times"]["mode"]:
        raise ValueError(
            "Invalid mode, must be one of: ", params["times"]["mode"]
        )
    if args.centroid_type not in ["weighted", "unweighted"]:
        raise ValueError(
            "Invalid centroid_type, must be one of: ['weighted', 'unweighted']"
        )
    if args.chunk:
        if not re.match(r"^\d+-\d+$", args.chunk):
            raise ValueError(
                "Invalid chunk argument. Must be two numbers separated by a dash (e.g., '1-2')."
            )

    # Split and check chunk value
    chunk_start_idx, chunk_end_idx = map(int, args.chunk.split("-"))
    chunk_size = chunk_end_idx - chunk_start_idx
    max_split_size = min(params["times"]["max_split_size"], chunk_size)
    chunk_msg = f", chunk: {args.chunk}" if args.chunk else ""
    print(
        f"Starting routing for version: {params['times']['version']},",
        f"mode: {args.mode}, year: {args.year}, geography: {args.geography},",
        f"state: {args.state}, centroid type: {args.centroid_type}"
        + chunk_msg,
    )

    ##### FILE PATHS #####

    # Setup file paths for inputs (pre-made network file and OD points)
    input = {}
    input["main"] = {
        "path": Path(
            f"year={args.year}/geography={args.geography}/"
            f"state={args.state}/{args.state}.parquet",
        )
    }
    input["dirs"] = {
        "valhalla_tiles": Path(
            DOCKER_PATH,
            f"intermediate/valhalla_tiles/year={args.year}/",
            f"geography=state/state={args.state}",
        )
    }
    input["files"] = {
        "valhalla_tiles_file": Path(
            DOCKER_PATH,
            f"intermediate/valhalla_tiles/year={args.year}",
            f"geography=state/state={args.state}/valhalla_tiles.tar.zst",
        ),
        "origins_file": Path(
            DOCKER_PATH, f"intermediate/cenloc/{input['main']['path']}"
        ),
        "destinations_file": Path(
            DOCKER_PATH, f"intermediate/destpoint/{input['main']['path']}"
        ),
    }

    # Setup file paths for all outputs both locally and on the remote
    output = {}
    output["prefix"] = {
        "local": Path(DOCKER_PATH / "output"),
        "s3": Path(params["s3"]["data_bucket"]),
    }
    output["main"] = {
        "path": Path(
            f"version={params['times']['version']}/mode={args.mode}/",
            f"year={args.year}/geography={args.geography}/state={args.state}/",
            f"centroid_type={args.centroid_type}",
        ),
        "file": Path(
            f"part-{args.chunk}.parquet" if args.chunk else "part-0.parquet"
        ),
    }
    output["dirs"] = {
        "times": Path("times", output["main"]["path"]),
        "origins": Path("points", output["main"]["path"], "point_type=origin"),
        "destinations": Path(
            "points", output["main"]["path"], "point_type=destination"
        ),
        "missing_pairs": Path("missing_pairs", output["main"]["path"]),
        "metadata": Path("metadata", output["main"]["path"]),
    }
    for loc in ["local", "s3"]:
        output[loc] = {
            "times_file": Path(
                output["prefix"][loc],
                output["dirs"]["times"],
                output["main"]["file"],
            ),
            "origins_file": Path(
                output["prefix"][loc],
                output["dirs"]["origins"],
                output["main"]["file"],
            ),
            "destinations_file": Path(
                output["prefix"][loc],
                output["dirs"]["destinations"],
                output["main"]["file"],
            ),
            "missing_pairs_file": Path(
                output["prefix"][loc],
                output["dirs"]["missing_pairs"],
                output["main"]["file"],
            ),
            "metadata_file": Path(
                output["prefix"][loc],
                output["dirs"]["metadata"],
                output["main"]["file"],
            ),
        }

    # Make sure outputs have somewhere to write to
    for path in output["dirs"].values():
        path = output["prefix"]["local"] / path
        path.mkdir(parents=True, exist_ok=True)

    ##### DATA PREP #####

    # Load origins and destinations
    od_cols = {
        "weighted": {"geoid": "id", "x_4326_wt": "lon", "y_4326_wt": "lat"},
        "unweighted": {"geoid": "id", "x_4326": "lon", "y_4326": "lat"},
    }[args.centroid_type]

    origins = (
        pd.read_parquet(input["files"]["origins_file"])
        .loc[:, od_cols.keys()]
        .rename(columns=od_cols)
        .sort_values(by="id")
    )
    n_origins = len(origins)

    # Subset the origins if a chunk is used
    if args.chunk:
        origins = origins.iloc[chunk_start_idx:chunk_end_idx]

    destinations = (
        pd.read_parquet(input["files"]["destinations_file"])
        .loc[:, od_cols.keys()]
        .rename(columns=od_cols)
        .sort_values(by="id")
    )
    n_destinations = len(destinations)
    n_origins_chunk = len(origins)
    n_destinations_chunk = len(destinations)

    print(
        f"Routing from {len(origins)} origins",
        f"to {len(destinations)} destinations",
    )

    ##### CALCULATE TIMES #####

    # Initialize the Valhalla actor bindings
    actor = valhalla.Actor((Path.cwd() / "valhalla.json").as_posix())

    # Calculate times for each chunk and append to a list
    results = []
    for o in range(0, n_origins_chunk, max_split_size):
        for d in range(0, n_destinations_chunk, max_split_size):
            times = calculate_times(
                actor=actor,
                o_start_idx=o,
                d_start_idx=d,
                origins=origins,
                destinations=destinations,
                max_split_size=max_split_size,
                mode=args.mode,
            )
            results.append(times)

    print(
        "Finished calculating times in",
        f"{format_time(time.time() - script_start_time)}",
    )

    # Concatenate all results into a single DataFrame
    results_df = pd.concat(results, ignore_index=True)
    del results

    # Extract missing pairs to a separate dataframe
    missing_pairs_df = results_df[results_df["duration_sec"].isnull()]
    missing_pairs_df = (
        pd.DataFrame(missing_pairs_df)
        .drop(columns=["duration_sec", "distance_km"])
        .sort_values(by=["origin_id", "destination_id"])
    )

    # Drop missing pairs and sort for more efficient compression
    results_df = results_df.dropna(subset=["duration_sec"]).sort_values(
        by=["origin_id", "destination_id"]
    )

    ##### SAVE OUTPUTS #####

    out_types = ["local", "s3"] if args.write_to_s3 else ["local"]
    compression_type = params["output"]["compression"]["type"]
    compression_level = params["output"]["compression"]["level"]
    storage_options = {
        "s3": {
            "client_kwargs": {
                "endpoint_url": params["s3"]["endpoint"],
            }
        },
        "local": {},
    }
    print(
        f"Calculated times between {len(results_df)} pairs.",
        f"Times missing between {len(missing_pairs_df)} pairs.",
        f"Saving outputs to: {', '.join(out_types)}",
    )

    # Loop through files and write to both local and remote paths
    for out_type in out_types:
        results_df.to_parquet(
            create_write_path("times_file", out_type, output),
            engine="pyarrow",
            compression=compression_type,
            compression_level=compression_level,
            index=False,
            storage_options=storage_options[out_type],
        )
        origins.to_parquet(
            create_write_path("origins_file", out_type, output),
            engine="pyarrow",
            compression=compression_type,
            compression_level=compression_level,
            index=False,
            storage_options=storage_options[out_type],
        )
        destinations.to_parquet(
            create_write_path("destinations_file", out_type, output),
            engine="pyarrow",
            compression=compression_type,
            compression_level=compression_level,
            index=False,
            storage_options=storage_options[out_type],
        )
        missing_pairs_df.to_parquet(
            create_write_path("missing_pairs_file", out_type, output),
            engine="pyarrow",
            compression=compression_type,
            compression_level=compression_level,
            index=False,
            storage_options=storage_options[out_type],
        )

    ##### SAVE METADATA #####

    run_id = str(uuid.uuid4().hex[:8])
    git_commit_sha = os.getenv("GIT_COMMIT_SHA")
    git_commit_sha_short = git_commit_sha[:8] if git_commit_sha else None
    input_file_hashes = {
        f: get_md5_hash(input["files"][f]) for f in input["files"].keys()
    }
    output_file_hashes = {
        f: get_md5_hash(output["local"][f])
        for f in output["local"].keys()
        if f != "metadata_file"
    }

    # Create a metadata dataframe of all settings and data used for creating inputs
    # and generating times
    metadata = pd.DataFrame(
        {
            "run_id": run_id,
            "calc_datetime_finished": pd.Timestamp.now(tz="UTC"),
            "calc_time_elapsed_sec": time.time() - script_start_time,
            "calc_chunk_id": args.chunk,
            "calc_chunk_n_origins": n_origins_chunk,
            "calc_chunk_n_destinations": n_destinations_chunk,
            "calc_n_origins": n_origins,
            "calc_n_destinations": n_destinations,
            "git_commit_sha_short": git_commit_sha_short,
            "git_commit_sha_long": git_commit_sha,
            "param_network_buffer_m": params["input"]["network_buffer_m"],
            "param_destination_buffer_m": params["input"][
                "destination_buffer_m"
            ],
            "file_input_valhalla_tiles_md5": input_file_hashes[
                "valhalla_tiles_file"
            ],
            "file_input_origins_md5": input_file_hashes["origins_file"],
            "file_input_destinations_md5": input_file_hashes[
                "destinations_file"
            ],
            "file_output_times_md5": output_file_hashes["times_file"],
            "file_output_origins_md5": output_file_hashes["origins_file"],
            "file_output_destinations_md5": output_file_hashes[
                "destinations_file"
            ],
            "file_output_missing_pairs_md5": output_file_hashes[
                "missing_pairs_file"
            ],
            "valhalla_config_data": json.dumps(
                valhalla_data, separators=(",", ":")
            ),
        },
        index=[0],
    )

    for out_type in out_types:
        metadata.to_parquet(
            create_write_path("metadata_file", out_type, output),
            engine="pyarrow",
            compression=compression_type,
            compression_level=compression_level,
            index=False,
            storage_options=storage_options[out_type],
        )

    print(
        f"Finished routing for version: {params['times']['version']},"
        f"mode: {args.mode}, year: {args.year}, geography: {args.geography},",
        f"state: {args.state}, centroid type: {args.centroid_type}"
        + chunk_msg,
        f"in {format_time(time.time() - script_start_time)}",
    )
