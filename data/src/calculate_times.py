import argparse
import json
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pandas as pd
import valhalla
import yaml
from utils.utils import format_time

# Path within the valhalla-run Docker container that input/output directories
# are mounted to
DOCKER_PATH = Path("/data")

# Load parameters from file and instantiate S3 connection
params = yaml.safe_load(open(DOCKER_PATH / "params.yaml"))
max_chunk_size = params["times"]["max_chunk_size"]
session = boto3.Session(profile_name=params["s3"]["profile"])
s3 = session.client("s3", endpoint_url=params["s3"]["endpoint"])


def calculate_times(
    actor,
    o_start_idx: int,
    d_start_idx: int,
    origins: pd.DataFrame,
    destinations: pd.DataFrame,
    max_chunk_size: int,
    mode: str,
) -> pd.DataFrame:
    """Calculates travel times and distances between origins and destinations.

    Args:
        actor: Valhalla actor instance for making matrix API requests.
        o_start_idx: Starting index for the origins DataFrame.
        d_start_idx: Starting index for the destinations DataFrame.
        origins: DataFrame containing origin points with 'lat' and 'lon' columns.
        destinations: DataFrame containing destination points with 'lat' and 'lon' columns.
        max_chunk_size: Maximum number of points to process in one chunk.
        mode: Travel mode for the Valhalla API (e.g., 'auto', 'bicycle').

    Returns:
        DataFrame containing origin IDs, destination IDs, travel durations, and distances.
    """
    start_time = time.time()
    o_end_idx = min(o_start_idx + max_chunk_size, len(origins))
    d_end_idx = min(d_start_idx + max_chunk_size, len(destinations))
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, type=str)
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=True, type=str)
    parser.add_argument("--state", required=True, type=str)
    parser.add_argument("--centroid_type", required=True, type=str)
    parser.add_argument("--write-to-s3", action="store_true", default=False)
    args = parser.parse_args()

    if args.mode not in params["times"]["mode"]:
        raise ValueError(
            "Invalid mode, must be one of: ", params["times"]["mode"]
        )
    if args.centroid_type not in ["weighted", "unweighted"]:
        raise ValueError(
            "Invalid centroid_type, must be one of: ['weighted', 'unweighted']"
        )

    print(
        f"Starting routing for version: {params['times']['version']},"
        f"mode: {args.mode}, year: {args.year}, geography: {args.geography},",
        f"state: {args.state}, centroid type: {args.centroid_type}",
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
            f"intermediate/valhalla_tiles/year={args.year}/"
            f"geography=state/state={args.state}",
        )
    }
    input["files"] = {
        "origins_file": Path(
            DOCKER_PATH, f"intermediate/cenloc/{input['main']['path']}"
        ),
        "destinations_file": Path(
            DOCKER_PATH, f"intermediate/destpoint/{input['main']['path']}"
        ),
    }

    # Setup file paths for outputs. S3 paths if enabled
    if args.write_to_s3:
        output_prefix = params["s3"]["data_bucket"]
    else:
        output_prefix = DOCKER_PATH / "output"

    output = {}
    output["main"] = {
        "path": Path(
            (
                f"version={params['times']['version']}/mode={args.mode}/"
                f"year={args.year}/geography={args.geography}/state={args.state}/"
                f"centroid_type={args.centroid_type}"
            )
        ),
        "file": Path("part-0.parquet"),
        "prefix": Path(output_prefix),
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
    output["files"] = {
        "times_file": Path(
            output["main"]["prefix"],
            output["dirs"]["times"],
            output["main"]["file"],
        ),
        "origins_file": Path(
            output["main"]["prefix"],
            output["dirs"]["origins"],
            output["main"]["file"],
        ),
        "destinations_file": Path(
            output["main"]["prefix"],
            output["dirs"]["destinations"],
            output["main"]["file"],
        ),
        "missing_pairs_file": Path(
            output["main"]["prefix"],
            output["dirs"]["missing_pairs"],
            output["main"]["file"],
        ),
        "metadata_file": Path(
            output["main"]["prefix"],
            output["dirs"]["metadata"],
            output["main"]["file"],
        ),
    }

    # Make sure outputs have somewhere to write to
    if not args.write_to_s3:
        for path in output["files"].values():
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
    )
    destinations = (
        pd.read_parquet(input["files"]["destinations_file"])
        .loc[:, od_cols.keys()]
        .rename(columns=od_cols)
    )
    n_origins = len(origins)
    n_destinations = len(destinations)

    print(f"Found {len(origins)} origins and {len(destinations)} destinations")

    ##### CALCULATE TIMES #####

    # Initialize the Valhalla actor bindings
    actor = valhalla.Actor((Path.cwd() / "valhalla.json").as_posix())
    num_cores = os.cpu_count() or 1
    num_cores = max(1, num_cores - 1)
    print("Starting ThreadPoolExecutor with", num_cores, "cores")

    with ThreadPoolExecutor(num_cores) as executor:
        futures = [
            executor.submit(
                calculate_times,
                actor=actor,
                o_start_idx=o,
                d_start_idx=d,
                origins=origins,
                destinations=destinations,
                max_chunk_size=max_chunk_size,
                mode=args.mode,
            )
            for o in range(0, n_origins, max_chunk_size)
            for d in range(0, n_destinations, max_chunk_size)
        ]

        results = []
        for future in as_completed(futures):
            results.append(future.result())


    # df.to_parquet('s3://your-bucket-name/path/to/file.parquet', storage_options={'client': boto3.client('s3')})
