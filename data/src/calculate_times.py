import argparse
import json
from pathlib import Path

import boto3
import pandas as pd
import time
import valhalla
import yaml
from utils.utils import format_time

# Load parameters from file
params = yaml.safe_load(open("params.yaml"))
session = boto3.Session(profile_name=params["s3"]["profile"])
s3 = session.client("s3", endpoint_url=params["s3"]["endpoint"])


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
        raise ValueError("Invalid mode, must be one of: ", params["times"]["mode"])
    if args.centroid_type not in ["weighted", "unweighted"]:
        raise ValueError(
            "Invalid centroid_type, must be one of: ['weighted', 'unweighted']"
        )

    print(
        f"Starting routing for version: {params['times']['version']},"
        f"mode: {args.mode}, year: {args.year}, geography: {args.geography},",
        f"state: {args.state}, centroid type: {args.centroid_type}"
    )

    ##### FILE PATHS #####

    # Setup file paths for inputs (pre-made network file and OD points)
    input = {}
    input["main"] = {
        "path": Path((
            f"year={args.year}/geography={args.geography}/"
            f"state={args.state}/{args.state}.parquet"
        ))
    }
    input["dirs"] = {
        "valhalla_tiles": Path((
            f"intermediate/valhalla_tiles/year={args.year}/"
            f"geography=state/state={args.state}"
        ))
    }
    input["files"] = {
        "origins_file": Path(f"intermediate/cenloc/{input['main']['path']}"),
        "destinations_file": Path(
            f"intermediate/destpoint/{input['main']['path']}"
        )
    }

    # Setup file paths for outputs. S3 paths if enabled
    if args.write_to_s3:
        output_prefix = params["s3"]["data_bucket"]
    else:
        output_prefix = "output"

    output = {}
    output["main"] = {
        "path": Path((
            f"version={params['times']['version']}/mode={args.mode}/"
            f"year={args.year}/geography={args.geography}/state={args.state}/"
            f"centroid_type={args.centroid_type}"
        )),
        "file": Path("part-0.parquet"),
        "prefix": Path(output_prefix)
    }
    output["dirs"] = {
        "times": Path("times", output["main"]["path"]),
        "origins": Path("points", output["main"]["path"], "point_type=origin"),
        "destinations": Path("points", output["main"]["path"], "point_type=destination"),
        "missing_pairs": Path("missing_pairs", output["main"]["path"]),
        "metadata": Path("metadata", output["main"]["path"]),
    }
    output["files"] = {
        "times_file": Path(output["main"]["prefix"], output["dirs"]["times"], output["main"]["file"]),
        "origins_file": Path(output["main"]["prefix"], output["dirs"]["origins"], output["main"]["file"]),
        "destinations_file": Path(
            output["main"]["prefix"], output["dirs"]["destinations"], output["main"]["file"]
        ),
        "missing_pairs_file": Path(
            output["main"]["prefix"], output["dirs"]["missing_pairs"], output["main"]["file"]
        ),
        "metadata_file": Path(
            output["main"]["prefix"], output["dirs"]["metadata"], output["main"]["file"]
        ),
    }

    # Make sure outputs have somewhere to write to
    for dir_path in output["dirs"].values():
        dir_path.mkdir(parents=True, exist_ok=True)


    ##### DATA PREP #####

    # Load origins and destinations
    od_cols = {
        "weighted": {"geoid": "id", "x_4326_wt": "lon", "y_4326_wt": "lat"},
        "unweighted": {"geoid": "id", "x_4326": "lon", "y_4326": "lat"}
    }[args.centroid_type]

    origins = pd.read_parquet(input["files"]["origins_file"])\
        .loc[:, od_cols.keys()].rename(columns=od_cols)
    destinations = pd.read_parquet(input["files"]["destinations_file"])\
        .loc[:, od_cols.keys()].rename(columns=od_cols)
    n_origins = len(origins)
    n_destinations = len(destinations)

    print(f"Found {len(origins)} origins and {len(destinations)} destinations")


    ##### CALCULATE TIMES #####

    # Initialize the Valhalla actor bindings
    actor = valhalla.Actor((Path.cwd() / "valhalla.json").as_posix())

    # Use the Valhalla Python bindings to query the Valhalla matrix API, then
    # store the JSON results
    # responses = []
    for o in range(0, n_origins, params["times"]["max_chunk_size"]):
        for d in range(0, n_destinations, params["times"]["max_chunk_size"]):
            start_time = time.time()
            o_idx = min(o + params["times"]["max_chunk_size"], n_origins)
            d_idx = min(d + params["times"]["max_chunk_size"], n_destinations)
            print(
                "Calculating times for origins",
                f"{o}-{o_idx} and destinations {d}-{d_idx}"
            )

            origins_list = origins.iloc[o:o_idx]\
                .apply(lambda row: {"lat": row["lat"], "lon": row["lon"]}, axis=1).tolist()
            destinations_list = destinations.iloc[d:d_idx]\
                .apply(lambda row: {"lat": row["lat"], "lon": row["lon"]}, axis=1).tolist()
            request_json = json.dumps({
                "sources": origins_list,
                "targets": destinations_list,
                "costing": args.mode
            })
            out = actor.matrix(request_json)
            elapsed_time = time.time() - start_time
            print(f"Iteration time: {format_time(elapsed_time)}")
            print(out)

    breakpoint()

    # df.to_parquet('s3://your-bucket-name/path/to/file.parquet', storage_options={'client': boto3.client('s3')})
