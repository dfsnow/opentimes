import argparse
import os
import time
import uuid
from pathlib import Path

import pandas as pd
import yaml
from utils.logging import create_logger
from utils.times import (
    TravelTimeCalculator,
    TravelTimeConfig,
)
from utils.utils import format_time, get_md5_hash

logger = create_logger(__name__)

with open(Path.cwd() / "params.yaml") as file:
    params = yaml.safe_load(file)
os.environ["AWS_PROFILE"] = params["s3"]["profile"]


def main() -> None:
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

    # Create a travel times configuration and set of origin/destination inputs
    config = TravelTimeConfig(args, params=params, logger=logger)
    inputs = config.load_default_inputs()

    chunk_msg = f", chunk: {config.args.chunk}" if config.args.chunk else ""
    logger.info(
        "Starting times calculation with parameters: version=%s, "
        "mode=%s, year=%s, geography=%s, state=%s, centroid_type=%s%s",
        config.params["times"]["version"],
        config.args.mode,
        config.args.year,
        config.args.geography,
        config.args.state,
        config.args.centroid_type,
        chunk_msg,
    )
    logger.info(
        "Starting with %s origins to %s destinations (%s pairs)",
        len(inputs.origins),
        inputs.n_destinations,
        len(inputs.origins) * inputs.n_destinations,
    )

    # Calculate times from all origins to all destinations and return a single
    # DataFrame. Assumes an OSRM service is running locally at localhost:5333
    logger.info("Tiles loaded and coodinates ready, starting routing")
    tt_calc = TravelTimeCalculator(config, inputs)
    results_df = tt_calc.many_to_many()
    logger.info(
        "Finished calculating times for %s pairs in %s",
        len(results_df),
        format_time(time.time() - script_start_time),
    )

    # Extract any missing pairs to a separate DataFrame and sort all outputs
    # for efficient compression
    missing_pairs_df = results_df[results_df["duration_sec"].isnull()]
    missing_pairs_df = (
        missing_pairs_df.drop(columns=["duration_sec"])
        .sort_index()
        .reset_index()
    )
    results_df = (
        results_df.dropna(subset=["duration_sec"]).sort_index().reset_index()
    )

    # Loop through files and write to both local and remote paths
    out_locations = ["s3"] if args.write_to_s3 else ["local"]
    logger.info(
        "Calculated times between %s pairs (%s missing). "
        "Saving outputs to: %s",
        len(results_df),
        len(missing_pairs_df),
        ", ".join(out_locations),
    )
    for loc in out_locations:
        config.paths.write_to_parquet(results_df, "times", loc)
        config.paths.write_to_parquet(inputs.origins, "origins", loc)
        config.paths.write_to_parquet(inputs.destinations, "destinations", loc)
        config.paths.write_to_parquet(missing_pairs_df, "missing_pairs", loc)

    # Collect metadata and git information for the metadata table
    run_id = str(uuid.uuid4().hex[:8])
    git_commit_sha = str(os.getenv("GITHUB_SHA"))
    git_commit_sha_short = str(git_commit_sha[:8] if git_commit_sha else None)
    input_file_hashes = {
        f: get_md5_hash(config.paths.input["files"][f])
        for f in config.paths.input["files"].keys()
    }

    # Create a metadata DataFrame of all settings and data used for creating
    # inputs and generating times
    metadata_df = pd.DataFrame(
        {
            "run_id": run_id,
            "calc_datetime_finished": pd.Timestamp.now(tz="UTC"),
            "calc_time_elapsed_sec": time.time() - script_start_time,
            "calc_chunk_id": args.chunk,
            "calc_chunk_n_origins": inputs.n_origins,
            "calc_chunk_n_destinations": inputs.n_destinations,
            "calc_n_origins": inputs.n_origins_full,
            "calc_n_destinations": inputs.n_destinations_full,
            "calc_n_pairs": len(results_df),
            "calc_n_missing_pairs": len(missing_pairs_df),
            "git_commit_sha_short": git_commit_sha_short,
            "git_commit_sha_long": git_commit_sha,
            "param_network_buffer_m": params["input"]["network_buffer_m"],
            "param_destination_buffer_m": params["input"][
                "destination_buffer_m"
            ],
            "param_max_split_size": params["times"]["max_split_size"],
            "param_use_snapped": params["times"]["use_snapped"],
            "file_input_origins_md5": input_file_hashes["origins_file"],
            "file_input_destinations_md5": input_file_hashes[
                "destinations_file"
            ],
        },
        index=[0],
    )
    for loc in out_locations:
        config.paths.write_to_parquet(metadata_df, "metadata", loc)

    logger.info(
        "Finished routing for version: %s, mode: %s, year: %s, "
        "geography: %s, state: %s, centroid type: %s%s in %s",
        config.params["times"]["version"],
        config.args.mode,
        config.args.year,
        config.args.geography,
        config.args.state,
        config.args.centroid_type,
        chunk_msg,
        format_time(time.time() - script_start_time),
    )


if __name__ == "__main__":
    main()
