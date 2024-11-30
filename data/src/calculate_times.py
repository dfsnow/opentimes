import argparse
import json
import os
import time
import uuid
from pathlib import Path

import pandas as pd
import valhalla  # type: ignore
import yaml
from utils.constants import DOCKER_INTERNAL_PATH
from utils.logging import create_logger
from utils.times import TravelTimeCalculator, TravelTimeConfig
from utils.utils import format_time, get_md5_hash

logger = create_logger(__name__)

with open(DOCKER_INTERNAL_PATH / "params.yaml") as file:
    params = yaml.safe_load(file)
with open(DOCKER_INTERNAL_PATH / "valhalla.json", "r") as f:
    valhalla_data = json.load(f)
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
        "Starting routing for version: %s, mode: %s, year: %s, "
        "geography: %s, state: %s, centroid type: %s%s",
        config.params["times"]["version"],
        config.args.mode,
        config.args.year,
        config.args.geography,
        config.args.state,
        config.args.centroid_type,
        chunk_msg,
    )
    logger.info(
        "Routing from %s origins to %s destinations",
        len(inputs.origins_chunk),
        inputs.n_destinations,
    )

    # Initialize the default Valhalla actor bindings
    actor = valhalla.Actor((Path.cwd() / "valhalla.json").as_posix())

    # Calculate times for each chunk and append to a list
    tt_calc = TravelTimeCalculator(actor, config, inputs)
    results_df = tt_calc.get_times()

    logger.info(
        "Finished calculating times in %s",
        format_time(time.time() - script_start_time),
    )
    logger.info(
        "Routed from %s origins to %s destinations",
        inputs.n_origins,
        inputs.n_destinations,
    )

    # Extract missing pairs to a separate DataFrame
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

    # Loop through files and write to both local and remote paths
    out_locations = ["local", "s3"] if args.write_to_s3 else ["local"]
    logger.info(
        "Calculated times between %s pairs. Times missing between %s pairs. "
        "Saving outputs to: %s",
        len(results_df),
        len(missing_pairs_df),
        ", ".join(out_locations),
    )
    for loc in out_locations:
        config.paths.write_to_parquet(results_df, "times", loc)
        config.paths.write_to_parquet(inputs.origins_chunk, "origins", loc)
        config.paths.write_to_parquet(inputs.destinations, "destinations", loc)
        config.paths.write_to_parquet(missing_pairs_df, "missing_pairs", loc)

    # Construct and save a metadata DataFrame
    run_id = str(uuid.uuid4().hex[:8])
    git_commit_sha = str(os.getenv("GITHUB_SHA"))
    git_commit_sha_short = str(git_commit_sha[:8] if git_commit_sha else None)
    input_file_hashes = {
        f: get_md5_hash(config.paths.input["files"][f])
        for f in config.paths.input["files"].keys()
    }
    output_file_hashes = {
        f: get_md5_hash(config.paths.output["local"][f])
        for f in config.paths.output["local"].keys()
        if f != "metadata_file"
    }

    # Create a metadata dataframe of all settings and data used for creating inputs
    # and generating times
    metadata_df = pd.DataFrame(
        {
            "run_id": run_id,
            "calc_datetime_finished": pd.Timestamp.now(tz="UTC"),
            "calc_time_elapsed_sec": time.time() - script_start_time,
            "calc_chunk_id": args.chunk,
            "calc_chunk_n_origins": inputs.n_origins_chunk,
            "calc_chunk_n_destinations": inputs.n_destinations_chunk,
            "calc_n_origins": inputs.n_origins,
            "calc_n_destinations": inputs.n_destinations,
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
