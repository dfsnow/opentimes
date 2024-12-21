import os
from pathlib import Path

# Path relative to the mounts within the Valhalla Docker container
DOCKER_INTERNAL_PATH = Path(os.environ.get("DOCKER_INTERNAL_PATH", Path.cwd()))

# This is a dictionary that determines the construction of the public
# OpenTimes files. partition_levels is the number of directories present in
# the raw (non-public) data bucket before reaching the actual Parquet files.

# public_file_columns and order_by_columns define the columns present and order
# of the public output files, respectively
DATASET_DICT: dict = {
    "0.0.1": {
        "times": {
            "partition_columns": [
                "version",
                "mode",
                "year",
                "geography",
                "state",
                "centroid_type",
            ],
            "public_file_columns": [
                "centroid_type",
                "origin_id",
                "destination_id",
                "duration_sec",
                "distance_km",
            ],
            "order_by_columns": [
                "origin_id",
                "destination_id",
            ],
            "sorting_columns": [1, 2],
        },
        "points": {
            "partition_columns": [
                "version",
                "mode",
                "year",
                "geography",
                "state",
                "centroid_type",
                "point_type",
            ],
            "public_file_columns": [
                "centroid_type",
                "point_type",
                "id",
                "lat",
                "lon",
            ],
            "order_by_columns": [
                "id",
            ],
            "sorting_columns": [2],
        },
        "missing_pairs": {
            "partition_columns": [
                "version",
                "mode",
                "year",
                "geography",
                "state",
                "centroid_type",
            ],
            "public_file_columns": [
                "centroid_type",
                "origin_id",
                "destination_id",
            ],
            "order_by_columns": [
                "origin_id",
                "destination_id",
            ],
            "sorting_columns": [1, 2],
        },
        "metadata": {
            "partition_columns": [
                "version",
                "mode",
                "year",
                "geography",
                "state",
                "centroid_type",
            ],
            "public_file_columns": [
                "centroid_type",
                "run_id",
                "calc_datetime_finished",
                "calc_time_elapsed_sec",
                "calc_chunk_id",
                "calc_chunk_n_origins",
                "calc_chunk_n_destinations",
                "calc_n_origins",
                "calc_n_destinations",
                "git_commit_sha_short",
                "git_commit_sha_long",
                "param_network_buffer_m",
                "param_destination_buffer_m",
                "file_input_valhalla_tiles_md5",
                "file_input_origins_md5",
                "file_input_destinations_md5",
                "file_output_times_md5",
                "file_output_origins_md5",
                "file_output_destinations_md5",
                "file_output_missing_pairs_md5",
                "valhalla_config_data",
            ],
            "order_by_columns": [
                "calc_chunk_id",
            ],
            "sorting_columns": [4],
        },
    }
}
