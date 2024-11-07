# This file is a dictionary that determines the construction of the public
# OpenTimes files. partition_levels is the number of directories present in
# the raw (non-public) data bucket before reaching the actual Parquet files.
#
# public_file_columns and order_by_columns define the columns present and order
# of the public output files, respectively
DATASET_DICT: dict = {
    "0.0.1": {
        "times": {
            "partition_levels": 6,
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
        },
        "points": {
            "partition_levels": 7,
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
        },
        "missing_pairs": {
            "partition_levels": 6,
            "public_file_columns": [
                "centroid_type",
                "origin_id",
                "destination_id",
            ],
            "order_by_columns": [
                "origin_id",
                "destination_id",
            ],
        },
        "metadata": {
            "partition_levels": 6,
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
        },
    }
}
