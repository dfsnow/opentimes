DATASET_DICT: dict = {
    "0.0.1": {
        "times": {
            "partition_levels": 6,
            "public_file_columns": [
                "state",
                "centroid_type",
                "origin_id",
                "destination_id",
                "time_min",
            ],
            "public_file_order_by": [
                "state",
                "centroid_type",
                "origin_id",
                "destination_id",
            ],
        },
        "points": {
            "partition_levels": 7,
            "public_file_columns": [
                "state",
                "centroid_type",
                "point_type",
                "id",
                "lat",
                "lon",
                "snap_lat",
                "snap_lon",
                "distance_m",
                "snapped",
            ],
            "public_file_order_by": [
                "state",
                "centroid_type",
                "point_type",
                "id",
            ],
        },
        "missing_pairs": {
            "partition_levels": 6,
            "public_file_columns": [
                "state",
                "centroid_type",
                "origin_id",
                "destination_id",
            ],
            "public_file_order_by": [
                "state",
                "centroid_type",
                "origin_id",
                "destination_id",
            ],
        },
        "metadata": {
            "partition_levels": 6,
            "public_file_columns": [
                "state",
                "centroid_type",
                "r5_version",
                "r5_network_version",
                "r5r_version",
                "r5_max_trip_duration",
                "r5_walk_speed",
                "r5_bike_speed",
                "r5_max_lts",
                "r5_max_rides",
                "r5_time_window",
                "r5_percentiles",
                "r5_draws_per_minute",
                "git_sha_short",
                "git_sha_long",
                "git_message",
                "git_author",
                "git_email",
                "param_network_buffer_m",
                "param_destination_buffer_m",
                "param_snap_radius",
                "param_use_elevation",
                "param_elevation_cost_function",
                "param_elevation_zoom",
                "file_input_pbf_path",
                "file_input_tiff_path",
                "file_input_network_md5",
                "file_input_origins_md5",
                "file_input_destinations_md5",
                "file_output_times_md5",
                "file_output_origins_md5",
                "file_output_destinations_md5",
                "file_output_missing_pairs_md5",
                "calc_n_origins",
                "calc_n_destinations",
                "calc_chunk_id",
                "calc_chunk_n_origins",
                "calc_chunk_n_destinations",
                "calc_time_finished",
                "calc_time_elapsed_sec",
            ],
            "public_file_order_by": [
                "state",
                "centroid_type",
                "chunk_id",
            ],
        },
    }
}