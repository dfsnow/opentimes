options(java.parameters = "-Xmx16G")

library(arrow)
library(digest)
library(dplyr)
library(glue)
library(here)
library(jsonlite)
library(optparse)
library(r5r)
library(tictoc)
library(yaml)
source("./src/utils/utils.R")

# Parse script arguments in the style of Python and check for valid values
option_list <- list(
  make_option("--mode", type = "character"),
  make_option("--year", type = "character"),
  make_option("--geography", type = "character"),
  make_option("--state", type = "character"),
  make_option("--centroid_type", type = "character"),
  make_option("--chunk", type = "character"),
  make_option(
    "--write-local", type = "logical",
    action = "store_true", dest = "local",
    default = FALSE
  )
)
opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# Check for valid values of the script arguments
if (!opt$mode %in% c("car", "walk", "bicycle", "transit")) {
  stop("Invalid mode argument. Must be one of 'car', 'walk', 'bicycle', or 'transit'.")
}
if (!opt$centroid_type %in% c("weighted", "unweighted")) {
  stop("Invalid centroid_type argument. Must be one of 'weighted' or 'unweighted'.")
}
if (!is.null(opt$chunk)) {
  if (!grepl("^\\d+-\\d+$", opt$chunk)) {
    stop("Invalid chunk argument. Must be two numbers separated by a dash (e.g., '1-2').")
  }
}

# Load parameters from file
params <- read_yaml(here::here("params.yaml"))

# Recover the chunk indices from the script argument if present. Must add one to
# the indices to match R's 1-based indexing
start_msg <- glue::glue(
  "Starting routing for version: {params$times$version}, mode: {opt$mode}, ",
  "year: {opt$year}, geography: {opt$geography}, state: {opt$state}, ",
  "centroid type: {opt$centroid_type}"
)
if (!is.null(opt$chunk)) {
  chunk_used <- TRUE
  chunk_indices <- as.numeric(strsplit(opt$chunk, "-")[[1]]) + 1
  message(start_msg, ", chunk: ", opt$chunk)
} else {
  chunk_used <- FALSE
  message(start_msg)
}

# Setup the R2 bucket connection. Requires a custom profile and endpoint
Sys.setenv("AWS_PROFILE" = params$s3$profile)
data_bucket <- arrow::s3_bucket(
  bucket = params$s3$bucket,
  endpoint_override = params$s3$endpoint
)

# Setup file paths for inputs (pre-made network file and OD points)
input_path <- glue::glue(
  "year={opt$year}/geography={opt$geography}/",
  "state={opt$state}/{opt$state}.parquet"
)
network_dir <- here::here(glue::glue(
  "intermediate/network/",
  "year={opt$year}/geography=state/state={opt$state}"
))
origins_file <- here::here("intermediate/cenloc", input_path)
destinations_file <- here::here("intermediate/destpoint", input_path)

# Load the R5 JAR, network, and network settings
if (!file.exists(here::here(network_dir, "network.dat"))) {
  stop("Network file not found. Run 'dvc repro' to create the network file.")
}
setup_r5_jar("./jars/r5-custom.jar")
network_settings <- read_json(here::here(network_dir, "network_settings.json"))
r5r_core <- r5r::setup_r5(
  data_path = network_dir,
  verbose = FALSE,
  temp_dir = FALSE,
  overwrite = FALSE
)

# Select columns based on centroid type (pop. weighted or unweighted)
od_cols <- switch(
  opt$centroid_type,
  weighted = c("id" = "geoid", "lon" = "x_4326_wt", "lat" = "y_4326_wt"),
  unweighted = c("id" = "geoid", "lon" = "x_4326", "lat" = "y_4326")
)
origins = read_parquet(origins_file) %>%
  select(all_of(od_cols))
destinations <- read_parquet(destinations_file) %>%
  select(all_of(od_cols))
n_origins <- nrow(origins)
n_destinations <- nrow(destinations)

# If a chunk is used, subset the origins to the chunk indices
if (chunk_used) {
  origins <- origins %>% slice(chunk_indices[1]:chunk_indices[2])
}
message(glue::glue(
  "Routing from {nrow(origins)} origins ",
  "to {nrow(destinations)} destinations"
))

# Snap lat/lon points to the street network
origins_snapped <- origins %>%
  find_snap(
    r5r_core = r5r_core,
    points = .,
    mode = "WALK",
    radius = as.numeric(params$times$snap_radius)
  ) %>%
  rename(id = point_id, distance_m = distance, snapped = found) %>%
  mutate(
    snap_lat = ifelse(is.na(snap_lat), lat, snap_lat),
    snap_lon = ifelse(is.na(snap_lon), lon, snap_lon)
  )
destinations_snapped <- destinations %>%
  find_snap(
    r5r_core = r5r_core,
    points = .,
    mode = "WALK",
    radius = as.numeric(params$times$snap_radius)
  ) %>%
  rename(id = point_id, distance_m = distance, snapped = found) %>%
  mutate(
    snap_lat = ifelse(is.na(snap_lat), lat, snap_lat),
    snap_lon = ifelse(is.na(snap_lon), lon, snap_lon)
  )

# Setup file paths for travel time outputs. If a chunk is used, include it in
# the output path as a partition key
output_path <- glue::glue(
  "version={params$times$version}/mode={opt$mode}/year={opt$year}/",
  "geography={opt$geography}/state={opt$state}/",
  "centroid_type={opt$centroid_type}"
)
if (chunk_used) {
  output_path <- file.path(output_path, glue::glue("chunk={opt$chunk}"))
}

times_dir <- file.path("times", output_path)
origins_dir <- file.path("points", output_path, "point_type=origin")
destinations_dir <- file.path("points", output_path, "point_type=destination")
missing_pairs_dir <- file.path("missing_pairs", output_path)
metadata_dir <- file.path("metadata", output_path)

# If the output is local, create the necessary directories if they don't exist.
# Otherwise, convert the dirs to S3 paths
if (opt$local) {
  for (dir in c(times_dir, origins_dir,
                destinations_dir, missing_pairs_dir, metadata_dir)) {
    if (!dir.exists(dir)) {
      dir.create(here::here(dir), recursive = TRUE)
    }
  }
  times_file <- here::here("output", times_dir, "part-0.parquet")
  origins_file <- here::here("output", origins_dir, "part-0.parquet")
  destinations_file <- here::here("output", destinations_dir, "part-0.parquet")
  missing_pairs_file <- here::here("output", missing_pairs_dir, "part-0.parquet")
  metadata_file <- here::here("output", metadata_dir, "part-0.parquet")
} else {
  times_file <- data_bucket$path(file.path(times_dir, "part-0.parquet"))
  origins_file <- data_bucket$path(file.path(origins_dir, "part-0.parquet"))
  destinations_file <- data_bucket$path(file.path(destinations_dir, "part-0.parquet"))
  missing_pairs_file <- data_bucket$path(file.path(missing_pairs_dir, "part-0.parquet"))
  metadata_file <- data_bucket$path(file.path(metadata_dir, "part-0.parquet"))
}

# Generate the actual travel time matrix. Use the snapped lat/lon points
tictoc::tic("Generated travel time matrix")
ttm <- travel_time_matrix(
  r5r_core = r5r_core,
  origins = origins_snapped %>%
    select(id, lon = snap_lon, lat = snap_lat),
  destinations = destinations_snapped %>%
    select(id, lon = snap_lon, lat = snap_lat),
  mode = toupper(opt$mode),
  max_trip_duration = params$times$r5$max_trip_duration,
  walk_speed = params$times$r5$walk_speed,
  bike_speed = params$times$r5$bike_speed,
  max_lts = params$times$r5$max_lts,
  max_rides = params$times$r5$max_rides,
  time_window = params$times$r5$time_window,
  percentiles = params$times$r5$percentiles,
  draws_per_minute = params$times$r5$draws_per_minute,
  progress = !params$times$verbose,
  verbose = params$times$verbose
) %>%
  rename(
    origin_id = from_id,
    destination_id = to_id,
    time_min = travel_time_p50
  )
tictoc::toc(log = TRUE)

# Check for missing point combinations
missing_pairs <- expand.grid(
  origin_id = origins_snapped$id,
  destination_id = destinations_snapped$id,
  stringsAsFactors = FALSE
) %>%
  anti_join(ttm, by = c("origin_id", "destination_id"))

# Save the travel time matrix, input points, and missing pairs to disk
write_parquet(
  x = ttm,
  sink = times_file,
  compression = params$output$compression$type,
  compression_level = params$output$compression$level
)
write_parquet(
  x = origins_snapped,
  sink = origins_file,
  compression = params$output$compression$type,
  compression_level = params$output$compression$level
)
write_parquet(
  x = destinations_snapped,
  sink = destinations_file,
  compression = params$output$compression$type,
  compression_level = params$output$compression$level
)
write_parquet(
  x = missing_pairs,
  sink = missing_pairs_file,
  compression = params$output$compression$type,
  compression_level = params$output$compression$level
)

# Capture the repo state via git and input/output file hashes
git_commit <- git2r::revparse_single(git2r::repository(), "HEAD")
file_list <- c(
  network_file = here::here(network_dir, "network.dat"),
  origins_file = origins_file,
  destinations_file = destinations_file,
  times_file = here::here(times_dir, "part-0.parquet"),
  origins_snapped_file = here::here(origins_dir, "part-0.parquet"),
  destinations_snapped_file = here::here(destinations_dir, "part-0.parquet"),
  missing_pairs_file = here::here(missing_pairs_dir, "part-0.parquet")
)
md5_list <- lapply(file_list, digest::digest, algo = "md5")

# Create a metadata dataframe of all settings and data used for creating inputs
# and generating times
metadata <- tibble::tibble(
  r5_version = r5r:::r5r_env$r5_jar_version,
  r5_network_version = network_settings$r5_network_version,
  r5r_version = network_settings$r5r_version,
  r5_max_trip_duration = params$times$r5$max_trip_duration,
  r5_walk_speed = params$times$r5$walk_speed,
  r5_bike_speed = params$times$r5$bike_speed,
  r5_max_lts = params$times$r5$max_lts,
  r5_max_rides = params$times$r5$max_rides,
  r5_time_window = params$times$r5$time_window,
  r5_percentiles = params$times$r5$percentiles,
  r5_draws_per_minute = params$times$r5$draws_per_minute,
  git_sha_short = substr(git_commit$sha, 1, 8),
  git_sha_long = git_commit$sha,
  git_message = gsub("\n", "", git_commit$message),
  git_author = git_commit$author$name,
  git_email = git_commit$author$email,
  param_network_buffer_m = params$input$network_buffer_m,
  param_destination_buffer_m = params$input$destination_buffer_m,
  param_snap_radius = params$times$snap_radius,
  param_use_elevation = network_settings$use_elevation,
  param_elevation_cost_function = network_settings$elevation_cost_function,
  param_elevation_zoom = params$input$elevation_zoom,
  file_pbf_path = network_settings$pbf_file_name,
  file_in_network_md5 = md5_list$network_file,
  file_in_origins_md5 = md5_list$origins_file,
  file_in_destinations_md5 = md5_list$destinations_file,
  file_out_times_md5 = md5_list$times_file,
  file_out_origins_md5 = md5_list$origins_snapped_file,
  file_out_destinations_md5 = md5_list$destinations_snapped_file,
  file_out_missing_pairs_md5 = md5_list$missing_pairs_file,
  file_tiff_path = dplyr::na_if(network_settings$tiff_file_name, ""),
  calc_n_origins = n_origins,
  calc_n_destinations = n_destinations,
  calc_chunk_id = opt$chunk,
  calc_chunk_n_origins = nrow(origins_snapped),
  calc_chunk_n_destinations = nrow(destinations_snapped),
  calc_time_finished = as.POSIXct(Sys.time(), tz="UTC"),
  calc_time_elapsed_sec = tictoc::tic.log(format = FALSE)[[1]]$toc -
    tictoc::tic.log(format = FALSE)[[1]]$tic,
)

write_parquet(
  x = metadata,
  sink = metadata_file,
  compression = params$output$compression$type,
  compression_level = params$output$compression$level
)

# Cleanup Java connection and memory
r5r::stop_r5(r5r_core)
rJava::.jgc(R.gc = TRUE)
