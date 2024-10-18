##### SETUP #####
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
    "--write-to-s3", type = "logical",
    action = "store_true", dest = "write_to_s3",
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


##### FILE PATHS #####

# Setup file paths for inputs (pre-made network file and OD points)
input <- list()
input$path <- glue::glue(
  "year={opt$year}/geography={opt$geography}/",
  "state={opt$state}/{opt$state}.parquet"
)
input$network_dir <- here::here(glue::glue(
  "intermediate/network/",
  "year={opt$year}/geography=state/state={opt$state}"
))
input$origins_file <- here::here("intermediate/cenloc", input$path)
input$destinations_file <- here::here("intermediate/destpoint", input$path)

# Setup file paths for outputs. If a chunk is used, include it in
# the output file name
output <- list()
output$path <- glue::glue(
  "version={params$times$version}/mode={opt$mode}/year={opt$year}/",
  "geography={opt$geography}/state={opt$state}/",
  "centroid_type={opt$centroid_type}"
)
if (chunk_used) {
  output$file <- glue::glue("part-{opt$chunk}.parquet")
} else {
  output$file <- "part-0.parquet"
}

# Create directories pointers to check for existence and create if necessary
output$dirs$times <- file.path("times", output$path)
output$dirs$origins <- file.path("points", output$path, "point_type=origin")
output$dirs$destinations <- file.path("points", output$path, "point_type=destination")
output$dirs$missing_pairs <- file.path("missing_pairs", output$path)
output$dirs$metadata <- file.path("metadata", output$path)
for (dir in output$dirs) {
  if (!dir.exists(dir)) {
    dir.create(here::here("output", dir), recursive = TRUE, showWarnings = FALSE)
  }
}

# Create paths for output file relative to the project root
output$local$times_file <- here::here("output", output$dirs$times, output$file)
output$local$origins_file <- here::here("output", output$dirs$origins, output$file)
output$local$destinations_file <- here::here("output", output$dirs$destinations, output$file)
output$local$missing_pairs_file <- here::here("output", output$dirs$missing_pairs, output$file)
output$local$metadata_file <- here::here("output", output$dirs$metadata, output$file)

# If the output gets written to S3, then also create S3 paths
if (opt$write_to_s3) {
  # Setup the R2 bucket connection. Requires a custom profile and endpoint
  Sys.setenv("AWS_PROFILE" = params$s3$profile)
  data_bucket <- arrow::s3_bucket(
    bucket = params$s3$bucket,
    endpoint_override = params$s3$endpoint
  )
  output$s3$times_file <- data_bucket$path(file.path(output$dirs$times, output$file))
  output$s3$origins_file <- data_bucket$path(file.path(output$dirs$origins, output$file))
  output$s3$destinations_file <- data_bucket$path(file.path(output$dirs$destinations, output$file))
  output$s3$missing_pairs_file <- data_bucket$path(file.path(output$dirs$missing_pairs, output$file))
  output$s3$metadata_file <- data_bucket$path(file.path(output$dirs$metadata, output$file))
}


##### DATA PREP #####

# Load the R5 JAR, network, and network settings
if (!file.exists(here::here(input$network_dir, "network.dat"))) {
  stop("Network file not found. Run 'dvc repro' to create the network file.")
}
setup_r5_jar("./jars/r5-custom.jar")
network_settings <- read_json(here::here(
  input$network_dir,
  "network_settings.json"
))
r5r_core <- r5r::setup_r5(
  data_path = input$network_dir,
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
origins = read_parquet(input$origins_file) %>%
  select(all_of(od_cols))
destinations <- read_parquet(input$destinations_file) %>%
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


##### CALCULATE TIMES #####

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


##### SAVE OUTPUTS #####

# Local outputs are always saved to disk. If the write-to-s3 flag is set, then
# outputs are also saved to their S3 equivalent location
paths <- list()
paths[[1]] <- output$local
if (opt$write_to_s3) {
  paths[[2]] <- output$s3
}


# Save the travel time matrix, input points, and missing pairs to disk.
# The times dataset is partitioned by origin_id for better query performance
for (path in paths) {
  write_parquet(
    x = ttm,
    sink = path$times_file,
    compression = params$output$compression$type,
    compression_level = params$output$compression$level
  )
  write_parquet(
    x = origins_snapped,
    sink = path$origins_file,
    compression = params$output$compression$type,
    compression_level = params$output$compression$level
  )
  write_parquet(
    x = destinations_snapped,
    sink = path$destinations_file,,
    compression = params$output$compression$type,
    compression_level = params$output$compression$level
  )
  write_parquet(
    x = missing_pairs,
    sink = path$missing_pairs_file,
    compression = params$output$compression$type,
    compression_level = params$output$compression$level
  )
}

# Capture the repo state via git and input/output file hashes
git_commit <- git2r::revparse_single(git2r::repository(), "HEAD")
main_file_list <- c(
  input_network_file = here::here(input$network_dir, "network.dat"),
  input_origins_file = input$origins_file,
  input_destinations_file = input$destinations_file,
  output_times_file = output$local$times_file,
  output_origins_file = output$local$origins_file,
  output_destinations_file = output$local$destinations_file,
  output_missing_pairs_file = output$local$missing_pairs_file
)
main_md5_list <- lapply(main_file_list, digest::digest, algo = "md5", file = TRUE)

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
  file_input_pbf_path = network_settings$pbf_file_name,
  file_input_tiff_path = dplyr::na_if(network_settings$tiff_file_name, ""),
  file_input_network_md5 = main_md5_list$input_network_file,
  file_input_origins_md5 = main_md5_list$input_origins_file,
  file_input_destinations_md5 = main_md5_list$input_destinations_file,
  file_output_times_md5 = main_md5_list$output_times_file,
  file_output_origins_md5 = main_md5_list$output_origins_file,
  file_output_destinations_md5 = main_md5_list$output_destinations_file,
  file_output_missing_pairs_md5 = main_md5_list$output_missing_pairs_file,
  calc_n_origins = n_origins,
  calc_n_destinations = n_destinations,
  calc_chunk_id = ifelse(is.null(opt$chunk), NA_character_, opt$chunk),
  calc_chunk_n_origins = nrow(origins_snapped),
  calc_chunk_n_destinations = nrow(destinations_snapped),
  calc_time_finished = as.POSIXct(Sys.time(), tz="UTC"),
  calc_time_elapsed_sec = tictoc::tic.log(format = FALSE)[[1]]$toc -
    tictoc::tic.log(format = FALSE)[[1]]$tic
)

for (path in paths) {
  write_parquet(
    x = metadata,
    sink = path$metadata_file,
    compression = params$output$compression$type,
    compression_level = params$output$compression$level
  )
}

# Cleanup Java connection and memory
r5r::stop_r5(r5r_core)
rJava::.jgc(R.gc = TRUE)
