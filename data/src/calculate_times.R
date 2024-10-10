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
  make_option("--version", type = "character"),
  make_option("--mode", type = "character"),
  make_option("--year", type = "character"),
  make_option("--geography", type = "character"),
  make_option("--state", type = "character"),
  make_option("--centroid_type", type = "character")
)
opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

if (!opt$mode %in% c("car", "walk", "bicycle", "transit")) {
  stop("Invalid mode argument. Must be one of 'car', 'walk', 'bicycle', or 'transit'.")
}
if (!opt$centroid_type %in% c("weighted", "unweighted")) {
  stop("Invalid centroid_type argument. Must be one of 'weighted' or 'unweighted'.")
}

# Load parameters from file
params <- read_yaml(here::here("params.yaml"))

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
setup_r5_jar("./jars/r5-custom.jar")
network_settings <- read_json(here::here(network_dir, "network_settings.json"))
r5r_core <- r5r::setup_r5(
  data_path = network_dir,
  verbose = FALSE,
  temp_dir = FALSE,
  overwrite = FALSE
)

# Select columns based on centroid type
od_cols <- switch(
  opt$centroid_type,
  weighted = c("id" = "geoid", "lon" = "x_4326_wt", "lat" = "y_4326_wt"),
  unweighted = c("id" = "geoid", "lon" = "x_4326", "lat" = "y_4326")
)
origins = read_parquet(origins_file) %>%
  select(all_of(od_cols))
destinations <- read_parquet(destinations_file) %>%
  select(all_of(od_cols))

# Snap lat/lon points to the street network
origins_snapped <- find_snap(
  r5r_core = r5r_core,
  points = origins,
  mode = "WALK",
  radius = as.numeric(params$times$snap_radius)
) %>%
  rename(id = point_id, distance_m = distance, snapped = found) %>%
  mutate(
    snap_lat = ifelse(is.na(snap_lat), lat, snap_lat),
    snap_lon = ifelse(is.na(snap_lon), lon, snap_lon)
  )
destinations_snapped <- find_snap(
  r5r_core = r5r_core,
  points = destinations,
  mode = "WALK",
  radius = as.numeric(params$times$snap_radius)
) %>%
  rename(id = point_id, distance_m = distance, snapped = found) %>%
  mutate(
    snap_lat = ifelse(is.na(snap_lat), lat, snap_lat),
    snap_lon = ifelse(is.na(snap_lon), lon, snap_lon)
  )

# Setup file paths for travel time outputs
output_path <- glue::glue(
  "version={opt$version}/mode={opt$mode}/year={opt$year}/",
  "geography={opt$geography}/state={opt$state}/",
  "centroid_type={opt$centroid_type}"
)
times_dir <- here::here("output/times", output_path)
origins_dir <- here::here("output/points", output_path, "point_type=origin")
destinations_dir <- here::here("output/points", output_path, "point_type=destination")
missing_pairs_dir <- here::here("output/missing_pairs", output_path)
metadata_dir <- here::here("output/metadata", output_path)
for (dir in c(times_dir, origins_dir,
              destinations_dir, missing_pairs_dir, metadata_dir)) {
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
  }
}

# Generate the actual travel time matrix. Use the snapped lat/lon points
tictoc::tic("Generating travel time matrix")
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
  sink = here::here(times_dir, "part-0.parquet"),
  compression = params$output$compression$type,
  compression_level = params$output$compression$level
)
write_parquet(
  x = origins_snapped,
  sink = here::here(origins_dir, "part-0.parquet"),
  compression = params$output$compression$type,
  compression_level = params$output$compression$level
)
write_parquet(
  x = destinations_snapped,
  sink = here::here(destinations_dir, "part-0.parquet"),
  compression = params$output$compression$type,
  compression_level = params$output$compression$level
)
write_parquet(
  x = missing_pairs,
  sink = here::here(missing_pairs_dir, "part-0.parquet"),
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
  time_finished = as.POSIXct(Sys.time(), tz="UTC"),
  time_elapsed = tictoc::tic.log(format = FALSE)[[1]]$toc -
    tictoc::tic.log(format = FALSE)[[1]]$tic,
)

write_parquet(
  x = metadata,
  sink = here::here(metadata_dir, "part-0.parquet"),
  compression = params$output$compression$type,
  compression_level = params$output$compression$level
)
