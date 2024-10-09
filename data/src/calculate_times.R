options(java.parameters = "-Xmx16G")

library(arrow)
library(dplyr)
library(glue)
library(here)
library(optparse)
library(r5r)
library(tictoc)
library(yaml)
source("./src/utils/utils.R")

# Parse script arguments in the style of Python and check for valid values
option_list <- list(
  make_option("--year", type = "character"),
  make_option("--geography", type = "character"),
  make_option("--state", type = "character"),
  make_option("--mode", type = "character"),
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
network_dir <- here::here(glue::glue(
  "intermediate/network/",
  "year={opt$year}/geography=state/state={opt$state}"
))
origins_file <- here::here(glue::glue(
  "intermediate/cenloc/year={opt$year}/",
  "geography={opt$geography}/state={opt$state}/{opt$state}.parquet"
))
destinations_file <- here::here(glue::glue(
  "intermediate/destpoint/year={opt$year}/",
  "geography={opt$geography}/state={opt$state}/{opt$state}.parquet"
))

# Load the R5 JAR and network
setup_r5_jar("./jars/r5-custom.jar")
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
  radius = params$times$snap_radius
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
  radius = params$times$snap_radius
) %>%
  rename(id = point_id, distance_m = distance, snapped = found) %>%
  mutate(
    snap_lat = ifelse(is.na(snap_lat), lat, snap_lat),
    snap_lon = ifelse(is.na(snap_lon), lon, snap_lon)
  )

# Setup file paths for travel time outputs
times_dir <- here::here(glue::glue(
  "output/times/mode={opt$mode}/",
  "year={opt$year}/geography={opt$geography}/state={opt$state}/",
  "centroid_type={opt$centroid_type}"
))
points_dir <- here::here(glue::glue(
  "output/points/mode={opt$mode}/",
  "year={opt$year}/geography={opt$geography}/state={opt$state}/",
  "centroid_type={opt$centroid_type}"
))
origins_dir <- here::here(points_dir, "point_type=origin")
destinations_dir <- here::here(points_dir, "point_type=destination")
for (dir in c(times_dir, origins_dir, destinations_dir)) {
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
  verbose = params$times$verbose
) %>%
  rename(
    origin_id = from_id,
    destination_id = to_id,
    time_min = travel_time_p50
  )
tictoc::toc()

# Check for missing point combinations
point_pairs <- expand.grid(
  from_id = origins_snapped$id,
  to_id = destinations_snapped$id,
  stringsAsFactors = FALSE
)
missing_pairs <- point_pairs %>%
  anti_join(ttm, by = c("from_id", "to_id")) %>%
  mutate(
    from_id = as.character(from_id),
    to_id = as.character(to_id)
  )

# Save the travel time matrix and input points to disk
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