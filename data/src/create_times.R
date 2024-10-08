options(java.parameters = "-Xmx16G")

library(arrow)
library(dplyr)
library(glue)
library(here)
library(optparse)
library(r5r)
library(tictoc)
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

if (!opt$mode %in% c("CAR", "WALK", "BICYCLE", "TRANSIT")) {
  stop("Invalid mode argument. Must be one of 'CAR', 'WALK', 'BICYCLE', or 'TRANSIT'.")
}
if (!opt$centroid_type %in% c("weighted", "unweighted")) {
  stop("Invalid centroid_type argument. Must be one of 'weighted' or 'unweighted'.")
}

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
snap_mode <- ifelse(opt$mode == "TRANSIT", "WALK", opt$mode)
origins_snapped <- find_snap(
  r5r_core = r5r_core,
  points = origins,
  mode = snap_mode,
  radius = 5e5
) %>%
  rename(
    id = point_id,
    lat_nosnap = lat, lon_nosnap = lon,
    lat = snap_lat, lon = snap_lon,
    distance_m = distance, snapped = found
  ) %>%
  mutate(
    lat = ifelse(is.na(lat), lat_nosnap, lat),
    lon = ifelse(is.na(lon), lon_nosnap, lon)
  )
destinations_snapped <- find_snap(
  r5r_core = r5r_core,
  points = destinations,
  mode = snap_mode,
  radius = 5e5
) %>%
  rename(
    id = point_id,
    lat_nosnap = lat, lon_nosnap = lon,
    lat = snap_lat, lon = snap_lon,
    distance_m = distance, snapped = found
  ) %>%
  mutate(
    lat = ifelse(is.na(lat), lat_nosnap, lat),
    lon = ifelse(is.na(lon), lon_nosnap, lon)
  )

# Setup file paths for travel time outputs
times_dir <- here::here(glue::glue(
  "output/times/mode={tolower(opt$mode)}/",
  "year={opt$year}/geography=state/state={opt$state}/",
  "centroid_type={opt$centroid_type}"
))
points_dir <- here::here(glue::glue(
  "output/points/mode={tolower(opt$mode)}/",
  "year={opt$year}/geography=state/state={opt$state}/",
  "centroid_type={opt$centroid_type}"
))

for (dir in c(times_dir, points_dir)) {
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
  }
}

# Generate the actual travel time matrix
tictoc::tic("Generating travel time matrix")
ttm <- travel_time_matrix(
  r5r_core = r5r_core,
  origins = origins_snapped,
  destinations = destinations_snapped,
  mode = opt$mode,
  walk_speed = 4.5,
  bike_speed = 15,
  max_lts = 3,
  max_trip_duration = 600L,
  progress = TRUE
)
tictoc::toc()