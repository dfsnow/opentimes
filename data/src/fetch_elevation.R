library(elevatr)
library(glue)
library(here)
library(optparse)
library(sf)

# Parse script arguments in the style of Python
option_list <- list(
  make_option("--year", type = "character"),
  make_option("--state", type = "character")
)
opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# Setup file paths and outputs
osmclip_file <- here::here(glue::glue(
  "intermediate/osmclip/",
  "year={opt$year}/geography=state/state={opt$state}/{opt$state}.geojson"
))
elevation_dir <- here::here(glue::glue(
  "input/elevation/",
  "geography=state/state={opt$state}"
))

# Load the buffered state file
state <- st_read(osmclip_file) %>%
  st_transform(5071)
elev <- get_elev_raster(
  locations = state,
  z = 14,
  src = "aws",
  expand = 50000,  # buffer additional 50km 
  override_size_check = TRUE,
  verbose = TRUE
)
write_raster(elev, here::here(elevation_dir, "elevation.tif"))