library(elevatr)
library(glue)
library(here)
library(optparse)
library(progress)
library(sf)
library(terra)
library(yaml)
source("./src/utils/utils.R")

# Parse script arguments in the style of Python
option_list <- list(
  make_option("--year", type = "character"),
  make_option("--state", type = "character")
)
opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# Load parameters from file
params <- read_yaml(here::here("params.yaml"))

# Setup file paths and outputs
osmclip_file <- here::here(glue::glue(
  "intermediate/osmclip/",
  "year={opt$year}/geography=state/state={opt$state}/{opt$state}.geojson"
))
elevation_dir <- here::here(glue::glue(
  "input/elevation/",
  "geography=state/state={opt$state}"
))
if (!dir.exists(elevation_dir)) {
  dir.create(elevation_dir, recursive = TRUE)
}

# Load the buffered state file
state <- st_read(osmclip_file) %>%
  st_transform(4326)
elev <- fetch_elev_tiles(
  locations = state,
  prj = st_crs(state),
  z = params$input$elevation_zoom
)

# Write the .tif output to a single giant file
terra::writeRaster(
  x = elev,
  filename = here::here(elevation_dir, glue::glue("{opt$state}.tif")),
  overwrite = TRUE,
  memfrac = 0.8
)

# Clear the temporary directory of .tif files
tif_file_tiles <- list.files(tempdir(), pattern = ".tif", full.names = TRUE)
invisible(file.remove(tif_file_tiles))