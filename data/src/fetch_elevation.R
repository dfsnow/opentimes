library(elevatr)
library(glue)
library(here)
library(optparse)
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

# Load the buffered state file
state <- st_read(osmclip_file) %>%
  st_transform(4326)

# Break the state into chunks, since fetching the entire thing at once
# doesn't really work
state_grid <- st_make_grid(state, n = 8, what = "polygons") %>%
  purrr::map(\(x) st_set_crs(st_bbox(x), 4326))

# Grab the elevation raster for each chunk
elev_lst <- list()
for (i in seq_len(length(state_grid))) {
  message(glue::glue("Collecting raster grid: {i} / {length(state_grid)}"))
  elev <- get_aws_terrain(
    locations = state_grid[[i]],
    prj = st_crs(state_grid[[i]]),
    z = params$input$elevation_zoom
  )
  elev_lst[[i]] <- elev
}

# Create a raster collection then merge to a single raster
elev_sprc <- terra::sprc(elev_lst)
elev_final <- merge(elev_sprc)

# Write the .tif output to a single giant file
terra::writeRaster(
  x = elev_final,
  filename = here::here(elevation_dir, glue::glue("{opt$state}.tif")),
  overwrite = TRUE,
  memfrac = 0.8
)