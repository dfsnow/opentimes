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

# Lower the elevation zoom for Alaska, since most of it contains no roads
# and the state is enormous
zoom_level <- ifelse(
  opt$state == "02",
  as.numeric(params$input$elevation_zoom) - 1,
  params$input$elevation_zoom
)

# Load the buffered state file and use a dumb intersection trick to remove any
# parts of the buffer that cross dateline, otherwise the bbox will wrap around
# the entire earth
state <- st_read(osmclip_file) %>%
  st_transform(4326) %>%
  st_make_valid() %>%
  st_intersection(
    st_as_sfc(st_bbox(
      c(
        xmin = -179.999,
        xmax = 0,
        ymax = 90,
        ymin = -90
      ),
      crs = st_crs(4326)
    ))
  )

# Fetch the actual raster elevation tiles within the buffer
elev <- fetch_elev_tiles(
  locations = state,
  prj = st_crs(state),
  z = zoom_level,
  expand = 0
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