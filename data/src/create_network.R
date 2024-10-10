options(java.parameters = "-Xmx16G")

library(glue)
library(here)
library(optparse)
library(r5r)
source("src/utils/utils.R")

# Parse script arguments in the style of Python
option_list <- list(
  make_option("--year", type = "character"),
  make_option("--state", type = "character")
)
opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# Setup file paths and symlinks
file_path <- glue::glue("year={opt$year}/geography=state/state={opt$state}")
osmextract_dir <- here::here("intermediate/osmextract", file_path)
network_dir <- here::here("intermediate/network", file_path)
elevation_dir <- here::here(
  "input/elevation",
  glue::glue("geography=state/state={opt$state}")
)

# Create directories and symlinks (if missing)
if (!dir.exists(network_dir)) {
  dir.create(network_dir, recursive = TRUE)
}
create_links(osmextract_dir, network_dir, pattern = ".*\\.osm.pbf")
create_links(elevation_dir, network_dir, pattern = ".*\\.tif")

# Create the network.dat file
setup_r5_jar("./jars/r5-custom.jar")
message("Creating network.dat file:")
r5r::setup_r5(
  data_path = network_dir,
  verbose = TRUE,
  temp_dir = FALSE,
  overwrite = TRUE
)

# Cleanup any residual files
mapdb_files <- list.files(
  network_dir,
  pattern = ".*\\.mapdb(\\.p)?$",
  full.names = TRUE
)
if (length(mapdb_files) > 0) {
  message("Removing unneeded files:")
  sapply(mapdb_files, unlink)
}
