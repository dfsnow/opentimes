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
osmextract_dir <- here::here(glue::glue(
  "intermediate/osmextract/",
  "year={opt$year}/geography=state/state={opt$state}"
))
network_dir <- here::here(glue::glue(
  "intermediate/network/",
  "year={opt$year}/geography=state/state={opt$state}"
))

if (!dir.exists(network_dir)) {
  dir.create(network_dir, recursive = TRUE)
}
create_links(osmextract_dir, network_dir, pattern = ".*\\.osm.pbf")

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