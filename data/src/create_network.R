options(java.parameters = "-Xmx16G")

library(digest)
library(glue)
library(here)
library(optparse)
library(r5r)

# Function to symlink files for the network creation into the working directory
create_links <- function(source_dir, target_dir, pattern) {
  existing_links <- list.files(target_dir, pattern = pattern, full.names = TRUE)
  if (length(existing_links) > 0) {
    message("Removing existing symlinks:")
    sapply(existing_links, unlink)
  }

  files <- list.files(source_dir, pattern = pattern, full.names = TRUE)
  if (length(files) > 0) {
    sapply(files, function(file) {
      file.symlink(from = file, to = file.path(target_dir, basename(file)))
    })
  }
}


# Parse script arguments in the style of Python
option_list <- list(
  make_option("--year", type = "character"),
  make_option("--state", type = "character")
)
opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# If the downloaded copy of R5 used by R isn't the custom JAR with limits
# removed, replace it
r5_file_url <- r5r:::fileurl_from_metadata(r5r:::r5r_env$r5_jar_version)
r5_filename <- basename(r5_file_url)

downloaded_r5_path <- file.path(r5r:::r5r_env$cache_dir, r5_filename)
custom_r5_path <- here::here("./jars/r5-custom.jar")
if (!file.exists(downloaded_r5_path)) {
  message("Downloading R5 JAR:")
  file.copy(from = custom_r5_path, to = downloaded_r5_path, overwrite = TRUE)
}

downloaded_r5_md5 <- digest(object = downloaded_r5_path, algo = "md5", file = TRUE)
custom_r5_md5 <- digest(object = custom_r5_path, algo = "md5", file = TRUE)
if (downloaded_r5_md5 != custom_r5_md5) {
  file.copy(from = custom_r5_path, to = downloaded_r5_path, overwrite = TRUE)
}

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
message("Creating network.dat file:")
r5r::setup_r5(
  data_path = network_dir,
  verbose = FALSE,
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
