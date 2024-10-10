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


# Simplified version of the tile fetching code from:
# https://github.com/USEPA/elevatr/blob/main/R/get_elev_raster.R
fetch_elev_tiles <- function(locations, z, prj, expand = NULL,
                             ncpu = future::availableCores() - 1,
                             tmp_dir = tempdir(), ...) {
  bbx <- elevatr:::proj_expand(locations, prj, expand)
  base_url <- "https://s3.amazonaws.com/elevation-tiles-prod/geotiff"
  tiles <- elevatr:::get_tilexy(bbx, z)
  urls <- sprintf("%s/%s/%s/%s.tif", base_url, z, tiles[, 1], tiles[, 2])
  nurls <- length(urls)
  dir <- tempdir()
  
  progressr::handlers(
    progressr::handler_progress(
      format = " Accessing raster elevation [:bar] :percent",
      clear = FALSE,
      width = 60,
      enable = TRUE
    )
  )
  
  progressr::with_progress(
    {
      future::plan(future::multisession, workers = ncpu)
      p <- progressr::progressor(along = urls)
      dem_list <- furrr::future_map(
        urls,
        function(x) {
          p()
          tmpfile <- tempfile(tmpdir = tmp_dir, fileext = ".tif")
          resp <- httr::GET(
            x,
            httr::user_agent("elevatr R package (https://github.com/jhollist/elevatr)"),
            httr::write_disk(tmpfile, overwrite = TRUE), ...
          )
          if (!grepl("image/tif", httr::http_type(resp))) {
            stop(paste("This url:", x, "did not return a tif"), call. = FALSE)
          }
          tmpfile
        }
      )
    },
    enable = TRUE,
    delay_stdout = TRUE,
    delay_conditions = "condition"
  )
  
  merged_elevation_grid <- merge_rasters_mc(dem_list, target_prj = prj)
  merged_elevation_grid
}


# Better version of merge_rasters from elevatr that works on any number
# of files
merge_rasters_mc <- function(raster_list,
                             target_prj,
                             method = "bilinear") {
  files <- unlist(raster_list)
  chunk_size <- 5000
  temp_files <- list()
  chunks <- split(seq_along(files), ceiling(seq_along(files) / chunk_size))
  message(paste("Mosaicing and projecting", length(files), "files"))
  
  # Split files into chunks, then merge the splits
  for (i in seq_along(chunks)) {
    message(paste("Processing chunk:", i, "/", length(chunks)))
    chunk <- files[chunks[[i]]]
    temp_destfile <- tempfile(fileext = ".tif")
    sf::gdal_utils(
      util = "warp", source = unlist(chunk), destination = temp_destfile,
      options = c("-r", method, "-multi", "-wo", "NUM_THREADS=ALL_CPUS")
    )
    temp_files <- c(temp_files, temp_destfile)
  }
  
  # Merge all temporary .tif files into a single final file
  final_destfile <- tempfile(fileext = ".tif")
  sf::gdal_utils(
    util = "warp", source = unlist(temp_files), destination = final_destfile,
    options = c("-r", method, "-multi", "-wo", "NUM_THREADS=ALL_CPUS")
  )
  
  final_destfile2 <- tempfile(fileext = ".tif")
  sf::gdal_utils(
    util = "warp", source = final_destfile, destination = final_destfile2,
    options = c(
      "-r", method, "-t_srs", sf::st_crs(target_prj)$wkt,
      "-multi", "-wo", "NUM_THREADS=ALL_CPUS"
    )
  )
  
  return(terra::rast(final_destfile2))
}


# Setup custom R5 JAR from repo (has network size limits disabled)
setup_r5_jar <- function(path) {
  # If the downloaded copy of R5 used by R isn't the custom JAR with limits
  # removed, replace it
  r5_file_url <- r5r:::fileurl_from_metadata(r5r:::r5r_env$r5_jar_version)
  r5_filename <- basename(r5_file_url)
  
  downloaded_r5_path <- file.path(r5r:::r5r_env$cache_dir, r5_filename)
  custom_r5_path <- here::here(path)
  if (!file.exists(downloaded_r5_path)) {
    message("Downloading R5 JAR:")
    file.copy(from = custom_r5_path, to = downloaded_r5_path, overwrite = TRUE)
  }
  
  downloaded_r5_md5 <- digest::digest(
    object = downloaded_r5_path,
    algo = "md5",
    file = TRUE
  )
  custom_r5_md5 <- digest::digest(
    object = custom_r5_path,
    algo = "md5",
    file = TRUE
  )
  if (downloaded_r5_md5 != custom_r5_md5) {
    file.copy(from = custom_r5_path, to = downloaded_r5_path, overwrite = TRUE)
  }
}