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