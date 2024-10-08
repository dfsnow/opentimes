options(java.parameters = "-Xmx16G")

library(arrow)
library(dplyr)
library(here)
library(r5r)



rJava::.jinit()
rJava::.jcall("java.lang.System", "S", "getProperty", "java.version")

r5r_core <- setup_r5(
  here("intermediate/osmextract/year=2020/geography=state/state=01"),
  verbose = TRUE,
  temp_dir = FALSE,
  overwrite = FALSE
)


mode <- c("CAR")
max_trip_duration <- 500 # minutes

destinations <- read_parquet("intermediate/destpoint/year=2020/geography=tract/state=01/01.parquet") %>%
  select(id = geoid, lon = x_4326, lat = y_4326)
origins = read_parquet("intermediate/cenloc/year=2020/geography=tract/state=01/01.parquet") %>%
  select(id = geoid, lon = x_4326, lat = y_4326)

temp <- r5r::find_snap(r5r_core, origins, mode = "CAR")

# calculate a travel time matrix
ttm <- travel_time_matrix(r5r_core = r5r_core,
                          origins = origins,
                          destinations = destinations,
                          mode = "CAR",
                          max_trip_duration = max_trip_duration,
                          progress = TRUE
                          )
