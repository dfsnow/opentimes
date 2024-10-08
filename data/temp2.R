library(ggplot2)
library(dplyr)
library(here)
library(sf)
library(tigris)


states <- tigris::states(year = 2020, cb = TRUE) %>%
  st_transform(4326) %>%
  filter(STUSPS %in% c("AL", "FL", "GA", "MS", "TN", "AR", "LA", "KY", "IL", "MO", "SC", "NC"))
counties <- tigris::counties(year = 2020, cb = TRUE) %>%
  st_transform(4326)
buff <- read_sf("intermediate/osmclip/year=2020/geography=state/state=01/01.geojson")

ttm <- read_parquet("temp_python.parquet")
jeff <- ttm %>%
  filter(from_id == "01073")

jeff_sf <- counties %>%
  inner_join(jeff, by = c("GEOID" = "to_id")) %>%
  select(travel_time) %>%
  mutate(tt = cut(
    travel_time,
    breaks = seq(0, 500, 60),
    labels = c(
      "<=60", "60-120", "120-180", "180-240", "240-300",
      "300-360", "360-420", "420-480"
    )
  ))

origins

ggplot(jeff_sf) +
  ggspatial::annotation_map_tile(zoomin = 0) +
  geom_sf(data = buff, fill = "grey80", alpha = 0.5) +
  # geom_point(data = destinations, aes(x = lon, y = lat), color = "slateblue", size = 1) +
  # geom_point(data = origins, aes(x = lon, y = lat), color = "red", size = 1) +
  geom_sf(data = jeff_sf, aes(fill = travel_time)) +
  geom_sf(data = states %>% filter(STUSPS == "AL"), fill = NA, color = "black", linewidth = 1) +
  coord_sf(xlim = c(-93.05, -80.49), ylim = c(27.09,37.42)) +
  scale_fill_viridis_c(direction = -1, name = "Travel time\n(minutes)") +
  theme_minimal()
