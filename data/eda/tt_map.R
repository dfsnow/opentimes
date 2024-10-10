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

ttm <- read_parquet("output/times/mode=car/year=2020/geography=county/state=01/centroid_type=weighted/part-0.parquet")
missing <- read_parquet("output/missing_pairs/mode=car/year=2020/geography=county/state=01/centroid_type=weighted/part-0.parquet")

jeff <- ttm %>%
  filter(origin_id == "01071")

jeff_sf <- counties %>%
  inner_join(jeff, by = c("GEOID" = "destination_id")) %>%
  select(time_min) %>%
  mutate(tt = cut(
    time_min,
    breaks = seq(0, 500, 60),
    labels = c(
      "<=60", "60-120", "120-180", "180-240", "240-300",
      "300-360", "360-420", "420-480"
    )
  ))


ggplot(jeff_sf) +
  ggspatial::annotation_map_tile(zoomin = 0) +
  geom_sf(data = buff, fill = "grey80", alpha = 0.5) +
  # geom_point(data = destinations, aes(x = lon, y = lat), color = "slateblue", size = 1) +
  # geom_point(data = origins, aes(x = lon, y = lat), color = "red", size = 1) +
  geom_sf(data = jeff_sf, aes(fill = time_min)) +
  geom_sf(data = states %>% filter(STUSPS == "AL"), fill = NA, color = "black", linewidth = 1) +
  coord_sf(xlim = c(-93.05, -80.49), ylim = c(27.09,37.42)) +
  scale_fill_viridis_c(direction = -1, name = "Travel time\n(minutes)") +
  theme_minimal()
