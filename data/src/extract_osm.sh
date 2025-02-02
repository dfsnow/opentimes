#!/bin/bash

# Use osmium to clip the North America OSM .pbf to each state (+ a buffer)
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <year> <state>"
  exit 1
fi

year=$1
state=$2

OUT_DIR=./intermediate/osmextract/year=${year}/geography=state/state=${state}
mkdir -p "$OUT_DIR"

osmium extract --strategy complete_ways \
    -p intermediate/osmclip/year="$year"/geography=state/state="$state"/"$state".geojson \
    ./input/osm/year="$year"/us-"$year".osm.pbf --overwrite \
    -o "$OUT_DIR"/tmp.osm.pbf
osmium tags-filter "$OUT_DIR"/tmp.osm.pbf \
    w/highway w/public_transport=platform \
    w/railway=platform w/park_ride r/type=restriction --overwrite \
    -o "$OUT_DIR"/"$state".osm.pbf

rm "$OUT_DIR"/tmp.osm.pbf
