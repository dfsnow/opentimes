#!/bin/bash

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <mode> <year> <state>"
  exit 1
fi

mode=$1
year=$2
state=$3

BUILD_DIR=build
IN_FILE=intermediate/osmextract/year=${year}/geography=state/state=${state}/${state}.osm.pbf
OUT_DIR=intermediate/osrmnetwork/mode=${mode}/year=${year}/geography=state/state=${state}

rm -rf ./"$BUILD_DIR"/*
mkdir -p ./"$OUT_DIR"
ln ./"$IN_FILE" ./"$BUILD_DIR"/"$state".osm.pbf

docker run --rm -t -v ./"$BUILD_DIR":/data osrm/osrm-backend \
    osrm-extract -p /opt/"$mode".lua /data/"$state".osm.pbf
docker run --rm -t -v ./"$BUILD_DIR":/data osrm/osrm-backend \
    osrm-contract /data/"$state".osrm

rm ./"$BUILD_DIR"/"$state".osm.pbf
