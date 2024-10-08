#!/bin/bash

OUT_DIR=./intermediate/osmextract/year=2020/geography=state/state=02

mkdir -p $OUT_DIR

osmium extract --strategy complete_ways \
    -b -89.252396,40.298754,-88.654786,40.649747 \
    ./input/osm/year=2020/us-2020.osm.pbf --overwrite \
    -o "$OUT_DIR"/tmp.osm.pbf &&

osmium tags-filter "$OUT_DIR"/tmp.osm.pbf \
    w/highway w/public_transport=platform \
    w/railway=platform w/park_ride r/type=restriction --overwrite \
    -o "$OUT_DIR"/02.osm.pbf

rm $OUT_DIR/tmp.osm.pbf
