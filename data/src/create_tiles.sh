#!/bin/bash

# Use tippecanoe to create vector tiles from Census CB data
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <year>"
  exit 1
fi

year=$1
version=$(yq e '.times.version' params.yaml)

TRACT_IN=./input/cb/year=${year}/geography=tract/tract.geojson
BLOCK_GROUP_IN=./input/cb/year=${year}/geography=block_group/block_group.geojson
OUT_DIR=./output/tiles/version=${version}/year=${year}/geography=combined/
OUT_FILE=tiles-${version}-${year}-combined.pmtiles
# INDEX_FILE=tiles-${version}-${year}-${geography}.json
mkdir -p "$OUT_DIR"

tippecanoe -f -z6 -l geometry -o "${OUT_DIR}"tract.pmtiles \
    --coalesce-densest-as-needed --simplify-only-low-zooms \
    --no-simplification-of-shared-nodes \
    -y id -T id:string "${TRACT_IN}"

tippecanoe -f -Z6 -z10 -d2 -D3 -l geometry \
    -o "${OUT_DIR}"block_group.pmtiles \
    --coalesce-densest-as-needed \
    --no-simplification-of-shared-nodes \
    -y id -T id:string \
    --extend-zooms-if-still-dropping "${BLOCK_GROUP_IN}"

tile-join --force -o "${OUT_DIR}""$OUT_FILE" \
    "${OUT_DIR}"tract.pmtiles "${OUT_DIR}"block_group.pmtiles


# Below is hacky bash to collect the count of parquet files for each mode,
# geography, and state. Needed by the map JS to iterate through the Parquet
# files.
# modes=("car" "bicycle" "foot")
# final_output="{"
# for mode in "${modes[@]}"; do
#     output=$(aws s3 ls s3://opentimes-public/times/version="$version"/mode="$mode"/year="$year"/geography="$geography"/ \
#         --recursive \
#         --endpoint-url https://fcb279b22cfe4c98f903ad8f9e7ccbb2.r2.cloudflarestorage.com \
#         --profile cloudflare)
#
#     state_counts=()
#     while read -r line; do
#         if [[ $line == *.parquet ]]; then
#             # Extract the state directory from the file path
#             state=$(echo "$line" | awk -F'/' '{for(i=1;i<=NF;i++) if($i ~ /^state=/) print substr($i, 7)}')
#             state_counts+=("$state")
#         fi
#     done <<< "$output"
#
#     json_output=$(printf "%s\n" "${state_counts[@]}" | awk '
#     {
#         count[$1]++
#     }
#     END {
#         printf "{"
#         for (state in count) {
#             if (count[state] > 1) {
#                 printf "\"%s\": %d,", state, count[state]
#             }
#         }
#         printf "}"
#     }' | sed 's/,}/}/')
#
#     final_output+="\"$mode\": $json_output,"
# done
#
# final_output=$(echo "$final_output" | sed 's/,$/}/')
# echo "$final_output" > "${OUT_DIR}""${INDEX_FILE}"
