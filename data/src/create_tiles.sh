#!/bin/bash

# Use tippecanoe to create vector tiles from Census CB data
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <year> <geography>"
  exit 1
fi

year=$1
geography=$2
version=$(yq e '.times.version' params.yaml)

IN_FILE=./input/cb/year=${year}/geography=${geography}/${geography}.geojson
OUT_DIR=./output/tiles/version=${version}/year=${year}/geography=${geography}/
OUT_FILE=tiles-${version}-${year}-${geography}.pmtiles
INDEX_FILE=tiles-${version}-${year}-${geography}.json
mkdir -p "$OUT_DIR"

tippecanoe -f -zg -l geometry -o "${OUT_DIR}""${OUT_FILE}" \
    --coalesce-densest-as-needed --simplify-only-low-zooms \
    --no-simplification-of-shared-nodes \
    -y id -T id:string \
    --extend-zooms-if-still-dropping "${IN_FILE}"

# Below is hacky bash to collect the count of parquet files for each mode,
# geography, and state. Needed by the map JS to iterate through the Parquet
# files.
modes=("car" "bicycle" "foot")
final_output="{"
for mode in "${modes[@]}"; do
    output=$(aws s3 ls s3://opentimes-public/times/version="$version"/mode="$mode"/year="$year"/geography="$geography"/ \
        --recursive \
        --endpoint-url https://fcb279b22cfe4c98f903ad8f9e7ccbb2.r2.cloudflarestorage.com \
        --profile cloudflare)

    state_counts=()
    while read -r line; do
        if [[ $line == *.parquet ]]; then
            # Extract the state directory from the file path
            state=$(echo "$line" | awk -F'/' '{for(i=1;i<=NF;i++) if($i ~ /^state=/) print substr($i, 7)}')
            state_counts+=("$state")
        fi
    done <<< "$output"

    json_output=$(printf "%s\n" "${state_counts[@]}" | awk '
    {
        count[$1]++
    }
    END {
        printf "{"
        for (state in count) {
            if (count[state] > 1) {
                printf "\"%s\": %d,", state, count[state]
            }
        }
        printf "}"
    }' | sed 's/,}/}/')

    final_output+="\"$mode\": $json_output,"
done

final_output=$(echo "$final_output" | sed 's/,$/}/')
echo "$final_output" > "${OUT_DIR}""${INDEX_FILE}"
