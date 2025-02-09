#!/bin/bash

# gh api /repos/dfsnow/opentimes/actions/runs/13102724143/jobs --paginate \
#     -q '.jobs[] | select(.conclusion == "failure") | .name' | \
#     awk -F'[(), ]+' '{print $2,$3}' | tr ' ' ',' > missing.csv

while IFS=, read -r year state
do
    echo "Starting $year $state"
    geographies_array=($(yq e -o=json '.input.census.geography.all' ./params.yaml | jq -r '.[]'))
    echo "Geographies:"
    echo "${geographies_array[@]}"

    rm -rf ./build/*
    ./src/create_osrmnetwork.sh foot "$year" "$state"

    docker run --rm --name osrm -d -p 5333:5000 -v "./build:/data" \
        osrm/osrm-backend:v5.25.0 osrm-routed --algorithm ch \
        --max-table-size 100000000 /data/"$state".osrm

    # Wait for OSRM to load all network data
    for i in {1..60}; do
        if docker logs osrm | grep -q "running and waiting for requests"; then
            echo "OSRM is running and waiting for requests"
            break
        fi
        sleep 5
    done

    for geo in "${geographies_array[@]}"; do
        chunks_array=($(uv run ./src/split_chunks.py \
            --year "$year" --geography "$geo" --state "$state" | jq -r '.[]'))

        for chunk in "${chunks_array[@]}"; do
            echo "Starting job with parameters: mode=foot, year="$year", geography=${geo}, state="$state", centroid_type=weighted, chunk=${chunk}"
            uv run ./src/calculate_times.py \
                --mode foot --year "$year" \
                --geography "$geo" --state "$state" \
                --centroid-type weighted --chunk "$chunk" \
                --write-to-s3
            done
        done

    docker stop osrm

done < missing.csv
