+++
title = "Data"
+++

# Getting started

Below is an example of the data, where the `id`-suffixed columns are Census
[GEOIDs](https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html)
for counties, `duration_sec` is the trip duration in seconds, and `distance_km`
is the trip distance in kilometers.

| origin_id | destination_id | duration_sec | distance_km |
|:---------:|:--------------:|-------------:|------------:|
| 17031     | 17031          | 0            | 0.000       |
| 17031     | 17043          | 2288         | 32.026      |
| 17031     | 17197          | 3261         | 58.993      |
| 17031     | 18089          | 3398         | 62.119      |
| ...       | ...            | ...          | ...         |

OpenTimes is 40 billion of rows of this data. See the
[Data]({{< ref "data" >}}) section to learn how to download it.

# Getting the data

OpenTimes

### Packages

### DuckDB database

### Raw Parquet files

OpenTimes

- Packages
- Duckdb pointer DB
- Raw parquet files

## Data organization

- DB schema (mermaid)
  - Description of cols
- how data is stored
  - partitioned parquet
  - 1 or 2 row groups per origin

## Coverage

- Census hierarchy (mermaid)
- missing points
- states
- 300km buffer
- Snapped to network

## caveats and limitations

- No traffic
- Some holes
- Limited distance


