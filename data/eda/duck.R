library(DBI)
library(duckdb)

con <- dbConnect(duckdb(), dbdir = ":memory:", read_only = FALSE)

dbExecute(
  conn = con,
  "
  INSTALL httpfs;
  LOAD httpfs;
  INSTALL aws;
  LOAD aws;
  CREATE SECRET (
    TYPE R2,
    PROVIDER CREDENTIAL_CHAIN,
    ACCOUNT_ID 'fcb279b22cfe4c98f903ad8f9e7ccbb2'
  );
  "
)

dbExecute(
  conn = con,
  "
  CREATE OR REPLACE VIEW times AS
  SELECT
      version,
      mode,
      year,
      geography,
      state,
      centroid_type,
      origin_id,
      destination_id,
      time_min
  FROM read_parquet('r2://opentimes-data/times/*/*/*/*/*/*/*/*.parquet', hive_partitioning = true, hive_types_autocast = 0)
  "
)

tictoc::tic()
ttm <- dbGetQuery(
  conn = con,
  "
  SELECT *
  FROM times
  WHERE version = '0.0.1'
  AND mode = 'car'
  AND year = '2020'
  AND geography = 'county'
  AND state = '01'
  AND centroid_type = 'weighted'
  AND origin_id = '01019'
  "
)
tictoc::toc()

dbExecute(
  conn = con,
  "
  COPY (
    SELECT
      geography,
      state,
      centroid_type,
      origin_id,
      destination_id,
      time_min
    FROM times
    WHERE version = '0.0.1'
  )
    TO 'r2://opentimes-public/version=0.0.1/mode=car/year=2020/test.parquet'
    (FORMAT 'parquet', CODEC 'zstd', COMPRESSION_LEVEL 9);
  "
)

ttm <- dbGetQuery(
  conn = con,
  "
  SELECT *
  FROM read_parquet('https://data.opentimes.org/points/0.0.1/car/2020/points-0.0.1-car-2020.parquet')
  "
)
# 1.285 seconds is the time to beat