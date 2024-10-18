library(DBI)
library(duckdb)

con <- dbConnect(duckdb(), dbdir = "my-db.duckdb", read_only = FALSE)

dbExecute(
  conn = con,
  "
  INSTALL httpfs;
  LOAD httpfs;
  CREATE SECRET (
    TYPE R2,
    KEY_ID '',
    SECRET '',
    ACCOUNT_ID ''
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

# 1.285 seconds is the time to beat