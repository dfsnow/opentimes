library(DBI)
library(duckdb)
library(glue)
library(here)
library(yaml)

params <- read_yaml(here::here("params.yaml"))
Sys.setenv("AWS_PROFILE" = params$s3$profile)

# Example of loading pointer DB from URL i.e. from the finished data
con <- dbConnect(duckdb(), dbdir = ":memory:", read_only = TRUE)
dbExecute(
  conn = con,
  "
  INSTALL httpfs;
  LOAD httpfs;
  ATTACH 'https://data.opentimes.org/databases/0.0.1.duckdb' AS opentimes;
  "
)

ttm <- dbGetQuery(
  conn = con,
  "
  SELECT *
  FROM opentimes.times
  WHERE version = '0.0.1'
      AND mode = 'car'
      AND year = '2024'
      AND origin_id = '02020000101'
  "
)

dbGetQuery(
  conn = con,
  "SUMMARIZE opentimes.times"
)

# Example of reading directly from the raw data bucket
con_raw <- dbConnect(duckdb(), dbdir = ":memory:", read_only = FALSE)

dbExecute(
  conn = con_raw,
  glue::glue("
  INSTALL aws;
  LOAD aws;
  INSTALL httpfs;
  LOAD httpfs;
  CREATE SECRET (
      TYPE R2,
      PROVIDER CREDENTIAL_CHAIN,
      ACCOUNT_ID '{params$s3$account_id}'
  );
  ")
)

ttm_raw <- dbGetQuery(
  conn = con_raw,
  glue::glue("
  SELECT *
  FROM read_parquet(
      'r2://{params$s3$data_bucket}/times/*/*/*/*/*/*/*.parquet',
      hive_partitioning = true,
      hive_types_autocast = false,
      filename = true
  )
  WHERE version = '0.0.1'
      AND mode = 'auto'
      AND year = '2020'
  ")
)
