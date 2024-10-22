library(DBI)
library(duckdb)

# Example of loading pointer DB for URL
con <- dbConnect(duckdb(), dbdir = ":memory:", read_only = FALSE)
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