import argparse
import math
from pathlib import Path

import pandas as pd
from utils.census import extract_centroids, load_shapefile, split_geoid
from utils.logging import create_logger

logger = create_logger(__name__)

BLOCKLOC_FINAL_COLS = [
    "county",
    "tract",
    "block_group",
    "block",
    "population",
    "x_4326",
    "y_4326",
    "x_5071",
    "y_5071",
]


def create_blockloc(year: str, state: str) -> None:
    """
    Combine Census block population data with block location data from
    TIGER/Line files.

    Args:
        year: The year of the decennial Census.
        state: The two-digit state FIPS code.
    """

    # Pop. data only exists for decennial years, so round down to the nearest
    pop_year = math.floor(int(year) / 10) * 10
    pop_file = (
        Path.cwd()
        / "input"
        / "blockpop"
        / f"year={pop_year}"
        / f"state={state}"
        / f"{state}.parquet"
    )
    loc_file = (
        Path.cwd()
        / "input"
        / "tiger"
        / f"year={year}"
        / "geography=block"
        / f"state={state}"
        / f"{state}.zip"
    )
    output_dir = (
        Path.cwd()
        / "intermediate"
        / "blockloc"
        / f"year={year}"
        / f"state={state}"
    )
    output_file = output_dir / f"{state}.parquet"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load the block-level population file. Missing state here since it's
    # a Hive-partition and not included in the actual data
    df = pd.read_parquet(pop_file)
    df["state"] = state
    logger.info(f"Loaded {len(df)} rows from {pop_file}")

    # Load and cleanup block shapefile file
    gdf = load_shapefile(loc_file)
    original_row_count = len(gdf)
    cols_to_keep = ["geoid", "intptlon", "intptlat"]
    gdf.drop(
        columns=[col for col in gdf.columns if col not in cols_to_keep],
        inplace=True,
    )
    logger.info(f"Loaded {len(gdf)} rows from {loc_file}")

    # Load the Census WGS84 centroid for conversion to planar projection
    gdf = extract_centroids(gdf)
    gdf = split_geoid(gdf, "geoid")

    # Join population data to location data and re-order columns. Drop any
    # columns used as partition keys (year, state)
    join_cols = ["state", "county", "tract", "block"]
    gdf = gdf.merge(df, left_on=join_cols, right_on=join_cols, how="left")
    gdf = gdf[BLOCKLOC_FINAL_COLS]

    # Check for additional rows or empty values after the join
    if len(gdf) != original_row_count:
        raise ValueError("Row count mismatch after join operation.")
    if gdf.isnull().any().any():
        raise ValueError("Missing values detected after join operation.")

    gdf.to_parquet(output_file, engine="pyarrow", index=False)
    logger.info(
        f"Merged block locations and populations. Wrote to: {output_file}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--state", required=False, type=str)
    args = parser.parse_args()
    create_blockloc(args.year, args.state)


if __name__ == "__main__":
    main()
