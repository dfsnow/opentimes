import argparse
from pathlib import Path

from geopandas.geoseries import points_from_xy
from numpy._core.shape_base import block
from utils.census import (
    calculate_weighted_mean,
    extract_centroids,
    load_shapefile,
    points_to_gdf,
    split_geoid,
    transform_5071_to_4326,
)

import pandas as pd
import geopandas as gpd

COLS_DICT = {
    "state": ["state"],
    "county": ["state", "county"],
    "tract": ["state", "county", "tract"],
    "block_group": ["state", "county", "tract", "block_group"],
    "county_subdivision": ["geoid"],
    "zcta": ["geoid"],
}

FINAL_COLS = [
    "geoid",
    "x_4326",
    "y_4326",
    "x_4326_wt",
    "y_4326_wt",
    "x_5071",
    "y_5071",
    "x_5071_wt",
    "y_5071_wt",
]


def create_cenloc(year: str, geography: str, state: str | None = None) -> None:
    """
    Combines TIGER/Line shapefiles with Census block populations to determine
    the population-weighted centroid of every Census geography.

    :param year: The year of the TIGER/Line data.
    :param geography: The geography type of the shapefile.
    :param state: (Optional) The two-digit state code for the shapefile.
    """
    blockloc_path = Path.cwd() / "intermediate" / "blockloc" / f"year={year}"
    tiger_dir = (
        Path.cwd()
        / "input"
        / "tiger"
        / f"year={year}"
        / f"geography={geography}"
    )
    output_dir = (
        Path.cwd()
        / "intermediate"
        / "cenloc"
        / f"year={year}"
        / f"geography={geography}"
    )

    if not state:
        output_file = output_dir / f"{geography}.parquet"
        tiger_file = tiger_dir / f"{geography}.zip"
    else:
        blockloc_path = blockloc_path / f"state={state}"
        tiger_dir = tiger_dir / f"state={state}"
        tiger_file = tiger_dir / f"{state}.zip"
        output_dir = output_dir / f"state={state}"
        output_file = output_dir / f"{state}.zip"

    output_dir.mkdir(parents=True, exist_ok=True)

    # Load the target TIGER shapefile and drop unneeded columns
    tiger_gdf = load_shapefile(tiger_file)
    original_row_count = len(tiger_gdf)
    tiger_cols_to_keep = ["geoid", "intptlon", "intptlat", "geometry"]
    tiger_gdf = tiger_gdf.drop(
        columns=[
            col for col in tiger_gdf.columns if col not in tiger_cols_to_keep
        ]
    )

    # Load the block locations and pop. data associated with the target year.
    # Load the state value from partition key if a national file, else add it
    # from the script arguments
    blockloc = pd.read_parquet(blockloc_path)
    blockloc["state"] = (
        blockloc["state"].astype(str).str.zfill(2) if not state else state
    )

    # Find weighted centroids by joining block populations, then taking the
    # weighted mean of the Alber's coordinates for each geography. Use
    # attribute join for blocks if available, else spatial join
    join_cols = COLS_DICT[geography]
    if geography in ["state", "county", "tract", "block_group"]:
        # Split the GEOID components in the TIGER data for later attribute join
        # Don't need to do this in the block data since they're already split
        tiger_gdf = split_geoid(tiger_gdf, "geoid")

        block_centroids = calculate_weighted_mean(
            df=blockloc,
            group_cols=join_cols,
            weight_col="population",
            value_cols=["x_5071", "y_5071"],
        )
        block_centroids = transform_5071_to_4326(block_centroids)
        block_centroids = suffix_coord_cols(block_centroids)

        # Join the weighted centroid from the blocks back to the TIGER data
        gdf = tiger_gdf.merge(
            block_centroids, left_on=join_cols, right_on=join_cols, how="inner"
        )
    else:
        # If the geography is not part of the Census hierarchy then we need to
        # do a spatial join, since block GEOIDs won't contain the FIPS codes
        # necessary for an attribute join
        blockloc = points_to_gdf(blockloc, "x_5071", "y_5071", "EPSG:5071")
        blockloc = blockloc[["x_5071", "y_5071", "geometry", "population"]]

        tiger_gdf.to_crs("EPSG:5071", inplace=True)
        gdf = tiger_gdf.sjoin(blockloc, how="inner", predicate="contains")
        gdf.drop(columns=["index_right", "geometry"], inplace=True)

        block_centroids = calculate_weighted_mean(
            df=gdf,
            group_cols=join_cols,
            weight_col="population",
            value_cols=["x_5071", "y_5071"],
        )

        block_centroids = transform_5071_to_4326(block_centroids)
        block_centroids = suffix_coord_cols(block_centroids)
        gdf = tiger_gdf.merge(
            block_centroids, left_on=join_cols, right_on=join_cols, how="inner"
        )

    # Extract the original centroid of the TIGER data from the INTPT cols
    gdf = gdf.join(
        extract_centroids(gdf.copy())[["x_4326", "y_4326", "x_5071", "y_5071"]]
    )
    gdf = gdf[FINAL_COLS]

    # Check for additional new rows or empty values after the join
    if len(gdf) >= original_row_count:
        raise ValueError("Row count mismatch after join operation.")
    if gdf.isnull().any().any():
        raise ValueError("Missing values detected after join operation.")

    gdf.to_parquet(output_file, engine="pyarrow", index=False)


def suffix_coord_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Add a _wt suffix to coordinate column names."""
    columns_to_rename = ["x_4326", "y_4326", "x_5071", "y_5071"]
    renaming_mapping = {col: f"{col}_wt" for col in columns_to_rename}
    df = df.rename(columns=renaming_mapping)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Combine TIGER shapefiles with Census block populations"
    )
    parser.add_argument(
        "--year",
        required=True,
        help="The year of the TIGER/Line data.",
        type=str,
    )
    parser.add_argument(
        "--geography",
        required=True,
        help="The geography type of the shapefile.",
        type=str,
    )
    parser.add_argument(
        "--state",
        required=False,
        help="The two-digit state code for the shapefile.",
        type=str,
    )

    args = parser.parse_args()

    create_cenloc(args.year, args.geography, args.state)
