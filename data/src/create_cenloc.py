import argparse
from pathlib import Path

import pandas as pd
from utils.census import (
    calculate_weighted_mean,
    extract_centroids,
    load_shapefile,
    points_to_gdf,
    split_geoid,
    transform_5071_to_4326,
)

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
    blockloc_dir = Path.cwd() / "intermediate" / "blockloc" / f"year={year}"
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
        blockloc_dir = blockloc_dir / f"state={state}"
        tiger_dir = tiger_dir / f"state={state}"
        tiger_file = tiger_dir / f"{state}.zip"
        output_dir = output_dir / f"state={state}"
        output_file = output_dir / f"{state}.parquet"

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
    tiger_gdf.to_crs("EPSG:5071", inplace=True)

    # Load the block locations and pop. data associated with the target year.
    # Convert the block location point columns to geometry
    blockloc = pd.read_parquet(blockloc_dir)
    blockloc = points_to_gdf(blockloc, "x_5071", "y_5071", "EPSG:5071")
    blockloc = blockloc[["x_5071", "y_5071", "geometry", "population"]]

    # Spatially join the block-level data to the original TIGER polygon. Need
    # a spatial join here since GEOIDs (for an attribute join) can change over
    # time e.g. all Connecticut counties changed GEOID in 2022, so their 2020
    # county GEOID substrings don't match their 2022 county GEOIDs
    gdf = tiger_gdf.sjoin(blockloc, how="inner", predicate="contains")
    gdf.drop(columns=["index_right", "geometry"], inplace=True)

    block_centroids = calculate_weighted_mean(
        df=gdf,
        group_cols="geoid",
        weight_col="population",
        value_cols=["x_5071", "y_5071"],
    )

    block_centroids = transform_5071_to_4326(block_centroids)
    block_centroids = suffix_coord_cols(block_centroids)
    gdf = tiger_gdf.merge(block_centroids, on="geoid", how="inner")

    # Extract the original centroid of the TIGER data from the INTPT cols
    gdf = gdf.join(
        extract_centroids(gdf.copy())[["x_4326", "y_4326", "x_5071", "y_5071"]]
    )
    gdf = gdf[FINAL_COLS]

    # Add the state FIPS code to each geography if it's missing. This is
    # usually easy to recover from the FIPS code. For ZCTAs you need to
    # spatial join the state since it is (annoyingly) not in the GEOID/FIPS
    if geography == "zcta":
        state_file = (
            Path.cwd()
            / "input"
            / "tiger"
            / f"year={year}"
            / "geography=state"
            / "state.zip"
        )
        state_gdf = load_shapefile(state_file)
        state_gdf.to_crs("EPSG:5071", inplace=True)
        state_gdf = state_gdf[["geoid", "geometry"]]
        state_gdf = state_gdf.rename(columns={"geoid": "state"})
        gdf = points_to_gdf(gdf, "x_5071", "y_5071", "EPSG:5071")
        gdf = gdf.sjoin(state_gdf, how="inner", predicate="within")
        gdf.drop(columns=["index_right", "geometry"], inplace=True)
    elif not state:
        gdf["state"] = split_geoid(gdf.copy(), "geoid")["state"]

    # Check for additional new rows or empty values after the join
    if len(gdf) > original_row_count:
        raise ValueError("Row count mismatch after join operation.")
    if gdf.isnull().any().any():
        raise ValueError("Missing values detected after join operation.")

    # For inputs that are national files (e.g. all states are in one big file)
    # split them by state and write them as individual state-level files
    if not state:
        for state in gdf["state"].unique():
            state_gdf = gdf[gdf["state"] == state]
            state_gdf = state_gdf.drop(columns=["state"])
            state_output_dir = output_dir / f"state={state}"
            state_output_dir.mkdir(parents=True, exist_ok=True)
            state_gdf.to_parquet(
                state_output_dir / f"{state}.parquet",
                engine="pyarrow",
                index=False,
            )
    else:
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
