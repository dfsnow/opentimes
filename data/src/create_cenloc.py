import argparse
from pathlib import Path
from utils.census import (
        calculate_weighted_mean,
        extract_centroids,
        load_shapefile,
        split_geoid,
        transform_5071_to_4327,
        )

import pandas as pd


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
    if not state:
        tiger_file = tiger_dir / f"{geography}.zip"
    else:
        tiger_dir = tiger_dir / f"state={state}"
        tiger_file = tiger_dir / f"{state}.zip"

    # Load the target TIGER shapefile and grab the unweighted centroid
    tiger_gdf = load_shapefile(tiger_file)
    original_row_count = len(tiger_gdf)
    tiger_cols_to_keep = ["geoid", "intptlon", "intptlat", "geometry"]
    tiger_gdf = tiger_gdf.drop(
            columns=[
                col for col in tiger_gdf.columns if col not in tiger_cols_to_keep
                ]
            )
    tiger_gdf = tiger_gdf.join(
            extract_centroids(tiger_gdf)[["x_4326", "y_4326", "x_5071", "y_5071"]]
            )

    # Load the block locations and pop data associated with the target year
    blockloc = pd.read_parquet(blockloc_path)
    blockloc["state"] = blockloc["state"].astype(str).str.zfill(2)

    # Find weighted centroids by joining block populations, then taking the
    # weighted mean of the Alber's coordinates for each geography. Use
    # attribute join for blocks if available, else spatial join
    if geography in ["state", "county", "tract", "block_group"]:
        tiger_gdf = split_geoid(tiger_gdf, "geoid")

        geography_to_join_cols = {
                "state": ["state"],
                "county": ["state", "county"],
                "tract": ["state", "county", "tract"],
                "block_group": ["state", "county", "tract", "block_group"],
                }
        join_cols = geography_to_join_cols[geography]

        out = calculate_weighted_mean(
                df=blockloc,
                group_cols=join_cols,
                weight_col="population",
                value_cols=["x_5071", "y_5071"],
                )
        out = transform_5071_to_4327(out)
        columns_to_rename = ["x_4326", "y_4326", "x_5071", "y_5071"]
        renaming_mapping = {col: f"{col}_wt" for col in columns_to_rename}
        out = out.rename(columns=renaming_mapping)

    breakpoint()


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
