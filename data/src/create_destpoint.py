import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd
from utils.census import points_to_gdf


def create_destpoint(year: str, geography: str, state: str) -> None:
    """
    Find all centroids (weighted and unweighted) of a given Census geography
    within a buffered version of the containing state.

    :param year: The year of the point data.
    :param geography: The geography type of the point data.
    :param state: The two-digit state code for the point data.
    """
    cenloc_dir = (
        Path.cwd()
        / "intermediate"
        / "cenloc"
        / f"year={year}"
        / f"geography={geography}/"
    )
    osmclip_file = (
        Path.cwd()
        / "intermediate"
        / "osmclip"
        / f"year={year}"
        / "geography=state"
        / f"state={state}"
        / f"{state}.geojson"
    )
    output_dir = (
        Path.cwd()
        / "intermediate"
        / "destpoint"
        / f"year={year}"
        / f"geography={geography}"
        / f"state={state}"
    )
    output_file = output_dir / f"{state}.parquet"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load the buffered state boundary
    osmclip = gpd.read_file(osmclip_file)
    osmclip.to_crs("EPSG:5071", inplace=True)

    # Load the Census geography centroids (weighted and unweighted), and keep
    # only centroids of either kind that are within the buffered state
    cenloc = pd.read_parquet(cenloc_dir)
    cenloc_gdf = points_to_gdf(cenloc, "x_5071", "y_5071", "EPSG:5071")
    cenloc_gdf = cenloc_gdf.sjoin(osmclip, how="inner", predicate="within")
    cenloc_gdf_wt = points_to_gdf(
        cenloc, "x_5071_wt", "y_5071_wt", "EPSG:5071"
    )
    cenloc_gdf_wt = cenloc_gdf_wt.sjoin(
        osmclip, how="inner", predicate="within"
    )

    cenloc_final = cenloc[
        cenloc["geoid"].isin(cenloc_gdf["geoid"])
        | cenloc["geoid"].isin(cenloc_gdf_wt["geoid"])
    ]
    cenloc_final = cenloc_final[
        [c for c in cenloc.columns if c not in ["geometry", "state"]]
    ]

    cenloc_final.to_parquet(output_file, engine="pyarrow", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a set of destination points from a buffered state"
    )
    parser.add_argument(
        "--year",
        required=True,
        help="The year of the point data.",
        type=str,
    )
    parser.add_argument(
        "--geography",
        required=True,
        help="The geography type of the point data.",
        type=str,
    )
    parser.add_argument(
        "--state",
        required=True,
        help="The two-digit state code for the point data.",
        type=str,
    )

    args = parser.parse_args()

    create_destpoint(args.year, args.geography, args.state)
