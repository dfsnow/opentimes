import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd
from utils.census import load_shapefile, points_to_gdf


def create_destpoint(
    year: str, geography: str, state: str, buffer: int = 0
) -> None:
    """
    Find all centroids (weighted and unweighted) of a given Census geography
    within a buffered version of the containing state.

    Args:
        year: The year of the point data.
        geography: The geography type of the point data.
        state: The two-digit state FIPS code for the point data.
        buffer: The amount to buffer the input shapefile (in meters) when
            when determining destination points.

    Returns:
        None
    """
    cenloc_dir = (
        Path.cwd()
        / "intermediate"
        / "cenloc"
        / f"year={year}"
        / f"geography={geography}/"
    )
    tiger_file = (
        Path.cwd()
        / "input"
        / "tiger"
        / f"year={year}"
        / "geography=state"
        / "state.zip"
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
    boundary = load_shapefile(tiger_file)
    boundary = boundary[boundary["geoid"] == state]
    boundary = boundary[["geometry"]]
    boundary.to_crs(crs="EPSG:5071", inplace=True)
    if buffer:
        boundary["geometry"] = boundary["geometry"].buffer(distance=buffer)

    # Load the Census geography centroids (weighted and unweighted), and keep
    # only centroids of either kind that are within the buffered state
    cenloc = pd.read_parquet(cenloc_dir)
    cenloc_gdf = points_to_gdf(cenloc, "x_5071", "y_5071", "EPSG:5071")
    cenloc_gdf = cenloc_gdf.sjoin(boundary, how="inner", predicate="within")
    cenloc_gdf_wt = points_to_gdf(
        cenloc, "x_5071_wt", "y_5071_wt", "EPSG:5071"
    )
    cenloc_gdf_wt = cenloc_gdf_wt.sjoin(
        boundary, how="inner", predicate="within"
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=True, type=str)
    parser.add_argument("--state", required=True, type=str)
    parser.add_argument("--buffer", required=False, type=int)
    args = parser.parse_args()

    create_destpoint(args.year, args.geography, args.state, args.buffer)
