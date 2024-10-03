import argparse
from pathlib import Path

import geopandas as gpd
from utils.census import load_shapefile


def create_osmclip(year: str, state: str, buffer: int = 0) -> None:
    """
    Converts TIGER/Line shapefile to a buffered GeoJSON used as a clipping
    boundary for the OpenStreetMap road network.

    :param year: The year of the TIGER/Line data.
    :param state: The two-digit state code for the shapefile.
    :param buffer: The amount to buffer the input shapefile (in meters).
    """
    tiger_path = (
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
        / "osmclip"
        / "geography=state"
        / f"state={state}"
    )
    output_file = output_dir / f"{state}.geojson"

    output_dir.mkdir(parents=True, exist_ok=True)

    gdf = load_shapefile(tiger_path)
    gdf = gdf[["geometry"]]
    gdf.to_crs(crs="EPSG:5071", inplace=True)
    if buffer:
        gdf["geometry"] = gdf["geometry"].buffer(distance=buffer)
    gdf.to_crs(crs="EPSG:4326", inplace=True)
    gdf.to_file(output_file, driver="GeoJSON")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a buffered clipping boundary for the OSM road network"
    )
    parser.add_argument(
        "--year",
        required=True,
        help="The year of the TIGER/Line data.",
        type=str,
    )
    parser.add_argument(
        "--state",
        required=True,
        help="The two-digit state code for the shapefile.",
        type=str,
    )
    parser.add_argument(
        "--buffer",
        required=False,
        help="The amount to buffer the input shapefile in meters.",
        type=int,
    )

    args = parser.parse_args()

    create_osmclip(args.year, args.state, args.buffer)
