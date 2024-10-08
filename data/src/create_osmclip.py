import argparse
from pathlib import Path

from utils.census import load_shapefile


def create_osmclip(year: str, state: str, buffer: int = 0) -> None:
    """
    Converts TIGER/Line shapefile to a buffered GeoJSON used as a clipping
    boundary for the OpenStreetMap road network.

    Args:
        year: The year of the TIGER/Line data.
        state: The two-digit state FIPS code for the shapefile.
        buffer: The amount to buffer the input shapefile (in meters).

    Returns:
        None
    """
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
        / "osmclip"
        / f"year={year}"
        / "geography=state"
        / f"state={state}"
    )
    output_file = output_dir / f"{state}.geojson"

    output_dir.mkdir(parents=True, exist_ok=True)

    gdf = load_shapefile(tiger_file)
    gdf = gdf[gdf["geoid"] == state]
    gdf = gdf[["geometry"]]
    if buffer:
        gdf.to_crs(crs="EPSG:5071", inplace=True)
        gdf["geometry"] = gdf["geometry"].buffer(distance=buffer)
    gdf.to_crs(crs="EPSG:4326", inplace=True)
    gdf.to_file(output_file, driver="GeoJSON")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--state", required=True, type=str)
    parser.add_argument("--buffer", required=False, type=int)
    args = parser.parse_args()

    create_osmclip(args.year, args.state, args.buffer)
