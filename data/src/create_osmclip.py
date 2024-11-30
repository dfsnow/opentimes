import argparse
from pathlib import Path

import geopandas as gpd
from shapely.geometry import box
from utils.census import load_shapefile
from utils.logging import create_logger

logger = create_logger(__name__)


def create_osmclip(year: str, state: str, buffer: int = 0) -> None:
    """
    Convert a TIGER/Line shapefile to a buffered GeoJSON used as a clipping
    boundary for the OpenStreetMap road network.

    Args:
        year: The year of the TIGER/Line data.
        state: The two-digit state FIPS code for the shapefile.
        buffer: The amount to buffer the input shapefile (in meters).
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
    logger.info(f"Loaded state boundary for {state}")
    if buffer:
        gdf = gdf.to_crs(crs="EPSG:5071")
        gdf["geometry"] = gdf["geometry"].buffer(distance=buffer)
        logger.info(f"Buffered state boundary by {buffer} meters")

    # Clip to a large bbox surrounding the U.S. to prevent wrapping dateline
    bbox = box(-177.0, -32, -16.0, 70.0)
    bbox_gdf = gpd.GeoDataFrame({"geometry": [bbox]}, crs="EPSG:4326")
    gdf.reset_index(drop=True, inplace=True)
    gdf = gdf.intersection(bbox_gdf.to_crs(crs="EPSG:5071"), align=True)
    logger.info("Clipped state boundary to U.S. bounding box")

    gdf = gdf.to_crs(crs="EPSG:4326")
    gdf.to_file(output_file, driver="GeoJSON")
    logger.info(f"Wrote to: {output_file}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--state", required=True, type=str)
    parser.add_argument("--buffer", required=False, type=int)
    args = parser.parse_args()
    create_osmclip(args.year, args.state, args.buffer)


if __name__ == "__main__":
    main()
