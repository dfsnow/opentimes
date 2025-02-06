import argparse
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests as r
from utils.census import load_shapefile
from utils.constants import TIGER_BASE_URL
from utils.logging import create_logger

logger = create_logger(__name__)


def download_and_load_shapefile(geoid: str, year: str, temp_dir: Path):
    remote_file_name = f"tl_{year}_{geoid}_areawater.zip"
    url = f"{TIGER_BASE_URL}{year}/AREAWATER/{remote_file_name}"
    temp_file = Path(temp_dir) / remote_file_name

    try:
        response = r.get(url, stream=True)
        response.raise_for_status()

        with open(temp_file, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        gdf = load_shapefile(temp_file)
        logger.info(f"File downloaded successfully: {url}")
        return gdf

    except r.exceptions.RequestException as e:
        logger.error(f"Failed to download file: {e}")
        return None


def fetch_water_shapefile(
    year: str, state: str | None = None, area_threshold: float = 0.99
) -> None:
    """
    Fetch all TIGER/Line area water files in a state and combine them.

    The output directory is partitioned by year, geography, and optionally state.

    Args:
        year: The year of the TIGER/Line data.
        state: The two-digit state FIPS code for the shapefile.
        area_threshold: The percentile rank cutoff of water areas to use
            in the erase operation, ranked by size.
    """
    county_file = (
        Path.cwd()
        / "input"
        / "tiger"
        / f"year={year}"
        / "geography=county"
        / "county.zip"
    )
    output_dir = (
        Path.cwd() / "input" / "tigerwater" / f"year={year}" / f"state={state}"
    )
    output_file = output_dir / f"{state}.geojson"
    output_dir.mkdir(parents=True, exist_ok=True)

    county_gdf = load_shapefile(county_file)
    county_gdf = county_gdf[county_gdf["statefp"] == state]
    geoids = county_gdf["geoid"].tolist()

    water_gdfs = []
    with tempfile.TemporaryDirectory() as temp_dir:
        with ThreadPoolExecutor() as executor:
            future_to_geoid = {
                executor.submit(
                    download_and_load_shapefile, geoid, year, Path(temp_dir)
                ): geoid
                for geoid in geoids
            }
            for future in as_completed(future_to_geoid):
                gdf = future.result()
                if gdf is not None:
                    water_gdfs.append(gdf)

    gdf_concat = pd.concat(water_gdfs, ignore_index=True)
    gdf_concat["water_rank"] = gdf_concat["awater"].rank(pct=True)
    gdf_filtered = gdf_concat[gdf_concat["water_rank"] >= area_threshold]
    gdf_filtered = gdf_filtered[["awater", "geometry"]]
    gdf_filtered.to_file(output_file)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--state", required=False, type=str)
    args = parser.parse_args()
    fetch_water_shapefile(year=args.year, state=args.state)


if __name__ == "__main__":
    main()
