import argparse
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests as r
import yaml
from utils.census import load_shapefile
from utils.constants import TIGER_BASE_URL
from utils.logging import create_logger

logger = create_logger(__name__)

with open("params.yaml") as file:
    params = yaml.safe_load(file)

# Translate geography names to their TIGER equivalents
TIGER_GEO_NAMES = {
    "county": "county",
    "county_subdivision": "cousub",
    "tract": "tract",
    "block_group": "bg",
}


def download_and_load_shapefile(
    year: str, geography: str, state: str, temp_dir: Path
):
    remote_file_name = (
        f"cb_{year}_{state}_{TIGER_GEO_NAMES[geography]}_500k.zip"
    )
    url = f"{TIGER_BASE_URL}GENZ{year}/shp/{remote_file_name}"
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


def fetch_cb_shapefile(
    year: str,
    geography: str,
    temp_dir: str = tempfile.gettempdir(),
) -> None:
    """
    Fetch TIGER/Line cartographic boundary shapefiles for a
    given year, geography, and state.

    Args:
        year: The year of the TIGER/Line data.
        geography: The Census geography type of the shapefile.
    """
    version = params["times"]["version"]
    states = params["input"]["state"]

    output_dir = (
        Path.cwd()
        / "output"
        / "tiles"
        / f"version={version}"
        / f"year={year}"
        / f"geography={geography}"
    )
    output_file = output_dir / f"tiles-{version}-{year}-{geography}.geojson"
    output_dir.mkdir(parents=True, exist_ok=True)

    gdf_list = []
    with tempfile.TemporaryDirectory() as temp_dir:
        with ThreadPoolExecutor() as executor:
            future_to_geoid = {
                executor.submit(
                    download_and_load_shapefile,
                    year,
                    geography,
                    state,
                    Path(temp_dir),
                ): state
                for state in states
            }
            for future in as_completed(future_to_geoid):
                gdf = future.result()
                if gdf is not None:
                    gdf_list.append(gdf)

    gdf_concat = pd.concat(gdf_list, ignore_index=True)
    gdf_concat.to_file(output_file)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=False, type=str)
    args = parser.parse_args()
    fetch_cb_shapefile(year=args.year, geography=args.geography)


if __name__ == "__main__":
    main()
