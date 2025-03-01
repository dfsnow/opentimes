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
    "state": {"type": "national", "name": "state"},
    "county": {"type": "national", "name": "county"},
    "zcta": {"type": "national", "name": "zcta520"},
    "county_subdivision": {"type": "state", "name": "cousub"},
    "tract": {"type": "state", "name": "tract"},
    "block_group": {"type": "state", "name": "bg"},
}


def download_and_load_shapefile(
    year: str, geography: str, state: str | None, temp_dir: Path
):
    if not state:
        geo_prefix = "_us_"
    else:
        geo_prefix = f"_{state}_"

    remote_file_name = (
        f"cb_{year}{geo_prefix}{TIGER_GEO_NAMES[geography]['name']}_500k.zip"
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
    if TIGER_GEO_NAMES[geography]["type"] == "national":
        states = [None]
    else:
        states = params["input"]["state"]

    output_dir = (
        Path.cwd() / "input" / "cb" / f"year={year}" / f"geography={geography}"
    )
    output_file = output_dir / f"{geography}.geojson"
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
    gdf_concat = gdf_concat.to_crs(epsg=4326)
    gdf_concat = gdf_concat[["geoid", "geometry"]].rename(
        columns={"geoid": "id"}
    )
    gdf_concat.to_file(output_file, layer="geometry")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=False, type=str)
    args = parser.parse_args()
    fetch_cb_shapefile(year=args.year, geography=args.geography)


if __name__ == "__main__":
    main()
