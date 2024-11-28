import argparse
from pathlib import Path

import requests as r
from utils.logging import create_logger

logger = create_logger(__name__)

# Base URL for TIGER/Line shapefiles
BASE_URL = "https://www2.census.gov/geo/tiger/TIGER"

# Dictionary translation of common geography names to TIGER equivalents
TIGER_GEO_NAMES = {
    "block": "tabblock20",
    "block_group": "bg",
    "county": "county",
    "county_subdivision": "cousub",
    "place": "place",
    "puma": "puma20",
    "state": "state",
    "tract": "tract",
    "zcta": "zcta520",
}


def fetch_shapefile(
    year: str, geography: str, state: str | None = None
) -> None:
    """
    Fetches a TIGER/Line shapefile and saves it to a directory.

    The output directory is partitioned by year, geography, and optionally state.

    Args:
        year: The year of the TIGER/Line data.
        geography: The geography type of the shapefile.
        state: The two-digit state FIPS code for the shapefile.
    """
    base_dir = (
        Path.cwd()
        / "input"
        / "tiger"
        / f"year={year}"
        / f"geography={geography}"
    )
    output_dir = base_dir / f"state={state}" if state else base_dir
    output_file = output_dir / f"{state if state else geography}.zip"
    output_dir.mkdir(parents=True, exist_ok=True)

    tiger_geo_name = TIGER_GEO_NAMES[geography]
    file_prefix = f"tl_{year}_{'us' if not state else state}"
    remote_file_name = f"{file_prefix}_{tiger_geo_name}.zip"
    url = f"{BASE_URL}{year}/{tiger_geo_name.upper()}/{remote_file_name}"

    try:
        response = r.get(url, stream=True)
        response.raise_for_status()

        with open(output_file, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logger.info(f"File downloaded successfully: {output_file}")

    except r.exceptions.RequestException as e:
        logger.error(f"Failed to download file: {e}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=True, type=str)
    parser.add_argument("--state", required=False, type=str)
    args = parser.parse_args()

    fetch_shapefile(year=args.year, geography=args.geography, state=args.state)


if __name__ == "__main__":
    main()
