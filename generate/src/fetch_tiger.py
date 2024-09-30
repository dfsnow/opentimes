import argparse
from pathlib import Path

import requests as r

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
    Fetches a TIGER/Line shapefile and saves it to a directory partitioned by
    year, geography, and (optionally) state.

    :param year: The year of the TIGER/Line data.
    :param geography: The geography type of the shapefile.
    :param state: (Optional) The two-digit state code for the shapefile.
    """

    output_dir = (
        Path.cwd()
        / "input"
        / "tiger"
        / f"year={year}"
        / f"geography={geography}"
    )
    if not state:
        output_file = output_dir / f"{geography}.zip"
    else:
        output_dir = output_dir / f"state={state}"
        output_file = output_dir / f"{state}.zip"

    output_dir.mkdir(parents=True, exist_ok=True)

    # Construct the URL to fetch TIGER data
    tiger_geo_name = TIGER_GEO_NAMES[geography]
    if not state:
        remote_file_name = f"tl_{year}_us_{tiger_geo_name}.zip"
    else:
        remote_file_name = f"tl_{year}_{state}_{tiger_geo_name}.zip"
    url = f"{BASE_URL}{year}/{tiger_geo_name.upper()}/{remote_file_name}"
    try:
        response = r.get(url, stream=True)
        response.raise_for_status()

        with open(output_file, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"File downloaded successfully: {output_file}")
    except r.exceptions.RequestException as e:
        print(f"Failed to download file: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch TIGER/Line shapefiles."
    )
    parser.add_argument(
        "--year", required=True, help="The year of the TIGER/Line data."
    )
    parser.add_argument(
        "--geography",
        required=True,
        help="The geography type of the shapefile.",
    )
    parser.add_argument(
        "--state",
        required=False,
        help="The two-digit state code for the shapefile.",
    )

    args = parser.parse_args()

    fetch_shapefile(args.year, args.geography, args.state)
