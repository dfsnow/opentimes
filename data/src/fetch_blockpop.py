import argparse
import os
from pathlib import Path

import pandas as pd
import requests as r
from utils.logging import create_logger

logger = create_logger(__name__)

BASE_URL = "https://api.census.gov/data/"
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")


def fetch_blockpop(year: str, state: str) -> None:
    """
    Fetch block-level population data from the Census API. Pulls from the
    PL 94-171 dataset, which is used for redistricting.

    Args:
        year: The year of the decennial Census.
        state: The two-digit state FIPS code.
    """
    output_dir = (
        Path.cwd() / "input" / "blockpop" / f"year={year}" / f"state={state}"
    )
    output_file = output_dir / f"{state}.parquet"
    output_dir.mkdir(parents=True, exist_ok=True)

    pop_var = "P001001" if year == "2010" else "P1_001N"
    url = BASE_URL + (
        f"{year}/dec/pl?get={pop_var}&for=block:*&in=state:"
        f"{state}&in=county:*&in=tract:*&key={CENSUS_API_KEY}"
    )

    try:
        response = r.get(url, stream=True)
        response.raise_for_status()

        if "Invalid Key" in response.text:
            raise ValueError("Invalid Census API key provided")

        data = pd.DataFrame(
            response.json()[1:],
            columns=["population", "state", "county", "tract", "block"],
        )
        data["population"] = data["population"].astype("int32")
        # Drop state column because it already exists as a Hive-partition key
        data.drop(columns=["state"], inplace=True)
        data.to_parquet(output_file, engine="pyarrow", index=False)

        logger.info(f"File downloaded successfully: {output_file}")
    except r.exceptions.RequestException as e:
        logger.error(f"Failed to download file: {e}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--year", required=True, choices=["2010", "2020"], type=str
    )
    parser.add_argument("--state", required=False, type=str)
    args = parser.parse_args()
    fetch_blockpop(args.year, args.state)


if __name__ == "__main__":
    main()
