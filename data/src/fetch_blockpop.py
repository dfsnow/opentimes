import argparse
import os
from pathlib import Path

import pyarrow
import pandas as pd
import pyarrow.parquet as pq
import requests as r
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.census.gov/data/"
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")


def fetch_blockpop(year: str, state: str) -> None:
    """
    Fetches block-level population data from the Census API. Pulls from the
    PL 94-171 dataset, which is used for redistricting.

    :param year: The year of the decennial Census.
    :param state: The two-digit state code.
    """

    output_dir = (
        Path.cwd()
        / "input"
        / "blockpop"
        / f"year={year}"
        / f"state={state}"
    )
    output_file = output_dir / f"{state}.parquet"

    output_dir.mkdir(parents=True, exist_ok=True)

    # Construct the URL to fetch block population data
    pop_var = "P001001" if year == "2010" else "P1_001N"
    url = BASE_URL + (
        f"{year}/dec/pl?get={pop_var}&for=block:*&in=state:"
        f"{state}&in=county:*&in=tract:*&key={CENSUS_API_KEY}"
    )

    try:
        response = r.get(url, stream=True)
        response.raise_for_status()

        data = pd.DataFrame(
            response.json()[1:],
            columns=["population", "state", "county", "tract", "block"]
        )
        data["population"] = data["population"].astype("int32")
        data["state"] = data["state"].astype("string")

        schema = pyarrow.schema([
            ("population", pyarrow.int32()),
            ("state", pyarrow.string()),
            ("county", pyarrow.string()),
            ("tract", pyarrow.string()),
            ("block", pyarrow.string())
        ])

        breakpoint()

        table = pyarrow.Table.from_pandas(data, schema=schema)
        pq.write_table(table, output_file)

        print(f"File downloaded successfully: {output_file}")
    except r.exceptions.RequestException as e:
        print(f"Failed to download file: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch decennial Census block populations"
    )
    parser.add_argument(
        "--year",
        required=True,
        choices=["2010", "2020"],
        help="The year of the decennial Census."
    )
    parser.add_argument(
        "--state",
        required=False,
        help="The two-digit state code.",
    )

    args = parser.parse_args()

    fetch_blockpop(args.year, args.state)
