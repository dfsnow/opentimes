import os
from pathlib import Path

import duckdb
import yaml


def create_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """
    Instantiate a DuckDB connection to connect to the OpenTimes R2 bucket.
    Loads the necessary extensions to read remote Parquet files and loads
    the R2 credentials chain from the [cloudflare] profile of the AWS
    credentials file.
    """
    with open("params.yaml") as file:
        params = yaml.safe_load(file)

    os.environ["AWS_PROFILE"] = params["s3"]["profile"]

    con = duckdb.connect(database=":memory:")
    for ext in ["parquet", "httpfs", "aws"]:
        con.install_extension(ext)
        con.load_extension(ext)
    # Num threads is 4x available to support faster remote queries. See:
    # https://duckdb.org/docs/guides/performance/how_to_tune_workloads.html#querying-remote-files
    con.execute("SET threads=16;")
    con.execute(
        f"""
        CREATE SECRET (
            TYPE R2,
            PROVIDER CREDENTIAL_CHAIN,
            ACCOUNT_ID '{params["s3"]["account_id"]}'
        );
        """
    )

    return con


def create_duckdb_file(
    tree: dict,
    datasets: list[str],
    version: str,
    modes: list[str],
    years: list[str],
    geographies: list[str],
    bucket_name: str,
    base_url: str,
    path: str,
) -> None:
    """Create a DuckDB database object pointing to all bucket Parquet files."""

    Path(path).unlink(missing_ok=True)
    con = duckdb.connect()
    con.execute("SET autoinstall_known_extensions=1;")
    con.execute("SET autoload_known_extensions=1;")
    con.execute(f"ATTACH '{path}' AS opentimes;")
    con.execute("CREATE SCHEMA IF NOT EXISTS opentimes.public;")
    for dataset in datasets:
        all_dataset_files = []
        for mode in modes:
            for year in years:
                for geography in geographies:
                    dataset_files = [
                        f"{base_url}/{dataset}/{p}"
                        for p in flatten_file_paths(
                            tree, dataset, version, mode, year, geography
                        )
                    ]
                    if not dataset_files:
                        continue

                    all_dataset_files += dataset_files

        if all_dataset_files:
            con.execute(
                f"""
                CREATE OR REPLACE VIEW opentimes.public.{dataset} AS
                SELECT *
                FROM read_parquet(['{"', '".join(all_dataset_files)}'])
                """
            )

    con.close()


def flatten_file_paths(
    tree: dict,
    dataset: str,
    version: str,
    mode: str,
    year: str,
    geography: str,
) -> list[str]:
    """
    Flatten a nested dictionary to return full paths of
    .parquet filenames for a given dataset and version.
    """
    items = []

    def recurse(subtree: dict, parent_key: str = ""):
        for k, v in subtree.items():
            new_key = f"{parent_key}/{k}" if parent_key else k
            if isinstance(v, dict):
                if "filename" in v and v["filename"].endswith(".parquet"):
                    items.append(new_key)
                else:
                    recurse(v, new_key)

    version_str = f"version={version}"
    mode_str = f"mode={mode}"
    year_str = f"year={year}"
    geography_str = f"geography={geography}"
    if (
        dataset in tree
        and version_str in tree[dataset]
        and mode_str in tree[dataset][version_str]
        and year_str in tree[dataset][version_str][mode_str]
        and geography_str in tree[dataset][version_str][mode_str][year_str]
    ):
        recurse(tree[dataset][version_str][mode_str][year_str][geography_str])
    return [
        f"{version_str}/{mode_str}/{year_str}/{geography_str}/{item}"
        for item in items
    ]
