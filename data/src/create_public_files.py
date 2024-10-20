import argparse
import re

import yaml
from utils.datasets import DATASET_DICT
from utils.utils import create_duckdb_connection


def create_public_files(
    dataset: str, version: str, mode: str, year: str, geography: str
) -> None:
    """
    Janky function to pull data from the S3 output bucket and repartition it
    into files that live on a public-facing bucket. Goal is to consolidate
    many small files into a deterministic set of Parquet and CSV files.

    Args:
        dataset: Dataset name, one of ['times', 'points', 'missing_pairs', 'metadata'].
        version: Version of the data e.g., 0.0.1.
        mode: Travel mode, one of ['walk', 'bicycle', 'car', 'transit'].
        year: Year of the data.
        year: Census geography of the data. See params.yaml for list.

    Returns:
        None
    """
    params = yaml.safe_load(open("params.yaml"))
    con = create_duckdb_connection()

    # Check that the input strings are valid
    datasets = list(DATASET_DICT[version].keys())
    if dataset not in datasets:
        raise ValueError(
            f"Input datasets must be one of: {', '.join(datasets)}"
        )
    semver_pattern = re.compile(r"^\d+\.\d+\.\d+$")
    if not semver_pattern.match(version):
        raise ValueError(
            "Input version must be in semver format (e.g., 0.0.1)"
        )
    if mode not in params["times"]["mode"]:
        raise ValueError(
            f"Input mode must be one of: {', '.join(params['times']['mode'])}"
        )
    if year not in params["input"]["year"]:
        raise ValueError(
            f"Input years must be one of: {', '.join(params['input']['year'])}"
        )
    geographies = params["input"]["census"]["geography"]["all"]
    if geography not in geographies:
        raise ValueError(
            f"Input years must be one of: {', '.join(geographies)}"
        )

    filename = f"{dataset}-{version}-{mode}-{year}-{geography}.parquet"
    partitions = "/*" * DATASET_DICT[version][dataset]["partition_levels"]
    print("Creating file:", filename)

    con.sql(
        f"""
        COPY (
            SELECT
                {', '.join(DATASET_DICT[version][dataset]['public_file_columns'])},
                regexp_extract(filename, 'part-(\\d+-\\d+)\\.parquet', 1) AS chunk_id
            FROM read_parquet(
                'r2://{params['s3']['data_bucket']}/{dataset}{partitions}/*.parquet',
                hive_partitioning = true,
                hive_types_autocast = false,
                filename = true
            )
            WHERE version = '{version}'
                AND mode = '{mode}'
                AND year = '{year}'
                AND geography = '{geography}'
            ORDER BY {', '.join(DATASET_DICT[version][dataset]['public_file_order_by'])}
        )
        TO 'r2://{params['s3']['public_bucket']}/{dataset}/version={version}/mode={mode}/year={year}/geography={geography}/{filename}'
        (
            FORMAT 'parquet',
            COMPRESSION '{params['output']['compression']['type']}',
            COMPRESSION_LEVEL {params['output']['compression']['level']}
        );
        """
    )

    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, type=str)
    parser.add_argument("--version", required=True, type=str)
    parser.add_argument("--mode", required=True, type=str)
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=True, type=str)
    args = parser.parse_args()

    create_public_files(args.dataset, args.version, args.mode, args.year, args.geography)
