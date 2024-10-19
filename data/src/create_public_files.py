import argparse

import yaml
from utils.inventory import create_duckdb_connection


def create_public_files(version: str, mode: str, year: str) -> None:
    """
    Janky function to pull data from the S3 output bucket and repartition it
    into files that live on a public-facing bucket. Goal is to consolidate
    many small files into a deterministic set of Parquet and CSV files.

    Args:
        version: The version of the data to pull.
        mode: The travel mode to pull.
        year: The year of the data to pull.

    Returns:
        None
    """
    params = yaml.safe_load(open("params.yaml"))
    con = create_duckdb_connection()

    # Dictionary of datasets and their respective number of partition folders
    datasets = {
        "times": 6,
        "points": 7,
        "missing_pairs": 6,
        "metadata": 6
    }

    for dataset in datasets.keys():
        filename = f"{dataset}-{version}-{mode}-{year}.parquet"
        partitions = "/*" * datasets[dataset]
        print("Creating file:", filename)

        con.sql(
            f"""
            COPY (
                SELECT *
                FROM read_parquet(
                    'r2://{params['s3']['data_bucket']}/{dataset}{partitions}/*.parquet',
                    hive_partitioning = true,
                    hive_types_autocast = 0
                )
                WHERE version = '{version}'
                    AND mode = '{mode}'
                    AND year = '{year}'
            )
            TO 'r2://{params['s3']['public_bucket']}/{dataset}/{version}/{mode}/{year}/{filename}'
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
    parser.add_argument("--version", required=True, type=str)
    parser.add_argument("--mode", required=True, type=str)
    parser.add_argument("--year", required=True, type=str)
    args = parser.parse_args()

    create_public_files(args.version, args.mode, args.year)
