import argparse
import re

import yaml
from utils.inventory import create_duckdb_connection


def create_public_files(version: str, mode: str, year: str, dataset: str) -> None:
    """
    Janky function to pull data from the S3 output bucket and repartition it
    into files that live on a public-facing bucket. Goal is to consolidate
    many small files into a deterministic set of Parquet and CSV files.

    Args:
        version: Comma-separated version of the data.
        mode: Comma-separated travel mode.
        year: Comma-separated year of the data.
        dataset: Comma-separated dataset name.

    Returns:
        None
    """
    params = yaml.safe_load(open("params.yaml"))
    con = create_duckdb_connection()

    # Dictionary of datasets and their respective number of partition folders
    dataset_dict = {
        "times": 6,
        "points": 7,
        "missing_pairs": 6,
        "metadata": 6
    }

    # Split the comma-separated input strings and check that they're valid
    semver_pattern = re.compile(r'^\d+\.\d+\.\d+$')
    version_list = [v.strip() for v in version.split(',')]
    mode_list = [m.strip() for m in mode.split(',')]
    year_list = [y.strip() for y in year.split(',')]
    dataset_list = [d.strip() for d in dataset.split(',')]

    if not all(semver_pattern.match(v) for v in version_list):
        raise ValueError("All input versions must be in semver format (e.g., 0.0.1)")
    if not all(m in params["times"]["mode"] for m in mode_list):
        raise ValueError(f"Input modes must be one of: {', '.join(params['times']['mode'])}")
    if not all(y in params["input"]["year"] for y in year_list):
        raise ValueError(f"Input years must be one of: {', '.join(params["input"]["year"])}")
    datasets = list(dataset_dict.keys())
    if not all(d in datasets for d in dataset_list):
        raise ValueError(f"Input datasets must be one of: {', '.join(datasets)}")

    for v in version_list:
        for m in mode_list:
            for y in year_list:
                for d in dataset_list:
                    filename = f"{d}-{v}-{m}-{y}.parquet"
                    partitions = "/*" * dataset_dict[d]
                    print("Creating file:", filename)

                    con.sql(
                        f"""
                        COPY (
                            SELECT *
                            FROM read_parquet(
                                'r2://{params['s3']['data_bucket']}/{d}{partitions}/*.parquet',
                                hive_partitioning = true,
                                hive_types_autocast = 0
                            )
                            WHERE version = '{v}'
                                AND mode = '{m}'
                                AND year = '{y}'
                        )
                        TO 'r2://{params['s3']['public_bucket']}/{d}/{v}/{m}/{y}/{filename}'
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
    parser.add_argument("--dataset", required=True, type=str)
    args = parser.parse_args()

    create_public_files(args.version, args.mode, args.year, args.dataset)
