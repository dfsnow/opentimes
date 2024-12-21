import argparse
import os
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import s3fs
import yaml
from utils.constants import DATASET_DICT
from utils.logging import create_logger

BATCH_SIZE = 122_880

logger = create_logger(__name__)


def write_batches_to_files(
    batches,
    filesystem,
    output_loc: str,
    output_file: str,
    max_file_size: int,
    compression: str,
    compression_level: str,
    sorting_columns: list[str],
) -> None:
    file_path = Path()
    file_index = 0
    current_file_size = 0
    writer = None
    accumulated_batches = []
    accumulated_size = 0

    def create_new_writer(file_index, r2):
        file_path = os.path.join(
            output_loc, f"{output_file}-{file_index}.parquet"
        )
        writer = pq.ParquetWriter(
            where=str(file_path),
            filesystem=r2,
            schema=table.schema,
            compression=compression,
            compression_level=compression_level,
            sorting_columns=[pq.SortingColumn(c) for c in sorting_columns],
            use_byte_stream_split=True,
            data_page_version="2.0",
        )
        return writer, file_path

    for batch in batches:
        accumulated_batches.append(batch)
        accumulated_size += batch.num_rows

        if accumulated_size >= BATCH_SIZE:
            table = pa.Table.from_batches(accumulated_batches)
            accumulated_batches = []
            accumulated_size = 0

            if writer is None or current_file_size >= max_file_size:
                if writer is not None:
                    writer.close()
                writer, file_path = create_new_writer(file_index, filesystem)
                current_file_size = 0
                file_index += 1

            writer.write_table(table)
            current_file_size = os.path.getsize(file_path)

    if accumulated_batches:
        table = pa.Table.from_batches(accumulated_batches)
        if writer is None or current_file_size >= max_file_size:
            if writer is not None:
                writer.close()
            writer, file_path = create_new_writer(file_index, filesystem)

        writer.write_table(table)

    if writer is not None:
        writer.close()


def create_public_files(
    dataset: str,
    version: str,
    mode: str,
    year: str,
    geography: str,
    state: str | None,
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
        geography: Census geography of the data. See params.yaml for list.
        state: Two-digit FIPS code of the state.
    """
    with open("params.yaml") as file:
        params = yaml.safe_load(file)
    os.environ["AWS_PROFILE"] = params["s3"]["profile"]

    # Connect to R2 backend using stored S3 credentials
    r2 = s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": params["s3"]["endpoint_url"]},
        s3_additional_kwargs={"ACL": "private"},
    )

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
            f"Input year must be one of: {', '.join(params['input']['year'])}"
        )
    geographies = params["input"]["census"]["geography"]["all"]
    if geography not in geographies:
        raise ValueError(
            f"Input geography must be one of: {', '.join(geographies)}"
        )
    input_states: list = [state] if state else params["input"]["state"]
    known_states = params["input"]["state"]
    if not all(s in known_states for s in input_states):
        raise ValueError(
            f"Input state must be one of: {', '.join(known_states)}"
        )

    for state in input_states:
        filename = f"{dataset}-{version}-{mode}-{year}-{geography}-{state}"
        partition_cols = pa.schema(
            [
                pa.field(c, pa.string())
                for c in DATASET_DICT[version][dataset]["partition_columns"]
            ]
        )

        # Load, filter, and sort the dataset before converting it to record
        # batches for writing to fixed-size files
        df_batches = (
            ds.dataset(
                f"{params['s3']['data_bucket']}/{dataset}",
                partitioning=ds.partitioning(
                    schema=partition_cols, flavor="hive"
                ),
                filesystem=r2,
            )
            .filter(
                (ds.field("version") == version)
                & (ds.field("mode") == mode)
                & (ds.field("year") == year)
                & (ds.field("geography") == geography)
                & (ds.field("state") == state)
            )
            .sort_by(
                [
                    (c, "ascending")
                    for c in DATASET_DICT[version][dataset]["order_by_columns"]
                ]
            )
            .to_batches(
                columns=DATASET_DICT[version][dataset]["public_file_columns"],
                batch_size=BATCH_SIZE,
            )
        )

        write_batches_to_files(
            batches=df_batches,
            filesystem=r2,
            output_loc=(
                f"s3://{params['s3']['public_bucket']}/{dataset}/"
                f"version={version}/mode={mode}/year={year}/"
                f"geography={geography}/state={state}"
            ),
            output_file=filename,
            max_file_size=params["output"]["max_file_size"],
            compression=params["output"]["compression"]["type"],
            compression_level=params["output"]["compression"]["level"],
            sorting_columns=DATASET_DICT[version][dataset]["sorting_columns"],
        )

        logger.info(f"Created file: {filename}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, type=str)
    parser.add_argument("--version", required=True, type=str)
    parser.add_argument("--mode", required=True, type=str)
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=True, type=str)
    parser.add_argument("--state", required=False, type=str)
    args = parser.parse_args()
    create_public_files(
        args.dataset,
        args.version,
        args.mode,
        args.year,
        args.geography,
        args.state,
    )


if __name__ == "__main__":
    main()
