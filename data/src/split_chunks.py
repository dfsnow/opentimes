import argparse
from pathlib import Path

import yaml
from utils.utils import split_od_files_to_json

with open(Path.cwd() / "params.yaml") as file:
    params = yaml.safe_load(file)


def split_chunks(
    year: str,
    geography: str,
    state: str,
    origin_n_chunks: int = 64,
    origin_min_chunk_size: int = 500,
    destination_n_chunks: int = 4,
    destination_min_chunk_size: int = 10000,
) -> None:
    """
    Split Parquet files into N chunks, where each chunk is at least a certain
    size. The multiple of origin_min_chunk_size and destination_min_chunk_size
    is the smallest possible number of OD pairs processed by a single job, but
    more OD pairs get squeezed into each job as origin_n_chunks decreases.

    The maximum number of chunks is equal to
    origin_n_chunks * destination_n_chunks. By default, it's 256, which is
    the maximum number of GitHub jobs per workflow.

    Args:
        year: The year of the input origins data.
        geography: The geography type of the origins data.
        state: The two-digit state FIPS code of the origins data.
        origin_n_chunks: The maximum number of origin chunks. Defaults to 128.
        origin_min_chunk_size: The minimum size of each origin chunk.
            Defaults to 500.
        destination_n_chunks: The maximum number of destination chunks.
            Defaults to 2.
        origin_min_chunk_size: The minimum size of each destination chunk.
            Defaults to 10000.
    """
    origin_file = (
        Path.cwd()
        / "intermediate"
        / "cenloc"
        / f"year={year}"
        / f"geography={geography}"
        / f"state={state}"
        / f"{state}.parquet"
    )
    destination_file = (
        Path.cwd()
        / "intermediate"
        / "destpoint"
        / f"year={year}"
        / f"geography={geography}"
        / f"state={state}"
        / f"{state}.parquet"
    )

    file_chunks = split_od_files_to_json(
        origin_file=origin_file,
        origin_n_chunks=origin_n_chunks,
        origin_min_chunk_size=origin_min_chunk_size,
        destination_file=destination_file,
        destination_n_chunks=destination_n_chunks,
        destination_min_chunk_size=destination_min_chunk_size,
    )

    print(file_chunks)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=True, type=str)
    parser.add_argument("--state", required=True, type=str)
    parser.add_argument(
        "--origin_n_chunks",
        required=False,
        type=str,
        default=params["actions"]["origin_n_chunks"],
    )
    parser.add_argument(
        "--origin_min_chunk_size",
        required=False,
        type=str,
        default=params["actions"]["origin_min_chunk_size"],
    )
    parser.add_argument(
        "--destination_n_chunks",
        required=False,
        type=str,
        default=params["actions"]["destination_n_chunks"],
    )
    parser.add_argument(
        "--destination_min_chunk_size",
        required=False,
        type=str,
        default=params["actions"]["destination_min_chunk_size"],
    )
    args = parser.parse_args()
    split_chunks(
        args.year,
        args.geography,
        args.state,
        int(args.origin_n_chunks),
        int(args.origin_min_chunk_size),
        int(args.destination_n_chunks),
        int(args.destination_min_chunk_size),
    )


if __name__ == "__main__":
    main()
