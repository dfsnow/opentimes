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
    origin_n_chunks: str | None = None,
    origin_min_chunk_size: str | None = None,
    destination_n_chunks: str | None = None,
    destination_min_chunk_size: str | None = None,
) -> None:
    """
    Split Parquet files into N chunks, where each chunk is at least a certain
    size. The multiple of origin_min_chunk_size and destination_min_chunk_size
    is the smallest possible number of OD pairs processed by a single job, but
    more OD pairs get squeezed into each job as origin_n_chunks decreases.

    The maximum number of chunks is equal to
    origin_n_chunks * destination_n_chunks. By default, all chunk settings
    pull from the params.yaml file.

    Args:
        year: The year of the input origins data.
        geography: The geography type of the origins data.
        state: The two-digit state FIPS code of the origins data.
        origin_n_chunks: The maximum number of origin chunks.
        origin_min_chunk_size: The minimum size of each origin chunk.
        destination_n_chunks: The maximum number of destination chunks.
        origin_min_chunk_size: The minimum size of each destination chunk.
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
        origin_n_chunks=int(
            params["actions"]["origin_n_chunks"]
            if not origin_n_chunks
            else origin_n_chunks
        ),
        origin_min_chunk_size=int(
            params["actions"]["origin_min_chunk_size"]
            if not origin_min_chunk_size
            else origin_min_chunk_size
        ),
        destination_file=destination_file,
        destination_n_chunks=int(
            params["actions"]["destination_n_chunks"]
            if not destination_n_chunks
            else destination_n_chunks
        ),
        destination_min_chunk_size=int(
            params["actions"]["destination_min_chunk_size"]
            if not destination_min_chunk_size
            else destination_min_chunk_size
        ),
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
    )
    parser.add_argument(
        "--origin_min_chunk_size",
        required=False,
        type=str,
    )
    parser.add_argument(
        "--destination_n_chunks",
        required=False,
        type=str,
    )
    parser.add_argument(
        "--destination_min_chunk_size",
        required=False,
        type=str,
    )
    args = parser.parse_args()
    split_chunks(
        args.year,
        args.geography,
        args.state,
        args.origin_n_chunks,
        args.origin_min_chunk_size,
        args.destination_n_chunks,
        args.destination_min_chunk_size,
    )


if __name__ == "__main__":
    main()
