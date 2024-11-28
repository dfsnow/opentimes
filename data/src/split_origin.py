import argparse

import yaml
from utils.constants import DOCKER_INTERNAL_PATH
from utils.utils import split_file_to_str

params = yaml.safe_load(open(DOCKER_INTERNAL_PATH / "params.yaml"))


def split_origin(
    year: str,
    geography: str,
    state: str,
    n_chunks: int = 256,
    min_chunk_size: int = 5,
) -> None:
    """
    Split a Parquet file of origins into N chunks, where each chunk is at least
    a certain size.

    Args:
        year: The year of the input origins data.
        geography: The geography type of the origins data.
        state: The two-digit state FIPS code of the origins data.
        n_chunks: The maximum number of chunks. Defaults to 256.
        min_chunk_size: The minimum size of each chunk. Defaults to 5.
    """
    origins_file = (
        DOCKER_INTERNAL_PATH
        / "intermediate"
        / "cenloc"
        / f"year={year}"
        / f"geography={geography}"
        / f"state={state}"
        / f"{state}.parquet"
    )

    file_chunks = split_file_to_str(
        origins_file, n_chunks=n_chunks, min_chunk_size=min_chunk_size
    )

    print(file_chunks)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=True, type=str)
    parser.add_argument("--state", required=True, type=str)
    parser.add_argument(
        "--n_chunks",
        required=False,
        type=str,
        default=params["actions"]["n_chunks"],
    )
    parser.add_argument(
        "--min_chunk_size",
        required=False,
        type=str,
        default=params["actions"]["min_chunk_size"],
    )
    args = parser.parse_args()
    split_origin(
        args.year,
        args.geography,
        args.state,
        int(args.n_chunks),
        int(args.min_chunk_size),
    )


if __name__ == "__main__":
    main()
