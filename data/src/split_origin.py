import argparse
from pathlib import Path

import yaml
from utils.utils import split_file_to_str

DOCKER_PATH = Path("/data")
params = yaml.safe_load(open(DOCKER_PATH / "params.yaml"))


def split_origin(
    year: str,
    geography: str,
    state: str,
    n_chunks: int = 256,
    min_chunk_size: int = 5,
) -> None:
    origins_file = (
        Path.cwd()
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


if __name__ == "__main__":
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
        args.n_chunks,
        args.min_chunk_size,
    )
