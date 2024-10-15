import argparse
from pathlib import Path

from utils.inventory import split_file_to_str


def split_origin(
    year: str, geography: str, state: str, n_chunks: int = 256
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

    file_chunks = split_file_to_str(origins_file, n_chunks)

    print(file_chunks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=True, type=str)
    parser.add_argument("--state", required=True, type=str)
    parser.add_argument("--n_chunks", required=False, type=int, default=256)
    args = parser.parse_args()

    split_origin(args.year, args.geography, args.state, args.n_chunks)
