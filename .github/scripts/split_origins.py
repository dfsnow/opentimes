import math
import argparse
from pathlib import Path

import pandas as pd

def fetch_origins(
        year: str, geography: str, state: str | None = None,
        n_chunks: int = 256, min_chunk_size = 5
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

    origins = pd.read_parquet(origins_file)
    nrows = len(origins)

    # if 3000 rows, then 256 chunks of size N
    # if 30 rows, then N chunks of size 5
    # 1280
    chunk_ranges = []
    if nrows > n_chunks * min_chunk_size:
        chunk_size = math.ceil(nrows / n_chunks)
        for i in range(nrows // chunk_size):
            start = i * chunk_size
            end = ((i + 1) * chunk_size) - 1
            chunk_ranges.append((start, end))
    else:
        n_chunks_small = nrows // min_chunk_size
        for i in range(n_chunks_small):
            start = i * min_chunk_size
            end = ((i + 1) * min_chunk_size) - 1
            chunk_ranges.append((start, end))

    if chunk_ranges[-1][1] < len(origins):
        start, _ = chunk_ranges[-1]
        chunk_ranges[-1] = (start, len(origins) - 1)

    chunk_strings = [f"{start}-{end}" for start, end in chunk_ranges]
    chunk_string = 'chunks=["' + '", "'.join(chunk_strings) + '"]'

    print(chunk_string)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=str)
    parser.add_argument("--geography", required=True, type=str)
    parser.add_argument("--state", required=False, type=str)
    args = parser.parse_args()

    fetch_origins(year=args.year, geography=args.geography, state=args.state)
