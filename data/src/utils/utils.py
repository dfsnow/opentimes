import hashlib
import math
import os
from pathlib import Path

import duckdb
import pandas as pd
import yaml


def create_duckdb_connection() -> duckdb.DuckDBPyConnection:
    params = yaml.safe_load(open("params.yaml"))
    os.environ["AWS_PROFILE"] = params["s3"]["profile"]

    con = duckdb.connect(database=":memory:")
    for ext in ["parquet", "httpfs", "aws"]:
        con.install_extension(ext)
        con.load_extension(ext)
    # Num threads is 4x available to support faster remote queries. See:
    # https://duckdb.org/docs/guides/performance/how_to_tune_workloads.html#querying-remote-files
    con.execute("SET threads=16;")
    con.execute(
        f"""
        CREATE SECRET (
            TYPE R2,
            PROVIDER CREDENTIAL_CHAIN,
            ACCOUNT_ID '{params["s3"]["account_id"]}'
        );
        """
    )

    return con


def format_size(size):
    """Return a human-readable size string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024


def format_time(seconds):
    """Return a human-readable time string."""
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)}H {int(minutes)}M {int(seconds)}s"


def get_md5_hash(file_path):
    """Return the MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def split_file_to_str(file: str | Path, **kwargs) -> str:
    """
    Splits the contents of a Parquet file into index strings.

    Args:
        file: The path to the Parquet file.
        **kwargs: Additional keyword arguments passed to the
            split_range function.

    Returns:
        A string representation of the chunked ranges in
            the format '["start-end", ...]'.
    """
    origins_df = pd.read_parquet(file)

    chunk_idx = split_range(len(origins_df), **kwargs)
    zfill_size = len(str(chunk_idx[-1][1]))
    chunk_str = [
        f"{str(start).zfill(zfill_size)}-{str(end).zfill(zfill_size)}"
        for start, end in chunk_idx
    ]
    chunk_out = '["' + '", "'.join(chunk_str) + '"]'

    return chunk_out


def split_range(
    n: int, n_chunks: int = 256, min_chunk_size: int = 5
) -> list[tuple]:
    """
    Splits a range of integers into smaller chunks.

    Args:
        n: The total number of elements in the range.
        n_chunks: The maximum number of chunks. Defaults to 256.
        min_chunk_size: The minimum size of each chunk. Defaults to 5.

    Returns:
        A list of tuples, where each tuple represents
            the start and end indices of a chunk.
    """
    chunk_ranges = []
    if n > n_chunks * min_chunk_size:
        chunk_size = math.ceil(n / n_chunks)
        for i in range(n // chunk_size):
            start = i * chunk_size
            end = ((i + 1) * chunk_size) - 1
            chunk_ranges.append((start, end))
    else:
        n_chunks_small = max(1, n // min_chunk_size)
        for i in range(n_chunks_small):
            start = i * min_chunk_size
            end = ((i + 1) * min_chunk_size) - 1
            if end >= n:
                end = n - 1
            chunk_ranges.append((start, end))

    if chunk_ranges[-1][1] < n:
        start, _ = chunk_ranges[-1]
        chunk_ranges[-1] = (start, n - 1)

    return chunk_ranges
