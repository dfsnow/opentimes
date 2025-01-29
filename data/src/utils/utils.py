import hashlib
import itertools
import math
from pathlib import Path

import pandas as pd


def create_empty_df(
    o_start_idx: int,
    d_start_idx: int,
    o_end_idx: int,
    d_end_idx: int,
    origin_id: pd.Series,
    destination_id: pd.Series,
) -> pd.DataFrame:
    """
    Gets an empty DataFrame with the Cartesian product of the origin and
    destination IDs specified by the indices. Used to return an empty
    DataFrame of IDs when at max depth or unroutable.
    """
    df = pd.merge(
        origin_id.iloc[o_start_idx:o_end_idx].rename("origin_id"),
        destination_id.iloc[d_start_idx:d_end_idx].rename("destination_id"),
        how="cross",
    )
    df["duration_sec"] = pd.Series([], dtype=float)
    return df


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


def split_file_to_str(file: str | Path, **kwargs) -> list[str]:
    """
    Splits the contents of a Parquet file into chunks and return the chunk
    boundaries as a hyphen-separated string. This is used to split a file
    into smaller chunks for parallelization.
    Args:
        file: The path to the Parquet file.
        **kwargs: Additional keyword arguments passed to the
            split_range function.
    Returns:
        A list of hyphen-separated strings representing the chunked ranges in
        the format "start-end".
    """
    df = pd.read_parquet(file)
    chunk_idx = split_range(len(df), **kwargs)
    zfill_size = len(str(chunk_idx[-1][1]))
    chunk_str = [
        f"{str(start).zfill(zfill_size)}-{str(end).zfill(zfill_size)}"
        for start, end in chunk_idx
    ]
    return chunk_str


def split_od_files_to_json(
    origin_file: str | Path,
    origin_n_chunks: int,
    origin_min_chunk_size: int,
    destination_n_chunks: int,
    destination_min_chunk_size: int,
    destination_file: str | Path,
) -> str:
    origin_chunks = split_file_to_str(
        file=origin_file,
        n_chunks=origin_n_chunks,
        min_chunk_size=origin_min_chunk_size,
    )
    destination_chunks = split_file_to_str(
        file=destination_file,
        n_chunks=destination_n_chunks,
        min_chunk_size=destination_min_chunk_size,
    )
    zipped_chunks = [
        f"{origin}_{destination}"
        for origin, destination in list(
            itertools.product(origin_chunks, destination_chunks)
        )
    ]

    return '["' + '", "'.join(zipped_chunks) + '"]'


def split_range(
    n: int, n_chunks: int = 256, min_chunk_size: int = 5
) -> list[tuple]:
    """
    Splits an integer into smaller chunks, where each chunk must be a minimum
    size and there can be no more than N total chunks.
    Args:
        n: The total number of elements in the range.
        n_chunks: The maximum number of chunks. Defaults to 256.
        min_chunk_size: The minimum size of each chunk. Defaults to 5.
    Returns:
        A list of tuples, where each tuple represents the zero-indexed
        start and end indices of a chunk.
    """
    chunk_ranges = []
    if n > n_chunks * min_chunk_size:
        chunk_size = math.ceil(n / n_chunks)
        for i in range(n // chunk_size):
            start = i * chunk_size
            end = (i + 1) * chunk_size
            chunk_ranges.append((start, end))
    else:
        n_chunks_small = max(1, math.ceil(n / min_chunk_size))
        for i in range(n_chunks_small):
            start = i * min_chunk_size
            end = min((i + 1) * min_chunk_size, n)
            chunk_ranges.append((start, end))

    if chunk_ranges[-1][1] < n:
        start, _ = chunk_ranges[-1]
        chunk_ranges[-1] = (start, n)

    return chunk_ranges
