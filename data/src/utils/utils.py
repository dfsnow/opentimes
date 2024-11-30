import hashlib
import math
import os
import sys
from contextlib import contextmanager
from pathlib import Path

import pandas as pd


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
    Splits the contents of a Parquet file into chunks and return the chunk
    boundaries as JSON. This is used to split a file of origins into smaller
    chunks for parallelization.

    Args:
        file: The path to the Parquet file.
        **kwargs: Additional keyword arguments passed to the
            split_range function.

    Returns:
        A JSON string representing the chunked ranges in
        the format '["start-end", ...]'.
    """
    origins_df = pd.read_parquet(file)
    chunk_idx = split_range(len(origins_df), **kwargs)
    zfill_size = len(str(chunk_idx[-1][1]))
    chunk_str = [
        f"{str(start).zfill(zfill_size)}-{str(end).zfill(zfill_size)}"
        for start, end in chunk_idx
    ]
    return '["' + '", "'.join(chunk_str) + '"]'


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
            end = ((i + 1) * chunk_size) - 1
            chunk_ranges.append((start, end))
    else:
        n_chunks_small = max(1, math.ceil(n / min_chunk_size))
        for i in range(n_chunks_small):
            start = i * min_chunk_size
            end = min((i + 1) * min_chunk_size - 1, n - 1)
            chunk_ranges.append((start, end))

    if chunk_ranges[-1][1] < n:
        start, _ = chunk_ranges[-1]
        chunk_ranges[-1] = (start, n - 1)

    return chunk_ranges


# https://stackoverflow.com/a/17954769
@contextmanager
def suppress_stdout():
    """Redirect stdout to /dev/null. Useful for sinking Valhalla output."""
    fd = sys.stdout.fileno()

    def _redirect_stdout(to):
        sys.stdout.close()
        os.dup2(to.fileno(), fd)
        sys.stdout = os.fdopen(fd, "w")

    with os.fdopen(os.dup(fd), "w") as old_stdout:
        with open(os.devnull, "w") as file:
            _redirect_stdout(to=file)
        try:
            yield
        finally:
            _redirect_stdout(to=old_stdout)
