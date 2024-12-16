import hashlib
import math
import os
import sys
from contextlib import contextmanager
from copy import deepcopy
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
    df["distance_km"] = pd.Series([], dtype=float)
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


def group_by_column_sets(
    df: pd.DataFrame, x: str, y: str
) -> list[pd.DataFrame]:
    """Find the unique sets of two columns and return them as a list."""
    grouped = df.groupby(x)[y].apply(set).reset_index()
    unique_sets = grouped[y].drop_duplicates()

    result = []
    for unique_set in unique_sets:
        group = df[df[x].isin(grouped[grouped[y] == unique_set][x])]
        group = group.drop_duplicates()
        group = group.reset_index(drop=True)
        result.append(group)
    return result


def merge_overlapping_df_list(
    df_list: list[pd.DataFrame],
    overlap_threshold: float = 0.5,
) -> list[pd.DataFrame]:
    """
    Merge a list of DataFrames that have overlapping values in their columns.
    Merges "up into" the largest DataFrame first. This is used to reduce the
    number of perfectly unique sets produced by group_by_column_sets().
    """

    def overlap_percentage(df1, df2, col):
        overlap = pd.merge(df1[[col]], df2[[col]], how="inner", on=col)
        return len(set(overlap[col])) / min(len(df1[col]), len(df2[col]))

    # Copy the input so we don't modify it
    df_list_c = deepcopy(df_list)

    # Merge into largest dataframes first
    df_list_c.sort(key=len, reverse=True)

    merged_dfs = []
    while df_list_c:
        base_df = df_list_c.pop(0)
        merged = base_df
        to_merge = []
        for df in df_list_c:
            for col in df.columns:
                if overlap_percentage(base_df, df, col) >= overlap_threshold:
                    to_merge.append((df, col))
                    break
        for df, col in to_merge:
            # Remove the dataframe from the main list if it's been merged
            for i in range(len(df_list_c) - 1, -1, -1):
                if df_list_c[i][df_list_c[i].columns].equals(df[df.columns]):
                    df_list_c.pop(i)
            merged = (
                pd.concat([merged, df])
                .drop_duplicates()
                .reset_index(drop=True)
            )
        merged_dfs.append(merged)
    return merged_dfs


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
