import hashlib

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
