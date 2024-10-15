import math

def split_range(n: int, n_chunks: int = 256, min_chunk_size = 5) -> list[tuple]:
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
        n_chunks_small = n // min_chunk_size
        for i in range(n_chunks_small):
            start = i * min_chunk_size
            end = ((i + 1) * min_chunk_size) - 1
            chunk_ranges.append((start, end))

    if chunk_ranges[-1][1] < n:
        start, _ = chunk_ranges[-1]
        chunk_ranges[-1] = (start, n - 1)

    return(chunk_ranges)

