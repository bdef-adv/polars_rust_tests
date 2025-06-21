import json
import polars as pl
import numpy as np
import time
from typing import Iterator


def generate_data_in_chunks(data_size: int, start: int, end: int, chunk_size: int = 100_000) -> Iterator[pl.DataFrame]:
    """
    Generator that yields chunks of data without storing the entire dataset in memory.

    Args:
        data_size: Total number of rows to generate
        chunk_size: Number of rows per chunk

    Yields:
        Polars DataFrame chunks
    """
    remaining = data_size

    while remaining > 0:
        current_chunk_size = min(chunk_size, remaining)

        # Generate timestamps that are naturally sorted across chunks
        timestamps = np.sort(np.random.randint(
            start,
            end,
            size=current_chunk_size
        ))

        chunk_df = pl.DataFrame({
            'timestamp': timestamps,
            'value': np.random.randint(0, 2000, size=current_chunk_size).astype(np.int16)
        })

        yield chunk_df
        remaining -= current_chunk_size


def write_data_streaming_parquet(filepath: str, data_size: int, start: int, end: int, chunk_size: int = 100_000):
    """
    More efficient approach using Parquet format which supports true streaming writes.

    Args:
        filepath: Output file path (should end with .parquet)
        data_size: Total number of rows
        chunk_size: Rows per chunk to keep in memory
    """
    print(f"Generating {data_size:,} rows in chunks of {chunk_size:,} (Parquet)...")

    # Collect all chunks into a list for writing
    chunks = []
    for _i, chunk in enumerate(generate_data_in_chunks(data_size, start, end, chunk_size)):
        chunks.append(chunk)

    # Write all chunks at once - Polars optimizes this internally
    combined_df = pl.concat(chunks)
    combined_df.write_ipc(filepath)

    print(f"Written {len(combined_df):,} rows to {filepath}")


def find_matches(filename_a: str, filename_b: str, engine: str = "auto", current_size: int = None) -> pl.DataFrame:
    print(f"Starting processing with engine={engine}")
    start_time = time.time()

    #print(f"Prepare LazyFrame for {filename_a} ({time.time() - start_time:.4f} seconds)")
    lf_a = (
        pl.scan_ipc(filename_a).with_row_index("a_idx")
            .sort("value")
    )

    #print(f"Prepare LazyFrame for {filename_b} ({time.time() - start_time:.4f} seconds)")
    lf_b = (
        pl.scan_ipc(filename_b).with_row_index("b_idx")
            .with_columns(pl.col("timestamp").alias("timestamp_b"))
            .sort("value")
    )

    #print(f"Preparing query plan for result LazyFrame ({time.time() - start_time:.4f} seconds)")
    result_lf = (
        lf_a
        .join_asof(lf_b, left_on="timestamp", right_on="timestamp_b",
                           by="value", strategy="backward")
        .filter(pl.col("timestamp_b") < pl.col("timestamp"))
        .filter(pl.col("b_idx").is_not_null())  # Add this to remove non-matches
        .group_by("a_idx", maintain_order=True)
        .agg(pl.first("b_idx"))
        .sort("a_idx")
    )

    print(f"Collecting DataFrame from LazyFrame ({time.time() - start_time:.4f} seconds)")
    result_lf.collect(engine=engine)

    end_time = time.time()
    print(f"LazyFrame processing with engine={engine} finished in {end_time - start_time:.4f} seconds with size={current_size}.")

    return end_time - start_time


if __name__ == "__main__":
    FILENAME_A = "data_a.arrow"
    FILENAME_B = "data_b.arrow"
    CHUNK_SIZE = 500_000

    DATA_SIZE_A = 100_000_000
    DATA_SIZE_B = 50_000_000

    INC_SIZE = 50_000_000
    MAX_SIZE = 800_000_000

    write_data_streaming_parquet(FILENAME_A, DATA_SIZE_A, 1000, 500000, CHUNK_SIZE)

    timings = {
        "auto": {},
        "streaming": {},
        "gpu": {}
    }

    for increment in range(0, int(MAX_SIZE/INC_SIZE)):
        data_size_b = DATA_SIZE_B + (increment+1) * INC_SIZE
        filename_b = f"{FILENAME_B}_{data_size_b}"
        write_data_streaming_parquet(filename_b, data_size_b, 0, 4000000, CHUNK_SIZE)

        #find_matches(FILENAME_A, FILENAME_B, "auto", data_size_b)
        timings["streaming"][data_size_b] = find_matches(FILENAME_A, filename_b, "streaming", data_size_b)
        timings["gpu"][data_size_b] = find_matches(FILENAME_A, filename_b, "gpu", data_size_b)

        print(f"\n\n\n{json.dumps(timings, indent=4)}\n\n")
