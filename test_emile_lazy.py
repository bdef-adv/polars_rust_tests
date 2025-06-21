import polars as pl
import numpy as np
import os
import time



def find_matches_with_lazy_polars(path_a: str, path_b: str):
    """
    Performs the find-first-match logic using Polars LazyFrames for optimized performance.

    Args:
        path_a: File path to the stream A arrow file.
        path_b: File path to the stream B arrow file.

    Returns:
        A Polars DataFrame with 'a_idx' and 'b_idx' of the matches.
    """
    print("--- Starting LazyFrame Polars Implementation ---")
    start_time = time.time()

    # 1. Load data as LazyFrames and add original row indices
    #    LazyFrames defer execution until .collect() is called
    lf_a = pl.scan_ipc(path_a).with_row_index("a_idx")
    lf_b = pl.scan_ipc(path_b).with_row_index("b_idx")

    # 2. Build the complete lazy query chain
    #    All operations are registered but not executed yet
    result_lf = (
        lf_a
        .join(lf_b, on="value", how="inner")  # Join on matching values
        .filter(pl.col("timestamp_right") < pl.col("timestamp"))  # Time condition filter
        .group_by("a_idx", maintain_order=True)  # Group by a_idx
        .agg(pl.first("b_idx"))  # Get first b_idx for each group
        .sort("a_idx")  # Sort results by a_idx
    )

    # 3. Execute the entire optimized query plan with .collect()
    #    Polars will optimize the entire query before execution
    result_df = result_lf.collect()

    end_time = time.time()
    print(f"LazyFrame processing finished in {end_time - start_time:.4f} seconds.")

    return result_df

def find_matches_with_streaming(path_a: str, path_b: str):
    """
    Enhanced version using streaming execution for even better memory efficiency
    with very large datasets.

    Args:
        path_a: File path to the stream A arrow file.
        path_b: File path to the stream B arrow file.

    Returns:
        A Polars DataFrame with 'a_idx' and 'b_idx' of the matches.
    """
    print("--- Starting Streaming LazyFrame Implementation ---")
    start_time = time.time()

    # Build the lazy query with streaming capabilities
    result_lf = (
        pl.scan_ipc(path_a).with_row_index("a_idx")
        .join(pl.scan_ipc(path_b).with_row_index("b_idx"), on="value", how="inner")
        .filter(pl.col("timestamp_right") < pl.col("timestamp"))
        .group_by("a_idx", maintain_order=True)
        .agg(pl.first("b_idx"))
        .sort("a_idx")
    )

    # Execute with streaming for memory efficiency
    # streaming=True processes data in chunks, reducing memory usage
    result_df = result_lf.collect(engine="streaming")

    end_time = time.time()
    print(f"Streaming processing finished in {end_time - start_time:.4f} seconds.")

    return result_df

def find_matches_with_gpu(path_a: str, path_b: str):
    """
    Enhanced version using streaming execution for even better memory efficiency
    with very large datasets.

    Args:
        path_a: File path to the stream A arrow file.
        path_b: File path to the stream B arrow file.

    Returns:
        A Polars DataFrame with 'a_idx' and 'b_idx' of the matches.
    """
    print("--- Starting GPU LazyFrame Implementation ---")
    start_time = time.time()

    # Build the lazy query with streaming capabilities
    result_lf = (
        pl.scan_ipc(path_a).with_row_index("a_idx")
        .join(pl.scan_ipc(path_b).with_row_index("b_idx"), on="value", how="inner")
        .filter(pl.col("timestamp_right") < pl.col("timestamp"))
        .group_by("a_idx", maintain_order=True)
        .agg(pl.first("b_idx"))
        .sort("a_idx")
    )

    # Execute with streaming for memory efficiency
    # streaming=True processes data in chunks, reducing memory usage
    result_df = result_lf.collect(engine="gpu")

    end_time = time.time()
    print(f"GPU processing finished in {end_time - start_time:.4f} seconds.")

    return result_df


def compare_implementations(path_a: str, path_b: str):
    """
    Compare the performance of different implementations.
    """
    print("=== Performance Comparison ===\n")

    # Original DataFrame implementation (for reference)
    def original_implementation():
        start_time = time.time()

        df_a = pl.read_ipc(path_a).with_row_index("a_idx")
        df_b = pl.read_ipc(path_b).with_row_index("b_idx")

        joined_df = df_a.join(df_b, on="value", how="inner")
        filtered_df = joined_df.filter(pl.col("timestamp_right") < pl.col("timestamp"))
        result_df = (
            filtered_df.group_by("a_idx", maintain_order=True)
            .agg(pl.first("b_idx"))
            .sort("a_idx")
        )

        end_time = time.time()
        print(f"Original DataFrame: {end_time - start_time:.4f} seconds")
        return result_df, end_time - start_time

    # Run comparisons
    original_result, original_time = original_implementation()
    lazy_result = find_matches_with_lazy_polars(path_a, path_b)
    streaming_result = find_matches_with_streaming(path_a, path_b)
    gpu_result = find_matches_with_gpu(path_a, path_b)

    # Verify results are identical
    assert original_result.equals(lazy_result), "LazyFrame results differ from original!"
    assert original_result.equals(streaming_result), "Streaming results differ from original!"
    assert original_result.equals(gpu_result), "GPU results differ from original!"

    print(f"\n✓ All implementations produce identical results")
    print(f"Found {len(original_result):,} matches total")

if __name__ == '__main__':
    # --- Configuration ---
    DATA_SIZE_A = 1_000_000
    DATA_SIZE_B = 1_000_000

    arrow_file_a = 'stream_a.arrow'
    arrow_file_b = 'stream_b.arrow'

    # --- Data Generation (using LazyFrames for consistency) ---
    print("Generating mock data...")

    # Generate data and write to files
    df_a_gen = pl.DataFrame({
        'timestamp': np.sort(np.random.randint(1000, 500000, size=DATA_SIZE_A)),
        'value': np.random.randint(0, 2000, size=DATA_SIZE_A).astype(np.int16)
    })
    df_a_gen.write_ipc(arrow_file_a)
    df_a_gen = None

    df_b_gen = pl.DataFrame({
        'timestamp': np.sort(np.random.randint(0, 400000, size=DATA_SIZE_B)),
        'value': np.random.randint(0, 2000, size=DATA_SIZE_B).astype(np.int16)
    })
    df_b_gen.write_ipc(arrow_file_b)
    df_a_gen = None
    print("Mock data generated.\n")

    # --- Run optimized implementations ---

    # Method 1: Basic LazyFrame optimization
    lazy_results = find_matches_with_lazy_polars(arrow_file_a, arrow_file_b)
    print(f"\nLazyFrame found {len(lazy_results):,} matches.")
    print("\n--- First 10 Matches (LazyFrame) ---")
    print(lazy_results.head(10))

    print("\n" + "="*50 + "\n")

    # Method 2: Streaming LazyFrame (for large datasets)
    streaming_results = find_matches_with_streaming(arrow_file_a, arrow_file_b)
    print(f"\nStreaming found {len(streaming_results):,} matches.")
    print("\n--- First 10 Matches (Streaming) ---")
    print(streaming_results.head(10))

    print("\n" + "="*50 + "\n")

    # Method 3: GPU LazyFrame (for large datasets)
    gpu_results = find_matches_with_gpu(arrow_file_a, arrow_file_b)
    print(f"\GPU found {len(gpu_results):,} matches.")
    print("\n--- First 10 Matches (Streaming) ---")
    print(gpu_results.head(10))

    print("\n" + "="*50 + "\n")

    # Method 3: Performance comparison
    compare_implementations(arrow_file_a, arrow_file_b)

    # Cleanup
    os.remove(arrow_file_a)
    os.remove(arrow_file_b)
    print("\nTemporary files cleaned up.")
