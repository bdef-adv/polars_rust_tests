import polars as pl
import numpy as np
import os
import time

def find_matches_with_polars(path_a: str, path_b: str):
    """
    Performs the find-first-match logic purely using the Polars library.

    Args:
        path_a: File path to the stream A arrow file.
        path_b: File path to the stream B arrow file.

    Returns:
        A Polars DataFrame with 'a_idx' and 'b_idx' of the matches.
    """
    print("--- Starting Polars Implementation ---")
    start_time = time.time()

    # 1. Load data and add original row indices. This is crucial for the final output.
    df_a = pl.read_ipc(path_a).with_row_index("a_idx")
    df_b = pl.read_ipc(path_b).with_row_index("b_idx")

    # 2. Join the two DataFrames on the 'value' column.
    #    This creates a wide DataFrame with all possible (a, b) pairs
    #    that have a matching value.
    joined_df = df_a.join(df_b, on="value", how="inner")

    # 3. Filter the joined pairs based on the time condition.
    #    We only want pairs where the event in B happened before the event in A.
    #    Polars automatically renames the timestamp from the right frame to 'timestamp_right'.
    filtered_df = joined_df.filter(pl.col("timestamp_right") < pl.col("timestamp"))

    # 4. For each 'a_idx', find the first corresponding 'b_idx'.
    #    Because the original data was sorted by time and Polars' joins are order-preserving,
    #    the first row in each group will be the correct one.
    result_df = (
        filtered_df.group_by("a_idx", maintain_order=True)
        .agg(pl.first("b_idx"))
        .sort("a_idx")
    )

    end_time = time.time()
    print(f"Polars processing finished in {end_time - start_time:.4f} seconds.")

    return result_df

if __name__ == '__main__':
    # --- Configuration ---
    DATA_SIZE_A = 1_000_000
    DATA_SIZE_B = 1_000_000

    arrow_file_a = 'stream_a.arrow'
    arrow_file_b = 'stream_b.arrow'

    # --- Data Generation (if necessary) ---
    df_a_gen = pl.DataFrame({
        'timestamp': np.sort(np.random.randint(1000, 500000, size=DATA_SIZE_A)),
        'value': np.random.randint(0, 2000, size=DATA_SIZE_A).astype(np.int16)
    })
    df_a_gen.write_ipc(arrow_file_a)

    df_b_gen = pl.DataFrame({
        'timestamp': np.sort(np.random.randint(0, 400000, size=DATA_SIZE_B)),
        'value': np.random.randint(0, 2000, size=DATA_SIZE_B).astype(np.int16)
    })
    df_b_gen.write_ipc(arrow_file_b)
    print("Mock data generated.")

    # --- Run the Polars implementation ---
    polars_results = find_matches_with_polars(arrow_file_a, arrow_file_b)

    print(f"\nFound {len(polars_results):,} matches.")
    print("\n--- First 10 Matches Found by Polars ---")
    print(polars_results.head(10))
