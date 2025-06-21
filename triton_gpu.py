import torch
import triton
import triton.language as tl
import numpy as np
import pandas as pd
import os
import time

# ==============================================================================
# 1. CPU Verification Logic
# ==============================================================================
def verify_with_cpu(df_a: pd.DataFrame, df_b: pd.DataFrame, gpu_results_df: pd.DataFrame):
    """
    Performs the find-first-match logic purely on the CPU using Pandas
    and compares the result against the GPU output.
    """
    print("\n--- Starting CPU Verification ---")
    print("This may take a while depending on the data size...")

    # Reset indices to ensure they align with our a_idx/b_idx logic
    df_a = df_a.reset_index().rename(columns={'index': 'original_a_idx'})
    df_b = df_b.reset_index().rename(columns={'index': 'original_b_idx'})

    cpu_matches = []

    start_time = time.time()

    # Iterate through each row in DataFrame A
    for a_idx, a_row in df_a.iterrows():
        a_val = a_row['value']
        a_time = a_row['timestamp']

        # 1. Filter DataFrame B to find all potential matches
        potential_matches = df_b[
            (df_b['value'] == a_val) & (df_b['timestamp'] < a_time)
        ]

        # 2. If any matches are found, take the first one.
        if not potential_matches.empty:
            first_match = potential_matches.iloc[0]
            b_idx = first_match['original_b_idx']
            cpu_matches.append({'a_idx': a_idx, 'b_idx': int(b_idx)})

    end_time = time.time()
    print(f"CPU processing finished in {end_time - start_time:.2f} seconds.")

    if not cpu_matches:
        if gpu_results_df.empty:
            print("CPU and GPU both found 0 matches. Results are consistent!")
            return
        else:
            raise AssertionError("CPU found no matches, but GPU did!")

    cpu_results_df = pd.DataFrame(cpu_matches).astype({'a_idx': 'int32', 'b_idx': 'int32'})
    gpu_results_for_compare = gpu_results_df[['a_idx', 'b_idx']].astype('int32').reset_index(drop=True)

    print("\n--- Comparing GPU and CPU Results ---")
    try:
        pd.testing.assert_frame_equal(gpu_results_for_compare, cpu_results_df)
        print("✅ SUCCESS: The GPU results perfectly match the CPU results!")
    except AssertionError as e:
        print("❌ FAILURE: The GPU results do NOT match the CPU results.")
        print("Printing detailed error information:")
        print(e)

# ==============================================================================
# 2. High-Performance Triton Kernel
# ==============================================================================
@triton.jit
def find_first_kernel(
    a_values_ptr, a_times_ptr,
    sorted_b_times_ptr, original_b_indices_sorted_ptr,
    lookup_starts_ptr, lookup_counts_ptr,
    output_b_indices_ptr,
    size_a, min_b_val,
):
    a_idx = tl.program_id(0)
    if a_idx >= size_a:
        return
    a_time = tl.load(a_times_ptr + a_idx)
    a_val = tl.load(a_values_ptr + a_idx)
    lookup_table_idx = a_val.to(tl.int32) - min_b_val
    b_group_start = tl.load(lookup_starts_ptr + lookup_table_idx)
    b_group_count = tl.load(lookup_counts_ptr + lookup_table_idx)
    if b_group_count > 0:
        first_b_in_group_time = tl.load(sorted_b_times_ptr + b_group_start)
        if first_b_in_group_time < a_time:
            final_b_idx = tl.load(original_b_indices_sorted_ptr + b_group_start)
            tl.store(output_b_indices_ptr + a_idx, final_b_idx)

# ==============================================================================
# 3. Main Host Script
# ==============================================================================
def main():
    """
    Main execution function.
    """
    # Use smaller sizes for quick tests and verification
    # Use larger sizes for performance benchmarking
    DATA_SIZE_A = 100_000
    DATA_SIZE_B = 1_000_000
    VERIFY_WITH_CPU = True # Set to False for large benchmark runs

    # Safety check for CPU verification
    if VERIFY_WITH_CPU and DATA_SIZE_A > 200_000:
        print("⚠️ WARNING: CPU verification is enabled with a large dataset.")
        print("This will be extremely slow. Set VERIFY_WITH_CPU to False for benchmarks.")
        return

    arrow_file_a = 'stream_a.arrow'
    arrow_file_b = 'stream_b.arrow'

    print("--- Preparing Data ---")
    if not (os.path.exists(arrow_file_a) and os.path.exists(arrow_file_b)):
        print("Generating mock feeds...")
        df_a_gen = pd.DataFrame({
            'timestamp': np.sort(np.random.randint(1000, 500000, size=DATA_SIZE_A)),
            'value': np.random.randint(0, 2000, size=DATA_SIZE_A, dtype=np.int16)
        })
        df_a_gen.to_feather(arrow_file_a)
        df_b_gen = pd.DataFrame({
            'timestamp': np.sort(np.random.randint(0, 400000, size=DATA_SIZE_B)),
            'value': np.random.randint(0, 2000, size=DATA_SIZE_B, dtype=np.int16)
        })
        df_b_gen.to_feather(arrow_file_b)
        print("Mock data generated.")

    print("\n--- Loading Data from Arrow Files ---")
    df_a = pd.read_feather(arrow_file_a).head(DATA_SIZE_A)
    df_b = pd.read_feather(arrow_file_b).head(DATA_SIZE_B)
    size_a, size_b = len(df_a), len(df_b)
    print(f"Loaded {size_a} records from Stream A and {size_b} from Stream B.")

    # --- GPU Execution ---
    print("\n--- Moving Data to GPU ---")
    a_values_torch = torch.from_numpy(df_a['value'].to_numpy()).cuda()
    a_times_torch = torch.from_numpy(df_a['timestamp'].to_numpy(dtype=np.int64)).cuda()
    b_values_torch = torch.from_numpy(df_b['value'].to_numpy()).cuda()
    b_times_torch = torch.from_numpy(df_b['timestamp'].to_numpy(dtype=np.int64)).cuda()
    output_b_indices_torch = torch.full((size_a,), -1, dtype=torch.int32, device='cuda')

    print("\n--- Starting GPU Pre-processing Stage ---")
    prep_start = torch.cuda.Event(enable_timing=True)
    prep_end = torch.cuda.Event(enable_timing=True)
    prep_start.record()

    _, b_sort_indices = torch.sort(b_values_torch, stable=True)
    sorted_b_times = b_times_torch[b_sort_indices]
    sorted_b_values = b_values_torch[b_sort_indices]
    original_b_indices = torch.arange(size_b, device='cuda', dtype=torch.int32)
    original_b_indices_sorted = original_b_indices[b_sort_indices]
    unique_b_vals, b_counts = torch.unique_consecutive(sorted_b_values, return_counts=True)
    b_starts = torch.zeros_like(b_counts)
    b_starts[1:] = torch.cumsum(b_counts[:-1], dim=0)
    min_b_val, max_b_val = int(df_b['value'].min()), int(df_b['value'].max())
    lookup_table_size = max_b_val - min_b_val + 1
    lookup_starts = torch.full((lookup_table_size,), 0, dtype=torch.int32, device='cuda')
    lookup_counts = torch.full((lookup_table_size,), 0, dtype=torch.int32, device='cuda')
    lookup_table_indices = unique_b_vals.long() - min_b_val
    lookup_starts.scatter_(0, lookup_table_indices, b_starts.to(torch.int32))
    lookup_counts.scatter_(0, lookup_table_indices, b_counts.to(torch.int32))

    torch.cuda.synchronize()
    prep_end.record()
    prep_time_s = prep_start.elapsed_time(prep_end) / 1000.0
    print(f"Pre-processing complete in {prep_time_s:.4f} seconds.")

    print("\n--- Launching Corrected Find-First Kernel ---")
    kernel_start = torch.cuda.Event(enable_timing=True)
    kernel_end = torch.cuda.Event(enable_timing=True)
    kernel_start.record()
    find_first_kernel[(size_a,)](
        a_values_torch, a_times_torch,
        sorted_b_times, original_b_indices_sorted,
        lookup_starts, lookup_counts,
        output_b_indices_torch,
        size_a, min_b_val
    )
    torch.cuda.synchronize()
    kernel_end.record()
    kernel_time_s = kernel_start.elapsed_time(kernel_end) / 1000.0
    print(f"Kernel execution complete in {kernel_time_s:.4f} seconds.")

    total_time_s = prep_time_s + kernel_time_s
    print(f"Total end-to-end GPU time: {total_time_s:.4f} seconds")
    if total_time_s > 0:
        print(f"End-to-end Throughput: {size_a / total_time_s:,.2f} 'A' elements/sec")

    # --- Processing and Verification ---
    found_mask = output_b_indices_torch != -1
    num_matches = torch.sum(found_mask).item()
    print(f"\nFound {num_matches:,} matches for {size_a:,} 'A' elements.")

    if num_matches > 0:
        results_df = pd.DataFrame({
            'a_idx': torch.arange(0, size_a, device='cuda')[found_mask].cpu().numpy(),
            'b_idx': output_b_indices_torch[found_mask].cpu().numpy()
        }).sort_values(by=['a_idx']).reset_index(drop=True)

        print("\n--- First 10 GPU Matches Found ---")
        print(results_df.head(10))

        if VERIFY_WITH_CPU:
            verify_with_cpu(df_a, df_b, results_df)

if __name__ == '__main__':
    main()
