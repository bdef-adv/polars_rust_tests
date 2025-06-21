import time


import polars as pl



def operation_A(frame_a: pl.LazyFrame, frame_b: pl.LazyFrame) -> pl.LazyFrame:
    return frame_a.join_asof(frame_b, on="timestamp", by="value")


def load_lazyframe_from_parquet(glob: str) -> pl.LazyFrame:
    return pl.scan_parquet(glob)


if __name__ == "__main__":
    start_time = time.time()
    lf_a = load_lazyframe_from_parquet("miriad-5.8M/data/*.parquet")
    print(lf_a.explain())
    print(lf_a.collect(engine="streaming"))

    print(f"Elapsed: {time.time() - start_time}s")
