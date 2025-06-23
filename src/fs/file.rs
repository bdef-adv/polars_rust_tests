use polars::prelude::*;

#[allow(unused)]
pub fn load_lazyframe_from_parquet(
    path: &str
) -> Result<LazyFrame, PolarsError> {
    let args = ScanArgsParquet::default();
    LazyFrame::scan_parquet(path, args)
}


#[allow(unused)]
pub fn load_lazyframe_from_ipc(
    path: &str
) -> Result<LazyFrame, PolarsError> {
    let args = ScanArgsIpc::default();
    Ok(LazyFrame::scan_ipc(path, args)?.with_streaming(true))
}
