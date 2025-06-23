pub(crate) mod file;

use polars::prelude::*;
use polars::error::ErrString;
use file::{load_lazyframe_from_ipc,load_lazyframe_from_parquet};


pub fn get_feed_from_file(
    format: &str,
    source: &str
) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    const LOG_HEADER: &str = "data::get_feed_from_file";
    println!("{LOG_HEADER} Fetching feed from file with format={format} and source={source}");

    match format {
        "parquet" => {
            return Ok(load_lazyframe_from_parquet(source)?)
        },
        "ipc" => {
            return Ok(load_lazyframe_from_ipc(source)?)
        },
        _ => {
            return Err(Box::new(PolarsError::InvalidOperation(ErrString::new_static("Format unrecognized"))))
        }
    }
}
