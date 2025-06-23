pub(crate) mod join;

use join::{join,JoinRawParameters};

use polars::prelude::*;
use polars::error::ErrString;
use serde_json::Value;

pub fn get_feed_from_operation(
    feed_left: LazyFrame,
    feed_right: LazyFrame,
    operation: &str,
    parameters: &Value
) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    const LOG_HEADER: &str = "data::get_feed_from_operation";
    println!("{LOG_HEADER} Fetching feed from operation={operation}");

    match operation {
        "join" => {
            let parameters = JoinRawParameters::from_map(
                parameters
                    .as_object()
                    .unwrap()
            )?;
            return Ok(join(feed_left, feed_right, &parameters))
        },
        _ => {
            return Err(Box::new(PolarsError::InvalidOperation(ErrString::new_static("Operation is unknown"))))
        }
    }
}
