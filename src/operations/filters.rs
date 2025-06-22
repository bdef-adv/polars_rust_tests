use polars::{error::ErrString, prelude::*};

use serde_json::Value;

pub fn filter_value(
    feed_left: LazyFrame,
    column: &String,
    filter: &String,
    value: &Value,
    r#type: &String
) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    let mut filter_expr: Expr = col(column);

    match r#type.as_str() {
        "int16" => {
            let value: i16 = value.as_i64().unwrap() as i16;

            match filter.as_str() {
                "gte" => {
                    filter_expr = filter_expr.gt_eq(lit(value))
                },
                "gt" => {
                    filter_expr = filter_expr.gt(lit(value))
                },
                "lte" => {
                    filter_expr = filter_expr.lt_eq(lit(value))
                },
                "lt" => {
                    filter_expr = filter_expr.lt(lit(value))
                },
                "eq" => {
                    filter_expr = filter_expr.eq(lit(value))
                },
                _ => {
                    return Err(Box::new(PolarsError::InvalidOperation(ErrString::new_static("Invalid filter"))))
                }
            }

            return Ok(feed_left.filter(filter_expr))
        },
        _ => {
            return Err(Box::new(PolarsError::InvalidOperation(ErrString::new_static("Invalid type"))))
        }
    };
}
