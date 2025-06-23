pub(crate) mod filter_value;
pub(crate) mod correlation;
pub(crate) mod duplicate_values;

use correlation::correlate_columns;
use filter_value::filter_value;
use duplicate_values::find_duplicate_values_in_column;

use polars::prelude::*;
use polars::error::ErrString;
use serde_json::{Value,Map};


pub fn get_feed_from_filters(
    origin_feed: LazyFrame,
    filters: &Vec<Value>,
) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    const LOG_HEADER: &str = "data::get_feed_from_filters";
    println!("{LOG_HEADER} Fetching feed from filters={filters:?}");

    let mut filtered_lf: LazyFrame = origin_feed.clone();

    for _filter in filters.iter() {
        let filter_obj: &Map<String, Value> = _filter.as_object().unwrap();
        let filter_name = filter_obj.get("filter_name").expect("filter_name must be given").as_str().unwrap();

        match filter_name {
            "filter_value" => {
                let parameters = filter_obj
                    .get("parameters")
                    .expect("parameters is expected for operation 'filter_value'")
                    .as_object()
                    .expect("parameters must be an object");

                let column: String = parameters.get("column")
                    .expect("column is expected for filter_value parameters")
                    .as_str()
                    .unwrap()
                    .to_owned();

                let filter: String = parameters.get("filter")
                    .expect("filter is expected for filter_value parameters")
                    .as_str()
                    .unwrap()
                    .to_owned();

                let value: &Value = parameters.get("value")
                    .expect("value is expected for filter_value parameters");

                let r#type: String = parameters.get("type")
                    .expect("type is expected for filter_value parameters")
                    .as_str()
                    .unwrap()
                    .to_owned();

                filtered_lf = filter_value(filtered_lf, &column, &filter, value, &r#type)?
            },
            "correlation" => {
                let parameters = filter_obj
                    .get("parameters")
                    .expect("parameters is expected for operation 'filter_value'")
                    .as_object()
                    .expect("parameters must be an object");

                let columns: Vec<String> = parameters.get("columns")
                    .expect("columns is expected for filter_value parameters")
                    .as_array()
                    .unwrap()
                    .to_owned()
                    .iter()
                    .map(|x| x.as_str().unwrap().to_string())
                    .collect();

                filtered_lf = correlate_columns(filtered_lf, &columns);
            },
            "duplicate_values" => {
                let parameters = filter_obj
                    .get("parameters")
                    .expect("parameters is expected for operation 'filter_value'")
                    .as_object()
                    .expect("parameters must be an object");

                let column: &str = parameters.get("column")
                    .expect("column is expected for filter_value parameters")
                    .as_str()
                    .unwrap();

                filtered_lf = find_duplicate_values_in_column(filtered_lf, &column);
            },
            _ => {
                return Err(Box::new(PolarsError::InvalidOperation(ErrString::new_static("Operation is unknown"))))
            }
        }
    }

    Ok(filtered_lf)
}
