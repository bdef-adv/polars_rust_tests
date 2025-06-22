use std::io::Cursor;
use std::time::Instant;
use std::collections::HashMap;

use serde_json::{Value,Map};

use polars::{error::ErrString, prelude::*};

use crate::logger_elapsed;
use crate::operations::join::join_timestamp_value;
use crate::operations::filters::filter_value;
use crate::utils::read_config_file;

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


#[allow(unused)]
pub fn correlate_columns(
    lf: LazyFrame,
    columns: &Vec<String>,
) -> LazyFrame {
    let mut aggs: Vec<Expr> = vec![];

    for column in columns.iter() {
        for column_2 in columns.iter() {
            aggs.push(
                spearman_rank_corr(
                    col(column),
                    col(column_2),
                    1,
                    false
                )
                .alias(format!("{column}X{column_2}"))
            );
        }
    }

    return lf.select(aggs)
}


#[allow(unused)]
pub fn find_duplicate_values_in_column(
    lf: LazyFrame,
    column: &str
) -> LazyFrame {
    lf
        .group_by([col(column)])
        .agg([len().alias("count")])
        .filter(col("count").gt(1))
        .select([col(column), col("count")])
        .sort(vec!["count"], SortMultipleOptions {
            descending: vec![true],
            nulls_last: vec![true],
            multithreaded: true,
            maintain_order: true
        })
}


#[allow(unused)]
pub fn display_lazyframe(
    lf: LazyFrame
) -> Result<bool, PolarsError> {
    println!("{}", lf.collect()?);
    Ok(true)
}


#[allow(unused)]
pub fn lazyframe_as_str(
    lf: LazyFrame
) -> Result<String, PolarsError> {
    Ok(format!("{}\n", lf.collect()?))
}


#[allow(unused)]
pub fn lazyframe_as_arrowbytes(lf: LazyFrame) -> PolarsResult<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut cursor = Cursor::new(&mut buffer);

    let mut df: DataFrame = lf.collect()?;

    IpcWriter::new(&mut cursor)
        .finish(&mut df)?;

    Ok(buffer)
}


fn get_feed_from_filters(
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
            _ => {
                return Err(Box::new(PolarsError::InvalidOperation(ErrString::new_static("Operation is unknown"))))
            }
        }
    }

    Ok(filtered_lf)
}


fn get_feed_from_file(
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


fn get_feed_from_operation(
    feed_left: LazyFrame,
    feed_right: LazyFrame,
    operation: &str
) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    const LOG_HEADER: &str = "data::get_feed_from_operation";
    println!("{LOG_HEADER} Fetching feed from operation={operation}");

    match operation {
        "join_timestamp_value" => {
            return Ok(join_timestamp_value(feed_left, feed_right))
        },
        _ => {
            return Err(Box::new(PolarsError::InvalidOperation(ErrString::new_static("Operation is unknown"))))
        }
    }
}


fn get_feed_from_config(
    feed_name: &String,
    all_feeds: &Map<String, Value>
) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    const LOG_HEADER: &str = "data::get_feed_from_config";

    let feed_definition: &Map<String, Value> = all_feeds.get(feed_name).unwrap().as_object().expect("Feed should be an object");
    match feed_definition.get("type").expect("No type defined").as_str().expect("Type is not a string") {
        "file" => {
            println!("{LOG_HEADER} Fetching feed {feed_name} from file");
            let format = feed_definition.get("format").expect("format expected for type 'file'").as_str().unwrap();
            let source = feed_definition.get("source").expect("source expected for type 'file'").as_str().unwrap();

            return get_feed_from_file(format, source);
        },
        "operation" => {
            println!("{LOG_HEADER} Fetching feed {feed_name} from operation");
            let operation = feed_definition.get("operation").expect("operation expected for type 'operation'").as_str().unwrap();

            let feed_left_name = feed_definition.get("feed_left").expect("feed_left expected for type 'operation'").as_str().unwrap().to_owned();
            let feed_right_name = feed_definition.get("feed_right").expect("feed_right expected for type 'operation'").as_str().unwrap().to_owned();

            let feed_left = get_feed_from_config(
                &feed_left_name,
                all_feeds
            )?;
            let feed_right = get_feed_from_config(
                &feed_right_name,
                all_feeds
            )?;

            return get_feed_from_operation(feed_left, feed_right, operation);
        },
        "filter" => {
            println!("{LOG_HEADER} Fetching feed {feed_name} from filter");
            let filters = feed_definition.get("filters").expect("filters expected for type 'filter'").as_array().unwrap();
            let origin = feed_definition.get("origin").expect("origin is expected for type 'filter'").as_str().unwrap().to_owned();
            let origin_feed = get_feed_from_config(&origin, all_feeds)?;

            return get_feed_from_filters(origin_feed, filters);
        },
        _ => {
            return Err(Box::new(PolarsError::InvalidOperation(ErrString::new_static("Bad operation type"))))
        }
    }
}


#[allow(unused)]
pub fn get_feed(
    feed_name: &String
) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    const LOG_HEADER: &str = "data::get_feed";

    let timer = Instant::now();

    let feeds_config_path: String = "config/feeds.json".to_string();

    logger_elapsed!(timer, "{LOG_HEADER} Fetching feed {feed_name} from config {feeds_config_path}");

    let feeds_config_content: String = read_config_file(feeds_config_path.as_str())
                                            .expect("Could not read feeds config file");
    let feeds_config_map: HashMap<String, Value> = serde_json::from_str(&feeds_config_content)
                                                .expect("Could not load feeds config file into HashMap");

    let all_feeds: &Map<String, Value> = feeds_config_map
        .get("feeds")
        .expect("feeds config file does not contain the \"feeds\" key")
        .as_object()
        .expect("feeds needs to be an object");

    logger_elapsed!(timer, "All feeds: {all_feeds:?}");

    if !all_feeds.contains_key(feed_name) {
        return Err(Box::new(PolarsError::NoData(ErrString::new_static("Feed was not found in configuration"))));
    }

    match get_feed_from_config(feed_name, all_feeds) {
        Ok(lf) => {
            return Ok(lf)
        },
        Err(err) => {
            return Err(err)
        }
    }

    return Err(Box::new(PolarsError::NoData(ErrString::new_static("Feed was not found in configuration"))));
}
