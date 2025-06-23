pub(crate) mod parameters;

use std::time::Instant;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value,Map};

use polars::{error::ErrString, prelude::*};

use crate::logger_elapsed;
use crate::filters::get_feed_from_filters;
use crate::operations::get_feed_from_operation;
use crate::fs::get_feed_from_file;
use crate::utils::read_config_file;
use parameters::FeedParameters;


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

            let parameters = feed_definition.get("parameters").unwrap_or(&Value::Null).to_owned();

            return get_feed_from_operation(feed_left, feed_right, operation, &parameters);
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


#[allow(unused)]
pub fn new_temporary_feed(
    feed_name: &String,
    params: &FeedParameters
) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    const LOG_HEADER: &str = "data::get_feed";

    let timer = Instant::now();

    let feeds_config_path: String = "config/feeds.json".to_string();

    logger_elapsed!(timer, "{LOG_HEADER} Fetching feed {feed_name} from config {feeds_config_path}");

    let feeds_config_content: String = read_config_file(feeds_config_path.as_str())
                                            .expect("Could not read feeds config file");
    let feeds_config_map: HashMap<String, Value> = serde_json::from_str(&feeds_config_content)
                                                .expect("Could not load feeds config file into HashMap");

    let mut all_feeds: Map<String, Value> = feeds_config_map
        .get("feeds")
        .expect("feeds config file does not contain the \"feeds\" key")
        .as_object()
        .expect("feeds needs to be an object")
        .to_owned();

    all_feeds.insert(
        feed_name.to_owned(),
        Value::from(
            params.as_map()
        )
    );

    logger_elapsed!(timer, "All feeds: {all_feeds:?}");

    if !all_feeds.contains_key(feed_name) {
        return Err(Box::new(PolarsError::NoData(ErrString::new_static("Feed was not found in configuration"))));
    }

    match get_feed_from_config(feed_name, &all_feeds) {
        Ok(lf) => {
            return Ok(lf)
        },
        Err(err) => {
            return Err(err)
        }
    }

    return Err(Box::new(PolarsError::NoData(ErrString::new_static("Feed was not found in configuration"))));
}
