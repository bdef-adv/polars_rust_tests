pub(crate) mod operation_parameters;
pub(crate) mod filter_parameters;

use serde::{Deserialize, Serialize};
use serde_json::{Map,Value};

use operation_parameters::OperationParameters;
use filter_parameters::FilterParameters;

#[allow(unused)]
#[derive(Debug, Serialize, Deserialize)]
pub struct FeedParameters {
    pub feed_name: String,
    pub r#type: String,

    // Operations
    pub feed_left: Option<String>,
    pub feed_right: Option<String>,
    pub operation: Option<String>,
    pub parameters: Option<OperationParameters>,

    // Filters
    pub origin: Option<String>,
    pub filters: Option<Vec<FilterParameters>>,
}

impl FeedParameters {
    pub fn as_map(&self) -> Map<String, Value> {
        let mut params: Map<String, Value> = Map::with_capacity(8);
        params.insert("type".into(), Value::from(self.r#type.clone()));
        if let Some(operation) = self.operation.clone() {
            params.insert(
                "operation".into(),
                Value::from(operation)
            );
            params.insert(
                "feed_left".into(),
                Value::from(self.feed_left.clone()
                    .expect("operation need 'feed_left'"))
            );
            params.insert(
                "feed_right".into(),
                Value::from(self.feed_right.clone()
                    .expect("operation need 'feed_right'"))
            );
            params.insert(
                "parameters".into(),
                Value::from(
                    self.parameters
                    .clone()
                    .unwrap_or(OperationParameters::default())
                    .as_map()
                )
            );
        }

        if let Some(origin) = self.origin.clone() {
            params.insert(
                "origin".into(),
                Value::from(origin)
            );
            println!("Filters: {:?}", self.filters.clone().unwrap());
            params.insert(
                "filters".into(),
                Value::from(
                    self.filters
                    .clone()
                    .unwrap_or(Vec::with_capacity(0))
                    .iter()
                    .map(|fp| Value::from(fp.as_map()))
                    .collect::<Vec<Value>>()
                )
            );
        }

        return params;
    }
}
