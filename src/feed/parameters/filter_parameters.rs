use serde::{Deserialize, Serialize};
use serde_json::{Value,Map};

#[allow(unused)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilterParameters {
    pub column: Option<String>,
    pub columns: Option<Vec<String>>,
    pub filter: Option<String>,
    pub value: Option<Value>,
    pub r#type: Option<String>
}

impl FilterParameters {
    pub fn as_map(&self) -> Map<String, Value> {
        let mut params: Map<String, Value> = Map::with_capacity(5);
        if let Some(column) = self.column.clone() {
            params.insert(
                "column".into(),
                Value::from(column)
            );
        }
        if let Some(columns) = self.columns.clone() {
            params.insert(
                "columns".into(),
                Value::from(columns)
            );
        }
        if let Some(filter) = self.filter.clone() {
            params.insert(
                "filter".into(),
                Value::from(filter)
            );
        }
        if let Some(value) = self.value.clone() {
            params.insert(
                "value".into(),
                Value::from(value)
            );
        }
        if let Some(r#type) = self.r#type.clone() {
            params.insert(
                "type".into(),
                Value::from(r#type)
            );
        }


        return params;
    }

    pub fn default() -> Self {
        Self {
            column: None,
            columns: None,
            filter: None,
            value: None,
            r#type: None
        }
    }
}