use serde::{Deserialize, Serialize};
use serde_json::{Value,Map};


#[allow(unused)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OperationParameters {
    pub left_on: Option<String>,
    pub right_on: Option<String>,
    pub left_by: Option<String>,
    pub right_by: Option<String>,
    pub join_type: Option<String>,
    pub suffix: Option<String>,
    pub validation: Option<String>,
    pub join_nulls: Option<bool>,
    pub coalesce: Option<String>
}

impl OperationParameters {
    pub fn as_map(&self) -> Map<String, Value> {
        let mut params: Map<String, Value> = Map::with_capacity(9);
        if let Some(left_on) = self.left_on.clone() {
            params.insert(
                "left_on".into(),
                Value::from(left_on)
            );
        }
        if let Some(left_by) = self.left_by.clone() {
            params.insert(
                "left_by".into(),
                Value::from(left_by)
            );
        }
        if let Some(right_on) = self.right_on.clone() {
            params.insert(
                "right_on".into(),
                Value::from(right_on)
            );
        }
        if let Some(right_by) = self.right_by.clone() {
            params.insert(
                "right_by".into(),
                Value::from(right_by)
            );
        }
        if let Some(join_type) = self.join_type.clone() {
            params.insert(
                "join_type".into(),
                Value::from(join_type)
            );
        }
        if let Some(suffix) = self.suffix.clone() {
            params.insert(
                "suffix".into(),
                Value::from(suffix)
            );
        }
        if let Some(validation) = self.validation.clone() {
            params.insert(
                "validation".into(),
                Value::from(validation)
            );
        }
        if let Some(join_nulls) = self.join_nulls.clone() {
            params.insert(
                "join_nulls".into(),
                Value::from(join_nulls)
            );
        }
        if let Some(coalesce) = self.coalesce.clone() {
            params.insert(
                "coalesce".into(),
                Value::from(coalesce)
            );
        }


        return params;
    }

    pub fn default() -> Self {
        Self {
            left_on: None,
            right_on: None,
            left_by: None,
            right_by: None,
            join_type: None,
            suffix: None,
            validation: None,
            join_nulls: None,
            coalesce: None
        }
    }
}