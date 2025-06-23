use polars::prelude::*;

use serde::{Deserialize, Serialize};
use serde_json::{Map,Value};

#[allow(unused)]
#[derive(Debug, Serialize, Deserialize)]
pub struct JoinRawParameters {
    pub left_on: String,
    pub right_on: String,
    pub left_by: String,
    pub right_by: String,
    pub suffix: Option<String>,
    pub join_type: Option<String>,
    pub validation: Option<String>,
    pub join_nulls: Option<bool>,
    pub coalesce: Option<String>
}

impl JoinRawParameters {
    pub fn from_map(map: &Map<String, Value>) -> Result<Self, serde_json::Error> {
        let value = Value::Object(map.clone());
        serde_json::from_value(value)
    }
}


#[allow(unused)]
#[derive(Debug)]
pub struct JoinParameters {
    pub left_on: String,
    pub right_on: String,
    pub left_by: String,
    pub right_by: String,
    pub suffix: Option<PlSmallStr>,
    pub join_type: JoinType,
    pub validation: JoinValidation,
    pub join_nulls: bool,
    pub coalesce: JoinCoalesce
}

impl JoinParameters {
    fn from(parameters: &JoinRawParameters) -> Self {
        let mut join_type: JoinType = JoinType::Inner;
        match &parameters.join_type {
            Some(join_type_str) => {
                match join_type_str.as_str() {
                    "left" => {join_type = JoinType::Left},
                    "right" => {join_type = JoinType::Right},
                    "full" => {join_type = JoinType::Full},
                    "cross" => {join_type = JoinType::Cross},
                    _ => {}
                }
            }
            _ => {}
        }

        let mut validation: JoinValidation = JoinValidation::ManyToMany;
        match &parameters.validation {
            Some(validation_str) => {
                match validation_str.as_str() {
                    "manytoone" => {validation = JoinValidation::ManyToOne},
                    "onetomany" => {validation = JoinValidation::OneToMany},
                    "onetoone" => {validation = JoinValidation::OneToOne},
                    _ => {}
                }
            }
            _ => {}
        }

        let mut coalesce: JoinCoalesce = JoinCoalesce::JoinSpecific;
        match &parameters.coalesce {
            Some(coalesce_str) => {
                match coalesce_str.as_str() {
                    "keepcolumns" => {coalesce = JoinCoalesce::KeepColumns},
                    "coalescecolumns" => {coalesce = JoinCoalesce::CoalesceColumns},
                    _ => {}
                }
            }
            _ => {}
        }

        JoinParameters {
            left_on: parameters.left_on.clone(),
            right_on: parameters.right_on.clone(),
            left_by: parameters.left_by.clone(),
            right_by: parameters.left_by.clone(),
            suffix: Some(
                parameters.suffix.clone().unwrap_or("_right".to_string()).into()
            ),
            join_type,
            validation,
            join_nulls: parameters.join_nulls.unwrap_or(false),
            coalesce
        }
    }
}


pub fn join(
    feed_left: LazyFrame,
    feed_right: LazyFrame,
    join_raw_parameters: &JoinRawParameters
) -> LazyFrame {
    let params = JoinParameters::from(&join_raw_parameters);

    feed_left
        .join(
            feed_right,
            [col(params.left_on), col(params.left_by)],

            [col(params.right_on), col(params.right_by)],
            JoinArgs {
                how: params.join_type,
                validation: params.validation,
                suffix: params.suffix,
                slice: None,
                join_nulls: params.join_nulls,
                coalesce: params.coalesce
            }
        )
}
