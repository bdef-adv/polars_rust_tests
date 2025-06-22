use polars::prelude::*;

pub fn join_timestamp_value(feed_left: LazyFrame, feed_right: LazyFrame) -> LazyFrame {
    feed_left
        .join(
            feed_right,
            [col("timestamp"), col("value")],

            [col("timestamp"), col("value")],
            JoinArgs {
                how: JoinType::Inner,
                validation: JoinValidation::ManyToMany,
                suffix: Some("_right".into()),
                slice: None,
                join_nulls: false,
                coalesce: JoinCoalesce::JoinSpecific
            }
        )
}
