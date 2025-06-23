use polars::prelude::*;

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
