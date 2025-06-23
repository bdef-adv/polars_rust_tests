use polars::prelude::*;

#[allow(unused)]
pub fn correlate_columns(
    lf: LazyFrame,
    columns: &Vec<String>,
) -> LazyFrame {
    let mut aggs: Vec<Expr> = Vec::with_capacity(columns.len()^2);

    for column in columns.iter() {
        for column_2 in columns.iter() {
            if column_2 == column {
                continue;
            }
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
