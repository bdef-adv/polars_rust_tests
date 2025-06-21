use std::time::Instant;
use polars::prelude::*;

#[macro_export]
macro_rules! logger_elapsed {
    ($timer:expr, $($arg:tt)*) => {
        let millis_limit = 2;
        let duration = $timer.elapsed();
        let time_unit: char = if duration.as_millis() <= millis_limit {'µ'} else {'m'};
        let duration_displayed: u128 = if duration.as_millis() <= millis_limit {
            duration.as_micros()
        } else {
            duration.as_millis()
        };

        let text = format!($($arg)*);
        let formatted_msg = format!("{} {}{}s", text, duration_displayed, time_unit);

        println!("{formatted_msg}");
    };
}

pub fn load_lazyframe_from_parquet(
    path: &str
) -> Result<LazyFrame, PolarsError> {
    let args = ScanArgsParquet::default();
    LazyFrame::scan_parquet(path, args)
}

pub fn load_lazyframe_from_ipc(
    path: &str
) -> Result<LazyFrame, PolarsError> {
    let args = ScanArgsIpc::default();
    Ok(LazyFrame::scan_ipc(path, args)?.with_streaming(true))
}

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

pub fn _find_duplicate_values_in_column(
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

pub fn display_lazyframe(
    lf: LazyFrame
) -> Result<bool, PolarsError> {
    println!("{}", lf.collect()?);
    Ok(true)
}


pub fn operation_a(feed_left: LazyFrame, feed_right: LazyFrame) -> LazyFrame {
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

pub fn operation_b(feed_left: LazyFrame, feed_right: LazyFrame) -> LazyFrame {
    return feed_left
}


fn main() {
    let timer = Instant::now();

    logger_elapsed!(timer, "Initializing LazyFrames:");
    let feed_a = load_lazyframe_from_ipc("arrow_data/data_a.arrow").unwrap();
    let feed_b = load_lazyframe_from_ipc("arrow_data/data_b.arrow_100000000").unwrap();
    let feed_c = load_lazyframe_from_ipc("arrow_data/data_b.arrow_300000000").unwrap();
    logger_elapsed!(timer, "Initialized LazyFrames:");

    let feed_d = operation_a(feed_a, feed_b);
    let feed_e = operation_b(feed_d, feed_c);

    display_lazyframe(feed_e).unwrap();

    logger_elapsed!(timer, "Elapsed:");
}
