use std::time::Instant;
use polars::prelude::*;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;


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

) -> Result<LazyFrame, PolarsError> {
    let args = ScanArgsParquet::default();
    LazyFrame::scan_parquet("miriad-5.8M/data/*.parquet", args)
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

fn main() {
    let timer = Instant::now();

    logger_elapsed!(timer, "Initializing LazyFrame:");
    let mut lf = load_lazyframe_from_parquet().unwrap();
    logger_elapsed!(timer, "Initialized LazyFrame:");

    println!("Duplicate values: {}", _find_duplicate_values_in_column(lf.clone(), "year").collect().unwrap());

    logger_elapsed!(timer, "Pre-filtering:");
    let column_to_filter_in: &str = "year";
    let values_to_filter: Vec<f64> = vec![2017.0, 2018.0];
    lf = lf
        .filter(
            col(column_to_filter_in).is_in(lit(Series::new("".into(), values_to_filter)))
        );

    let columns_to_correlate = ["passage_position", "year", "paper_id"];
    let columns_to_correlate: Vec<String> = columns_to_correlate.iter().map(|s| s.to_string()).collect();
    let lf_correlation = correlate_columns(lf.clone(), &columns_to_correlate);
    logger_elapsed!(timer, "Pre-filtered:");

    println!("\n\nLazyFrame explained\n {}", lf_correlation.explain(false).unwrap());
    println!("\n\nLazyFrame explained optimized\n {}", lf_correlation.explain(true).unwrap());

    logger_elapsed!(timer, "Collecting DataFrame from LazyFrame:");
    let df = lf_correlation.collect().unwrap();
    println!("{df}");
    println!("All columns: {:?}", df.get_column_names());
    logger_elapsed!(timer, "Collected DataFrame from LazyFrame:");


    //println!("Duplicate values: {}", _find_duplicate_values_in_column(lf.clone(), "originh").collect().unwrap());

    logger_elapsed!(timer, "Elapsed:");
}
