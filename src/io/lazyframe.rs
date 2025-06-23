use std::io::Cursor;

use polars::prelude::*;


#[allow(unused)]
pub fn display_lazyframe(
    lf: LazyFrame
) -> Result<bool, PolarsError> {
    println!("{}", lf.collect()?);
    Ok(true)
}


#[allow(unused)]
pub fn lazyframe_as_str(
    lf: LazyFrame
) -> Result<String, PolarsError> {
    Ok(format!("{}\n", lf.collect()?))
}


#[allow(unused)]
pub fn lazyframe_as_arrowbytes(lf: LazyFrame) -> PolarsResult<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut cursor = Cursor::new(&mut buffer);

    let mut df: DataFrame = lf.collect()?;

    IpcWriter::new(&mut cursor)
        .finish(&mut df)?;

    Ok(buffer)
}
