use polars::prelude::*;
//use polars_io::prelude::*;
use std::time::Instant;

fn read_from_csv() -> PolarsResult<DataFrame> {
    println!("reading from a csv file");
    let df = CsvReader::from_path("./data/store.csv")?
            .has_header(true)
            //.with_row_count()
            .finish()
            .unwrap();
    
    let df1 = df.clone().lazy().select([
             col("price_hour"),
    ]).collect()?;
    println!("{:?}", df1);

    let df2 = df
        .clone()
        .lazy()
        .select([col("price_hour")
            .filter(col("price_hour").gt(60))
            .count()])
        .collect()
        .unwrap();
    println!("df2 :{}", df2);

    let df3 = df
        .clone()
        .lazy()
        .select([col("price_hour")
            .filter(col("price_hour").gt(60))])
        .collect()
        .unwrap();
    println!("df3: {}", df3);

    example(&df)?;
    check_is_duplicated(&df)?;
    check_is_unique_row(&df)?;
    create_unique_df(&df, polars::prelude::UniqueKeepStrategy::First)?;
    Ok(df)
}

//expected enum `Result`, found `()`=> PolarsResult => must be a Result
fn example(df: &DataFrame) -> PolarsResult<DataFrame> {
    let df4 = df.select(["price_hour", "name"])?;
    println!("df4: {:?}", df4);
    Ok(df4)
}

//returns booleans
fn check_is_duplicated(df: &DataFrame) -> Result<ChunkedArray<BooleanType>, PolarsError> {
    let df5 = df.is_duplicated()?;
    println!("df5: {:?}", df5);
    Ok(df5)
}

//returns booleans
fn check_is_unique_row(df: &DataFrame) -> Result<ChunkedArray<BooleanType>, PolarsError> {
    let df6 = df.is_unique()?;
    println!("df6: {:?}", df6);
    Ok(df6)
}

/* doc: unstable distinct, See unique_stable
pub fn unique(
    &self,
    subset: Option<&[String]>, => Option
    keep: UniqueKeepStrategy
) -> Result<DataFrame, PolarsError>
*/
/* doc:
pub fn unique_stable(
    &self,
    subset: Option<&[String]>,
    keep: UniqueKeepStrategy
) -> Result<DataFrame, PolarsError>
*/
fn create_unique_df(
    df: &DataFrame,
    keep: UniqueKeepStrategy
) -> Result<DataFrame, PolarsError> {
    let df7 = df.unique_stable(Some(&["item".to_string()]), keep)?;
    println!("df7 dataframe with no duplicated row (item): {:?}", df7);
    //create a .csv file
    csv_writer(&df7);
    Ok(df7)
}

//create a new .csv file
fn csv_writer(df: &DataFrame) {
    let mut file = std::fs::File::create("./data/csv_without_duplicated_row").unwrap();
    CsvWriter::new(&mut file)
        .has_header(true)
        .finish(&mut df.clone())
        .unwrap()
}

fn main() {
    let start = Instant::now();
    let df = read_from_csv().ok();
    println!("dataframe in main {:?}", df);
    //ChunkedArray returns booleans
    let greater_value = df.expect("error")
        .column("price_hour").expect("error")
        .gt(30).expect("error");
    println!("greater than 30 {:?}", greater_value);
    //duration of execution
    let duration = start.elapsed();
    println!("It takes {:?} milliseconds", duration);
}
