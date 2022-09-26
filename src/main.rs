use polars::prelude::*;

fn main() {
    let table_a: LazyFrame = df! {
        "col_a" => [1, 2, 3],
        "col_b" => [1, 2, 3],
    }.unwrap().lazy();

    let table_b: LazyFrame = df! {
        "col_b" => [1, 2, 3],
        "col_c" => [1, 2, 3],
    }.unwrap().lazy();

    let table_a = table_a.select(vec![col("col_a").prefix("table_a_"), col("col_b").prefix("table_a_")]);

    let table_b = table_b.select(vec![col("col_b").prefix("table_b_"), col("col_c").prefix("table_b_")]);

    let joined_table = table_b.join(
        table_a,
        &[col("table_b_col_b")],
        &[col("table_a_col_b")],
        JoinType::Inner,
    );

    dbg!(joined_table.schema().unwrap());
}
