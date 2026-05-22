// The 'display' macro supports calling print!("{}", df) with
// adjusted column widths and row counts.

#[macro_export]
macro_rules! df_print {
    ($df:expr, $max_rows:literal, $max_col_width:literal) => {
        let max_rows_tmp = $df.print_max_rows;
        let max_col_width_tmp = $df.print_max_col_width;
        $df.print_max_rows = $max_rows;
        $df.print_max_col_width = $max_col_width;
        println!("{}", $df);
        $df.print_max_rows = max_rows_tmp;
        $df.print_max_col_width = max_col_width_tmp;
    };
    ($df:expr, $max_rows:literal) => {
        df_print!($df, $max_rows, 25);
    };
    ($df:expr) => {
        df_print!($df, 20, 25);
    };
}
