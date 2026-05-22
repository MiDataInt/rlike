//! The 'set' macros are used to create or update Columns in a DataFrame,
//! in place, creating new Columns, modifying existing ones, etc., using
//! data extracted from the same DataFrame.
//!
//! In contrast to the 'inject' macros, the 'set' macros:
//! - take a mutable reference to the input DataFrame and modify it in place
//! - use data from the same DataFrame to create or update columns
//! - expect ops that act on one non-NA T value (or set of values from different columns) at a time
//! - support a `filter()` action applied to the entire set of DataFrame operations prior to execution

// internal set macro, called iteratively to add/update one columns at a time
#[doc(hidden)]
#[macro_export]
macro_rules! __df_set {
    /* =============================================================================
    direct column value fills, i.e., with zero columns used to derive new column values
    ============================================================================= */
    // constant fill
    ($df:expr, $qry:expr, $out_names:expr, $out_name:ident:$out_type:ty = $fill_val:literal; $($tail:tt)*) => {
        $df.set_col_0::<$out_type>(&$qry, stringify!($out_name), vec![Some($fill_val); $df.n_row()]);
        $out_names.push(stringify!($out_name).to_string());
        __df_set!($df, $qry, $out_names, $($tail)*);
    };
    // NA fill
    ($df:expr, $qry:expr, $out_names:expr, $out_name:ident:$out_type:ty = None; $($tail:tt)*) => {
        $df.set_col_0::<$out_type>(&$qry, stringify!($out_name), vec![None; $df.n_row()]);
        $out_names.push(stringify!($out_name).to_string());
        __df_set!($df, $qry, $out_names, $($tail)*);
    };
    // single expression fill 
    ($df:expr, $qry:expr, $out_names:expr, $out_name:ident:$out_type:ty = Some($fill_val:expr); $($tail:tt)*) => {
        $df.set_col_0::<$out_type>(&$qry, stringify!($out_name), vec![Some($fill_val); $df.n_row()]);
        $out_names.push(stringify!($out_name).to_string());
        __df_set!($df, $qry, $out_names, $($tail)*);
    };
    // vector or slice fill
    ($df:expr, $qry:expr, $out_names:expr, $out_name:ident:$out_type:ty = $rl_values:expr; $($tail:tt)*) => {
        $df.set_col_0::<$out_type>(&$qry, stringify!($out_name), $rl_values.to_vec());
        $out_names.push(stringify!($out_name).to_string());
        __df_set!($df, $qry, $out_names, $($tail)*);
    };
    /* =============================================================================
    column data operations with na_replace value
    ============================================================================= */
    // unary operations, one input column to one output column
    ($df:expr, $qry:expr, $out_names:expr, $out_name:ident:$out_type:ty[$na_replace:expr] = $a_name:ident => $op:expr; $($tail:tt)*) => {
        $df.set_col_1::<_, $out_type>(&$qry, stringify!($a_name), stringify!($out_name), Some($na_replace), $op);
        $out_names.push(stringify!($out_name).to_string());
        __df_set!($df, $qry, $out_names, $($tail)*);
    };
    // binary operations, two input columns to one output column
    ($df:expr, $qry:expr, $out_names:expr, $out_name:ident:$out_type:ty[$na_replace:expr] = $a_name:ident, $b_name:ident => $op:expr; $($tail:tt)*) => {
        $df.set_col_2::<_, _, $out_type>(&$qry, stringify!($a_name), stringify!($b_name), stringify!($out_name), Some($na_replace), $op);
        $out_names.push(stringify!($out_name).to_string());
        __df_set!($df, $qry, $out_names, $($tail)*);
    };
    // ternary operations, three input columns to one output column
    ($df:expr, $qry:expr, $out_names:expr, $out_name:ident:$out_type:ty[$na_replace:expr] = $a_name:ident, $b_name:ident, $c_name:ident => $op:expr; $($tail:tt)*) => {
        $df.set_col_3::<_, _, _, $out_type>(&$qry, stringify!($a_name), stringify!($b_name), stringify!($c_name), stringify!($out_name), Some($na_replace), $op);
        $out_names.push(stringify!($out_name).to_string());
        __df_set!($df, $qry, $out_names, $($tail)*);
    };
    /* =============================================================================
    column data operations without na_replace value, NA retained
    ============================================================================= */
    // unary operations, one input column to one output column
    ($df:expr, $qry:expr, $out_names:expr, $out_name:ident:$out_type:ty = $a_name:ident => $op:expr; $($tail:tt)*) => {
        $df.set_col_1::<_, $out_type>(&$qry, stringify!($a_name), stringify!($out_name), None, $op);
        $out_names.push(stringify!($out_name).to_string());
        __df_set!($df, $qry, $out_names, $($tail)*);
    };
    // binary operations, two input columns to one output column
    ($df:expr, $qry:expr, $out_names:expr, $out_name:ident:$out_type:ty = $a_name:ident, $b_name:ident => $op:expr; $($tail:tt)*) => {
        $df.set_col_2::<_, _, $out_type>(&$qry, stringify!($a_name), stringify!($b_name), stringify!($out_name), None, $op);
        $out_names.push(stringify!($out_name).to_string());
        __df_set!($df, $qry, $out_names, $($tail)*);
    };
    // ternary operations, three input columns to one output column
    ($df:expr, $qry:expr, $out_names:expr, $out_name:ident:$out_type:ty = $a_name:ident, $b_name:ident, $c_name:ident => $op:expr; $($tail:tt)*) => {
        $df.set_col_3::<_, _, _, $out_type>(&$qry, stringify!($a_name), stringify!($b_name), stringify!($c_name), stringify!($out_name), None, $op);
        $out_names.push(stringify!($out_name).to_string());
        __df_set!($df, $qry, $out_names, $($tail)*);
    };
    /* =============================================================================
    base case, no more operations
    ============================================================================= */
    ($df:expr, $qry:expr, $out_names:expr $(,)?) => {
    };
}

/// Create new or overwrite existing column(s) in place using a mutable reference
/// to a DataFrame and caller-defined operation(s) applied to one or more columns. 
/// 
/// The `df_set!()` macro enables:
/// - strong, declared typing consistent with Rust conventions
/// - compact, easy-to-read, write, and debug statement syntax
/// - maximum flexibility in column operations provided as user-defined operation closures
/// - versatile operations including constant fill, vector fill, NA replacement, etc.
/// - embarassingly parallel row-wise processing of column-wise operations
/// - row filtering to only apply update to matching rows
/// 
/// The `df_set!()` call structure is 
///     `df_set!(&mut df, [filter(statement, ...),] do(statement, ...))`,
/// where 
///     - `filter()` is optional and filled with one or more condition statements
///     - `do()` is required and filled with one or more set statements
///     - as a shortcut, the `do()` wrapper can be omitted if filtering is not required, i.e., 
///       `df_set!(&mut df, ...)` is equivalent to `df_set!(&mut df, do(...))`.
///
/// Include query-like `filter(...)` statement(s) to only update df rows that match
/// the filter condition(s). See the `query` macro for details on creating filter statements.
/// Alternatively, individual set statements can implement conditional updates that don't 
/// propagate to the entire `df_set!()` call.
/// 
/// The set statement syntax is 
///     `out_col:type[na_replace] = in_col ... => |a ...| ...;`,
/// which reads from left-to-right as 
///     "create output column of this data type [with this NA replacement value]  
///      from these input columns mapped into this operation".
/// 
/// Set statements end with a semicolon, similar to other Rust statements.
/// 
/// Operations to be performed are provided as closures of form:
///     `|a| ...`, `|a, b| ...`, etc.
/// Up to three comma-separated input columns can specified as inputs to each closure.
/// Any names of your choosing can be used as closure arguments but `a`, `b`, `c` are 
/// a common shorthand. Alternatively, you can map to a named function with an 
/// appropriate signature, e.g., `out_col:type = in_col => My::method;`.
/// 
/// All column arguments a, b, ... are passed as references to the underlying primitive data type, 
/// i.e., &T. Your closure doesn't need to declare argument types unless you need to dereference them 
/// (see examples). For `df_set!()` (unlike `df_inject!()`) the closure expects to receive a single 
/// non-NA T value for each source column.
/// 
/// If the input data type cannot be inferred from the output type or named function, the data 
/// operation must itself declare the input data type, either using a function turbofish, e.g.,
/// `My::method::<i32>`, or within the closure specification, e.g., `|a: &i32| ...`. 
/// 
/// A value expression in the optional `[na_replace]` suffix after the output column data type 
/// will be used as a default fill value for input columns with an NA value. If `[na_replace]`
/// is omitted, any NA/missing input column value will result in an NA output value; data from
/// such rows is never passed to the closure.
/// 
/// One or more set statements are allowed per call. When multiple statements are used in a 
/// single call they are executed sequentially, so one statement can use a column created 
/// in a previous statement.
/// 
/// Output column names can be the same as input column names, which will overwrite/update 
/// that column without error or warning.
/// 
/// When updating RFactor/u16 columns, `df_set!()` will retain factor
/// labels from the existing column. However, `df_set!()` does not have a syntax to add factor 
/// labels to a new column. Add labels after the fact using `df.set_labels()` or `df_set_labels!()`.
/// 
/// # Example
/// ```
/// df_set!(&mut df2, 
///     const_fill:i32 = Some(33);                   // constant fill with row recycling
///     vec_fill:usize = (0..n_row).collect::<Vec<usize>>().to_rl(); // vector fill
///     int_col_add_1:i32 = int_col => |a| a + 1;    // new column from op on existing column
///     num_col:f64 = num_col => |a: &f64| a + 0.5;  // modify existing column in place
///     int_add_num:f64[0.0] = int_col, num_col =>   // multi-column operation with default value 0.0
///         |a: &i32, b: &f64| (*a as f64) + b;
/// );
/// df_set!(&mut df2,
///     filter( record_i:i32 => |a| a > Some(15); ),
///     do(     record_i:i32 = Some(15); )
/// );
/// ```
#[macro_export]
macro_rules! df_set {
    // handle filtered do
    ($df:expr, filter($($filter_tokens:tt)+), do($($tail:tt)+)) => {
        {
            let mut qry = Query::new();
            qry.check_filter_status($df.n_row());
            __qry_filter!($df, qry, $($filter_tokens)+);
            let df = $df; // take single, scoped mutable reference here, use iteratively as needed
            let mut out_names: Vec<String> = Vec::new();
            __df_set!(df, qry, out_names, $($tail)+);
        }
    };
    // handle unfiltered do
    ($df:expr, do($($tail:tt)+)) => {
        {
            let mut qry = Query::new();
            let df = $df; 
            let mut out_names: Vec<String> = Vec::new();
            __df_set!(df, qry, out_names, $($tail)+);
        }
    };
    // handle shortcut syntax df_set!(&mut df, ...)
    ($df:expr, $($tail:tt)+) => {
        df_set!($df, do($($tail)+));
    };
}
