//! The 'inject' macros are used to create or update Columns in a DataFrame, 
//! creating new Columns, modifying existing ones, etc., using data extracted
//! from a different source DataFrame or DataFrameSlice. Their greatest use is
//! in 'group and do' queries.
//!
//! In contrast to the 'set' macros, the 'inject' macros:
//! - take ownership of the input DataFrame and return it after modification
//! - use data from a source DataFrame or DataFrameSlice to create or update columns
//! - expect ops that act on an entire Vec<Option<T>> (or set of them from different columns)
//! - do not support a pre-execution `filter()` action within the call to `df_inject!()`

// internal inject macro, called iteratively to add/update one columns at a time
#[doc(hidden)]
#[macro_export]
macro_rules! __df_inject {
    /* =============================================================================
    direct column value fills, i.e., with zero columns used to derive new column values
    ============================================================================= */
    // constant fill, provided as bare literal
    ($df_dst:expr, $df_src:expr, $out_names:expr, $out_name:ident:$out_type:ty = $fill_val:literal; $($tail:tt)*) => {
        {
            $df_dst.inject_col_const::<$out_type>(stringify!($out_name), Some($fill_val));
            $out_names.push(stringify!($out_name).to_string());
            __df_inject!($df_dst, $df_src, $out_names, $($tail)*)
        }
    };
    // NA fill, provided as None
    ($df_dst:expr, $df_src:expr, $out_names:expr, $out_name:ident:$out_type:ty = None; $($tail:tt)*) => {
        {
            $df_dst.inject_col_const::<$out_type>(stringify!($out_name), None);
            $out_names.push(stringify!($out_name).to_string());
            __df_inject!($df_dst, $df_src, $out_names, $($tail)*)
        }
    };
    // constant fill, provided as Some(expr)
    ($df_dst:expr, $df_src:expr, $out_names:expr, $out_name:ident:$out_type:ty = Some($fill_val:expr); $($tail:tt)*) => {
        {
            $df_dst.inject_col_const::<$out_type>(stringify!($out_name), Some($fill_val));
            $out_names.push(stringify!($out_name).to_string());
            __df_inject!($df_dst, $df_src, $out_names, $($tail)*)
        }
    };
    // vector or slice fill, provided as Vec<Option<T>> or &[Option<T>] with NA_replace value
    ($df_dst:expr, $df_src:expr, $out_names:expr, $out_name:ident:$out_type:ty[$na_replace:expr] = $rl_values:expr; $($tail:tt)*) => {
        {
            $df_dst.inject_col_vec::<$out_type>(stringify!($out_name), $rl_values.to_vec().into(), Some($na_replace));
            $out_names.push(stringify!($out_name).to_string());
            __df_inject!($df_dst, $df_src, $out_names, $($tail)*)
        }
    };
    // vector or slice fill, provided as Vec<Option<T>> or &[Option<T>], NA persists
    ($df_dst:expr, $df_src:expr, $out_names:expr, $out_name:ident:$out_type:ty = $rl_values:expr; $($tail:tt)*) => {
        {
            $df_dst.inject_col_vec::<$out_type>(stringify!($out_name), $rl_values.to_vec().into(), None);
            $out_names.push(stringify!($out_name).to_string());
            __df_inject!($df_dst, $df_src, $out_names, $($tail)*)
        }
    };
    // /* =============================================================================
    // column data operations with na_replace value
    // ============================================================================= */
    // unary operations, one input column to one output column
    ($df_dst:expr, $df_src:expr, $out_names:expr, $out_name:ident:$out_type:ty[$na_replace:expr] = $a_name:ident => $op:expr; $($tail:tt)*) => {
        {
            let mut vals: Vec<Option<$out_type>> = df_get![$df_src, $a_name, $op];
            Do::na_replace(&mut vals, $na_replace);
            $df_dst.replace_or_add_col(stringify!($out_name), vals);
            $out_names.push(stringify!($out_name).to_string());
            __df_inject!($df_dst, $df_src, $out_names, $($tail)*)
        }
    };
    // binary operations, two input columns to one output column
    ($df_dst:expr, $df_src:expr, $out_names:expr, $out_name:ident:$out_type:ty[$na_replace:expr] = $a_name:ident, $b_name:ident => $op:expr; $($tail:tt)*) => {
        {
            let mut vals: Vec<Option<$out_type>> = df_get![$df_src, $a_name, $b_name, $op];
            Do::na_replace(&mut vals, $na_replace);
            $df_dst.replace_or_add_col(stringify!($out_name), vals);
            $out_names.push(stringify!($out_name).to_string());
            __df_inject!($df_dst, $df_src, $out_names, $($tail)*)
        }
    };
    // ternary operations, three input columns to one output column
    ($df_dst:expr, $df_src:expr, $out_names:expr, $out_name:ident:$out_type:ty[$na_replace:expr] = $a_name:ident, $b_name:ident, $c_name:ident => $op:expr; $($tail:tt)*) => {
        {
            let mut vals: Vec<Option<$out_type>> = df_get![$df_src, $a_name, $b_name, $c_name, $op];
            Do::na_replace(&mut vals, $na_replace);
            $df_dst.replace_or_add_col(stringify!($out_name), vals);
            $out_names.push(stringify!($out_name).to_string());
            __df_inject!($df_dst, $df_src, $out_names, $($tail)*)
        }
    };
    // /* =============================================================================
    // column data operations without na_replace value, NA retained
    // ============================================================================= */
    // unary operations, one input column to one output column
    ($df_dst:expr, $df_src:expr, $out_names:expr, $out_name:ident:$out_type:ty = $a_name:ident => $op:expr; $($tail:tt)*) => {
        {
            let vals: Vec<Option<$out_type>> = df_get![$df_src, $a_name, $op];
            $df_dst.replace_or_add_col(stringify!($out_name), vals);
            $out_names.push(stringify!($out_name).to_string());
            __df_inject!($df_dst, $df_src, $out_names, $($tail)*)
        }
    };
    // binary operations, two input columns to one output column
    ($df_dst:expr, $df_src:expr, $out_names:expr, $out_name:ident:$out_type:ty = $a_name:ident, $b_name:ident => $op:expr; $($tail:tt)*) => {
        {
            let vals: Vec<Option<$out_type>> = df_get![$df_src, $a_name, $b_name, $op];
            $df_dst.replace_or_add_col(stringify!($out_name), vals);
            $out_names.push(stringify!($out_name).to_string());
            __df_inject!($df_dst, $df_src, $out_names, $($tail)*)
        }
    };
    // ternary operations, three input columns to one output column
    ($df_dst:expr, $df_src:expr, $out_names:expr, $out_name:ident:$out_type:ty = $a_name:ident, $b_name:ident, $c_name:ident => $op:expr; $($tail:tt)*) => {
        {
            let vals: Vec<Option<$out_type>> = df_get![$df_src, $a_name, $b_name, $c_name, $op];
            $df_dst.replace_or_add_col(stringify!($out_name), vals);
            $out_names.push(stringify!($out_name).to_string());
            __df_inject!($df_dst, $df_src, $out_names, $($tail)*)
        }
    };
    /* =============================================================================
    base case, no more operations, return the modified DataFrame
    ============================================================================= */
    ($df_dst:expr, $df_src:expr, $out_names:expr $(,)?) => {
        {
            $df_dst
        }
    };
}
/// Modify a DataFrame in a manner similar to `df_set!()` but now taking ownership
/// of the input DataFrame, modifying it using caller-defined operation(s), and 
/// returning it as a new DataFrame. Also, `df_inject!()` uses a different DataFrame
/// or DataFrameSlice as the source of data for the operation(s), which act on the 
/// entire set of RLike values from one (or multiple) columns of the source DataFrame.
/// 
/// A common use for `df_inject!()` is in the context of Group and Do queries to 
/// add new columns to the group key columns.
/// 
/// The overall macro call structure is 
///     `df_inject!(df_dst, &df_src, statement, ...)`,
/// where 
///     - `df_dst` is the mutable DataFrame to be modified after taking ownership
///     - `df_src` is the DataFrame or DataFrameSlice from which to extract data
///     - `statement` is a set of statements that generate columns as Vec<Option<T>>
///
/// The inject statement syntax is:
///     `out_col:type[na_replace] = in_col ... => |a ...| ...;`,
/// which reads from left-to-right as:
///     "create output column of this name and data type [with this NA replacement value]  
///      from these input columns mapped into this operation".
/// 
/// Inject statements end with a semicolon, similar to other Rust statements.
/// 
/// Operations to be performed are provided as closures of form:
///     `|a| ...`, `|a, b| ...`, etc.
/// Up to three comma-separated input columns can specified as inputs to each closure.
/// Any names of your choosing can be used as closure arguments but `a`, `b`, `c` are 
/// a common shorthand.  Alternatively, you can map to a named function with an 
/// appropriate signature, e.g., `out_col:type = in_col => My::method;`.
/// 
/// All column arguments a, b, ... are passed as references to the underlying Vec<Option<T>>
/// or [Option<T>] slice. For `df_inject!()`, (unlike `df_set!()`) the closure expects to receive
/// a vector or slice of values for each source column over all rows, including NA values.
/// 
/// If the input data type cannot be inferred from the output type or named function, the data 
/// operation must itself declare the input data type, either using a function turbofish, e.g.,
/// `My::method::<i32>`, or within the closure specification, e.g., `|a: &i32| ...`. 
/// 
/// One or more inject statements are allowed per call. When multiple statements are used in a 
/// single call they are executed sequentially. Notably, unlike `df_set!()`, it is not possible
/// to use columns created in a previous statement in a later statement since the new column
/// is placed into the destination DataFrame whereas input data are read from the source DataFrame.
/// 
/// If a column name already exists in the destination DataFrame, it will be overwritten
/// without error or warning.
/// 
/// # Example
/// ```
/// df_inject!(grp, &rows,
///     grp_i:usize      = Some(grp_i);
///     row_count:usize  = Some(rows.n_row());
///     sum_record_i:i32 = record_i => Do::sum;
/// )
/// ```
#[macro_export]
macro_rules! df_inject {
    ($df_dst:expr, $df_src:expr, $($tail:tt)+) => {
        {
            let mut df_dst: DataFrame = $df_dst; // evaluate any DataFrame expressions before passing to __df_inject!
            let df_src = $df_src; // don't type here, could be a DataFrame or DataFrameSlice
            let mut out_names: Vec<String> = Vec::new();
            __df_inject!(df_dst, df_src, out_names, $($tail)+)
        }
    };
}
