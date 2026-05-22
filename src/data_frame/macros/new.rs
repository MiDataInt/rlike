//! The 'new' macro helps to create (and fill) a new DataFrame.

/* -----------------------------------------------------------------------------
DataFrame `new` constructor macro
----------------------------------------------------------------------------- */
/// Create a new DataFrame with Columns of different Vec<RL>, i.e., 
/// Vec<Option<T>> data types.
/// 
/// All Columns must have the same number of rows, or have zero or one rows which 
/// are recycled to the longest column length using NA or actual values, respectively.
/// 
/// The first supported argument format is a variadic list of `col_name = Vec<Option<T>>` 
/// expressions, where data types are inferred from the provided value vectors. 
/// 
/// Alternatively, provide column names and data types in format `col_name:data_type`,
/// (with factor labels as `col_name:u16[labels]`) to create a new DataFrame with zero rows, 
/// i.e., a DataFrame schema.
/// 
/// Note that `df_new!()` uses commas, not semicolons, to separate its arguments.
/// 
/// Calling df_new!() with no arguments creates an empty DataFrame with zero rows and
/// zero columns that can be populated later from disk or from pre-existing DataFrames 
/// using `df_read!()`, `df_inject!()`, `df_cbind!()`, `df_rbind!()`, or other macros.
/// 
/// # Examples:
/// 
/// ```
/// // fill a DataFrame as it is created
/// let df = df_new!( 
///     col1 = vec![1, 2, 3].to_rl(),            // to_rl() converts Vec<T> to Vec<RL>, i.e., Vec<Option<T>>
///     col2 = vec![Some(1.0), None, Some(3.0)], // None equates to NA/missing values
///     col3 = vec![Some(true); 3],
///     col4 = vec![Some(true)],                 // single row value repeated to longest column length
/// );
/// 
/// // create a new DataFrame schema with zero rows and a default capacity of 100K rows (typically 1M to 10M RAM)
/// pub const LABELS: [&str; 2] = ["A", "B"];
/// let df = df_new!(
///     col1:i32,
///     col2:f64,
///     col3:bool
///     col4:u16[LABELS] // factor column with labels provided at instantiation
/// );
/// 
/// // create a new DataFrame schema with zero rows and a caller-defined capacity
/// let df = df_new!(
///     capacity = 1000,
///     col1:i32,
///     col2:f64,
///     col3:bool
/// );
/// 
/// // create a new, empty DataFrame, suitable for reading, updating, or binding
/// let df = df_new!();
/// ```
#[macro_export]
macro_rules! df_new {

    // fill a new DataFrame as it is created from expressions that resolve to Vec<Option<T>
    // types are inferred from input data
    ($($col_name:ident $([$factor_labels:expr])? = $col_data:expr),+ $(,)?) => {
        {
            let mut df = DataFrame::new();
            $( 
                let col_name = stringify!($col_name);
                df.add_col(col_name, $col_data); 
                $({ df.set_labels(col_name, $factor_labels.to_vec()); })?
            )+
            df
        }
    };

    // create a new, empty DataFrame (i.e., a schema) with specified column names and data types (and factor labels)
    (capacity = $capacity:literal, $($col_name:ident:$data_type:ty $([$factor_labels:expr])? ),+ $(,)?) => {
        {
            let mut df = DataFrame::new();
            $({
                let col_data: Vec<Option<$data_type>> = Vec::with_capacity($capacity as usize);
                let col_name = stringify!($col_name);
                df.add_col(col_name, col_data); 
                $({ df.set_labels(col_name, $factor_labels.to_vec()); })?
            })+
            df
        }
    };

    // when not specified by caller, use a default capacity of 100K rows
    ($($col_name:ident:$data_type:ty),+ $(,)?) => {
        df_new!(capacity = 1e5, $($col_name:$data_type),+);
    };

    // empty DataFrame creation (no rows, no columns)
    () => {
        DataFrame::new()
    };
}
