//! The 'index' macros are used to perform keyed lookups
//! into a DataFrame to extract matching rows as a DataFrameSlice.

/// Set an index on a DataFrame to allow calls to `df_get_indexed!()` to 
/// recover a DataFrameSlice of rows matching specific key column values.
/// 
/// The indexing syntax is `let df = df_index!(mut df, col, ...)`.
/// The macro takes ownership of the input DataFrame and either returns 
/// it or replaces it.
/// 
/// If the DataFrame is already known to be grouped by the key columns,
/// `df_index!()` returns the same DataFrame with the newly calculated  
/// index but does not modify the DataFrame column or row contents.
/// Being grouped by the key columns is not the same as being sorted
/// by them, e.g., `df_query!(group(...), do(...))` returns a DataFrame 
/// that is grouped but not sorted.
/// 
/// If the DataFrame is not already grouped by the key columns, `df_index!()` 
/// sorts the DataFrame by the key columns and returns a new sorted (and thus
/// grouped) DataFrame with the calculated index. This is slower as it requires
/// sorting and re-allocating the DataFrame.
/// 
/// If the DataFrame is already known to be both sorted and aggregated by the 
/// key columns, `df_index!()` calculates the index in a manner that allows 
/// `df_get_indexed!()` to use a binary search to find the one matching row. 
/// Otherwise, the index is hash of group keys used to look up the matching rows.
/// 
/// In all cases, the returned DataFrame can now be passed to `df_get_indexed!()`
/// to extract a DataFrameSlice of rows matching specific key column values.
/// These are always contiguous rows in a DataFrameSlice of a grouped DataFrame. 
/// DataFrame indexing does not support retrieval of non-contiguous rows;
/// use `df_query!()` to retrieve a new copied DataFrame for such tasks.
/// 
/// # Example
/// ```
/// // let df = df_query!(&df, sort(col1, col2), select()); // optionally pre-sort the DataFrame
/// // let df = df_query!(&df, group(col1 + col2 ~ col3), aggregate(col3:i32 = col3 => Agg:sum)); // optionally pre-aggregate the DataFrame, etc.
/// df = df_index!(df, col1, col2); // set an index using col1 and col2 as key columns
/// let ds = df_get_indexed!(&df, col1 = 1, col2 = 2.0); // return a DataFrameSlice of all rows matching col1=1 and col2=2.0
/// ```
#[macro_export]
macro_rules! df_index {
    ($df:expr, $($key_col:ident),+ $(,)?) => {
        {
            let key_cols = vec![$( stringify!($key_col).to_string(), )+];
            $df.set_index(key_cols) // returns either df or a replacement DataFrame
        }
    };
}

/// Get a DataFrameSlice of rows matching specific key column values of an 
/// indexed DataFrame. Throws an error if the DataFrame is not properly indexed.
/// 
/// # Example
/// ```
/// df = df_index!(df, col1, col2); // set an index using col1 and col2 as key columns
/// let ds = df_get_indexed!(&df, col1 = 1, col2 = 2.0); // return a DataFrameSlice of all rows matching col1=1 and col2=2.0
/// ```
#[macro_export]
macro_rules! df_get_indexed {
    // get all rows that match specific key column values
    ($df:expr, $($col_name:ident = $col_val:expr),+ $(,)?) => {
        {
            let mut dk = DataFrame::new();
            $( dk.add_col(stringify!($col_name), vec![$col_val]); )+
            $df.get_indexed(dk)
        }
    };
}
