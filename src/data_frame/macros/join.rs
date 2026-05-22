// The 'join' macros join or merge two DataFrames together by
// row-wise comparison of key Column(s). 

// __df_join3 sets query sort parameters as needed and calls Join::execute_join()
#[doc(hidden)]
#[macro_export]
macro_rules! __df_join3 {
    ($dfs:expr, $qrys:expr, $join:expr) => {
        {
            $join.check_config();
            (0..$join.n_dfs).for_each(|i| {
                // set the query sort columns to the key columns
                $qrys[i].set_sort_cols($dfs[i], $join.key_cols.clone());
                $qrys[i].set_aggregate_status_flags($dfs[i]);
                // parse output columns expected for this df, key + non-key
                $join.cols_out.push( 
                    $join.key_cols.iter().chain($join.cols_non_key[i].iter()).cloned().collect::<Vec<String>>()
                );
            });
            $join.execute_join($dfs, $qrys)
        }
    };
}

// __df_join2 parses the join statement syntax and dispatches to __df_join3
// each join syntax pattern results in key_cols, left_cols, and right_cols, where left_cols
// and right_cols are either user-defined or determined on the fly from the table columns.
#[doc(hidden)]
#[macro_export]
macro_rules! __df_join2 {
    // only key columns specified
    (
        $dfs:expr, $qrys:expr, $join:expr, 
        $($key_col:ident $(+)*)+ ~ *; 
    ) => {
        {
            $join.key_cols = vec![$( stringify!($key_col).to_string(), )+];
            (0..$join.n_dfs).for_each(|i| {
                // all column names in this df, whether used in join or not
                $join.cols_all.push($dfs[i].col_names().clone());
                // non-key column names in this df
                $join.cols_non_key.push(
                    $join.cols_all[i].iter().filter(|col| !$join.key_cols.contains(col)).cloned().collect()
                );
            });
            __df_join3!($dfs, $qrys, $join)
        }
    };
    // key and select columns specified
    (
        $dfs:expr, $qrys:expr, $join:expr,
        $($key_col:ident $(+)*)+ ~ $($select_col:ident $(+)*)+; 
    ) => {
        {
            $join.key_cols = vec![$( stringify!($key_col).to_string(), )+];
            let select_cols = vec![$( stringify!($select_col).to_string(), )+];
            (0..$join.n_dfs).for_each(|i| {
                $join.cols_all.push($dfs[i].col_names().clone());
                $join.cols_non_key.push(
                    // select columns missing from a table are implicity dropped, including misspelled columns
                    select_cols.iter().filter(|col| $join.cols_all[i].contains(col)).cloned().collect()
                );
            });
            __df_join3!($dfs, $qrys, $join)
        }
    };
}

// __df_join1 munches df_join! arguments to extract additional dfs, filter(), sort, and left|inner|outer() actions
#[doc(hidden)]
#[macro_export]
macro_rules! __df_join1 {
    // process join filtering across all DataFrames individually
    ($dfs:expr, $qrys:expr, $join:expr, filter($($filter_tokens:tt)+), $($tail:tt)+) => {
        {
            (0..$join.n_dfs).for_each(|i| {
                $qrys[i].check_filter_status($dfs[i].n_row());
                __qry_filter!($dfs[i], $qrys[i], $($filter_tokens)+);
            });
            $join.filtered = true;
            __df_join1!($dfs, $qrys, $join, $($tail)+)
        }
    };
    // trigger join sorting
    ($dfs:expr, $qrys:expr, $join:expr, sort(), $($tail:tt)+) => {
        {
            $join.sorted = true;
            __df_join1!($dfs, $qrys, $join, $($tail)+)
        }
    };
    // process join types, dispatch to __df_join2 for column parsing
    ($dfs:expr, $qrys:expr, $join:expr, left($($join_tokens:tt)+) $(,)?) => {
        {
            $join.set_join_type(JoinType::Left);
            __df_join2!($dfs, $qrys, $join, $($join_tokens)+)
        }
    };
    ($dfs:expr, $qrys:expr, $join:expr, inner($($join_tokens:tt)+) $(,)?) => {
        {
            $join.set_join_type(JoinType::Inner);
            __df_join2!($dfs, $qrys, $join, $($join_tokens)+)
        }
    };
    ($dfs:expr, $qrys:expr, $join:expr, outer($($join_tokens:tt)+) $(,)?) => {
        {
            $join.set_join_type(JoinType::Outer);
            __df_join2!($dfs, $qrys, $join, $($join_tokens)+)
        }
    };
    // add an additional DataFrame to the join 
    ($dfs:expr, $qrys:expr, $join:expr, $dfn:expr, $($tail:tt)+) => {
        {
            let dfn: &DataFrame = $dfn; // evaluate any DataFrame expressions
            $dfs.push(dfn);
            $qrys.push(Query::new());
            $join.n_dfs += 1;
            __df_join1!($dfs, $qrys, $join, $($tail)+)
        }
    };
}

/// Execute a DataFrame join, i.e., a query that merges two or more DataFrames.
/// 
/// DataFrame joins result in the Cartesian expansion of matching rows, e.g.,
/// if two rows in df1 match three rows in df2 on the key columns, the output DataFrame 
/// will have six rows, one for each match. To avoid expansion, first run `df_query!()`
/// to execute `group()` and `aggregate()` operations on desired DataFrames, then
/// join the resulting DataFrames on the grouped columns as unique keys. 
/// 
/// If a `filter()` action is specified within the call to `df_join!()`, it is applied 
/// the same to all DataFrames. Therefore, all columns being filtered against must be 
/// present in all DataFrames. Filter columns may or may not be used in the join statement. 
/// Filter statement syntax is the same as for `df_query!()`. If filtering must be applied
/// differently to different DataFrames, perform that filtering upstream of the join.
/// 
/// Notably, pre-join calls to `df_query!()` to perform grouping or row filtering can be 
/// passed directly as arguments to `df_join!()`, e.g., 
/// `df_join!(&df1, &df_query!(&df2, ...), ...)`. 
/// Temporary DataFrames are created internally with a lifetime that lasts for the duration 
/// of the `df_join!()` call.
/// 
/// Two join algorithms are supported, hash and sorted merge joins, which are chosen
/// by the `sort()` action. When `sort()` is omitted, `df_join()` performs left or inner 
/// joins and returns the results in the row order of the original DataFrames using a 
/// hash join algorithm. When an empty `sort()` is included, `df_join()` performs left,  
/// inner, or outer joins and returns rows sorted by the key columns using a merge join. 
/// Unlike `df_query!()`, `df_join!()` does not support extended key columns in `sort()`.
/// 
/// The type of join is specified as the name of the last function argument, either
/// `left()`, `inner()`, or `outer()` (right joins are not supported). Each join type is
/// specified using a semicolon-terminated formula statement syntax similar to other macros: 
/// `key_col [+ key_col ...] ~ select_col [+ select_col ...];`, 
/// where key columns must be present in all DataFrames and select columns must be unique 
/// to one DataFrame. You do not need to specify select columns from all DataFrames, e.g.,
/// performing an inner join with no select columns from df2 filters df1 to only those rows 
/// that have a match in df2 without adding any external values from df2.
/// 
/// An alternative statement syntax replaces select columns with a wildcard, i.e., 
/// `key_col [+ key_col ...] ~ *;`, 
/// to return all columns from all DataFrames, in which case all key columns must be shared 
/// between the DataFrames and all other columns must be unique to just one DataFrame.
/// Select or rename columns before joining as needed - DataFrame is explictly 
/// named and does not use suffixing the create new column names on the fly.
/// 
/// For either select column statement syntax, there can be one to four key columns,
/// which can be sorted in reverse, i.e., descending, order by prefixing the column name
/// with '_', e.g., `inner( _col1 ~ col2 + col3; );`.
/// 
/// For left and outer joins, the syntax above results in NA values in select columns when 
/// a key is missing from the other table. At present, use a post-join call to `df_set!()` 
/// to replace NA values with some other default.
/// 
/// Key columns that contain NA and NaN values are used in the join operation, where 
/// NA is considered equal to NA and NaN is equal to NaN. Use `filter()` to remove NA or NaN 
/// values from key columns before joining to prevent them from being in the output DataFrame.
/// 
/// ## Examples
/// let df_join = df_join!(&df1, &df2, 
///     filter( col1:i32 => |&a| a > Some(20); ), // etc., see df_query!() for syntax
///     inner( col2 ~ col3 + col4; ); // e.g., col2 is in df1 and df2, col3 is in df1, col4 is in df2
/// )
/// let df_join = df_join!(&df1, &df2,
///     filter( col1:i32 => |&a| a > Some(20); ),
///     sort(), // sort the output by col2
///     outer( col2 ~ col3 + col4; );
/// )
/// let df_join = df_join!(&df1, &df2, 
///     filter( col2:i32 => |&a| a.is_some(); ), // drop NA key values from df1 and df2 before joining
///     left( col2 ~ col3 + col4; );
/// )
/// let df_join = df_join!(
///     &df1, 
///     &df_query!(&df2, filter(col4:i32 => |&a| a == Some(1);), select()), // filter only df2 on col4 prior to joining
///     inner( col2 ~ col3; ); // e.g., col2 is in df1 and df2, col3 is in df1, to filter df1 against df2 on col2
/// )
/// let df_join = df_join!(&df1, &df2, &df3, &df4, // fold join multiple DataFrames, first joining df1 and df2, joining the result to df3, etc.
///     inner( col2 + col3 ~ *; ); // col2 and col3 are in df1 and df2, all other df columns are included in the output
/// )
#[macro_export]
macro_rules! df_join {
    ($df1:expr, $df2:expr, $($tail:tt)+) => {
        {
            let df1: &DataFrame = $df1; // evaluate any DataFrame expressions
            let df2: &DataFrame = $df2;
            let mut dfs: Vec<&DataFrame> = vec![df1, df2];
            let mut qrys: Vec<Query> = vec![Query::new(), Query::new()];
            let mut join = Join::new(); // the object that performs the join
            __df_join1!(dfs, qrys, join, $($tail)+)
        }
    };
}
/* -----------------------------------------------------------------------------
DataFrame synonyms for join macros
----------------------------------------------------------------------------- */
/// The `df_inner!(&df1, &df2, ...)` macro is a convenience synonym for 
/// `df_join!(&df1, &df2, inner(...))`.  See `df_join!()` for details on 
/// syntax and usage.
#[macro_export]
macro_rules! df_inner {
    ($df1:expr, $df2:expr, $($tail:tt)+) => {
        df_join!($df1, $df2, inner($($tail)+))
    };
}
/// The `df_left!(&df1, &df2, ...)` macro is a convenience synonym for
/// `df_join!(&df1, &df2, left(...))`.  See `df_join!()` for details on
/// syntax and usage.
#[macro_export]
macro_rules! df_left {
    ($df1:expr, $df2:expr, $($tail:tt)+) => {
        df_join!($df1, $df2, left($($tail)+))
    };
}
/// The `df_outer!(&df1, &df2, ...)` macro is a convenience synonym for
/// `df_join!(&df1, &df2, sort(), outer(...))`.  See `df_join!()` for details on
/// syntax and usage.
#[macro_export]
macro_rules! df_outer {
    ($df1:expr, $df2:expr, $($tail:tt)+) => {
        df_join!($df1, $df2, sort(), outer($($tail)+))
    };
}
