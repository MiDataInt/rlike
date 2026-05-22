// The 'query' series of macros are used to filter, sort, and select data from a 
// single DataFrame to create a new output DataFrame (see 'join' for macros that
// create new DataFrames from two or more DataFrames).

/* -----------------------------------------------------------------------------
DataFrame internal __qry_filter macro for row filtering in queries; uses 
"half-lazy" evaluation where boolean 'reduce(filter)' is stored and applied later
----------------------------------------------------------------------------- */
#[doc(hidden)]
#[macro_export]
macro_rules! __qry_filter {
    // unary (one-column) operations
    ($df:expr, $qry:expr, $a_name:ident:$a_type:ty => $op:expr; $($tail:tt)*) => {
        $qry.set_col_filter_1::<$a_type>($df, stringify!($a_name), $op);
        __qry_filter!($df, $qry, $($tail)*);
    };
    // binary (two-column) operations
    ($df:expr, $qry:expr, $a_name:ident:$a_type:ty, $b_name:ident:$b_type:ty => $op:expr; $($tail:tt)*) => {
        $qry.set_col_filter_2::<$a_type, $b_type>($df, stringify!($a_name), stringify!($b_name), $op);
        __qry_filter!($df, $qry, $($tail)*);
    };
    // ternary (three-column) operations
    ($df:expr, $qry:expr, $a_name:ident:$a_type:ty, $b_name:ident:$b_type:ty, $c_name:ident:$c_type:ty => $op:expr; $($tail:tt)*) => {
        $qry.set_col_filter_3::<$a_type, $b_type, $c_type>($df, stringify!($a_name), stringify!($b_name), stringify!($c_name), $op);
        __qry_filter!($df, $qry, $($tail)*);
    };
    // no more operations to process
    ($df:expr, $qry:expr $(,)?) => {}; 
}
/* -----------------------------------------------------------------------------
DataFrame internal __qry_sort __qry_select and macro for row filtering in queries;
use lazy evaluation where these macros simply set the columns to be sorted or selected
----------------------------------------------------------------------------- */
#[doc(hidden)]
#[macro_export]
macro_rules! __qry_sort {
    ($df:expr, $qry:expr, $($sort_col:ident),*) => {
        {
            let col_names = vec![$( stringify!($sort_col).to_string(), )*];
            $qry.set_sort_cols($df, col_names);
        }
    };
}
#[doc(hidden)]
#[macro_export]
macro_rules! __qry_select {
    ($df:expr, $qry:expr $(,)?) => {
        $qry.execute_select_query($df)
    };
    ($df:expr, $qry:expr, $($select_col:ident),+ $(,)?) => {
        {
            let col_names = vec![$( stringify!($select_col).to_string(), )+];
            $qry.set_select_cols($df, col_names);
            $qry.execute_select_query($df)
        }
    };
}
#[doc(hidden)]
#[macro_export]
macro_rules! __qry_drop {
    ($df:expr, $qry:expr $(,)?) => {
        $qry.execute_select_query($df)
    };
    ($df:expr, $qry:expr, $($drop_col:ident),+ $(,)?) => {
        {
            let col_names = vec![$( stringify!($drop_col).to_string(), )+];
            $qry.set_drop_cols($df, col_names);
            $qry.execute_select_query($df)
        }
    };
}
/* -----------------------------------------------------------------------------
DataFrame internal __qry_group and __qry_aggregate macros for group and aggregate queries
----------------------------------------------------------------------------- */
#[doc(hidden)]
#[macro_export]
macro_rules! __qry_group {
    // syntax grp_col + grp_col [~ agg_col + agg_col], i.e., either no agg cols or enumerated agg cols
    ($df:expr, $qry:expr, $($group_col:ident $(+)*)+ $( ~ $($agg_col:ident $(+)*)+)?) => {
        {
            let group_cols = vec![$( stringify!($group_col).to_string(), )+];
            let agg_cols   = vec![$( $( stringify!($agg_col).to_string(), )+ )?];
            $qry.set_grouping_cols($df, group_cols, agg_cols);
        }
    };
    // syntax grp_col + grp_col ~ *, i.e., agg cols = all non-key columns via wildcard
    ($df:expr, $qry:expr, $($group_col:ident $(+)*)+ ~ *) => {
        {
            let group_cols = vec![$( stringify!($group_col).to_string(), )+];
            let agg_cols = $df.col_names().iter().filter(|col| !group_cols.contains(col)).cloned().collect();
            $qry.set_grouping_cols($df, group_cols, agg_cols);
        }
    };
}
#[doc(hidden)]
#[macro_export]
macro_rules! __qry_aggregate_col {
    // column data operations with na_replace value
    (
        $df_grp:expr, $df_fss:expr, $group_map:expr, 
        $out_name:ident:$out_type:ty[$na_replace:expr] = $a_name:ident => $op:expr; $($tail:tt)*
    ) => {
        Query::aggregate_column::<_, $out_type>(
            &mut $df_grp, $df_fss, stringify!($a_name), stringify!($out_name), &$group_map, Some($na_replace), $op
        );
        __qry_aggregate_col!($df_grp, $df_fss, $group_map, $($tail)*);
    };
    // column data operations without na_replace value, NA retained
    (
        $df_grp:expr, $df_fss:expr, $group_map:expr, 
        $out_name:ident:$out_type:ty = $a_name:ident => $op:expr; $($tail:tt)*
    ) => {
        Query::aggregate_column::<_, $out_type>(
            &mut $df_grp, $df_fss, stringify!($a_name), stringify!($out_name), &$group_map, None, $op
        );
        __qry_aggregate_col!($df_grp, $df_fss, $group_map, $($tail)*);
    };
    // no more operations to process
    ($df_src:expr, $df_grp:expr, $group_map:expr $(,)?) => {}; 
}
#[doc(hidden)]
#[macro_export]
macro_rules! __qry_aggregate {
    ($df_src:expr, $qry:expr, $($tail:tt)*) => {
        {
            // first perform filtering, sorting and grouping
            // then iteratively call the column aggregation macro, one agg statement at a time
            let (mut df_grp, df_fss_opt, group_map) = $qry.execute_group_by($df_src);
            if let Some(df_fss) = df_fss_opt {
                // rows changed during query, use the df returned in final filter+sort
                __qry_aggregate_col!(df_grp, &df_fss, group_map, $($tail)*);
            } else {
                // no rows changed during query, use the original df
                __qry_aggregate_col!(df_grp, $df_src, group_map, $($tail)*);
            }
            df_grp.status.transfer_sort_group_status(&$qry);
            df_grp
        }
    };
}
/* -----------------------------------------------------------------------------
DataFrame internal __qry_pivot queries
----------------------------------------------------------------------------- */
#[doc(hidden)]
#[macro_export]
macro_rules! __qry_pivot {
    // /* =============================================================================
    // pivot with na_replace value, fill_col, and custom operation
    // out_type[NA_value] = key_col [+ ...] ~ pivot_col as fill_col => |a| ... // e.g., Agg::sum applied to fill_col per group per pivot_col category
    // ============================================================================= */
    (
        $df_src:expr, $qry:expr, 
        $out_type:ty[$na_replace:expr] = $($key_col:ident $(+)*)+ ~ $pivot_col:ident as $fill_col:ident => $op:expr;
    ) => {
        {
            let mut key_cols = vec![$( stringify!($key_col).to_string(), )+];
            let pivot_col = stringify!($pivot_col).to_string();
            let fill_col = stringify!($fill_col).to_string();
            // fill type inferred from out_type if the same, otherwise, as op::<T> in call
            $qry.execute_pivot::<_, $out_type>($df_src, key_cols, pivot_col, fill_col, Some($na_replace), $op)
        }
    };
    // /* =============================================================================
    // pivot with NA in output, fill_col, and custom operation
    // out_type = key_col [+ ...] ~ pivot_col as fill_col => |a| ...
    // ============================================================================= */
    (
        $df_src:expr, $qry:expr, 
        $out_type:ty = $($key_col:ident $(+)*)+ ~ $pivot_col:ident as $fill_col:ident => $op:expr;
    ) => {
        {
            let mut key_cols = vec![$( stringify!($key_col).to_string(), )+];
            let pivot_col = stringify!($pivot_col).to_string();
            let fill_col = stringify!($fill_col).to_string();
            $qry.execute_pivot::<_, $out_type>($df_src, key_cols, pivot_col, fill_col, None, $op)
        }
    };
    // /* =============================================================================
    // pivot to boolean (has data)
    // bool = key_col [+ ...] ~ pivot_col // calls Agg::has_data, i.e., bool whether the pivot_col category was found at least once in the group
    // ============================================================================= */
    (
        $df_src:expr, $qry:expr, 
        bool = $($key_col:ident $(+)*)+ ~ $pivot_col:ident;
    ) => {
        {
            let mut key_cols = vec![$( stringify!($key_col).to_string(), )+];
            let pivot_col = stringify!($pivot_col).to_string();
            // F type bool is a placeholder, has no impact on execute_pivot()
            $qry.execute_pivot::<bool, bool>($df_src, key_cols, pivot_col.clone(), pivot_col, Some(false), Agg::has_data)
        }
    };
    /* =============================================================================
    pivot to count (number of rows)
    usize = key_col [+ ...] ~ pivot_col // calls Agg::count, i.e., the number of rows with the pivot_col category in the group
    ============================================================================= */
    (
        $df_src:expr, $qry:expr, 
        usize = $($key_col:ident $(+)*)+ ~ $pivot_col:ident;
    ) => {
        {
            let mut key_cols = vec![$( stringify!($key_col).to_string(), )+];
            let pivot_col = stringify!($pivot_col).to_string();
            // F type usize is a placeholder, has no impact on execute_pivot()
            $qry.execute_pivot::<usize, usize>($df_src, key_cols, pivot_col.clone(), pivot_col, Some(0), Agg::count)
        }
    };
}
/* -----------------------------------------------------------------------------
DataFrame query top-level macro, switchboard to select(), drop(), aggregate(), df(), vec:type(), pivot()
----------------------------------------------------------------------------- */
#[doc(hidden)]
#[macro_export]
macro_rules! __df_query {
    // select query with `select()`ed columns
    ($df:expr, $qry:expr
        $(, filter($($filter_tokens:tt)+))?
        $(, sort($($sort_tokens:tt)+))? // columns required for sorted select query
        , select($($select_tokens:tt)*) $(, $($tail:tt)+)?
    ) => {
        {
            $({
                $qry.check_filter_status($df.n_row());
                __qry_filter!($df, $qry, $($filter_tokens)+);
            })?
            $(__qry_sort!($df, $qry, $($sort_tokens)+);)?
            let mut df_qry = __qry_select!($df, $qry, $($select_tokens)*);
            __df_query_chain!(df_qry $(, $($tail)+ )?)
        }
    };
    // select query with `drop()`ed columns
    ($df:expr, $qry:expr
        $(, filter($($filter_tokens:tt)+))?
        $(, sort($($sort_tokens:tt)+))? // columns required for sorted select query
        , drop($($drop_tokens:tt)*)  $(, $($tail:tt)+)?
    ) => {
        {
            $({
                $qry.check_filter_status($df.n_row());
                __qry_filter!($df, $qry, $($filter_tokens)+);
            })?
            $(__qry_sort!($df, $qry, $($sort_tokens)+);)?
            let mut df_qry = __qry_drop!($df, $qry, $($drop_tokens)*);
            __df_query_chain!(df_qry $(, $($tail)+ )?)
        }
    };
    // group and aggregate query
    ($df:expr, $qry:expr
        $(, filter($($filter_tokens:tt)+))?
        $(, sort($($sort_tokens:tt)*))? // columns optional for group query
        $(, group($($group_tokens:tt)+))?
        , aggregate($($agg_tokens:tt)*)  $(, $($tail:tt)+)?
    ) => {
        {
            $({
                $qry.check_filter_status($df.n_row());
                __qry_filter!($df, $qry, $($filter_tokens)+);
            })?
            $(__qry_sort!($df, $qry, $($sort_tokens)*);)?
            $(__qry_group!($df, $qry, $($group_tokens)+);)?
            let mut df_qry = __qry_aggregate!($df, $qry, $($agg_tokens)*);
            __df_query_chain!(df_qry $(, $($tail)+ )?)
        }
    };
    // DataFrame group and do query
    ($df:expr, $qry:expr
        $(, filter($($filter_tokens:tt)+))?
        $(, sort($($sort_tokens:tt)*))? // columns optional for group query
        $(, group($($group_tokens:tt)+))?
        , do($op:expr)  $(, $($tail:tt)+)?
    ) => {
        {
            $({
                $qry.check_filter_status($df.n_row());
                __qry_filter!($df, $qry, $($filter_tokens)+);
            })?
            $(__qry_sort!($df, $qry, $($sort_tokens)*);)?
            $(__qry_group!($df, $qry, $($group_tokens)+);)?
            let mut df_qry = $qry.execute_do_df($df, $op);
            __df_query_chain!(df_qry $(, $($tail)+ )?)
        }
    };
    // Vec<T> group and do query
    ($df:expr, $qry:expr
        $(, filter($($filter_tokens:tt)+))?
        $(, sort($($sort_tokens:tt)*))? // columns optional for group query
        $(, group($($group_tokens:tt)+))?
        , do::<$data_type:ty>($op:expr) $(, $($tail:tt)+)?
    ) => {
        {
            $({
                $qry.check_filter_status($df.n_row());
                __qry_filter!($df, $qry, $($filter_tokens)+);
            })?
            $(__qry_sort!($df, $qry, $($sort_tokens)*);)?
            $(__qry_group!($df, $qry, $($group_tokens)+);)?
            let mut df_qry = $qry.execute_do_vec::<$data_type>($df, $op);
            __df_query_chain!(df_qry $(, $($tail)+ )?)
        }
    };
    // pivot query
    ($df:expr, $qry:expr
        $(, filter($($filter_tokens:tt)+))?
        $(, sort($($sort_tokens:tt)*))? // columns optional for group query
        , pivot($($pivot_tokens:tt)*) $(, $($tail:tt)+)?
    ) => {
        {
            $({
                $qry.check_filter_status($df.n_row());
                __qry_filter!($df, $qry, $($filter_tokens)+);
            })?
            $(__qry_sort!($df, $qry, $($sort_tokens)*);)?
            let mut df_qry = __qry_pivot!($df, $qry, $($pivot_tokens)*);
            __df_query_chain!(df_qry $(, $($tail)+ )?)
        }
    };
}

/// Execute a query on a single DataFrame.
/// 
/// # Shared query behaviors
/// 
/// The `df_query!()` allocates and copies data into a new DataFrame because it is 
/// most often the case that either the data rows will change due to filtering 
/// and sorting, or you are selecting columns with the intention of retaining both
/// the input and the new output data frames. For isolated column select operations 
/// that permanently drop unselected columns, use a direct call to  `df_retain!()`, 
/// which does not require allocation or copying.
/// 
/// To help streamline calling code, multiple queries can be chained together in
/// a single call to `df_query!()`. No special syntax is required, just continue
/// to call a second query sequence after calling `select()` or another action 
/// that leads to query execution.
/// 
/// # Pre-query filtering
/// 
/// The `filter()` action is used to specify row filtering operations to be applied
/// prior to other query actions. The `filter()` statement syntax is
/// `col:type [, col:type ...] => |a [, ...]| ...;`, where the 
/// closure takes references to Option<T>(s) as arguments and must return a bool. 
/// From left to right, a filter statement reads as
/// "use these columns of these data types mapped into this operation to filter rows". 
/// Up to three columns can be passed to each filter statement.
/// 
/// Multiple filter statements can be provided to a single `filter()` action call, which 
/// are `and`ed together. Execute `or` operations by using a single filter operation 
/// that acts on multiple columns. Together, complex logical operations can be built.
/// 
/// # Select and Drop queries
/// 
/// Any combination of `filter()` and `sort()` actions can be applied prior to calling 
/// either `select()` or `drop()` (the inverse of select) as the final query action, 
/// which returns a new DataFrame populated by copying as required from the input DataFrame.
/// 
/// An empty select query can be used to copy a DataFrame en bloc as 
/// `let df_copy = df_query!(&df, select());`, or synonymously,
/// `let df_copy = df_select!(&df);`.
/// 
/// ## Example
/// ```
/// let mut df_qry = df_query!(
///     &df,
///     filter( // filter rows based on (combinations of) column values
///         col1:i32 => |a| a > Some(20); // a, b, etc. passed as &Option<T> to allow filtering for None
///         col2:f64 => |a| a == Some(0.0);
///         col3:i32, col4:f64 => |a, b| a > Some(30) || b == Some(40.0);
///         col5:i32, col6:i32 => |a, b| a >= b; // can compare Some(a) and Some(b) directly
///     ),
///     sort(col1, _col2),        // sort by these columns, _col2 is descending
///     select(col1, col2, col3), // select these columns after filtering and sorting
/// );
/// 
/// # Group and aggregate queries
/// 
/// Grouping queries are used to aggregate data by one or more grouping key columns,
/// and then apply one or more column aggregation operations to the grouped data.
/// 
/// The `group()` action is used to specify the grouping columns and the required 
/// aggregation columns with syntax
/// `key_col [+ key_col ...] ~ agg_col [+ agg_col ...];`. 
/// The syntax echos R forumula syntax, where columns to the left of `~` are
/// grouping key columns and columns to the right are aggregation columns.
/// 
/// The `group()` action may or may not be preceded by a `sort()` action.
/// If no `sort()` action is provided, grouping is performed using hashes that
/// return results in the order groups are encounted in the input DataFrame.
/// If an empty `sort()` is provided, the `group()` key columns are used for sorting.
/// Provide a `sort(col, ...)` action with columns if you want to sort by more columns  
/// than just the grouping key columns, e.g., `sort(col1, col2), group(col1)` supports 
/// aggregation functions that expect input sorted on col2. Sort columns must start with 
/// the grouping key columns. Either type of `sort()` yields results sorted by group.
/// 
/// The `aggregate()` action is used to specify the column aggregation operations to be
/// applied to the grouped data. Each aggregation operation is of the form
/// `out_col:out_type[na_replace] = in_col:in_col_type [, in_col:in_type...] => |a ...| ...`,
/// which reads from left to right as
/// "create this output column of this data type [with this NA replacement value] by mapping
/// these columns into this aggregation operation". Only columns identified in the `group()`
/// statement can be used in `aggregate()` statements. Use a wildcard `*` in place of the 
/// agg_col list in `group()` to make all non-key columns available, but use wildcards 
/// wisely so that you don't end up copying a lot of data your never use.
/// 
/// ## Example
/// ```
/// use ag_free::rlike::types::Agg;
/// let mut df_qry = df_query!(
///     &df,
///     // optional filter() statement as above
///     sort(col1, col2, col3),           // sort by these columns (if more than grouping_cols)
///     group(col1 + col2 ~ col3 + col4 + col5), // grouping_cols + ... ~ agg_cols + ...
///     aggregate( // aggregation statements, yielding a single value from input Vec<T>
///         count:usize[0] = col3:i32 => Agg::count; // using RLike predefined aggregation functions
///         sum:i32[0] = col3:i32 => Agg::sum; // one aggregation column can be used to generate multiple output columns
///         mean:f64[0.0] = col3:i32 => Agg::mean;
///         first:f64[0.0] = col3:i32 => Agg::first; // equivalent to Agg:min given the sorting pattern
///         true_count:usize[0] = col4:bool => Agg::true_count;
///         true_freq:f64[0.0] = col4:bool => Agg::true_freq;
///         min_f64:f64[0.0] = col5:f64 => Agg::min_f64;
///         max_f64:f64[0.0] = col5:f64 => Agg::max_f64;
///         custom1:usize[0] = col3:i32 => |a: Vec<_>| a.len(); // using custom aggregation closures
///         custom2:i32[0] = col3:i32 => |a: Vec<i32>| a.iter().sum();
///     ),
/// );
/// ```
///
/// # Group and do queries
/// 
/// A different way to process grouped row data offers 
/// - a broader range of potential caller-defined actions on the grouped data
/// - transformation of grouped data into an entirely new DataFrame or Vec<T>
/// 
/// Initial steps are the same as for a group and aggregate query, with initial calls to 
/// `filter()`, `sort()`, and `group()` as needed. Subsequently, each set of grouped rows
/// is passed to a caller-defined operation provided as a single argument to either the 
/// `do()` or `do::<T>()` actions.
/// 
/// The operation closure passed to do actions is called once for each group of rows. 
/// The signature of the closure is
/// `|grp_i: usize, grp: DataFrame, rows: DataFrameSlice| -> DataFrame|Vec<T>`, where
/// - `grp_i` is a 0-referenced index of the target group that can be used as a simplified group key
/// - `grp` is an owned, single-row DataFrame containing the key column(s) of the target group
/// - `rows` is a DataFrameSlice from which the selected non-key columns matching the target group
///    can be accessed for copying or passing to vectorized operations
/// 
/// The `do()` action expects the operation closure to return a DataFrame resulting from group
/// processing. All DataFrames returned over all row groups are concatenated into a single output
/// DataFrame using `rbind()`. Accordingly, DataFrames returned by the operation closure must have 
/// the same column schema. However, there is no required relationship between the input and output 
/// DataFrame schemas.
/// 
/// The `do::<T>()` action expects the operation closure to return a vector of the specified 
/// data type, where T is replaced with any valid type, e.g., `do::<i32>()`. The resulting 
/// `Vec<Vec<T>>` collected over all row groups is flattened into a single output vector of 
/// type `Vec<T>`.
/// 
/// ## Examples
/// ```
/// use ag_free::rlike::types::Do;
/// let df_do = df_query!(
///     &df,
///     filter( integer:i32 => |a| a <= Some(6000); ),
///     sort(col1, col2),
///     group(col1 ~ col2 + col3 + col4 + col5),
///     do(|grp_i, mut grp, rows|{ // declare grp DataFrame as mutable to modify it with df_inject!()

///     })
///); 
/// let df_do = df_query!(
///     &df,
///     group(col1 ~ col2),
///     do::<i32>(|_grp_i, _grp, rows|{
///        df_get!(rows, integer).into_iter().flatten().collect::<Vec<i32>>() // remove NA values to yield primitive Vec
///        // df_get!(rows, integer, Do::na_rm) // same as above
///     })
/// ); 
/// ```
/// 
/// # Pivot queries
/// 
/// Pivot queries are used to create a new DataFrame by converting one pivot column's values 
/// into new category columns that are filled with values aggregated from grouped rows. This 
/// is a typical "long to wide" conversion of a data table. It is similar to a grouping query 
/// above except that the new aggregation columns are created based on the unique values 
/// (or factor labels) of the pivot column.
/// 
/// Like a grouping query, a pivot query can be preceded by a `filter()` and/or `sort()`
/// action prior to calling the `pivot()` action. 
/// 
/// The `pivot()` action is used to specify the output type, grouping key columns,
/// pivot column, and optional fill column and aggregation function. Two commmon
/// pivot actions use a simplified syntax for row counting and boolean assessment:
/// `bool = key_col [+ key_col ...] ~ pivot_col;`, and
/// `usize = key_col [+ key_col ...] ~ pivot_col;`,
/// where the first calls Agg:has_data and the second calls Agg::count such that 
/// group/category combinations with no matching rows report false and 0, respectively.
/// 
/// Custom aggregation fill operations are supported by the extended statement syntax
/// `out_type[NA_value] = key_col [+ key_col ...] ~ pivot_col as fill_col => |a| ...`,
/// where `[NA_value]` is an optional argument used to replace NA values in the output.
/// These statements read from left to right as
/// "fill pivot cells with this data type [with this NA replacement value] after grouping
/// by these group key columns and pivoting on this categoery column, using (as) this 
/// fill column mapped into this aggregation operation". Thus, the fill operation is called 
/// once for every combination of key column group and pivot column category.
/// 
/// If the input data type cannot be inferred from the output type, the fill operation
/// must itself declare the input data type, either using a function turbofish, e.g.,
/// `Agg::sum::<i32>`, or within the closure argument specification, e.g.,
/// `|a: Vec<i32>| ...`. 
/// 
/// ## Examples
/// ```
/// let df_pvt = df_query!(&df,
///     filter( col1:i32 => |a| a <= Some(5000); ), // all pivot operations can be pre-filtered
///     pivot( usize = col1 ~ col2; ) // standard pivot row count applied to col1/col2 combinations
/// );
/// let df_pvt = df_query!(&df,
///     sort(),
///     pivot( i32 = col1 ~ col2 as col3:i32 => Agg::sum; ) // standard fill operation on col3
/// );
/// let df_pvt = df_query!(&df,
///     pivot( f64 = _col1 ~ col2 as col3:i32 => |a: Vec<i32>| a[0] as f64; ) // custom fill operation, sort col1 descending
/// );
/// let df_pvt = df_query!(&df,
///     pivot( f64[0.0] = _col1 ~ col2 as col3:i32 => |a: Vec<i32>| a[0] as f64; ) // as above with NA replacement
/// );
/// ```
#[macro_export]
macro_rules! df_query {
    ($df:expr, $($tail:tt)+) => {
        {
            let df: &DataFrame = $df; // evaluate any DataFrame expression
            let mut qry = Query::new();
            __df_query!(df, qry, $($tail)+)
        }
    };
}
// handle the second and later query chunks in a chain
// these can start with a transitional set() action
// takes ownership of the prior query DataFrame output
#[doc(hidden)]
#[macro_export]
macro_rules! __df_query_chain {
    ($df_qry:expr, set($($set_tokens:tt)+), $($tail:tt)+) => {
        {
            df_set!(&mut $df_qry, $($set_tokens)+);
            let mut qry = Query::new(); // the next query in the chain after set()
            __df_query!(&$df_qry, qry, $($tail)+)
        }
    };
    ($df_qry:expr, $($tail:tt)+) => {
        {
            let mut qry = Query::new();
            __df_query!(&$df_qry, qry, $($tail)+)
        }
    };
    ($df_qry:expr $(,)?) => {
        $df_qry // last query chunk in a chain, return that DataFrame
    };
}
/* -----------------------------------------------------------------------------
DataFrame synonyms for query macros
----------------------------------------------------------------------------- */
/// The `df_select!(&df, ...)` macro is a convenience synonym for `df_query!(&df, select(...))`.
/// See `df_query!()` for details on syntax and usage.
#[macro_export]
macro_rules! df_select {
    ($df:expr, $($tail:tt)+) => {
        df_query!($df, select($($tail)+))
    };
    ($df:expr) => {
        df_query!($df, select())
    };
}
/// the `df_pivot!(&df, ...)` macro is a convenience synonym for `df_query!(&mut df, pivot(...))`.
/// See `df_query!()` for details on syntax and usage.
#[macro_export]
macro_rules! df_pivot {
    ($df:expr, $($tail:tt)+) => {
        df_query!($df, pivot($($tail)+))
    };
}
