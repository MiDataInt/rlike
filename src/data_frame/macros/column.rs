//! The 'column' macros help to modify a DataFrame's Columns in place,
//! including setting factor labels, selecting and dropping Columns, etc.

/* -----------------------------------------------------------------------------
DataFrame `df_label` macro to set factor labels
----------------------------------------------------------------------------- */
/// Set the labels for an RFactor/u16 column in a DataFrame, if not provided at 
/// instantiation using the `df_new!(col:u16[LABELS])` syntax.
/// 
/// Syntax is either:
/// - `df_set_labels!(&mut df, col = LABELS, ...);`, where LABELS is [&str] or Vec<&str>.
/// - `df_set_labels!(&mut df, col = A, B, C, ...);`, where labels are provided as unquoted string literals
#[macro_export]
macro_rules! df_set_labels {
    ($df:expr, $($col_name:ident = $labels:expr),+ $(,)?) => {
        $(
            $df.set_labels(stringify!($col_name), $labels.to_vec());
        )+
    };
    ($df:expr, $col_name:ident = $($label:literal),+ $(,)?) => {
        {
            let col_name = stringify!($col_name);
            let labels = vec![$(stringify($label))+];
            $df.set_labels(col_name, labels);
        }
    };
}

/* -----------------------------------------------------------------------------
DataFrame `retain` macro for in-place column retention and reordering
----------------------------------------------------------------------------- */
/// Retain one or more columns from a DataFrame provided as a list of column names.
/// Columns are retained in the order provided. All other columns are dropped.
/// 
/// Unlike, `select()` called as part of a query, `df_retain!()` 
/// modifies the input DataFrame in place without allocation or copying.
#[macro_export]
macro_rules! df_retain {
    ($df:expr, $($col_name:ident),+ $(,)?) => {
        {
            let col_names = vec![$( stringify!($col_name).to_string(), )+];
            $df.retain_cols(col_names);
        }
    };
}

/* -----------------------------------------------------------------------------
DataFrame `drop` macro for multiple column removal
----------------------------------------------------------------------------- */
/// Permanently drop one or more columns from a DataFrame provided as a list of column names.
#[macro_export]
macro_rules! df_drop {
    ($df:expr, $($col_name:ident),+ $(,)?) => {
        $(
            $df.drop_col(stringify!($col_name));
        )+
    };
}
