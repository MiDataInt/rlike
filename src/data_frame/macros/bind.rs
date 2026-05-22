// The 'bind' macros are used to combine DataFrames by column or row.

/* -----------------------------------------------------------------------------
cbind (bind by column)
----------------------------------------------------------------------------- */
/// Combine two or more DataFrames in a column-wise fashion. The input DataFrames
/// are consumed to create a new DataFrame with columns in the order they are encountered.
/// The new DataFrame takes ownership of all Columns without allocation or copying.
/// 
/// Strict row matching is enforced for row count, i.e., the two DataFrame instances must 
/// have the same row count, and to prevent column name collisions.
#[macro_export]
macro_rules! df_cbind {
    ($df1:expr, $df2:expr $(, $($df:expr),*)?) => {
        {
            let dfs: Vec<DataFrame> = vec![$df1, $df2, $($($df),*)?];
            dfs.into_iter().reduce(|df1, df2| df1.cbind(df2)).unwrap()
        }
    };
}
/// Combine two or more DataFrames in a column-wise fashion. The input DataFrames
/// are provided as references and copied to create a new DataFrame with columns 
/// in the order they are encountered. See `df_cbind!()` for other restrictions.
#[macro_export]
macro_rules! df_cbind_ref {
    ($df1:expr, $df2:expr $(, $($df:expr),*)?) => {
        {
            let df1: &DataFrame = $df1; // evaluate any DataFrame expressions
            let df2: &DataFrame = $df2;
            let mut df_bind = df1.cbind_ref(df2);
            // use single line to avoid multiple borrow error
            vec![$($($df),*)?].into_iter().fold(df_bind, |df1, df2| df1.cbind_ref(df2))
        }
    };
}
/// Combine one DataFrame and zero or more DataFrameSlices in a column-wise fashion.
/// The input DataFrame[Slices] are provided as references and copied to create a 
/// new DataFrame with columns in the order they are encountered. 
/// See `df_cbind!()` for other restrictions.
#[macro_export]
macro_rules! df_cbind_slice {
    ($df:expr $(, $($slice:expr),*)?) => {
        {
            let mut df_bind = DataFrame::new().cbind_ref($df);
            // use single line to avoid multiple borrow error
            vec![$($($slice),*)?].into_iter().fold(df_bind, |df, slice| df.cbind_slice(slice))
        }
    };
}

/* -----------------------------------------------------------------------------
rbind (bind by row)
----------------------------------------------------------------------------- */
/// Combine two or more DataFrames in a row-wise fashion. The input DataFrames
/// are consumed to create a new DataFrame with rows in the order they are encountered.
/// 
/// Strict column matching is enforced for column names, count, order, and data type, 
/// i.e., the two DataFrame instances must have the same schema.
#[macro_export]
macro_rules! df_rbind {
    ($df1:expr, $df2:expr $(, $($df:expr),*)?) => {
        {
            let dfs: Vec<DataFrame> = vec![$df1, $df2, $($($df),*)?];
            dfs.into_iter().reduce(|df1, df2| df1.rbind(df2)).unwrap()
        }
    };
}
/// Combine two or more DataFrames in a row-wise fashion. The input DataFrames
/// are provided as references and copied to create a new DataFrame with rows 
/// in the order they are encountered. See `df_rbind!()` for other restrictions.
#[macro_export]
macro_rules! df_rbind_ref {
    ($df1:expr, $df2:expr $(, $($df:expr),*)?) => {
        {
            let df1: &DataFrame = $df1; // evaluate any DataFrame expressions
            let df2: &DataFrame = $df2;
            let df_bind = df1.rbind_ref(df2);
            // use single line to avoid multiple borrow error
            vec![$($($df),*)?].into_iter().fold(df_bind, |df1, df2| df1.rbind_ref(df2))
        }
    };
}
