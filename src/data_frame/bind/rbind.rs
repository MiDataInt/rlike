//! Implement DataFrame rbind methods.

// dependencies
use super::{DataFrame, Column};
use crate::throw;

impl DataFrame {
    /* -----------------------------------------------------------------------------
    rbind owned DataFrames
    ----------------------------------------------------------------------------- */
    /// Append all rows of an another DataFrame (`other`) to the calling DataFrame (`self`).
    /// Strict column matching is enforced for column names, count, order, and data type, 
    /// i.e., the two DataFrame instances must have the same schema.
    /// 
    /// The updated `self` DataFrame is returned; `rbind` takes ownership of `other` and consumes it.
    pub fn rbind(mut self, other: DataFrame) -> Self {
        Column::check_col_names_match(&self.col_names, &other.col_names, "rbind_ref");
        self.rbind_add_rows(&other);
        self.status.reset();
        self.row_index.reset();
        self
    }    

    /* -----------------------------------------------------------------------------
    rbind by reference
    ----------------------------------------------------------------------------- */
    /// Bind all rows of the calling DataFrame (`self`) and an another DataFrame (`other`).
    /// Unlike `rbind()`, DataFrames are passed by reference and not altered or consumed.
    /// A new DataFrame is created by copying all rows from `self` and `other`. See `rbind()`
    /// for other restrictions.
    pub fn rbind_ref(&self, other: &DataFrame) -> Self {
        Column::check_col_names_match(&self.col_names, &other.col_names, "rbind_ref");
        let mut df_dst = DataFrame::from_schema(self);
        df_dst.rbind_add_rows(self);
        df_dst.rbind_add_rows(other);
        df_dst
    }

    // add rows from one DataFrame to another over all columns
    fn rbind_add_rows(&mut self, df_src: &DataFrame) {
        self.columns.iter_mut()
            .for_each(|(col_name, to_col)| {
                let from_col = df_src.columns.get(col_name).unwrap();
                if self.col_types[col_name] != df_src.col_types[col_name] {
                    throw!("DataFrame::rbind error: column {col_name} data types do not match.");
                }
                to_col.extend(from_col); // row data is always copied (ownership cannot be transferred)
            });
        self.n_row += df_src.n_row;
    }
}
