//! Implement DataFrame cbind methods.

// dependencies
use super::{DataFrame, Column, DataFrameSlice};
use crate::data_frame::r#trait::DataFrameTrait;

impl DataFrame {
    /* -----------------------------------------------------------------------------
    cbind owned DataFrames
    ----------------------------------------------------------------------------- */
    /// Append all columns of an another DataFrame (`other`) to the calling DataFrame (`self`).
    /// Strict row matching is enforced for row count, i.e., the two DataFrame instances must 
    /// have compatible row counts, and to prevent column name collisions between self and other.
    /// 
    /// The updated `self` DataFrame is returned; `cbind` takes ownership of `other` and consumes it.
    pub fn cbind(mut self, mut other: DataFrame) -> Self {
        if self.is_empty() { other } // no columns to bind
        else if other.is_empty() { self } 
        else {
            self.check_cbind_compatibility(&mut other);
            for (col_name, from_col) in other.columns {
                Column::check_col_name_collision(&self, &col_name, "cbind");
                self.columns.insert(col_name.clone(), from_col); // column data IS NOT cloned
                self.col_types.insert(col_name.clone(), other.col_types[&col_name].clone());
            }
            self.col_names.extend(other.col_names.clone()); // maintains column order
            self.n_col += other.n_col;
            DataFrame::touch(&mut self);
            self
        }
        // cbind cannot alter the sort/group status since self columns are unchanged
    }

    // check cbind row numbers to support recycling
    fn check_cbind_compatibility(&mut self, other: &mut DataFrame) {
        if self.n_row == 0 && other.n_row > 0 || // adding to a DataFrame with columns but no rows, recycle None as needed
           self.n_row == 1 && other.n_row > 1 {  // adding to a DataFrame one row, recycle df column Option<T> as needed
            for col in self.columns.values_mut() {
                col.recycle(self.n_row, other.n_row);
            }
            self.n_row = other.n_row;
        } else if other.n_row == 0 && self.n_row > 0 || // adding empty column(s) to a DataFrame with row data, recycle None
                  other.n_row == 1 && self.n_row > 1 {  // adding single-row column(s) to a DataFrame with multiple rows, recycle incoming Option<T>
            for col in other.columns.values_mut() {
                col.recycle(other.n_row, self.n_row);
            }
            other.n_row = self.n_row;
        } else { // both DataFrames have multiple rows, which must match exactly (multiples recycling not supported)
            Column::check_n_row_equality(self.n_row, other.n_row, "cbind");
        }
    }

    /* -----------------------------------------------------------------------------
    cbind by reference
    ----------------------------------------------------------------------------- */
    /// Bind all columns of the calling DataFrame (`self`) and an another DataFrame (`other`).
    /// Unlike `cbind()`, DataFrames are passed by reference and not altered or consumed.
    /// A new DataFrame is created by copying all columns from `self` and `other`. See `cbind()`
    /// for other restrictions.
    pub fn cbind_ref(&self, other: &DataFrame) -> Self {
        let mut df_dst = DataFrame::new();
        df_dst.cbind_ref_copy_cols(self);
        df_dst.cbind_ref_copy_cols(other);
        df_dst.n_row = self.n_row.max(other.n_row);
        DataFrame::touch(&mut df_dst);
        df_dst
    }

    // Copy all columns of an another DataFrame (`df_src`) to the calling DataFrame (`self`).
    fn cbind_ref_copy_cols(&mut self, df_src: &DataFrame) {
        for (col_name, from_col) in &df_src.columns {
            let mut from_col = from_col.clone(); // column data IS cloned
            self.check_cbind_ref_compatibility(df_src.n_row, &mut from_col);
            self.columns.insert(col_name.clone(), from_col); 
            self.col_types.insert(col_name.clone(), df_src.col_types[col_name].clone());
        }
        self.col_names.extend(df_src.col_names.clone());
        self.n_col += df_src.n_col;
    }

    // check cbind_ref row numbers to support recycling
    fn check_cbind_ref_compatibility(&mut self, df_src_n_row: usize, from_col: &mut Column) {
        if self.is_empty() { // incoming column is the first column
            self.n_row = df_src_n_row;
        } else if self.n_row == 0 && df_src_n_row > 0 || // adding to a DataFrame with columns but no rows, recycle None as needed
                  self.n_row == 1 && df_src_n_row > 1 {  // adding to a DataFrame one row, recycle df column Option<T> as needed
            for col in self.columns.values_mut() {
                col.recycle(self.n_row, df_src_n_row);
            }
            self.n_row = df_src_n_row;
        } else if df_src_n_row == 0 && self.n_row > 0 || // adding empty column to a DataFrame with row data, recycle None
                  df_src_n_row == 1 && self.n_row > 1 {  // adding single-row column to a DataFrame with multiple rows, recycle incoming Option<T>
            from_col.recycle(df_src_n_row, self.n_row);
        } else { // both DataFrame and incoming column have multiple rows, which must match exactly (multiples recycling not supported)
            Column::check_n_row_equality(self.n_row, df_src_n_row, "cbind_ref");
        }
    }

    /* -----------------------------------------------------------------------------
    cbind by a DataFrameSlice
    ----------------------------------------------------------------------------- */
    /// Bind all columns of the calling DataFrame (`self`) and a DataFrameSlice (`other`).
    /// Consumes and returns self, uses other by immutable reference.
    pub fn cbind_slice(mut self, df_src: &DataFrameSlice) -> Self {
        for col_name in df_src.col_names() {
            // cascades to add_col which handles recycling
            Column::copy_slice_rows(df_src, &mut self, col_name); // column data IS cloned
        }
        self
    }
}
