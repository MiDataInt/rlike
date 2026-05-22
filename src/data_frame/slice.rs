//! The `rlike::data_frame::slice` module supports DataFrame slices, i.e., 
//! an immutable reference to a contiguous subset of the rows of a DataFrame, 
//! to support read-only function calls for passing and examining a subset 
//! of DataFrame rows.

// dependencies
use super::{DataFrame, Column};

/* -----------------------------------------------------------------------------
DataFrameSlice structure definition.
----------------------------------------------------------------------------- */
/// A DataFrameSlice is an immutable reference to a DataFrame with metadata 
/// defining a slice of contiguous rows.
pub struct DataFrameSlice<'a> {
    pub data_frame: &'a DataFrame,
    pub start_row_i: usize, // 0-based index of the first slice row
    pub end_row_i:   usize, // 1-based index of the last slice row
    pub n_row:       usize,
}
impl <'a> DataFrameSlice<'a> {
    /* -----------------------------------------------------------------------------
    DataFrameSlice constructor
    ----------------------------------------------------------------------------- */
    /// Create a DataFrameSlice from a DataFrame and a range of rows.
    pub fn new(data_frame: &'a DataFrame, start_row_i: usize, n_row: usize) -> Self {
        Self { 
            data_frame, 
            start_row_i, 
            end_row_i: start_row_i + n_row,
            n_row 
        }
    }
    // /* -----------------------------------------------------------------------------
    // DataFrameSlice-specific getters
    // ----------------------------------------------------------------------------- */
    /// Copy all columns of a DataFrameSlice into a new DataFrame.
    pub fn to_df(&self) -> DataFrame {
        let mut df_dst = DataFrame::new();
        self.data_frame.col_names.iter().for_each(|col_name| {
            Column::copy_slice_rows(self, &mut df_dst, col_name);
        });
        df_dst
    }
}
