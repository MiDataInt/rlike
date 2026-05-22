//! The `rlike::data_frame::trait` module supports DataFrames and 
//! DataFrameSlices with a common set of methods for accessing 
//! DataFrame metadata and data.

// dependencies
use std::collections::HashMap;
use super::{query::QueryStatus, DataFrame, DataFrameSlice};
use crate::data_frame::column::get::ColVec;

/// A trait for DataFrame and DataFrameSlice to provide a common interface
/// for accessing DataFrame metadata and data.
/// 
/// Many methods are shared because DataFrameSlice is an immutable reference 
/// to a subset of the rows of a DataFrame.
pub trait DataFrameTrait<'a>{
    /* -----------------------------------------------------------------------------
    DataFrame[Slice] metadata getters
    ----------------------------------------------------------------------------- */
    /// Get the number of rows in a DataFrame[Slice].
    fn n_row(&self) -> usize;

    /// Get the number of columns in a DataFrame[Slice].
    fn n_col(&self) -> usize;

    /// Get the names of the columns in a DataFrame[Slice].
    fn col_names(&self) -> &Vec<String>;

    /// Get the types of the columns in a DataFrame[Slice].
    fn col_types(&self) -> &HashMap<String, String>;

    /// Determine if a DataFrame[Slice] is empty, i.e., has no rows and no columns.
    fn is_empty(&self) -> bool;

    /// Determine if a DataFrame[Slice] has a column schema with at least one data row.
    fn has_rows(&self) -> bool;

    /// Get the row sort and group status of a DataFrame[Slice].
    fn status(&self) -> &QueryStatus;

    /* -----------------------------------------------------------------------------
    DataFrameSlice data getters
    ----------------------------------------------------------------------------- */
    /// Return a column of a DataFrameSlice by column name as values copied into Vec<Option<T>>.
    fn get<T: Clone>(&self, col_name: &str) -> Vec<Option<T>>
    where Vec<Option<T>>: ColVec;
    
    /// Return a column of a DataFrameSlice by column name as an immutable reference.
    fn get_ref<T>(&self, col_name: &str) -> &'_ [Option<T>]
    where Vec<Option<T>>: ColVec;
}

// implement DataFrameTrait for DataFrame
impl DataFrameTrait<'_> for DataFrame {

    // DataFrame metadata getters
    fn n_row(&self) -> usize { self.n_row }
    fn n_col(&self) -> usize { self.n_col }
    fn col_names(&self) -> &Vec<String> { &self.col_names }
    fn col_types(&self) -> &HashMap<String, String> { &self.col_types }
    fn is_empty(&self)  -> bool { self.n_row == 0 && self.n_col == 0 }
    fn has_rows(&self)  -> bool { self.n_row > 0 }
    fn status(&self) -> &QueryStatus { &self.status }

    // DataFrame data getters
    fn get<T: Clone>(&self, col_name: &str) -> Vec<Option<T>> 
    where Vec<Option<T>>: ColVec {
        self.get_ref(col_name).to_vec()
    }
    fn get_ref<'a, T>(&'a self, col_name: &str) -> &'a [Option<T>] 
    where Vec<Option<T>>: ColVec {
        let col = self.get_column(col_name, "get_col_data");
        col.get_ref(col_name)
    }
}

// implement DataFrameTrait for DataFrame
impl DataFrameTrait<'_> for DataFrameSlice<'_> {

    // DataFrameSlice metadata getters
    fn n_row(&self) -> usize { self.n_row }
    fn n_col(&self) -> usize { self.data_frame.n_col } // column schema always matches the parent DataFrame
    fn col_names(&self) -> &Vec<String> { &self.data_frame.col_names }
    fn col_types(&self) -> &HashMap<String, String> { &self.data_frame.col_types }
    fn is_empty(&self)  -> bool { self.n_row == 0 && self.data_frame.n_col == 0 }
    fn has_rows(&self)  -> bool { self.n_row > 0 }
    fn status(&self) -> &QueryStatus { &self.data_frame.status }

    // DataFrameSlice data getters
    fn get<T: Clone>(&self, col_name: &str) -> Vec<Option<T>> 
    where Vec<Option<T>>: ColVec {
        self.get_ref(col_name).to_vec()
    }
    fn get_ref<T>(&self, col_name: &str) -> &'_ [Option<T>] 
    where Vec<Option<T>>: ColVec {
        &self.data_frame.get_ref(col_name)[self.start_row_i..self.end_row_i]
    }
}
