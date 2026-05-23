//! `rlike::data_frame::column::Column` defines the column type enumeration
//! and methods for matching types and dispatching actions on column data.
//! 
//! Column methods generally all entail `match data_type` statements to
//! dispatch column operations to the correct typed action. These create 
//! little overhead since they are only called once per column per operation.

// modules
mod types;
mod deserialize;
pub mod get;
mod query;

// dependencies
use serde::{Serialize, Deserialize};
use rayon::prelude::*;
pub use types::RFactorColumn;
use crate::data_frame::{DataFrame, DataFrameSlice};
use crate::data_frame::query::Query;

/* -----------------------------------------------------------------------------
Column data type enumeration, i.e., all row values for a column as Vec<Option<T>> or custom type
----------------------------------------------------------------------------- */
/// The `Column` enum defines the column type enumeration and methods for matching types.
#[derive(Clone, Serialize, Deserialize)]
pub enum Column {
    RInteger(Vec<Option<i32>>),
    RNumeric(Vec<Option<f64>>),
    RLogical(Vec<Option<bool>>),
    RSize(Vec<Option<usize>>),
    RFactor(RFactorColumn),
    // TODO: implement RDateTime as i64 UTC offset
    RString(Vec<Option<String>>),
}

/* -----------------------------------------------------------------------------
implement column
----------------------------------------------------------------------------- */
impl Column {
    /* -----------------------------------------------------------------------------
    column length and capacity management
    ----------------------------------------------------------------------------- */
    /// Reserve capacity for at least additional more rows to be inserted in a Column.
    pub fn reserve(&mut self, additional: usize) {
        match self {
            Column::RInteger(v)   => v.reserve(additional),
            Column::RNumeric(v)   => v.reserve(additional),
            Column::RLogical(v)  => v.reserve(additional),
            Column::RSize(v)    => v.reserve(additional),
            Column::RFactor(f)       => f.data.reserve(additional),
            Column::RString(v) => v.reserve(additional),
        }
    }
    /// Add bulk row data to a Column, e.g., during rbind operations.
    pub fn extend(&mut self, other: &Column) {
        match (self, other) { 
            (
                Column::RInteger(data), 
                Column::RInteger(other_data)
            ) => data.par_extend(other_data.par_iter().cloned()),
            (
                Column::RNumeric(data), 
                Column::RNumeric(other_data)
            ) => data.par_extend(other_data.par_iter().cloned()),
            (
                Column::RLogical(data), 
                Column::RLogical(other_data)
            ) => data.par_extend(other_data.par_iter().cloned()),
            (
                Column::RSize(data), 
                Column::RSize(other_data)
            ) => data.par_extend(other_data.par_iter().cloned()),
            (
                Column::RFactor(col_factor), 
                Column::RFactor(other_col_factor)
            ) => col_factor.data.par_extend(other_col_factor.data.par_iter().cloned()),
            (
                Column::RString(data), 
                Column::RString(other_data)
            ) => data.par_extend(other_data.par_iter().cloned()),
            _ => {
                panic!("DataFrame::extend error: type mismatch between columns.")
            }
        }
    }
    /// Extend a column by recycling its existing single Option<T> value, or None if currently empty.
    pub fn recycle(&mut self, n_row_in: usize, new_len: usize) {
        if n_row_in > 1 {
            panic!("DataFrame::extend error: cannot recycle a column with more than one row.");
        }
        let is_na = n_row_in == 0;
        match self { 
            Column::RInteger(v)   => v.resize(new_len, if is_na { None } else { v[0] }),
            Column::RNumeric(v)   => v.resize(new_len, if is_na { None } else { v[0] }),
            Column::RLogical(v)  => v.resize(new_len, if is_na { None } else { v[0] }),
            Column::RSize(v)    => v.resize(new_len, if is_na { None } else { v[0] }),
            Column::RFactor(f)       => f.data.resize(new_len, if is_na { None } else { f.data[0] }),
            Column::RString(v) => v.resize(new_len, if is_na { None  } else { v[0].clone() }),
        }
    }
    /* -----------------------------------------------------------------------------
    column and row schema integrity checks
    ----------------------------------------------------------------------------- */
    /// Check that the number of incoming rows matches the number of data rows.
    pub fn check_n_row_equality(nrow_curr: usize, nrow_new: usize, caller: &str) {
        if nrow_curr != nrow_new {
            panic!("DataFrame::{caller} row count mismatch: {nrow_curr} (df) != {nrow_new} (new)");
        }
    }
    /// Ensure uniqueness of incoming column names.
    pub fn check_col_name_collision(df: &DataFrame, col_name: &str, caller: &str) {
        if df.columns.contains_key(col_name) {
            panic!("DataFrame::{caller} error: incoming column {col_name} already exists in data.")
        }
    }
    /// Ensure that the column names match exactly between two DataFrames.
    pub fn check_col_names_match(col_names_curr:  &Vec<String>, col_names_other: &Vec<String>, caller: &str) {
        if col_names_curr != col_names_other {
            panic!("DataFrame::{caller} error: column names do not match: {col_names_curr:?} != {col_names_other:?}");
        }
    }
    // Ensure that a requested row is within the column row length
    pub fn check_i_bound(n_row: usize, row_i: usize, caller: &str) {
        if row_i >= n_row {
            panic!("DataFrame::{caller} error: row index {row_i} out of bounds.");
        }
    }
}
