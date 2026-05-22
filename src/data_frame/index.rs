//! The `rlike::data_frame::index` module supports DataFrame indexing, i.e., 
//! establishing row keys from one or more columns to support key-based
//! retrieval of DataFrameSlices.

// dependencies
use std::collections::HashMap;
use rayon::prelude::*;
use super::{Column, DataFrame, DataFrameSlice, DataFrameTrait, Query};
use crate::data_frame::key::{RowKey2, RowKey4, RowKey8};
use crate::throw;

/* -----------------------------------------------------------------------------
row key macros
----------------------------------------------------------------------------- */
// create a fn for setting row keys for all rows based on the number of columns
macro_rules! set_grp_keys_n {
    ($set_grp_keys_n:ident, $grp_keys_n:ident, $row_key:ident) => {
        fn $set_grp_keys_n(df: &mut DataFrame, key_cols: Vec<String>) {
            df.row_index.$grp_keys_n = $row_key::init_vec(df.n_row());
            for j in 0..key_cols.len() {
                let col_name = key_cols[j].clone();
                Column::$set_grp_keys_n(df, &col_name, j);
            }
        }
    }
}
// create a fn for creating a row key hash map based on the number of columns
macro_rules! set_grp_map_n {
    (
        $set_grp_keys_n:ident, $grp_keys_n:ident, 
        $set_grp_map_n:ident, $grp_map_n:ident, $n_bytes:expr
    ) => {
        fn $set_grp_map_n(df: &mut DataFrame, key_cols: Vec<String>) {
            Self::$set_grp_keys_n(df, key_cols); // use row_index field as temporary key store
            df.row_index.$grp_map_n = df.row_index.$grp_keys_n
                .par_iter()
                .enumerate()
                .collect::<Vec<(usize, &[u8; $n_bytes])>>()
                .par_chunk_by(|a, b| a.1 == b.1)
                .map(|grp| (*grp[0].1, (grp[0].0, grp.len())) )
                .collect();
            df.row_index.$grp_keys_n.clear(); // clear the temporary key store
        }
    }
}

/* -----------------------------------------------------------------------------
RowIndexType enum definition
----------------------------------------------------------------------------- */
/// Enum to declare the type of a RowIndex, i.e., how it is stored and used.
pub enum RowIndexType {
    None,
    Sorted,
    Hashed,
}

/* -----------------------------------------------------------------------------
RowIndex structure definition
----------------------------------------------------------------------------- */
/// A RowIndex stores information on the current indexing state of a DataFrame.
pub struct RowIndex {
    // the names of the columns used to create the current index
    pub key_cols:   Vec<String>, 
    // flag that, together with key_cols.len(), dictate which field carries the current index
    pub index_type: RowIndexType,
    // ordered, unique row keys at different key levels
    // searched by binary search when the DataFrame is known to be both sorted and aggregated by key_cols
    pub grp_keys_1: Vec<[u8; 9]>,
    pub grp_keys_2: Vec<[u8; 18]>,
    pub grp_keys_4: Vec<[u8; 36]>,
    pub grp_keys_8: Vec<[u8; 72]>,
    // the mapping of grouping keys at different key levels to contiguous row indices
    // used for hashed lookups when the DataFrame is not sorted or when multiple rows may match a key_col
    pub grp_map_1:  HashMap<[u8; 9],  (usize, usize)>,
    pub grp_map_2:  HashMap<[u8; 18], (usize, usize)>,
    pub grp_map_4:  HashMap<[u8; 36], (usize, usize)>,
    pub grp_map_8:  HashMap<[u8; 72], (usize, usize)>,
}
impl RowIndex {
    /* -----------------------------------------------------------------------------
    RowIndex constructor
    ----------------------------------------------------------------------------- */
    /// Create a new empty RowIndex.
    pub fn new() -> Self {
        Self {
            key_cols:    Vec::new(), 
            index_type:  RowIndexType::None,
            grp_keys_1:  Vec::new(),
            grp_keys_2:  Vec::new(),
            grp_keys_4:  Vec::new(),
            grp_keys_8:  Vec::new(),
            grp_map_1:   HashMap::new(),
            grp_map_2:   HashMap::new(),
            grp_map_4:   HashMap::new(),
            grp_map_8:   HashMap::new(),
        }   
    }
    /// Purge the stored index from a DataFrame.
    pub fn reset(&mut self) {
        self.key_cols.clear();
        self.index_type = RowIndexType::None;
        self.grp_keys_1.clear();
        self.grp_keys_2.clear();
        self.grp_keys_4.clear();
        self.grp_keys_8.clear();
        self.grp_map_1.clear();
        self.grp_map_2.clear();
        self.grp_map_4.clear();
        self.grp_map_8.clear();
    }

    /// Check if a DataFrame's index key columns include any of a list of columns.
    /// If so, reset the index to the default state.
    /// 
    /// Input column names are typically those whose values just changed and that 
    /// therefore may invalidate the current index.
    pub fn reset_if_changed(&mut self, col_names: Vec<String>) {
        if self.key_cols.iter().any(|x| col_names.contains(x)) {
            self.reset();
        }
        // otherwise, df remains indexed as before despite changes to other columns
    }
    /* -----------------------------------------------------------------------------
    RowIndex indexing (prior to retrieval)
    ----------------------------------------------------------------------------- */
    fn set_grp_keys_1(df: &mut DataFrame, key_cols: Vec<String>) {
        df.row_index.grp_keys_1 = Column::get_grp_keys_col(df, &key_cols[0]);
    }
    set_grp_keys_n!(set_grp_keys_2, grp_keys_2, RowKey2);
    set_grp_keys_n!(set_grp_keys_4, grp_keys_4, RowKey4);
    set_grp_keys_n!(set_grp_keys_8, grp_keys_8, RowKey8);
    set_grp_map_n!(set_grp_keys_1, grp_keys_1, set_grp_map_1, grp_map_1, 9);
    set_grp_map_n!(set_grp_keys_2, grp_keys_2, set_grp_map_2, grp_map_2, 18);
    set_grp_map_n!(set_grp_keys_4, grp_keys_4, set_grp_map_4, grp_map_4, 36);
    set_grp_map_n!(set_grp_keys_8, grp_keys_8, set_grp_map_8, grp_map_8, 72);
    
    /// Prepare a DataFrame for indexed row retrieval by creating a row index
    /// on one or more key columns.
    pub fn set_index(mut df: DataFrame, key_cols: Vec<String>) -> DataFrame {
        let idx = &mut df.row_index;

        // check the name and number of requested key columns
        for key_col in &key_cols {
            if !df.col_names.contains(key_col) {
                throw!("DataFrame::set_index error: column {} not found in DataFrame.", key_col);
            }
        }
        let n_key_cols = key_cols.len();
        if n_key_cols > 8 {
            throw!("DataFrame::set_index error: too many key columns, up to eight columns are supported.");
        }

        // determine the current status of the DataFrame
        let is_sorted_by = df.status.is_sorted_by(&key_cols, &vec![false; n_key_cols]); // negated sorts not yet supported
        let is_grouped_by = df.status.is_grouped_by(&key_cols);
        let is_aggregated_by = df.status.is_aggregated_by(&key_cols);
        let key_cols_match = idx.key_cols == key_cols;

        // if the DataFrame is already properly sorted to unique rows per group, use binary search
        if is_sorted_by && is_aggregated_by {
            if !key_cols_match || !idx.has_required_grp_keys(n_key_cols) {
                idx.reset();
                idx.key_cols = key_cols.clone();
                idx.index_type = RowIndexType::Sorted;
                     if n_key_cols == 1 { Self::set_grp_keys_1(&mut df, key_cols) } 
                else if n_key_cols == 2 { Self::set_grp_keys_2(&mut df, key_cols) } 
                else if n_key_cols <= 4 { Self::set_grp_keys_4(&mut df, key_cols) }
                                   else { Self::set_grp_keys_8(&mut df, key_cols) }
            }
            df

        // otherwise, if DataFrame is already properly grouped, fall back to a hash map of group keys
        } else if is_grouped_by {
            if !key_cols_match || !idx.has_required_grp_map(n_key_cols) {
                idx.reset();
                idx.key_cols = key_cols.clone();
                idx.index_type = RowIndexType::Hashed;
                     if n_key_cols == 1 { Self::set_grp_map_1(&mut df, key_cols) } 
                else if n_key_cols == 2 { Self::set_grp_map_2(&mut df, key_cols) } 
                else if n_key_cols <= 4 { Self::set_grp_map_4(&mut df, key_cols) }
                                   else { Self::set_grp_map_8(&mut df, key_cols) } 
            }
            df

        // otherwise, sort the DataFrame and call set_index() again on the new DataFrame
        } else {
            let mut qry = Query::new();
            qry.set_sort_cols(&df, key_cols.clone());
            df = qry.execute_select_query(&df);
            Self::set_index(df, key_cols)
        }
    }
    fn has_required_grp_keys(&self, n_key_cols: usize) -> bool {
             if n_key_cols == 1 { self.grp_keys_1.len() > 0 } 
        else if n_key_cols == 2 { self.grp_keys_2.len() > 0 } 
        else if n_key_cols <= 4 { self.grp_keys_4.len() > 0 } 
                           else { self.grp_keys_8.len() > 0 }
    }
    fn has_required_grp_map(&self, n_key_cols: usize) -> bool {
             if n_key_cols == 1 { self.grp_map_1.len() > 0 } 
        else if n_key_cols == 2 { self.grp_map_2.len() > 0 } 
        else if n_key_cols <= 4 { self.grp_map_4.len() > 0 } 
                           else { self.grp_map_8.len() > 0 }
    }
    /* -----------------------------------------------------------------------------
    RowIndex indexed retrieval
    ----------------------------------------------------------------------------- */
    pub fn get_indexed(
        df: &DataFrame, // the indexed DataFrame being queried
        mut dk: DataFrame   // a DataFrame[Slice] whose first (and usually only) row will be used as the key
    ) -> DataFrameSlice<'_> {
        let key_cols = df.row_index.key_cols.clone();
        let n_key_cols = key_cols.len();
        if let RowIndexType::None = df.row_index.index_type {
            throw!("DataFrame::get_indexed error: DataFrame must be indexed using df_index!() prior to indexed retrieval.");
        }
        for col_name in &key_cols {
            if !dk.col_names().contains(col_name) {
                throw!("DataFrame::get_indexed error: key column {} not found in query DataFrame[Slice].", col_name);
            }
        }
             if n_key_cols == 1 { Self::set_grp_keys_1(&mut dk, key_cols) }
        else if n_key_cols == 2 { Self::set_grp_keys_2(&mut dk, key_cols) } 
        else if n_key_cols <= 4 { Self::set_grp_keys_4(&mut dk, key_cols) }
                           else { Self::set_grp_keys_8(&mut dk, key_cols) }
        let mut x: (usize, usize) = (0, 0);
        let df_rows: Option<&(usize, usize)> = match df.row_index.index_type {
            RowIndexType::Sorted => {
                     if n_key_cols == 1 { Self::parse_bs(&mut x, df.row_index.grp_keys_1.binary_search(&dk.row_index.grp_keys_1[0]) ) } 
                else if n_key_cols == 2 { Self::parse_bs(&mut x, df.row_index.grp_keys_2.binary_search(&dk.row_index.grp_keys_2[0]) ) }
                else if n_key_cols <= 4 { Self::parse_bs(&mut x, df.row_index.grp_keys_4.binary_search(&dk.row_index.grp_keys_4[0]) ) }
                                   else { Self::parse_bs(&mut x, df.row_index.grp_keys_8.binary_search(&dk.row_index.grp_keys_8[0]) ) }
            },
            RowIndexType::Hashed => {
                     if n_key_cols == 1 { df.row_index.grp_map_1.get(&dk.row_index.grp_keys_1[0]) }
                else if n_key_cols == 2 { df.row_index.grp_map_2.get(&dk.row_index.grp_keys_2[0]) }
                else if n_key_cols <= 4 { df.row_index.grp_map_4.get(&dk.row_index.grp_keys_4[0]) }
                                   else { df.row_index.grp_map_8.get(&dk.row_index.grp_keys_8[0]) }
            },
            _ => throw!("DataFrame::get_indexed internal error.")
        };
        if let Some((start_row_i, n_row)) = df_rows {
            DataFrameSlice::new(df, *start_row_i, *n_row) // return a pointer to the matching df rows
        } else {
            DataFrameSlice::new(df, 0, 0) // return empty DataFrameSlice when no rows match
        }
    }
    fn parse_bs(x: &mut (usize, usize), result: Result<usize, usize>) -> Option<&(usize, usize)> {
        if let Ok(start_row_i) = result {
            *x = (start_row_i, 1); // return the one matching df row (only one since df was required to be aggregated)
            Some(x)
        } else {
            None 
        }
    }
}
