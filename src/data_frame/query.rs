//! The`rlike::data_frame::query`` module manages and executes DataFrame queries
//! by maintaining a metadata record of current and pending query operations.
//! Queries on executed on methods of the Query type.

// modules
mod select;
mod aggregate;
mod r#do;
mod pivot;

// dependencies
use std::marker::PhantomData;
use rayon::prelude::*;
use crate::data_frame::DataFrame;
use crate::data_frame::column::Column;
use crate::data_frame::key::{RowKey2, RowKey4, RowKey8};
use crate::data_frame::r#trait::DataFrameTrait;
use crate::throw;

/* -----------------------------------------------------------------------------
row key macros
----------------------------------------------------------------------------- */
// create a fn for setting row keys for all rows based on the number of columns
macro_rules! set_row_keys_n {
    ($set_row_keys_n:ident, $row_keys_n:ident, $row_key:ident) => {
        /// Set query row keys for different key column counts.
        pub fn $set_row_keys_n(&mut self, df_src: &DataFrame, cols: Vec<String>, _: bool) {
            self.$row_keys_n = $row_key::init_vec(self.i_map.len());
            for j in 0..cols.len() { // either sort_cols or group_cols
                let col_name = cols[j].clone();
                let negate = self.sort_negates[j]; // used for both sort and group, since group is subset of sort
                Column::$set_row_keys_n(df_src, self, &col_name, negate, j);
            }
            self.row_key_cols = cols.clone();
        }
    }
}

/* -----------------------------------------------------------------------------
Query status metadata, one instance per DataFrame instance; holds the current 
sort and group state, i.e., the result of previous query operation(s).
----------------------------------------------------------------------------- */
/// QueryStatus is a metadata record of the current sort and group state of a DataFrame instance.
#[derive(Clone, Debug)]
pub struct QueryStatus {
    // row sorting
    pub sort_cols:          Vec<String>,
    pub sort_negates:       Vec<bool>,
    pub is_sorted:          bool,
    // row grouping
    pub group_cols:         Vec<String>,
    pub is_grouped:         bool, // independent of is_sorted: can be sorted, grouped, both, or neither
    // row aggregation
    pub agg_cols:           Vec<String>,
    pub is_aggregated:      bool, // dependent on is_grouped: if true, there is one row per group
}
impl QueryStatus {
    /// Create a new QueryStatus instance.
    pub fn new() -> Self {
        Self {
            // row sorting
            sort_cols:  Vec::new(),
            sort_negates: Vec::new(),
            is_sorted:  false,
            // row grouping
            group_cols: Vec::new(),
            is_grouped: false,
            // row aggregation
            agg_cols:   Vec::new(),
            is_aggregated: false,
        }
    }
    /// Reset the query status to the default state when row order or sort columsn are changed.
    pub fn reset(&mut self) {
        self.sort_cols.clear();
        self.sort_negates.clear();
        self.is_sorted = false;
        self.group_cols.clear();
        self.is_grouped = false;
        self.agg_cols.clear();
        self.is_aggregated = false;
    }
    /// Transfer the query sorting and grouping to the new columnar data instance status.
    pub fn transfer_sort_group_status(&mut self, qry: &Query) {
        // row sorting
        self.sort_cols    =  qry.sort_cols.clone();
        self.sort_negates =  qry.sort_negates.clone();
        self.is_sorted    = !self.sort_cols.is_empty();
        // row grouping
        self.group_cols   =  if qry.group_cols.is_empty() { qry.sort_cols.clone() } else { qry.group_cols.clone() };
        self.is_grouped   = !self.group_cols.is_empty() || self.is_sorted;
        // row aggregation
        self.agg_cols     =  qry.agg_cols.clone();
        self.is_aggregated = self.is_grouped && !self.agg_cols.is_empty();
    }
    /// Determine if a DataFrame is currently sorted by the specified columns.
    pub fn is_sorted_by(&self, col_names: &[String], negates: &[bool]) -> bool {
        self.is_sorted &&
        self.sort_cols.starts_with(col_names) &&
        self.sort_negates.starts_with(negates)
    }
    /// Determine if a DataFrame is currently grouped by the specified columns.
    pub fn is_grouped_by(&self, col_names: &[String]) -> bool {
        self.is_grouped &&
        self.group_cols.starts_with(col_names)
    }
    /// Determine if a DataFrame is currently grouped and aggregated by the specified grouping columns.
    pub fn is_aggregated_by(&self, col_names: &[String]) -> bool {
        self.is_aggregated &&
        self.group_cols.starts_with(col_names) // group_cols, not agg_cols
    }
    /// Check if a DataFrame's sort or group columns include any of a list of columns.
    /// If so, reset the status to the default state.
    /// 
    /// Input column names are typically those whose values just changed and that 
    /// therefore may invalidate the current sort/group status.
    pub fn reset_if_changed(&mut self, col_names: Vec<String>) {
        // sort_cols includes group_cols, no need to check group_cols separately
        if self.sort_cols.iter().any(|x| col_names.contains(x)) {
            self.reset();
        }
        // otherwise, df remains sorted and grouped as before despite changes to other columns
    }
}

/* -----------------------------------------------------------------------------
Query metadata, one instance per pending DataFrame query.
----------------------------------------------------------------------------- */
/// Query is a metadata record of the pending query state to be applied to a DataFrame instance.
#[derive(Debug, Clone)]
pub struct Query<'a> {
    // row filtering
    pub kept_rows:          Vec<bool>, 
    pub has_been_filtered:  bool,
    pub needs_filtering:    bool,
    // column selection
    pub select_cols:        Vec<String>,
    pub has_been_selected:  bool,
    pub needs_selecting:    bool,
    // row sorting
    pub sort_cols:          Vec<String>, // the pending sort state of the data frame
    pub sort_negates:       Vec<bool>, // true if column is to be sorted in descending order
    pub is_sorted_query:    bool, // true if sort() has been called, with or without column names
    pub has_been_sorted:    bool, // true if sort(col, ...) has been called
    pub sorts_match:        bool, // correspondence between QueryStatus::sort_cols and Query::sort_cols
    pub needs_sorting:      bool,
    // row grouping
    pub group_cols:         Vec<String>, // the pending group state of the data frame
    pub has_been_grouped:   bool,
    // row aggregation
    pub agg_cols:           Vec<String>,
    pub has_agg_cols:       bool,
    // temporary flags and buffers while assembling group_by query
    pub rows_are_changing:  bool,
    phantom:                PhantomData<&'a Vec<(usize, usize, usize)>>,
    pub i_map:              Vec<(usize, usize, usize)>,
    pub row_keys_1:        Vec<[u8; 9]>, // used for sort first, then possibly updated for grouping
    pub row_keys_2:        Vec<[u8; 18]>,
    pub row_keys_4:        Vec<[u8; 36]>,
    pub row_keys_8:        Vec<[u8; 72]>,
    pub row_key_cols:      Vec<String>,
}
impl<'a> Query<'a> {
    /* -----------------------------------------------------------------------------
    Query constructors
    ----------------------------------------------------------------------------- */
    /// Create a new Query instance.
    pub fn new() -> Self {
        Self {
            // row filtering
            kept_rows:          Vec::new(),
            has_been_filtered:  false,
            needs_filtering:    false,
            // column selection
            select_cols:        Vec::new(),
            has_been_selected:  false,
            needs_selecting:    false,
            // row sorting
            sort_cols:          Vec::new(),
            sort_negates:       Vec::new(),
            is_sorted_query:    false,
            has_been_sorted:    false,
            sorts_match:        false,
            needs_sorting:      false,
            // row grouping
            group_cols:         Vec::new(),
            agg_cols:           Vec::new(),
            has_been_grouped:   false,
            has_agg_cols:       false,
            // aggregate flag
            rows_are_changing:  false,
            // temporary buffers while assembling group_by query
            phantom:            PhantomData,
            i_map:              Vec::new(),
            row_keys_1:         Vec::new(),
            row_keys_2:         Vec::new(),
            row_keys_4:         Vec::new(),
            row_keys_8:         Vec::new(),
            row_key_cols:       Vec::new(),
        }
    }
    /* -----------------------------------------------------------------------------
    Set Query fields during query assembly
    ----------------------------------------------------------------------------- */
    /// Check filter status of a columnar data instance; reset on first 
    /// filter statement in a query sequence.
    pub fn check_filter_status(&mut self, n_row: usize) {
        if self.is_sorted_query {
            throw!("DataFrame error: query filter() must be called before sort().")
        }
        if !self.has_been_filtered { // false on first filter call of query
            if self.kept_rows.len() == n_row { // reset all kept_row bool to true without allocation if possible
                self.kept_rows.fill(true);
            } else if self.kept_rows.capacity() > n_row {
                if self.kept_rows.len() > n_row {
                    self.kept_rows.truncate(n_row);
                    self.kept_rows.fill(true);
                } else {
                    self.kept_rows.fill(true);
                    self.kept_rows.resize(n_row, true);
                }
            } else {
                self.kept_rows = vec![true; n_row];
            }
        } else {
            throw!("DataFrame error: filter() should only be called once per query sequence.")
        }
        self.has_been_filtered = true;
    }    
    /// Set query select columns as a list of desired output columns
    pub fn set_select_cols(&mut self, df: &DataFrame, col_names: Vec<String>) {
        if self.has_been_selected {
            throw!("DataFrame error: select()/drop() should only be called once per query sequence.")
        }
        if !col_names.is_empty() {
            for col_name in col_names {
                if let Some(_) = df.columns.get(&col_name) {
                    self.select_cols.push(col_name);
                } else {
                    throw!("DataFrame::select() error: column {col_name} not found.");
                }
            }
            self.has_been_selected = true;
        }
    }
    /// Set query select columns as a list of columns to omit from the output.
    pub fn set_drop_cols(&mut self, df: &DataFrame, col_names: Vec<String>) {
        if self.has_been_selected {
            throw!("DataFrame error: select()/drop() should only be called once per query sequence.")
        }
        if !col_names.is_empty() {
            for col_name in &col_names {
                if let Some(_) = df.columns.get(col_name) {
                    // do nothing yet
                } else {
                    throw!("DataFrame::drop() error: column {col_name} not found.");
                }
            }
            self.select_cols = df.col_names.clone();
            for (i, col_name) in df.col_names.iter().enumerate().rev() {
                if col_names.contains(col_name) {
                    self.select_cols.remove(i); // in reverse order to ensure match integrity
                }
            }
            self.has_been_selected = true;
        }
    }
    /// Set query sort columns.
    pub fn set_sort_cols(&mut self, df: &DataFrame, col_names: Vec<String>) {
        if self.is_sorted_query {
            throw!("DataFrame error: sort() should only be called once per query sequence.")
        }
        self.is_sorted_query = true;
        if col_names.is_empty() {
            return ();
        }
        if col_names.len() > 8 {
            throw!("DataFrame error: sort() can currently sort up to 8 columns.")
        }
        self.has_been_sorted = true;
        self.sort_negates = Query::get_negation(&col_names);
        self.sort_cols = Query::remove_negation(&col_names);
        if df.status.sort_cols.starts_with(&self.sort_cols) && 
           df.status.sort_negates.starts_with(&self.sort_negates) {
            self.sorts_match = true; 
            return;
        }
        for col_name in &self.sort_cols {
            if let Some(_) = df.columns.get(col_name) {
                // do nothing yet
            } else {
                throw!("DataFrame::sort() error: column {col_name} not found.");
            }
        }
        self.sorts_match = false;
    }
    /// Set grouping columns of a columnar data instance.
    pub fn set_grouping_cols(&mut self, df: &DataFrame, group_cols: Vec<String>, agg_cols: Vec<String>) {
        if self.has_been_grouped {
            throw!("DataFrame error: group() should only be called once per query sequence.")
        }
        if group_cols.is_empty() {
            throw!("DataFrame error: query group() requires at least one column name.")
        }
        if group_cols.len() > 8 {
            throw!("DataFrame error: group() can currently group up to 8 columns.")
        }
        self.has_been_grouped = true;
        self.group_cols = group_cols;
        self.agg_cols = agg_cols;
        self.has_agg_cols = !self.agg_cols.is_empty();

        // Caller specified sort(col, ...) for this group_by query, check it against group_cols and prior sort
        let group_negates = Query::get_negation(&self.group_cols);
        let group_cols_clean = Query::remove_negation(&self.group_cols);
        if self.has_been_sorted {
            if !self.sort_cols.starts_with(&group_cols_clean){
                throw!("DataFrame error: group() columns must match sort() columns in order.");
            }
            if df.status.is_sorted && df.status.sort_cols.starts_with(&self.sort_cols) {
                self.sorts_match = true;
            }

        // Caller did not specify sort(col, ...) for this group_by query, so either...
        } else {
            // ... we can rely on the previous sort state of the data frame ...
            if df.status.is_sorted && 
               df.status.sort_cols.starts_with(&group_cols_clean) &&
               df.status.sort_negates.starts_with(&group_negates) {
                self.sort_cols = df.status.sort_cols.clone();
                self.sort_negates = df.status.sort_negates.clone();
                self.is_sorted_query = true;
                self.has_been_sorted = true;
                self.sorts_match = true;
            }
            // ... or this is an empty sort() or unsorted grouping operation, nothing to do
        }
        self.group_cols = group_cols_clean;
    }
    fn get_negation(col_names: &Vec<String>) -> Vec<bool> {
        col_names.iter()
            .map(|col_name| col_name.starts_with("_"))
            .collect::<Vec<_>>()
    }
    fn remove_negation(col_names: &Vec<String>) -> Vec<String> {
        col_names.iter()
        .map(|col_name| {
            if col_name.starts_with('_') { col_name[1..].to_string() } else { col_name.to_string()}
        }).collect::<Vec<_>>()
    }
    /* -----------------------------------------------------------------------------
    Query calculations during query execution
    ----------------------------------------------------------------------------- */
    /// Set query row keys for different key column counts.
    pub fn set_row_keys_1(&mut self, df_src: &DataFrame, cols: Vec<String>, negate: bool) {
        let col_name = &cols[0];
        self.row_keys_1 = Column::get_cell_keys_col(df_src, self, col_name, negate);
        self.row_key_cols = vec![col_name.to_string()];
    }
    set_row_keys_n!(set_row_keys_2, row_keys_2, RowKey2);
    set_row_keys_n!(set_row_keys_4, row_keys_4, RowKey4);
    set_row_keys_n!(set_row_keys_8, row_keys_8, RowKey8);
    /* -----------------------------------------------------------------------------
    Query post-execution actions
    ----------------------------------------------------------------------------- */
    /// Set aggregate query status flags after processing df_query!() macro before executing the query.
    pub fn set_aggregate_status_flags(&mut self, df: &DataFrame) {
        let has_data = df.n_row() > 0;
        self.needs_filtering = has_data && self.kept_rows.par_iter().any(|kept| !kept);
        self.needs_sorting   = has_data && self.is_sorted_query && !self.sorts_match;
        self.rows_are_changing = self.needs_filtering || self.needs_sorting;
    }
}
