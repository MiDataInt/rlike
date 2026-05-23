//! `rlike::DataFrame` offers a columnar data structure stored 
//! as HashMaps of named columns, where data are stored as one vector per column. 
//! Different columns can have different enumerated RLike data types. Thus, columns 
//! take the form `HashMap<String, Column>`, where a Column is `Vec<RLike>`, e.g., 
//! `Vec<Option<i32>>`, etc.
//!
//! You are encouraged to use a series of powerful macros to create and manipulate
//! DataFrames, which offer an expressive statement syntax. However, many actions
//! can be accomplished using Rust-like method chaining, albeit with more verbose 
//! code and a greater need to understand the underlying data structures.
//! 
//! See the DataFrame
//! [documentation](https://github.com/MiDataInt/mdi-pipelines-framework/tree/main/crates/mdi/src/rlike/data_frame)
//! and
//! [examples](https://github.com/wilsonte-umich/mdi-pipelines-framework/tree/main/crates/mdi/examples) 
//! for details on supported features and syntax guides. 
//! 
//! # Quick Start - get a new DataFrame up and running
//! ```rust
//! use mdi::data_frame::prelude::*;
//! 
//! // fill a DataFrame as it is created
//! let df = df_new!( 
//!     col1 = vec![1, 2, 3].to_rl(),            // to_rl() converts Vec<T> to Vec<RL>, i.e., Vec<Option<T>>
//!     col2 = vec![Some(1.0), None, Some(3.0)], // None equates to NA/missing values
//!     col3 = vec![Some(true); 3],
//!     col4 = vec![Some(true)],                 // single row value repeated to longest column length
//! );
//! eprint!ln!("DataFrame:\n{}", df); // print a structure summary of the DataFrame
//! 
//! // create a new DataFrame schema with zero rows and a default capacity of 100K rows
//! pub const LABELS: [&str; 2] = ["A", "B"];
//! let df = df_new!(
//!     col1:i32,
//!     col2:f64,
//!     col3:bool
//!     col4:u16[LABELS] // factor column with labels provided at instantiation
//! );
//! 
//! // create a new DataFrame schema with zero rows and a caller-defined capacity
//! let df = df_new!(
//!     capacity = 1000,
//!     col1:i32,
//!     col2:f64,
//!     col3:bool
//! );
//! 
//! // create a new, empty DataFrame, suitable for reading, updating, or binding
//! let df = df_new!();
//! 
//! // populate a DataFrame with an established schema by reading data from a file
//! df_read!(&mut df, "/path/to/data.csv");
//! 
//! // from there, use the `df_set!()`, `df_query!()` `df_cbind!()`, `df_rbind!()`,
//! // `df_join!()`, and other macros to manipulate the DataFrame, and 
//! // the `df_write!()` and `df_save!()` macros to export data in CSV or binary format.
//! ```

// modules
pub mod column;
pub mod macros;
pub mod display;
pub mod bind;
pub mod query;
pub mod key;
pub mod join;
pub mod slice;
pub mod index;
pub mod r#trait;
pub mod prelude;
pub mod io;

// dependencies
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use rayon::prelude::*;
use crate::types::Do;
use column::{Column, get::{ColVec, ColCell}};
use query::{QueryStatus, Query};
use slice::DataFrameSlice;
use index::RowIndex;
use r#trait::DataFrameTrait;

/* -----------------------------------------------------------------------------
DataFrame structure definition; metadata and a set of named Column instances.
----------------------------------------------------------------------------- */
/// A DataFrame is a columnar data structure stored as a HashMap of named Columns.
#[derive(Serialize, Deserialize)]
pub struct DataFrame {
    n_row:         usize,
    n_col:         usize,
    columns:       HashMap<String, Column>,
    col_names:     Vec<String>,
    col_types:     HashMap<String, String>,
    pub status:    QueryStatus,
    pub row_index: RowIndex,
    pub print_max_rows:      usize,
    pub print_max_col_width: usize,
}
impl DataFrame {
    /* -----------------------------------------------------------------------------
    DataFrame constructors and destructors
    ----------------------------------------------------------------------------- */
    /// Create a new, empty DataFrame, to which you subsequently add Columns.
    pub fn new() -> Self {
        Self {
            n_row:      0,
            n_col:      0,
            columns:    HashMap::new(),
            col_names:  Vec::new(),
            col_types:  HashMap::new(),
            status:     QueryStatus::new(),
            row_index:  RowIndex::new(),
            print_max_rows:      20,
            print_max_col_width: 25,
        }
    }
    /// Create a new, empty data frame with the same schema as an existing one.
    pub fn from_schema(df_src: &DataFrame) -> Self {
        let mut df_dst = DataFrame::new();
        df_src.col_names.iter().for_each(|col_name| {
            let col_type = df_src.col_types[col_name].clone();
            Column::add_empty_col(&mut df_dst, col_name, &col_type)
        });
        df_dst
    }
    /// Create a new data frame from one or more rows of an existing DataFrame,
    /// i.e., take a potentially non-contiguous slice of the rows of a DataFrame
    /// by copying values from cells.
    pub fn from_rows(&self, row_i: Vec<usize>) -> Self {
        let mut df_dst = DataFrame::new();
        self.col_names.iter().for_each(|col_name| {
            Column::copy_col_rows(self, &mut df_dst, col_name, &row_i);
        });
        df_dst
    }
    /// Reserve capacity for at least additional more rows to be inserted in a DataFrame. 
    /// 
    /// Calling `df.reserve(usize)` can limit copying and improve performance when you anticipate 
    /// adding many rows to a DataFrame, e.g., through iterative `rbind` operations.
    pub fn reserve(&mut self, additional: usize) {
        for (_, col) in &mut self.columns {
            col.reserve(additional);
        }
    }
    /// Add a new Column to a DataFrame. 
    /// 
    /// Columns are passed in as Vec<RL>, i.e., Vec<Option<T>>, to be owned by the DataFrame.
    /// 
    /// If any existing columns or the new column have zero or one rows, they are recycled 
    /// to the length of the other column(s) using None or the single Option<T> value, respectively. 
    /// Otherwise, the number of rows in the new column must match the number of rows in the 
    /// DataFrame, unless it is currently empty. 
    /// 
    /// All new columns must be named differently than any existing columns to prevent collisions.
    pub fn add_col<T: 'static + Clone>(&mut self, col_name: &str, mut col_data: Vec<Option<T>>) -> &mut Self 
    where Vec<Option<T>>: ColVec {
        let n_row_col = col_data.len();
        if self.is_empty() { // incoming column is the first column
            self.n_row = n_row_col;
        } else if self.n_row == 0 && n_row_col > 0 || // adding to a DataFrame with columns but no rows, recycle None as needed
                  self.n_row == 1 && n_row_col > 1 {  // adding to a DataFrame one row, recycle df column Option<T> as needed
            for col in self.columns.values_mut() {
                col.recycle(self.n_row, n_row_col);
            }
            self.n_row = n_row_col;
        } else if n_row_col == 0 && self.n_row > 0 || // adding empty column to a DataFrame with row data, recycle None
                  n_row_col == 1 && self.n_row > 1 {  // adding single-row column to a DataFrame with multiple rows, recycle incoming Option<T>
            col_data.resize(self.n_row, if n_row_col == 0 { None } else { col_data[0].clone() } );
        } else { // both DataFrame and incoming column have multiple rows, which must match exactly (multiples recycling not supported)
            Column::check_n_row_equality(self.n_row, n_row_col, "add_col");
        }
        let (col_type, col) = col_data.to_col();
        self.add_col_col(col_name, col_type.to_string(), col)
    }
    // internal function to add a column from a pre-assembled, pre-checked Column
    fn add_col_col(&mut self, col_name: &str, col_type: String, col: Column) -> &mut Self{
        self.columns.insert(col_name.to_string(), col);
        self.col_names.push(col_name.to_string()); // keep track of column creation order
        self.col_types.insert(col_name.to_string(), col_type);
        self.n_col += 1;
        self
    }
    /// Like DataFrame::add_col, but replace an existing column if it exists without error or warning.
    pub fn replace_or_add_col<T: 'static + Clone>(&mut self, col_name: &str, col_data: Vec<Option<T>>) 
    where Vec<Option<T>>: ColVec {
        if self.columns.contains_key(col_name) {
            self.status.reset_if_changed(vec![col_name.to_string()]);
            self.row_index.reset_if_changed(vec![col_name.to_string()]);
            let col_names_curr = self.col_names.clone();
            let is_factor = self.col_types[col_name] == "u16";
            let col_labels = if is_factor { self.get_labels(col_name) } else { Vec::new() };
            self.drop_col(col_name);
            self.add_col(col_name, col_data); // updates col_types appropriately
            if is_factor { self.set_labels(col_name, col_labels.iter().map(|x| x.as_str()).collect()); }
            self.col_names = col_names_curr; // thus, retain column order
        } else {
            self.add_col(col_name, col_data);
        }
    }
    /// Remove a Column from a DataFrame based on the column name.
    pub fn drop_col(&mut self, col_name: &str) -> &mut Self {
        if let Some(_col) = self.columns.remove(col_name) {
            self.status.reset_if_changed(vec![col_name.to_string()]);
            self.row_index.reset_if_changed(vec![col_name.to_string()]);
            self.col_names.retain(|name| name != col_name);
            self.col_types.remove(col_name);
            self.n_col -= 1;
        } else {
            panic!("DataFrame::drop_col error: column {col_name} not found.");
        }
        self
    }
    /// Keep only the columns in a DataFrame that are listed in argument `retain_col_names`.
    pub fn retain_cols(&mut self, retain_col_names: Vec<String>) -> &mut Self {
        for col_name in &retain_col_names {
            if !self.col_names.contains(col_name) {
                panic!("DataFrame::retain_cols error: column {col_name} not found.");
            }
        }
        for col_name in &self.col_names {
            if !retain_col_names.contains(col_name) {
                self.status.reset_if_changed(vec![col_name.to_string()]);
                self.row_index.reset_if_changed(vec![col_name.to_string()]);
                self.columns.remove(col_name);
                self.col_types.remove(col_name);
            }
        }
        self.n_col = retain_col_names.len();
        self.col_names = retain_col_names;
        self
    }
    /* -----------------------------------------------------------------------------
    DataFrame special column type support
    ----------------------------------------------------------------------------- */
    /// Establish pre-defined factor labels for an RFactor column.
    pub fn set_labels(&mut self, col_name: &str, labels: Vec<&str>) -> &mut Self {
        if self.col_types[col_name] != "u16" {
            panic!("DataFrame::set_labels error: column {col_name} is not type RFactor/u16.");
        }
        if let Some(col) = self.columns.get_mut(col_name) {
            col.set_labels(labels);
        } else {
            panic!("DataFrame::set_labels error: column {col_name} not found.");
        }
        self
    }
    /// Transfer factor labels for an RFactor column from df_src to self.
    pub fn copy_labels(&mut self, df_src: &Self, col_name: &str) {
        self.set_labels(
            col_name, 
            df_src.get_labels(col_name).iter().map(|s| s.as_str()).collect()
        );
    }
    /// Return the ordered factor labels for an RFactor column.
    pub fn get_labels(&self, col_name: &str) -> Vec<String> {
        if let Some(col) = self.columns.get(col_name) {
            col.get_labels()
        } else {
            panic!("DataFrame::get_labels error: column {col_name} not found.");
        }
    }
    /// Return the factor levels for an RFactor column as a column HashMap.
    pub fn get_levels(&self, col_name: &str) -> HashMap<String, u16> {
        if let Some(col) = self.columns.get(col_name) {
            col.get_levels()
        } else {
            panic!("DataFrame::get_levels error: column {col_name} not found.");
        }
    }
    /* -----------------------------------------------------------------------------
    DataFrame slices as collections of contiguous, read-only Column slices, i.e., a DataFrameSlice
    ----------------------------------------------------------------------------- */
    pub fn slice(&self, start_row_i: usize, n_row: usize) -> DataFrameSlice<'_> {
        DataFrameSlice::new(self, start_row_i, n_row)
    }
    /* -----------------------------------------------------------------------------
    DataFrame column-level getters
    ----------------------------------------------------------------------------- */
    // Ensure that a named Column exists in a DataFrame, and if so, return it.
    // These are internal functions; don't expect users to need to access Column objects.
    fn get_column<'a>(&'a self, col_name: &str, caller: &str) -> &'a Column {
        if let Some(col) = self.columns.get(col_name) {
            col
        } else {
            panic!("DataFrame::{caller} error: column {col_name} not found.")
        }
    }
    fn get_column_mut<'a>(&'a mut self, col_name: &str, caller: &str) -> &'a mut Column {
        if let Some(col) = self.columns.get_mut(col_name) {
            col
        } else {
            panic!("DataFrame::{caller} error: column {col_name} not found.")
        }
    }
    /// Return a mutable reference to the Vec<Option<T>> held in a DataFrame Column by column name.
    pub fn get_ref_mut<'a, T>(&'a mut self, col_name: &str) -> &'a mut Vec<Option<T>> 
    where Vec<Option<T>>: ColVec {
        let col = self.get_column_mut(col_name, "get_col_data_mut");
        col.get_ref_mut(col_name)
    }
    /* -----------------------------------------------------------------------------
    DataFrame column-level setters for `set` actions
    ----------------------------------------------------------------------------- */
    /// Create or update a column from Vec<T> as input (i.e. from zero current columns).
    pub fn set_col_0<
        O: Clone + 'static,
    >(
        &mut self, qry: &Query,
        out_name: &str,
        mut out_col_data: Vec<Option<O>>
    ) where Vec<Option<O>>: ColVec {
        if qry.has_been_filtered {
            let row_defaults = self.get_set_row_defaults(qry, out_name);
            qry.kept_rows.iter().enumerate().for_each(|(i, matches_filter)| {
                if !*matches_filter { out_col_data[i] = row_defaults[i].clone() }
            });
        }
        self.replace_or_add_col(out_name, out_col_data);
    }
    /// Create or update a column from a single column as input.
    pub fn set_col_1<A, O>(
        &mut self, qry: &Query, 
        a_name: &str, out_name: &str,
        na_val: Option<O>, op: impl Fn(&A) -> O + Send + Sync
    ) where 
        A: Copy + Send + Sync,
        O: Copy + Send + Sync + 'static,
        Vec<Option<A>>: ColVec,
        Vec<Option<O>>: ColVec 
    {
        let row_defaults: Vec<Option<O>> = self.get_set_row_defaults(qry, out_name); 
        let out_col_data: Vec<Option<O>> = self.get_ref(a_name)
            .par_iter()
            .enumerate()
            .map(|(i, opt_a)| {
                if qry.has_been_filtered && !qry.kept_rows[i] {
                    row_defaults[i].clone()
                } else {
                    match opt_a { 
                        Some(a) => Some(op(a)), 
                        _ => na_val
                    }
                }
            })
            .collect();
        self.replace_or_add_col(out_name, out_col_data);
    }
    /// Create or update a column from two columns as input.
    pub fn set_col_2<A, B, O>(
        &mut self, qry: &Query, 
        a_name: &str, b_name: &str, out_name: &str,
        na_val: Option<O>, op: impl Fn(&A, &B) -> O + Send + Sync
    ) where 
        A: Copy + Send + Sync,
        B: Copy + Send + Sync,
        O: Copy + Send + Sync + 'static,
        Vec<Option<A>>: ColVec,
        Vec<Option<B>>: ColVec,
        Vec<Option<O>>: ColVec 
    {
        let row_defaults: Vec<Option<O>> = self.get_set_row_defaults(qry, out_name); 
        let out_col_data: Vec<Option<O>> = self.get_ref(a_name)
            .par_iter()
            .zip( self.get_ref(b_name).par_iter() )
            .enumerate()
            .map(|(i, (opt_a, opt_b))| {
                if qry.has_been_filtered && !qry.kept_rows[i] {
                    row_defaults[i].clone()
                } else {
                    match (opt_a, opt_b) {
                        (Some(a), Some(b)) => Some(op(a, b)),
                        _ => na_val
                    }
                }
            })
            .collect();
        self.replace_or_add_col(out_name, out_col_data);
    }
    /// Create or update a column from three columns as input.
    pub fn set_col_3<A, B, C, O>(
        &mut self, qry: &Query, 
        a_name: &str, b_name: &str, c_name: &str, out_name: &str,
        na_val: Option<O>, op: impl Fn(&A, &B, &C) -> O + Send + Sync
    ) where 
        A: Copy + Send + Sync,
        B: Copy + Send + Sync,
        C: Copy + Send + Sync,
        O: Copy + Send + Sync + 'static,
        Vec<Option<A>>: ColVec,
        Vec<Option<B>>: ColVec,
        Vec<Option<C>>: ColVec,
        Vec<Option<O>>: ColVec 
    {
        let row_defaults: Vec<Option<O>> = self.get_set_row_defaults(qry, out_name); 
        let out_col_data: Vec<Option<O>> = self.get_ref(a_name)
            .par_iter()
            .zip( self.get_ref(b_name).par_iter() )
            .zip( self.get_ref(c_name).par_iter() )
            .enumerate()
            .map(|(i, ((opt_a, opt_b), opt_c))| {
                if qry.has_been_filtered && !qry.kept_rows[i] {
                    row_defaults[i].clone()
                } else {
                    match (opt_a, opt_b, opt_c) {
                        (Some(a), Some(b), Some(c)) => Some(op(a, b, c)),
                        _ => na_val
                    }
                }
            })
            .collect();
        self.replace_or_add_col(out_name, out_col_data);
    }
    /// Create or update a column from four columns as input.
    pub fn set_col_4<A, B, C, D, O>(
        &mut self, qry: &Query, 
        a_name: &str, b_name: &str, c_name: &str, d_name: &str, out_name: &str,
        na_val: Option<O>, op: impl Fn(&A, &B, &C, &D) -> O + Send + Sync
    ) where 
        A: Copy + Send + Sync,
        B: Copy + Send + Sync,
        C: Copy + Send + Sync,
        D: Copy + Send + Sync,
        O: Copy + Send + Sync + 'static,
        Vec<Option<A>>: ColVec,
        Vec<Option<B>>: ColVec,
        Vec<Option<C>>: ColVec,
        Vec<Option<D>>: ColVec,
        Vec<Option<O>>: ColVec 
    {
        let row_defaults: Vec<Option<O>> = self.get_set_row_defaults(qry, out_name); 
        let out_col_data: Vec<Option<O>> = self.get_ref(a_name)
            .par_iter()
            .zip( self.get_ref(b_name).par_iter() )
            .zip( self.get_ref(c_name).par_iter() )
            .zip( self.get_ref(d_name).par_iter() )
            .enumerate()
            .map(|(i, (((opt_a, opt_b), opt_c), opt_d))| {
                if qry.has_been_filtered && !qry.kept_rows[i] {
                    row_defaults[i].clone()
                } else {
                    match (opt_a, opt_b, opt_c, opt_d) {
                        (Some(a), Some(b), Some(c), Some(d)) => Some(op(a, b, c, d)),
                        _ => na_val
                    }
                }
            })
            .collect();
        self.replace_or_add_col(out_name, out_col_data);
    }
    // get the default contents of column rows that do not match a set filter
    fn get_set_row_defaults<O: Clone>(
        &self, qry: &Query, 
        out_name: &str
    ) -> Vec<Option<O>> 
    where 
        O: Clone + 'static,
        Vec<Option<O>>: ColVec
    {
        if !qry.has_been_filtered {
            vec![] // nothing to do; thus, copying and allocation only happens for filterered set calls
        } else if self.columns.contains_key(out_name){
            self.get(out_name) // unmatched rows in existing columns pass as is
        } else {
            vec![None; self.n_row] // umatched rows in new columns default to NA
        }
    }
    /* -----------------------------------------------------------------------------
    DataFrame column-level setters for `inject` actions
    ----------------------------------------------------------------------------- */
    /// Create or update a column from single Option<T> as input, recycling as needed.
    pub fn inject_col_const<
        O: Clone + 'static,
    >(
        &mut self,
        out_name: &str, 
        fill_val: Option<O>
    ) where Vec<Option<O>>: ColVec {
        let n_row = if self.n_row() > 1 { self.n_row() } else { 1 };
        let out_col_data = vec![fill_val; n_row];
        self.replace_or_add_col(out_name, out_col_data);
    }
    /// Create or update a column from Vec<Option<T>> as input, with option NA replacement.
    pub fn inject_col_vec<
        O: Copy + Send + Sync + 'static,
    >(
        &mut self,
        out_name: &str, 
        mut out_col_data: Vec<Option<O>>, na_replace: Option<O>
    ) where Vec<Option<O>>: ColVec {
        if let Some(na_replace) = na_replace {
            Do::na_replace(&mut out_col_data, na_replace);
        }
        self.replace_or_add_col(out_name, out_col_data);
    }
    // note: unlike set, do no deploy inject methods for column operations since df_inject!  
    // macro supports dynamic dispatch to either DataFrame or DataFrameSlice methods as df_src
    /* -----------------------------------------------------------------------------
    DataFrame cell-level getters and setters
    ----------------------------------------------------------------------------- */
    /// Return the String representation of a specific DataFrame cell by column name and integer row index.
    pub fn cell_string(&self, col_name: &str, row_i: usize) -> String {
        Column::check_i_bound(self.n_row, row_i, "get_as_string");
        self.get_column(col_name, "get_as_string").cell_string(row_i)
    }
    /// Return the Option<T> value of a specific DataFrame cell by column name and integer row index.
    pub fn cell<T: 'static + Clone>(&self, col_name: &str, row_i: usize) -> Option<T>  
    where Option<T>: ColCell {
        Column::check_i_bound(self.n_row, row_i, "cell");
        self.get_column(col_name, "cell").cell(col_name, row_i)
    }
    /// Set the Option<T> value of a specific cell in a DataFrame by integer row index and column name.
    pub fn set<T: 'static + Clone>(&mut self, col_name: &str, row_i: usize, value: Option<T>) 
    where Option<T>: ColCell {
        Column::check_i_bound(self.n_row, row_i, "set");
        self.get_column_mut(col_name, "set").set(row_i, value, col_name);
        self.status.reset_if_changed(vec![col_name.to_string()]);
        self.row_index.reset_if_changed(vec![col_name.to_string()]);
    }
    /* -----------------------------------------------------------------------------
    DataFrame support for indexed row retrieval
    ----------------------------------------------------------------------------- */
    /// Prepare a DataFrame for indexed row retrieval by creating a row index
    /// on one or more key columns.
    pub fn set_index(self, key_cols: Vec<String>) -> DataFrame {
        RowIndex::set_index(self, key_cols)
    }
    /// Return a DataFrameSlice of the rows in an indexed DataFrame that match the 
    /// specific key column values.
    pub fn get_indexed(&self, dk: DataFrame) -> DataFrameSlice<'_> {
        RowIndex::get_indexed(self, dk)
    }
}
