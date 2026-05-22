//! The `rlike::data_frame::join` module supports DataFrame join operations
//! by declaring a metadata object to help with passing of join config metadata.

// modules
pub mod sorted;
pub mod unsorted;

// dependencies
use super::DataFrame;
use crate::data_frame::query::Query;
use crate::data_frame::column::Column;
use crate::data_frame::r#trait::DataFrameTrait;
use crate::throw;

/// Enumeration of supported join types. JoinType::Pending is an initial undefined state.
#[derive(PartialEq, Clone)]
pub enum JoinType {
    Pending,
    Left,
    Inner,
    Outer
}

/// The Join struct simplified code by carrying join metadata in a single object.
pub struct Join {
    pub n_dfs:          usize,
    pub filtered:       bool,
    pub sorted:         bool,
    pub join_type:      JoinType,
    pub all_left:       bool,
    pub all_right:      bool,
    pub key_cols:       Vec<String>,
    pub cols_all:       Vec<Vec<String>>,
    pub cols_non_key:   Vec<Vec<String>>,
    pub cols_out:       Vec<Vec<String>>,
    pub n_rows_r:       usize,
}

impl Join{

    /* -----------------------------------------------------------------------------
    Join constructors
    ----------------------------------------------------------------------------- */

    /// Create a new Join object with the specified filtered and sorted flags and join type.
    pub fn new() -> Self {
        Self {
            n_dfs:          2,
            filtered:       false,
            sorted:         false,
            join_type:      JoinType::Pending,
            all_left:       false,
            all_right:      false,
            key_cols:       Vec::new(),
            cols_all:       Vec::new(),
            cols_non_key:   Vec::new(),
            cols_out:       Vec::new(),
            n_rows_r:       0,
        }
    }

    /// Set the join type of the Join object.
    pub fn set_join_type(&mut self, join_type: JoinType) {
        self.join_type = join_type.clone();
        self.all_left  = join_type == JoinType::Outer || join_type == JoinType::Left;
        self.all_right = join_type == JoinType::Outer;
    }

    /// Perform column consistency checks
    pub fn check_config(&self) {
        if self.n_dfs < 2 {
            throw!("DataFrame::join error: join requires at least two DataFrames.");
        }
        if self.key_cols.len() > 8 {
            throw!("DataFrame::join error: join only supports 1 to 8 key columns.");
        }
        if !self.sorted && self.join_type == JoinType::Outer {
            throw!("DataFrame::join error: outer join is not supported on unsorted hash joins.");
        }
        let mut non_key_cols = Vec::new();
        for i in 0..self.n_dfs {
            if Self::a_lacks_b_element(&self.cols_all[i], &self.key_cols){
                throw!("DataFrame::join error: one or more key columns missing from DataFrame {}.", i + 1);
            }
            if Self::has_shared_values(&self.cols_non_key[i],  &self.key_cols){
                throw!("DataFrame::join error: one or more non-key columns in DataFrame {} is also a key column.", i + 1);
            }
            if Self::has_shared_values(&self.cols_non_key[i], &non_key_cols){
                throw!("DataFrame::join error: all join select columns (not key columns) must be unique to one DataFrame.");
            }
            non_key_cols.extend(self.cols_non_key[i].clone());
        }
    }
    fn a_lacks_b_element(a: &Vec<String>, b: &Vec<String>) -> bool {
        b.iter().any(|x| !a.contains(x))
    } 
    fn has_shared_values(a: &Vec<String>, b: &Vec<String>) -> bool {
        a.iter().any(|x| b.contains(x))
    }

    /* -----------------------------------------------------------------------------
    Shared join execution
    ----------------------------------------------------------------------------- */
    /// Merge two or more DataFrames by row keys, i.e., left, inner, and outer joins.
    /// Typically called via macro `df_join!()`.
    pub fn execute_join(
        &mut self, dfs: Vec<&DataFrame>, mut qrys: Vec<Query>
    ) -> DataFrame {

        // use fold to perform sequential left-associative joins
        (1..self.n_dfs).fold(DataFrame::new(), |df_l, df_r_i| {
            let df_l_i = df_r_i - 1;
            let is_last = df_r_i == self.n_dfs - 1;
            let (left, right) = qrys.split_at_mut(df_r_i);
            let qry_l = &mut left[df_l_i]; // split_at_mut allows two mutable references into qrys
            let qry_r = &mut right[0];     // by guaranteeing that df_l_i and df_r_i are not the same index
            if df_r_i == 1 { // first join uses the first and second DataFrames
                self.join_two_dfs(dfs[df_l_i], dfs[df_r_i], qry_l, qry_r, df_l_i, df_r_i, is_last)
            } else { // subsequent joins use the previous join result and the next DataFrame
                self.join_two_dfs(&df_l, dfs[df_r_i], qry_l, qry_r, df_l_i, df_r_i, is_last)
            }
        })
    }
    
    // join two DataFrames; called as many times as needed by execute_join()
    fn join_two_dfs(
        &mut self, 
        df_l: &DataFrame, df_r: &DataFrame, 
        qry_l: &mut Query, qry_r: &mut Query,
        df_l_i: usize, df_r_i:usize, is_last: bool
    ) -> DataFrame {

        // create (and sort) imap of left and right DataFrames
        qry_l.set_i_map(df_l); // filter each DataFrame individually as needed
        qry_r.set_i_map(df_r); // qry.i_map = Vec<(src_i, flt_i, fs_i)>
        if self.sorted {
            if df_l_i == 0 { qry_l.sort_i_map(df_l); } // sets sort_keys_N and uses them to sort i_map
            qry_r.sort_i_map(df_r); // unsorted joins and df_j as df_l must set sort_keys_N downstream
        }

        // initialize the join output DataFrame
        let mut df_j = DataFrame::new();
        Column::add_empty_cols(df_l, &mut df_j, self.cols_out[df_l_i].clone());
        Column::add_empty_cols(df_r, &mut df_j, self.cols_non_key[df_r_i].clone());
        df_j.reserve(qry_l.i_map.len()); // a guess, could be less, could be more
        self.n_rows_r = qry_r.i_map.len();
        let n_key_col = self.key_cols.len();        

        // macro to perform join dispatch
        macro_rules! __dispatch_join {
            ($execute_join_n:ident) => {
                self.$execute_join_n(df_l, df_r, &mut df_j, qry_l, qry_r, df_l_i, df_r_i)
            };
        }

        // dispatch to the appropriate sorted join algorithm
        if self.sorted {
            let mut i_r = 
                 if n_key_col == 1 { __dispatch_join!(execute_join_sorted_1) } 
            else if n_key_col == 2 { __dispatch_join!(execute_join_sorted_2) } 
            else if n_key_col <= 4 { __dispatch_join!(execute_join_sorted_4) } 
                              else { __dispatch_join!(execute_join_sorted_8) };

            // fill in any tail of unmatched right rows on outer joins (sorted joins only)
            if self.all_right { 
                while i_r < self.n_rows_r { 
                    Join::push_join_row(
                        df_l, df_r, &mut df_j, 
                        &self.cols_non_key[df_l_i], &self.cols_out[df_r_i], 
                        None, Some(qry_r.i_map[i_r].0)
                    );
                    i_r += 1;
                }
            }
        
        // dispatch to the appropriate unsorted join algorithm
        } else {
                 if n_key_col == 1 { __dispatch_join!(execute_join_unsorted_1) } 
            else if n_key_col == 2 { __dispatch_join!(execute_join_unsorted_2) } 
            else if n_key_col <= 4 { __dispatch_join!(execute_join_unsorted_4) } 
                              else { __dispatch_join!(execute_join_unsorted_8) };
        }

        // in needed, update join and query metadata to allow df_j to act as df_l for next join
        // current df_r_i slots in Join and qrys hold info on df_j, which become df_l_i slots in the next join
        if !is_last {
            self.cols_all[df_r_i] = df_j.col_names().iter().cloned().collect();
            self.cols_non_key[df_r_i] = self.cols_all[df_r_i].iter().filter(|col| !self.key_cols.contains(col)).cloned().collect();
            self.cols_out[df_r_i] = self.key_cols.iter().chain(self.cols_non_key[df_r_i].iter()).cloned().collect();
            qry_r.kept_rows.clear(); // df_j is already filtered by the previous join
            qry_r.has_been_filtered = false;
            qry_r.needs_filtering = false;
            qry_r.sorts_match = true; // is a sorted join, df_j is already sorted by the previous join
            qry_r.needs_sorting = false;
            qry_r.rows_are_changing = false;
            qry_r.i_map.clear();
            qry_r.row_keys_1.clear();
            qry_r.row_keys_2.clear();
            qry_r.row_keys_4.clear();
            qry_r.row_keys_8.clear();
        }

        // return dj_j as the accumulator
        df_j
    }

    // shared helper function to push one row to the (un)sorted join table by cycling over all output columns
    fn push_join_row(
        df_l: &DataFrame, df_r: &DataFrame, df_j: &mut DataFrame, 
        cols_l: &Vec<String>, cols_r: &Vec<String>, // not using Join here, caller determines left and right cols based on row join type
        src_i_l: Option<usize>, src_i_r: Option<usize> // the source row indices being joined
    ) {
        cols_l.iter().for_each(|col_name| { // push left column values
            Column::push_col_val(df_l, df_j, col_name, src_i_l);
        });
        cols_r.iter().for_each(|col_name| { // push left column values
            Column::push_col_val(df_r, df_j, col_name, src_i_r);
        });
        df_j.n_row += 1;
    }
}
