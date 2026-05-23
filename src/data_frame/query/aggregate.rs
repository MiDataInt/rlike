//! Implement DataFrame group and aggregate queries, executed on Query objects.
//! Grouping operations are used by other query types also, including pivot and do.

// dependencies
use std::collections::HashMap;
use rayon::prelude::*;
use super::{DataFrame, Query};
use crate::data_frame::column::{Column, get::ColVec};
use crate::data_frame::r#trait::DataFrameTrait;

/* -----------------------------------------------------------------------------
macros for shared grouping query execution
----------------------------------------------------------------------------- */
// fill a group map with the appropriately indexed row keys
macro_rules! __create_group_map {
    ($row_keys_n:ident, $qry:expr, $group_map:expr, $slot:tt) => {
        $group_map = $qry.i_map.par_chunk_by(|im1, im2| {
            $qry.$row_keys_n[im1.$slot] == $qry.$row_keys_n[im2.$slot]
        }).collect::<Vec<&[(usize, usize, usize)]>>();
    };
}
// create a sorted group map for multiple column groups (single-column have distinct handling)
macro_rules! __multi_col_group_sorted {
    (
        $set_row_keys_n:ident, $df_src:expr,
        $row_keys_n:ident, $qry:expr, $group_eq_sort:expr, $group_map:expr
    ) => {
        if $group_eq_sort && !$qry.$row_keys_n.is_empty() {
            __create_group_map!($row_keys_n, $qry, $group_map, 1); // keys calculated before sorting use flt_i = i_map.1
        } else {
            $qry.$set_row_keys_n($df_src, $qry.group_cols.clone(), false); 
            __create_group_map!($row_keys_n, $qry, $group_map, 2); // keys calculated after sorting use fss_i = i_map.2
        };
    };
}
// collect and parse the row keys for unsorted groups
macro_rules! __multi_col_group_unsorted {
    ($row_keys_n:ident, $n_bytes:literal, $qry:expr) => {
        {
            // first pass: collect hashed lists of rows in groups as encountered in df_flt
            let mut group_i_maps: HashMap<[u8; $n_bytes], Vec<(usize, usize, usize)>> = HashMap::new();
            let mut group_keys: Vec<[u8; $n_bytes]> = vec![];
            $qry.i_map.iter().for_each(|im| {
                let group_key = $qry.$row_keys_n[im.1]; // flt_i
                if let Some(i_maps) = group_i_maps.get_mut(&group_key) {
                    i_maps.push(*im);
                } else {
                    group_i_maps.insert(group_key, vec![*im]);
                    group_keys.push(group_key); // groups recorded in order encountered in qry.i_map
                }
            });

            // second pass: use group_is to assemble a new df_fss i_map
            // eventual output will be grouped in the order groups were encountered without sorting
            $qry.i_map = Vec::with_capacity($qry.i_map.len());
            let _ = group_keys.iter().for_each(|group_key| {
                group_i_maps[group_key].iter().for_each(|im| {
                    let fss_i = $qry.i_map.len();
                    $qry.i_map.push((im.0, im.1, fss_i)); // src_i, flt_i, fss_i
                });
            });
        }
    };
}

/* -----------------------------------------------------------------------------
implement group and aggregate queries
----------------------------------------------------------------------------- */
impl Query<'_> {
    /* -----------------------------------------------------------------------------
    first execution call from macro: establishes df_grp (key cols), df_fss (non-key cols), and group_map
    ----------------------------------------------------------------------------- */
    /// Return a new DataFrame based on filtering, sorting, grouping and aggregating
    /// operations applied upstream. Yields grouping columns and associated unaggregated rows;
    /// aggregations are final DataFrame assembly are performed later.
    pub fn execute_group_by<'a>(
        &'a mut self, df_src: &DataFrame,
    ) -> (DataFrame, Option<DataFrame>, Vec<&'a [(usize, usize, usize)]>) {

        // validate the presence of usable grouping key columns
        if self.has_been_grouped {
            if self.is_sorted_query && !self.has_been_sorted {
                self.sort_cols = self.group_cols.clone();
                self.sort_negates = vec![false; self.sort_cols.len()];
                self.is_sorted_query = true;
                self.has_been_sorted = true;
            }
        } else {
            if self.has_been_sorted {
                self.group_cols = self.sort_cols.clone();
                self.has_been_grouped = true;
            } else {
                panic!("DataFrame::aggregate error: either sort(col, ...) or group(...) is required.");
            }
        }
        self.set_aggregate_status_flags(df_src);

        // if needed, apply filter and/or sort operations to the DataFrame to generate an i_map
        if self.rows_are_changing { // whether by sorting or filtering

            // check group and agg cols for existence
            let all_cols: Vec<String> = self.group_cols.iter().chain(self.agg_cols.iter()).cloned().collect();
            for col_name in &all_cols {
                df_src.columns.get(col_name).unwrap_or_else(|| 
                    panic!("DataFrame::group_by() error: column {col_name} not found.")
                );
            }

            // only need to fill agg_cols into df_fss, will recover group_col values from df_src
            self.select_cols = self.agg_cols.clone();
            let df_fss = self.filter_sort_select(df_src); // generates qry.i_map
            self.set_fss_i(); // used for aggregation and other purposes

            // return parsed groups, either sorted or unsorted
            let (df_grp, df_fss, group_map) = self.parse_groups(df_src, Some(df_fss));
            (df_grp, df_fss, group_map)

        // if the rows are not changing, we can use a simple initial i_map to find groups
        } else { 
            self.set_i_map(df_src); 

            // return parsed groups, either sorted or unsorted; self=df_src and df_fss are the same
            let (df_grp, df_fss, group_map) = self.parse_groups(df_src, None);
            (df_grp, df_fss, group_map)
        }
    }

    // analyze a DataFrame to find keyed groups and establish the group map of row indices
    fn parse_groups<'a>(
        &'a mut self, df_src: &DataFrame, mut df_fss: Option<DataFrame>
    ) -> (DataFrame, Option<DataFrame>, Vec<&'a [(usize, usize, usize)]>) {
        let group_cols = &self.group_cols.clone();
        let n_group_cols = group_cols.len();
        let group_map: Vec<&[(usize, usize, usize)]>;

        // parse groups for sorted queries
        if self.is_sorted_query {

            // Note that group_cols.len() may be less than sort_cols.len(), e.g., this 
            // allows min/max values to be calculated as first/last in a sorted group.
            // Therefore, group_keys may or may not be the same as sort_keys.
            let n_sort_cols  = self.sort_cols.len();
            let group_eq_sort = n_group_cols == n_sort_cols; // col name matches checked upstream

            // parse groups; previously sorted df_fss (or df_src) remains as is
            group_map = self.get_group_map_sorted(df_src, n_group_cols, group_eq_sort); 
        
        // parse groups for unsorted queries
        } else {

            // set the row keys, equivalent to sort keys although no sorting was/is performed
            if n_group_cols == 1 {
                self.set_row_keys_1(df_src, self.group_cols.clone(), false);
            } else if n_group_cols == 2 {
                self.set_row_keys_2(df_src, self.group_cols.clone(), false); 
            } else if n_group_cols <= 4 {
                self.set_row_keys_4(df_src, self.group_cols.clone(), false); 
            } else {
                self.set_row_keys_8(df_src, self.group_cols.clone(), false); 
            }

            // parse groups; df_fss updated to reflect newly grouped (but not sorted) output
            (df_fss, group_map) = self.get_group_map_unsorted(df_src, n_group_cols);
        }

        // initialize the group key(s) DataFrame, with group values taken as the first row value per group
        // aggregating column values are added later by second execution call
        let mut df_grp = DataFrame::new();
        for col_name in group_cols {
            Column::get_group_val_sorted(df_src, &mut df_grp, &col_name, &group_map);
        }
        (df_grp, df_fss, group_map)
    }

    // extract a sorted group map as slices of the sorted i_map to avoid another copy
    fn get_group_map_sorted<'a>(
        &'a mut self, df_src: &DataFrame,
        n_group_cols: usize, group_eq_sort: bool,
    ) -> Vec<&'a [(usize, usize, usize)]> {
        let group_map:Vec<&[(usize, usize, usize)]>;

        // single column group, allows String key
        if n_group_cols == 1 {
            let col_name = &self.group_cols[0];
            let col_type = df_src.col_types[col_name].as_str();
            if col_type == "alloc::string::String" {
                let col_data_src = df_src.get_ref::<String>(col_name);
                group_map = self.i_map.par_chunk_by(|(src_i1, _, _), (src_i2, _, _)| {
                    col_data_src[*src_i1] == col_data_src[*src_i2]
                }).collect::<Vec<&[(usize, usize, usize)]>>();
            } else { 
                // still use sort keys since they enforce consistent output ordering
                if group_eq_sort && !self.row_keys_1.is_empty() {
                    __create_group_map!(row_keys_1, self, group_map, 1); // sort keys calculated before sorting use flt_i = i_map.1
                } else { // group keys differ from sort keys or sort keys not available from pre-sorted df
                    let row_keys_fs = Column::get_cell_keys_col(df_src, self, col_name, false);
                    group_map = self.i_map.par_chunk_by(|im1, im2| {
                        row_keys_fs[im1.2] == row_keys_fs[im2.2] // group keys calculated after i_map sorting use fss_i = i_map.2
                    }).collect::<Vec<&[(usize, usize, usize)]>>();
                };
            }
        
        // multiple column grouping using packed keys, does not allow String
        } else if n_group_cols == 2 {
            __multi_col_group_sorted!(set_row_keys_2, df_src, row_keys_2, self, group_eq_sort, group_map);
        } else if n_group_cols <= 4 {
            __multi_col_group_sorted!(set_row_keys_4, df_src, row_keys_4, self, group_eq_sort, group_map);
        } else {
            __multi_col_group_sorted!(set_row_keys_8, df_src, row_keys_8, self, group_eq_sort, group_map);
        }
        group_map
    }

    // extract an unsorted group map as a vector of i_map entries; copy needed since i_map not sorted
    fn get_group_map_unsorted<'a>(
        &'a mut self, df_src: &DataFrame, 
        n_group_cols: usize
    ) -> (Option<DataFrame>, Vec<&'a [(usize, usize, usize)]>) {

        // single column group
        if n_group_cols == 1 {
            __multi_col_group_unsorted!(row_keys_1, 9, self);
        // multiple column grouping using packed keys
        } else if n_group_cols == 2 {
            __multi_col_group_unsorted!(row_keys_2, 18, self);
        } else if n_group_cols <= 4 {
            __multi_col_group_unsorted!(row_keys_4, 36, self);
        } else {
            __multi_col_group_unsorted!(row_keys_8, 72, self);
        }

        // shared third pass: create df_fss from qry.i_map
        let mut df_fss = DataFrame::new();
        for col_name in &self.agg_cols{ // parallelize df build by row
            Column::copy_col_map(df_src, &mut df_fss, self, col_name);
        }

        // fourth pass: create group_map from qry.i_map
        let group_map: Vec<&[(usize, usize, usize)]>;
        if n_group_cols == 1 {
            __create_group_map!(row_keys_1, self, group_map, 1); // group keys calculated without sorting use flt_i = i_map.1
        // multiple column grouping using packed keys, does not allow String
        } else if n_group_cols == 2 {
            __create_group_map!(row_keys_2, self, group_map, 1);
        } else if n_group_cols <= 4 {
            __create_group_map!(row_keys_4, self, group_map, 1);
        } else {
            __create_group_map!(row_keys_8, self, group_map, 1);
        }
        (Some(df_fss), group_map)
    }
    /* -----------------------------------------------------------------------------
    second execution call from macro: calculates group aggregates
    ----------------------------------------------------------------------------- */
    // perform a column aggregation operation, called iteratively by macro
    pub fn aggregate_column<A, O>(
        df_grp: &mut DataFrame, df_fss: &DataFrame, 
        a_name: &str, out_name: &str,
        group_map: &Vec<&[(usize, usize, usize)]>, 
        na_val: Option<O>, op: impl Fn(Vec<A>) -> O + Send + Sync
    ) where 
        A: Copy + Send + Sync,
        O: Copy + Send + Sync + 'static,
        Vec<Option<A>>: ColVec,
        Vec<Option<O>>: ColVec 
    {
        // group_map = [[(0, 0, 0), (2, 2, 1), (3, 3, 2), (5, 5, 3)], [(1, 1, 4), (4, 4, 5)]]
        // each outer element is a vector of sorted rows in a group/chunk
        // each tuple is one row in the group as (src_i, flt_i, fss_i)
        let a_col_data: &[Option<A>] = df_fss.get_ref(a_name);
        let out_col_data:Vec<Option<O>> = group_map.par_iter().map(|i_maps|{
            // filter_map() removes None values from the iterator, thus enforcing na.rm=TRUE
            let a_vals: Vec<A> = i_maps.iter().filter_map(|i_map| a_col_data[i_map.2]).collect();
            // if all values are NA, return NA replacement value
            if a_vals.len() == 0 { na_val } else { Some(op(a_vals)) }
        }).collect();
        df_grp.replace_or_add_col(out_name, out_col_data);
    }
}
