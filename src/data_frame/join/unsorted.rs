//! Unsorted join execution by the hash join algorithm

// dependencies
use std::collections::HashMap;
use super::{DataFrame, Join};
use crate::data_frame::query::Query;

// -----------------------------------------------------------------------------
// open __execute_join_unsorted macro for unsorted join execution
macro_rules! __execute_join_unsorted {
    ($execute_join_unsorted_n:ident, $set_row_keys_n:ident, $row_keys_n:ident, $n_bytes:literal) => {
// -----------------------------------------------------------------------------

/// Perform an unsorted join.
pub(super) fn $execute_join_unsorted_n(
    &self, 
    df_l: &DataFrame, df_r: &DataFrame, df_j: &mut DataFrame, 
    qry_l: &mut Query, qry_r: &mut Query,
    df_l_i: usize, df_r_i: usize,
) {
    
    // calculate row keys (not done previously since not sorted)
    qry_l.$set_row_keys_n(df_l, qry_l.sort_cols.clone(), false);
    qry_r.$set_row_keys_n(df_r, qry_r.sort_cols.clone(), false);

    // construct a HashMap of the right DataFrame, matching keys to a vector of row indices
    let mut i_map_r: HashMap<[u8; $n_bytes], Vec<usize>> = HashMap::new();
    qry_r.i_map.iter().for_each(|(src_i_r, flt_i_r, _)| {
        let key_r = qry_r.$row_keys_n[*flt_i_r];
        if let Some(src_is_r) = i_map_r.get_mut(&key_r) {
            src_is_r.push(*src_i_r);
        } else {
            i_map_r.insert(key_r, vec![*src_i_r]);
        }
    });

    // iterate over the left DataFrame rows, matching them to the right DataFrame
    qry_l.i_map.iter().for_each(|(src_i_l, flt_i_l, _)| {
        let key_l = qry_l.$row_keys_n[*flt_i_l];

        // inner joined rows
        if let Some(src_is_r) = i_map_r.get(&key_l) { 
            src_is_r.iter().for_each(|src_i_r| {
                Join::push_join_row(
                    df_l, df_r, df_j, 
                    &self.cols_out[df_l_i], &self.cols_non_key[df_r_i], // always this pattern since unsorted outer join not supported
                    Some(*src_i_l), Some(*src_i_r)
                );
            });

        // left join rows with no right side match
        } else if self.all_left { 
            Join::push_join_row(
                df_l, df_r, df_j, 
                &self.cols_out[df_l_i], &self.cols_non_key[df_r_i], 
                Some(*src_i_l), None
            );
        }
    });
}

// -----------------------------------------------------------------------------
// close __execute_join_unsorted macro for unsorted join execution
};}
// -----------------------------------------------------------------------------

/* -----------------------------------------------------------------------------
implement unsorted join methods on Join type
----------------------------------------------------------------------------- */
impl Join{
    __execute_join_unsorted!(execute_join_unsorted_1, set_row_keys_1, row_keys_1, 9);
    __execute_join_unsorted!(execute_join_unsorted_2, set_row_keys_2, row_keys_2, 18);
    __execute_join_unsorted!(execute_join_unsorted_4, set_row_keys_4, row_keys_4, 36);
    __execute_join_unsorted!(execute_join_unsorted_8, set_row_keys_8, row_keys_8, 72);
}
