//! Sorted join execution by the sort merge join algorithm

// dependencies
use super::{DataFrame, Join};
use crate::data_frame::query::Query;

// -----------------------------------------------------------------------------
// open __execute_join_sorted_n macro for sorted join execution
macro_rules! __execute_join_sorted_n {
    ($execute_join_sorted_n:ident, $set_row_keys_n:ident, $row_keys_n:ident, $n_bytes:literal) => {
// -----------------------------------------------------------------------------

/// Perform a sorted join.
pub(super) fn $execute_join_sorted_n(
    &self, 
    df_l: &DataFrame, df_r: &DataFrame, df_j: &mut DataFrame, 
    qry_l: &mut Query, qry_r: &mut Query,
    df_l_i: usize, df_r_i: usize,
) -> usize {
    
    // calculate row keys when df_l is previous df_j, which comes pre-sorted
    if df_l_i > 0 { 
        qry_l.set_row_keys_1(df_l, self.key_cols.clone(), qry_l.sort_negates[0]);
    }

    // initialize join process
    let mut i_r: usize = 0;
    let mut prev_min_i_r: Option<usize> = None;
    let mut prev_max_i_r: Option<usize> = None;
    let get_right_key = |i_r: usize| {
        if i_r >= self.n_rows_r { [0; $n_bytes] } else { qry_r.$row_keys_n[qry_r.i_map[i_r].1] }
    };
    let (mut key_r, mut prev_key_l) = (get_right_key(i_r), None);

    // process each sorted left row one at a time against all relevant right rows
    qry_l.i_map.iter().for_each(|(src_i_l, flt_i_l, _)| {
        let key_l = qry_l.$row_keys_n[*flt_i_l]; // as for grouping, sort_keys were used to sort i_map but are not themselves sorted
        match prev_key_l {

            // we are in a continuing left row run, do the same thing as the previous row ...
            Some(pkl) if key_l == pkl => { 
                match (prev_min_i_r, prev_max_i_r) { 

                    // ... either matching the same right rows ...
                    (Some(min_i_r), Some(max_i_r)) => {
                        for i_r in min_i_r..max_i_r {
                            Join::push_join_row(
                                df_l, df_r, df_j, 
                                &self.cols_out[df_l_i], &self.cols_non_key[df_r_i], 
                                Some(*src_i_l), Some(qry_r.i_map[i_r].0)
                            );
                        } 
                    },

                    // ... or committing an umatched left row
                    _ => { 
                        if self.all_left {
                            Join::push_join_row(
                                df_l, df_r, df_j, 
                                &self.cols_out[df_l_i], &self.cols_non_key[df_r_i], 
                                Some(*src_i_l), None
                            );
                        }
                    }
                }
            },

            // process a new left key
            _ => { 
                loop{

                    // handle left join with unmatched right rows
                    if i_r >= self.n_rows_r || key_l < key_r { 
                        if self.all_left {
                            Join::push_join_row(
                                df_l, df_r, df_j, 
                                &self.cols_out[df_l_i], &self.cols_non_key[df_r_i], 
                                Some(*src_i_l), None
                            );
                        }
                        (prev_min_i_r, prev_max_i_r) = (None, None);
                        break;

                    // handle inner join with matched right rows   
                    } else if key_l == key_r { 
                        prev_min_i_r = Some(i_r); // 0-indexed
                        while i_r < self.n_rows_r && key_l == key_r  {
                            Join::push_join_row(
                                df_l, df_r, df_j,  
                                &self.cols_out[df_l_i], &self.cols_non_key[df_r_i], 
                                Some(*src_i_l), Some(qry_r.i_map[i_r].0)
                            );
                            i_r += 1;
                            key_r = get_right_key(i_r);
                        }
                        prev_max_i_r = Some(i_r); // 1-indexed
                        break;

                    // handle right join portion of outer join with unmatched left rows 
                    } else { 
                        while i_r < self.n_rows_r && key_l > key_r  {
                            if self.all_right {
                                Join::push_join_row(
                                    df_l, df_r, df_j,  
                                    &self.cols_non_key[df_l_i], &self.cols_out[df_r_i],
                                    None, Some(qry_r.i_map[i_r].0)
                                );
                            }
                            i_r += 1;
                            key_r = get_right_key(i_r);
                        } // don't break loop, still need to process the current left row
                    }
                }
            }
        }
        prev_key_l = Some(key_l);
    });
    i_r
}

// -----------------------------------------------------------------------------
// close __execute_join_sorted_n macro for sorted join execution
};}
// -----------------------------------------------------------------------------

/* -----------------------------------------------------------------------------
implement sorted join methods on Join type
----------------------------------------------------------------------------- */
impl Join{
    __execute_join_sorted_n!(execute_join_sorted_1, set_row_keys_1, row_keys_1, 9);
    __execute_join_sorted_n!(execute_join_sorted_2, set_row_keys_2, row_keys_2, 18);
    __execute_join_sorted_n!(execute_join_sorted_4, set_row_keys_4, row_keys_4, 36);
    __execute_join_sorted_n!(execute_join_sorted_8, set_row_keys_8, row_keys_8, 72);
}
