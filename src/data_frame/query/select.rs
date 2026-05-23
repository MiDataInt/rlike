//! Implement DataFrame filter/sort/select (FSS) queries, executed on Query objects.
//! Many of methods are used by other query types also.

// dependencies
use super::{DataFrame, Query};
use crate::data_frame::column::{Column, get::ColVec};
use crate::data_frame::r#trait::DataFrameTrait;
use rayon::prelude::*;

// implement filter/sort/select queries
impl Query<'_> {
    /* -----------------------------------------------------------------------------
    implement filter() action pre-processing
    ----------------------------------------------------------------------------- */
    /// Perform initial evaluation of DataFrame single-column filter.
    pub fn set_col_filter_1<A>(
        &mut self, df_src: &DataFrame,
        a_name: &str, 
        op: impl Fn(Option<A>) -> bool + Send + Sync
    ) where 
        A: Clone + Send + Sync,
        Vec<Option<A>>: ColVec 
    {
        let a_col_data: &[Option<A>] = df_src.get_ref(a_name);
        let _ = self.kept_rows.par_iter_mut().enumerate()
            .for_each(|(i, kept)| if *kept {
                *kept = op(a_col_data[i].clone())
            });
    }

    /// Perform initial evaluation of DataFrame two-column filter.
    pub fn set_col_filter_2<A,B>(
        &mut self, df_src: &DataFrame,
        a_name: &str, b_name: &str, 
        op: impl Fn(Option<A>, Option<B>) -> bool + Send + Sync
    ) where 
        A: Clone + Send + Sync,
        B: Clone + Send + Sync,
        Vec<Option<A>>: ColVec,
        Vec<Option<B>>: ColVec
    {
        let a_col_data: &[Option<A>] = df_src.get_ref(a_name);
        let b_col_data: &[Option<B>] = df_src.get_ref(b_name);
        let _ = self.kept_rows.par_iter_mut().enumerate()
            .for_each(|(i, kept)| if *kept {
                *kept = op(a_col_data[i].clone(), b_col_data[i].clone())
            });
    }

    /// Perform initial evaluation of DataFrame three-column filter.
    pub fn set_col_filter_3<A, B, C>(
        &mut self, df_src: &DataFrame,
        a_name: &str, b_name: &str, c_name: &str,
        op: impl Fn(Option<A>, Option<B>, Option<C>) -> bool + Send + Sync
    ) where 
        A: Clone + Send + Sync,
        B: Clone + Send + Sync,
        C: Clone + Send + Sync,
        Vec<Option<A>>: ColVec,
        Vec<Option<B>>: ColVec,
        Vec<Option<C>>: ColVec
    {
        let a_col_data: &[Option<A>] = df_src.get_ref(a_name);
        let b_col_data: &[Option<B>] = df_src.get_ref(b_name);
        let c_col_data: &[Option<C>] = df_src.get_ref(c_name);
        let _ = self.kept_rows.par_iter_mut().enumerate()
            .for_each(|(i, kept)| if *kept {
                *kept = op(a_col_data[i].clone(), b_col_data[i].clone(), c_col_data[i].clone())
            });
    }
    /* -----------------------------------------------------------------------------
    execute filter/sort/select query
    ----------------------------------------------------------------------------- */
    /// Return a new DataFrame based on filtering, sorting, and selection 
    /// operations applied upstream.
    pub fn execute_select_query(mut self, df_src: &DataFrame) -> DataFrame {
        self.set_aggregate_status_flags(df_src);
        if self.select_cols.is_empty() {
            self.select_cols = df_src.col_names.clone();
        }
        let mut df_fss = self.filter_sort_select(df_src);
        df_fss.status.transfer_sort_group_status(&self);
        df_fss // df_fss = filtered, sorted, and selected DataFrame
    }
    /* -----------------------------------------------------------------------------
    implement filter() and sort() action execution processing
    ----------------------------------------------------------------------------- */
    // Coordinate filtering, sorting and column selection.
    pub(crate) fn filter_sort_select(&mut self, df_src: &DataFrame) -> DataFrame {
        let mut df_fss = DataFrame::new();

        // rows will change, process using i_map
        if self.rows_are_changing {
            self.set_i_map(df_src);
            df_fss.n_row = self.i_map.len(); // row count after any filtering

            // if no rows remain, add empty columns as needed
            if df_fss.n_row == 0 {
                for col_name in &self.select_cols{
                    Column::add_empty_col(&mut df_fss, col_name, df_src.col_types[col_name].as_str());
                }

            // otherwise finish sorting and column selection
            } else {
                if self.needs_sorting { self.sort_i_map(df_src); }
                for col_name in &self.select_cols{ // parallelize df_fss build by row
                    Column::copy_col_map(df_src, &mut df_fss, self, col_name);
                }
            }

        // rows remain unchanged, but need to subset columns for select
        } else {
            for col_name in &self.select_cols{ // parallelize df_fss build by row
                Column::copy_col(df_src, &mut df_fss, col_name);
            }
            df_fss.n_row = df_src.n_row;
        }

        // return the filtered, sorted, and selected DataFrame
        df_fss
    }

    // Establish the row index map for the DataFrame Query.
    pub(crate) fn set_i_map(&mut self, df_src: &DataFrame){
        self.i_map = (0..df_src.n_row)
            .into_par_iter()
            .map(|i| (i, i, i))
            .collect::<Vec<(usize, usize, usize)>>(); // src_i, flt_i, fss_i
        if self.needs_filtering {
            self.i_map = self.i_map
                .par_iter()
                
                // filters enforced here
                .filter(|(src_i, _, _)| self.kept_rows[*src_i])
                .collect::<Vec<&(usize, usize, usize)>>() 
                
                // maintain two indices, one for df_fss, one for df_src
                .into_par_iter()
                .enumerate() 
                .map(|(flt_i, (src_i, _, _))| (*src_i, flt_i, flt_i))
                .collect();
        }
    }

    // Establish sort keys and use them to sort the row index map.
    pub(crate) fn sort_i_map(&mut self, df_src: &DataFrame) {

        // NOTE: sort_i_map itself does not automatically update i_map.fss_i after sort
        // since this overhead is not needed for select queries
        // i_map.fss_i is therefore inaccurate once sort_i_map is executed on a query
        // calling code must call qry::set_fss_i() if fss_i must be renumbered as 0..df_fss.n_row

        // macro for shared code over different key lengths
        macro_rules! __do_sort_n {
            ($set_row_keys_n:ident, $row_keys_n:ident) => {
                // row_keys_n is filtered because i_map is filtered, therefore indexed by flt_i
                self.$set_row_keys_n(df_src, self.sort_cols.clone(), self.sort_negates[0]);
                self.i_map.par_sort_unstable_by_key(|(_, flt_i, _)| &self.$row_keys_n[*flt_i]);
            };
        }

        // single column sort, allows String
        let n_sort_cols = self.sort_cols.len();
        if n_sort_cols == 1 {
            let col_name = self.sort_cols[0].clone();
            let col_type = df_src.col_types[&col_name].as_str();
            if col_type == "alloc::string::String" {
                // callers can assemble their own arbitrary String sort key as needed
                // TODO: does not yet support negation/descending order
                let col_data = df_src.get_ref::<String>(&col_name);
                self.i_map.par_sort_unstable_by_key(|(src_i, _, _)| &col_data[*src_i]);
            } else {
                __do_sort_n!(set_row_keys_1, row_keys_1);
            }
        
        // multiple column sorting using packed keys, does not allow String
        } else if n_sort_cols == 2 {
            __do_sort_n!(set_row_keys_2, row_keys_2);
        } else if n_sort_cols <= 4 {
            __do_sort_n!(set_row_keys_4, row_keys_4);
        } else {
            __do_sort_n!(set_row_keys_8, row_keys_8);
        }
    }

    /// Reset i_map.fss_i to 0..df_fss.n_row after sorting prior to grouping
    pub(crate) fn set_fss_i(&mut self) {
        self.i_map.par_iter_mut().enumerate()
            .for_each(|(i, (_, _, fss_i))| *fss_i = i);
    }

    // Finish constructing the filtered and sorted output by mapped copying.
    pub(crate) fn map_column<T>(
        &self, df_dst: &mut DataFrame,
        col_data_src: &[Option<T>], col_name: &str
    ) 
    where T: 'static + Clone + Send + Sync,
          Vec<Option<T>>: ColVec 
    {
        let col_data_dst: Vec<Option<T>> = self.i_map
            .par_iter() // parallelize df_out build by row
            .map(|(src_i, _, _)| col_data_src[*src_i].clone())
            .collect();
        df_dst.add_col(col_name, col_data_dst);
    }  
}
