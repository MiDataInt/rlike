//! Implement DataFrame pivot queries, executed on Query objects.

// dependencies
use super::{DataFrame, Query};
use crate::data_frame::column::{Column, get::ColVec};
use crate::data_frame::r#trait::DataFrameTrait;
use rayon::prelude::*;

// implement pivot queries
impl Query<'_> {

    /// Execute a pivot operation on a DataFrame, i.e., convert one column's values into 
    /// new category columns filled with values aggregated by grouped rows.
    pub fn execute_pivot<F, O>(
        mut self, df_src: &DataFrame,
        key_cols: Vec<String>, pivot_col: String, fill_col: String,
        na_val: Option<O>, op: impl Fn(Vec<F>) -> O + Send + Sync
    ) -> DataFrame 
    where 
        F: Copy + Send + Sync + Default, // F = fill_col type, O = aggregate output type
        O: Copy + Send + Sync + 'static,
        Vec<Option<F>>: ColVec,
        Vec<Option<O>>: ColVec
    {

        // parse the pivot type and required columns
        let has_fill_col = pivot_col != fill_col;
        let mut agg_cols = vec![pivot_col.clone()];
        if has_fill_col { agg_cols.push(fill_col.clone());}

        // execute the requisite group_by query, the same as for typical column aggregation
        if self.is_sorted_query && self.sort_cols.is_empty() {
            self.is_sorted_query = false;
            self.set_sort_cols(df_src, key_cols.clone());
        }
        self.set_grouping_cols(df_src, key_cols, agg_cols);
        let (mut df_grp, df_fss_opt, group_map) = self.execute_group_by(df_src);

        // fill a pivot table with aggregated values
        let mut do_pivot = |df_fss: &DataFrame|{

            // parse the pivot data to a factor column
            let col_tmp: Column;
            let col_in: &Column = df_fss.columns.get(&pivot_col).unwrap(); // df_fss may or may not be df_src
            let pivot_col: &Column = match col_in {
                // already a factor, no action needed
                Column::RFactor(_) => col_in, 
                // otherwise, factorize columns capable of returning meaningful category labels
                _ => { 
                    col_tmp = col_in.factorize(&pivot_col, df_fss.n_row);
                    &col_tmp
                }
            };

            // get pivot categories
            let labels: Vec<String> = pivot_col.get_labels(); // one agg col per factor label
            let levels = pivot_col.get_levels();
            let pivot_data = pivot_col.get_ref("__internal");

            // get the fill data, as needed
            let fill_data: &[Option<F>] = if has_fill_col { df_fss.get_ref(&fill_col) } else { &Vec::new() };
            let pivot_default = Some(F::default()); // we don't need the value of a pivot column, just something to count

            // fill one catogory column at a time; column names are the factor labels
            // parallize category column fill operation by grouped rows
            for col_name in &labels {
                let col_data: Vec<Option<O>> = group_map.par_iter().map(|i_maps|{
                    let fill_vals: Vec<F> = i_maps.iter()
                        .filter_map(|i_map| { // collect fill values from group rows that match the pivot category
                            if pivot_data[i_map.2] == Some(levels[col_name]) {
                                if has_fill_col { fill_data[i_map.2] } else { pivot_default }
                            } else {
                                None
                            }
                    }).collect();
                    if fill_vals.len() == 0 { na_val } else { Some(op(fill_vals)) }
                }).collect();
                df_grp.replace_or_add_col(col_name, col_data);
            }
        };

        // process either df_fss or df_src depending on whether rows changed prior to grouping
        if let Some(df_fss) = df_fss_opt {
            do_pivot(&df_fss);
        } else {
            do_pivot(&df_src);
        }

        // return the pivot table
        df_grp.status.transfer_sort_group_status(&self);
        df_grp
    }
}
