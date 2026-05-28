//! Implement DataFrame group and do queries, executed on Query objects.

// dependencies
use super::{DataFrame, Query};
use crate::data_frame::DataFrameSlice;

// implement group and do queries
impl Query<'_> {

    /// Process row groups into an output DataFrame by applying an operation closure
    /// to each row group that generates DataFrames to be concatenated using `rbind()`.
    pub fn execute_do_df<'a>(
        mut self, df_src: &DataFrame,
        op: impl Fn(usize, DataFrame, DataFrameSlice) -> DataFrame,
    ) -> DataFrame {

        // execute group_by query
        let (df_grp, df_fss_opt, group_map) = self.execute_group_by(df_src);

        // iterate over the group_map to generate a DataFrame
        let finish_do_df = |df_fss: &DataFrame| -> DataFrame {
            if group_map.is_empty() { return DataFrame::new(); }

            // process once group 
            let do_df_group = |grp_i, i_maps: &[(usize, usize, usize)]|{
                op(
                    grp_i, 
                    df_grp.from_rows(vec![grp_i]), 
                    df_fss.slice(i_maps[0].2, i_maps.len())
                )
            };

            // initialize df_do with the first group
            let df_do =  do_df_group(0, group_map[0]);

            // iterate over the rest of the group_map to add rows to df_do
            let mut df_do = group_map.iter().enumerate().skip(1).fold(df_do, |df_do, (grp_i, i_maps)| {
                df_do.rbind( do_df_group(grp_i, i_maps) )
            });

            // we do not do anything to the output DataFrame status
            // we cannot assert anything about its columns and rows, it could be anything at all
            // the caller is responsible for describing the output DataFrame via its 
            // status fields if needed after the query
            DataFrame::touch(&mut df_do);
            df_do
        };

        // process either df_fss or df_src depending on whether rows changed prior to grouping
        if let Some(mut df_fss) = df_fss_opt {
            finish_do_df(&mut df_fss)
        } else {
            finish_do_df(df_src)
        }
    }

    /// Process row groups into an output Vec<T> by applying an operation closure
    /// to each row group that generates Vec<T> to be flattened by the calling macro.
    pub fn execute_do_vec<'a, T>(
        mut self, df_src: &mut DataFrame,
        op: impl Fn(usize, DataFrame, DataFrameSlice) -> Vec<T>
    ) -> Vec<T> {

        // execute group_by query
        let (df_grp, df_fss_opt, group_map) = self.execute_group_by(df_src);

        // iterate over the group_map to generate a Vec<T>
        let finish_do_vec = |df_fss: &mut DataFrame| -> Vec<T> {
            if group_map.is_empty() { return Vec::new(); }
            group_map.iter().enumerate().map(|(grp_i, i_maps)| {
                op(
                    grp_i, 
                    df_grp.from_rows(vec![grp_i]), 
                    df_fss.slice(i_maps[0].2, i_maps.len())
                )
            }).flatten().collect()
        };

        // process either df_fss or df_src depending on whether rows changed prior to grouping
        if let Some(mut df_fss) = df_fss_opt {
            finish_do_vec(&mut df_fss)
        } else {
            finish_do_vec(df_src)
        }
    }
}
