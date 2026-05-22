//! Implement column support for Query actions.

// dependencies
use std::mem;
use rayon::prelude::*;
use super::{DataFrame, DataFrameSlice, Column, Query};
use crate::data_frame::key::{CellKeyValue, CellKey};
use crate::data_frame::r#trait::DataFrameTrait;
use crate::data_frame::column::get::ColVec;
use crate::throw;

/* -----------------------------------------------------------------------------
error handling
----------------------------------------------------------------------------- */
macro_rules! string_key_error {
    ($col_name:expr, $caller:expr) => {
        throw!("DataFrame {} error: cannot pack row keys from RString column {}.", $caller, $col_name)
    };
}
macro_rules! unknown_type {
    ($col_type:expr, $caller:expr) => {
        throw!("DataFrame {} internal error: unknown or unhandled column type {}.", $caller, $col_type)
    };
}

/* -----------------------------------------------------------------------------
private trait with Column dispatch methods called on specific instances of generic types
----------------------------------------------------------------------------- */
trait CType: 'static + Sized + Clone + Send + Sync 
where Vec<Option<Self>>: ColVec {
    /* -----------------------------------------------------------------------------
    methods with defaults applied the same to all types
    ----------------------------------------------------------------------------- */
    // add empty column to DataFrame
    fn add_empty_col(df_dst: &mut DataFrame, col_name: &str){
        let v: Vec<Option<Self>> = Vec::with_capacity(1e5 as usize);
        df_dst.add_col(col_name, v);
    }
    // copy column data from one DataFrame to another using a qry.i_map to reorder rows
    fn copy_col_map(df_src: &DataFrame, df_dst: &mut DataFrame, qry: &Query, col_name: &str){
        let v_src = df_src.get_ref::<Self>(col_name);
        qry.map_column(df_dst, v_src, col_name);
    }
    // copy column data from one DataFrame to another without modification
    fn copy_col(df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str){
        let v_src = df_src.get_ref::<Self>(col_name);
        df_dst.add_col(col_name, v_src.to_vec());
    }
    // copy all rows of a column from one DataFrameSlice to a DataFrame without modification
    fn copy_slice_rows(ds_src: &DataFrameSlice, df_dst: &mut DataFrame, col_name: &str){
        let v_src = ds_src.get_ref::<Self>(col_name);
        df_dst.add_col(col_name, v_src.to_vec());
    }    
    /* -----------------------------------------------------------------------------
    methods without defaults, defined by type below
    ----------------------------------------------------------------------------- */
    // copy specified rows of a column from one DataFrame to another without modification
    fn copy_col_rows(df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str, row_i: &Vec<usize>);
    // get a vector of CellKeys for use when sorting or grouping a single non-String column
    fn get_cell_keys_col(df_src: &DataFrame, qry: &Query, col_name: &str, negate: bool) -> Vec<CellKey>;
    // get a vector of CellKeys for use when indexing a single non-String column
    fn get_grp_keys_col(df_src: &DataFrame, col_name: &str) -> Vec<CellKey>;
    // add a grouping column value to a new output grouped DataFrame using a group map
    fn get_group_val_sorted(df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str, group_map: &Vec<&[(usize, usize, usize)]>);
    // push one cell in a source DataFrame onto the same column in a destination DataFrame
    fn push_col_val(df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str, src_row_i: Option<usize>);
}

// implement the default methods for most types
// cannot put these as default methods in the trait because they require Copy, not available on all types
macro_rules! impl_c_type { ($($t:ty),+) => { $(impl CType for $t {
    fn copy_col_rows(df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str, row_i: &Vec<usize>){
        let v_src = df_src.get_ref::<Self>(col_name);
        df_dst.add_col(col_name, row_i.par_iter().map(|i| v_src[*i]).collect());
    }
    fn get_cell_keys_col(df_src: &DataFrame, qry: &Query, col_name: &str, negate: bool) -> Vec<CellKey>
    where Option<Self>: CellKeyValue {
        // input is df_src before sorting or filtering, indexed using i_map.src_i
        // output is the same length and sort order as i_map at the time the call was made
        let v_src: &[Option<Self>] = df_src.get_ref::<Self>(col_name);
        qry.i_map.par_iter().map(|(src_i, _, _)| v_src[*src_i].pack(negate)).collect()
    }
    fn get_grp_keys_col(df_src: &DataFrame, col_name: &str) -> Vec<CellKey>
    where Option<Self>: CellKeyValue {
        let v_src: &[Option<Self>] = df_src.get_ref::<Self>(col_name);
        v_src.par_iter().map(|v| v.pack(false)).collect()
    }
    fn get_group_val_sorted(
        df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str, group_map: &Vec<&[(usize, usize, usize)]>
    ) where Vec<Option<Self>>: ColVec {
        let v_src = df_src.get_ref::<Self>(col_name);
        let v_dst = group_map.par_iter().map(|gm| {
            v_src[gm[0].0] // df_src indexed by src_i in tuple slot 1
        }).collect::<Vec<Option<Self>>>();
        df_dst.add_col(col_name, v_dst);
    }
    fn push_col_val(df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str, src_row_i: Option<usize>){
        let v_dst = df_dst.get_ref_mut::<Self>(col_name);
        v_dst.push(if let Some(i) = src_row_i { df_src.get_ref::<Self>(col_name)[i] } else { None });
    }
})+ }; }
impl_c_type!(i32, f64, bool, usize, u16);

// implement methods that require cloning of non-Copy, non-Key String type
impl CType for String {
    fn copy_col_rows(df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str, row_i: &Vec<usize>){
        let v_src = df_src.get_ref::<Self>(col_name);
        df_dst.add_col(col_name, row_i.par_iter().map(|i| v_src[*i].clone()).collect());
    }
    fn get_cell_keys_col(_: &DataFrame, _: &Query, col_name: &str, _: bool) -> Vec<CellKey>{
        string_key_error!(col_name, "get_cell_keys_col");
    }
    fn get_grp_keys_col(_: &DataFrame, col_name: &str) -> Vec<CellKey>{
        string_key_error!(col_name, "get_cell_keys_col");
    }
    fn get_group_val_sorted(
        df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str, group_map: &Vec<&[(usize, usize, usize)]>
    ) where Vec<Option<Self>>: ColVec {
        let v_src = df_src.get_ref::<Self>(col_name);
        let v_dst = group_map.par_iter().map(|gm| {
            v_src[gm[0].0].clone() // df_src indexed by src_i in tuple slot 1
        }).collect::<Vec<Option<Self>>>();
        df_dst.add_col(col_name, v_dst);
    }
    fn push_col_val(df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str, src_row_i: Option<usize>){
        let v_dst = df_dst.get_ref_mut::<Self>(col_name);
        v_dst.push(if let Some(i) = src_row_i { df_src.get_ref::<Self>(col_name)[i].clone() } else { None });
    }
}

/* -----------------------------------------------------------------------------
macros for creating row key dispatch methods for different key lengths
----------------------------------------------------------------------------- */
// Fill slot in a Vec<RowKeyN> with the CellKeys from a single column for sorting and grouping.
macro_rules! set_row_keys_n {
    ($set_row_keys_n:ident, $row_keys_n:ident) => {
        /// Fill slot in a Vec<RowKeyN> with the CellKeys from a single column for sorting and grouping.
        pub fn $set_row_keys_n(df_src: &DataFrame, qry: &mut Query, col_name: &str, negate: bool, slot: usize){
            let n: usize = 9; // bytes per CellKey
            let col_type = df_src.col_types[col_name].as_str();
            macro_rules! __set_row_keys_n {
                ($data_type:ty) => {
                    {
                        let v_src = df_src.get_ref::<$data_type>(col_name);
                        // typically i_map is not sorted at this point, so resulting keys are indexed by flt_i
                        qry.$row_keys_n.par_iter_mut()
                            .zip(qry.i_map.par_iter())
                            .for_each(|(row_key, (src_i, _, _))| 
                                row_key[(slot * n)..(slot * n + n)].copy_from_slice(&v_src[*src_i].pack(negate))
                            );
                    }
                }
            }
            match col_type {
                "i32"   => __set_row_keys_n!(i32), // cannot use trait here since need to access specific qry.row_keys_n on each path
                "f64"   => __set_row_keys_n!(f64),
                "bool"  => __set_row_keys_n!(bool),
                "usize" => __set_row_keys_n!(usize),
                "u16"   => __set_row_keys_n!(u16),
                "alloc::string::String" => string_key_error!(col_name, "set_row_keys_n"),
                _ => unknown_type!(col_type, "set_row_keys_n")
            }
        }
    };
}
// Fill slot in a Vec<RowKeyN> with the CellKeys from a single column for indexing.
macro_rules! set_grp_keys_n {
    ($set_grp_keys_n:ident, $grp_keys_n:ident) => {
        /// Fill slot in a Vec<RowKeyN> with the CellKeys from a single column for indexing.
        pub fn $set_grp_keys_n(df_src: &mut DataFrame, col_name: &str, slot: usize){
            let n: usize = 9; // bytes per CellKey
            let col_type = df_src.col_types[col_name].as_str();
            macro_rules! __set_grp_keys_n {
                ($data_type:ty) => {
                    {
                        let mut grp_keys_n = mem::take(&mut df_src.row_index.$grp_keys_n); // take temporary ownership of the index vector
                        let v_src = df_src.get_ref::<$data_type>(col_name); // immutable reference to column data
                        grp_keys_n.par_iter_mut().enumerate().for_each(|(row_i, row_key)| { // df_src must already be sorted
                            row_key[(slot * n)..(slot * n + n)].copy_from_slice(&v_src[row_i].pack(false))
                        });
                        df_src.row_index.$grp_keys_n = grp_keys_n; // restore the index vector to the DataFrame
                    }
                }
            }
            match col_type {
                "i32"   => __set_grp_keys_n!(i32),
                "f64"   => __set_grp_keys_n!(f64),
                "bool"  => __set_grp_keys_n!(bool),
                "usize" => __set_grp_keys_n!(usize),
                "u16"   => __set_grp_keys_n!(u16),
                "alloc::string::String" => string_key_error!(col_name, "set_grp_keys_n"),
                _ => unknown_type!(col_type, "set_grp_keys_n")
            }
        }
    };
}

/* -----------------------------------------------------------------------------
implement column
----------------------------------------------------------------------------- */
impl Column {
    /* =============================================================================
    Add empty column(s) to DataFrame
    ============================================================================= */
    /// Add an empty column of a specific type to a DataFrame where n_row ==0.
    pub fn add_empty_col(df_dst: &mut DataFrame, col_name: &str, col_type: &str) {
        if df_dst.n_row != 0 {
            throw!("DataFrame::add_empty_col error: n_row must be 0 to add an empty column.")
        }
        match col_type {
            "i32"   => <i32   as CType>::add_empty_col(df_dst, col_name),
            "f64"   => <f64   as CType>::add_empty_col(df_dst, col_name),
            "bool"  => <bool  as CType>::add_empty_col(df_dst, col_name),
            "usize" => <usize as CType>::add_empty_col(df_dst, col_name),
            "u16"   => <u16   as CType>::add_empty_col(df_dst, col_name), // cascades to ColVec::to_col which handles RFactor
            "alloc::string::String" => <String as CType>::add_empty_col(df_dst, col_name),
            _ => unknown_type!(col_type, "add_empty_col")
        }
    }
    /// Add an empty columns to a DataFrame using an existing DataFrame as a schema.
    pub fn add_empty_cols(df_src: &DataFrame, df_dst: &mut DataFrame, col_names: Vec<String>) {
        for col_name in &col_names {
            let col_type = df_src.col_types[col_name].as_str();
            Column::add_empty_col(df_dst, col_name, col_type);
        }
    }
    /* =============================================================================
    Copy data from one DataFrame to another
    ============================================================================= */
    /// Copy a column from one DataFrame to another using a qry.i_map to reorder rows.
    pub fn copy_col_map(df_src: &DataFrame, df_dst: &mut DataFrame, qry: &Query, col_name: &str) {
        let col_type = df_src.col_types()[col_name].as_str();
        match col_type {
            "i32"   => <i32   as CType>::copy_col_map(df_src, df_dst, qry, col_name),
            "f64"   => <f64   as CType>::copy_col_map(df_src, df_dst, qry, col_name),
            "bool"  => <bool  as CType>::copy_col_map(df_src, df_dst, qry, col_name),
            "usize" => <usize as CType>::copy_col_map(df_src, df_dst, qry, col_name),
            "u16" => {
                <u16 as CType>::copy_col_map(df_src, df_dst, qry, col_name);
                df_dst.copy_labels(df_src, col_name);
            },
            "alloc::string::String" => <String as CType>::copy_col_map(df_src, df_dst, qry, col_name),
            _ => unknown_type!(col_type, "copy_col_map")
        };
    }
    /// Copy a column (all rows) from one DataFrame to another without modification.
    pub fn copy_col(df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str) {
        let col_type = df_src.col_types()[col_name].as_str();
        match col_type {
            "i32"   => <i32   as CType>::copy_col(df_src, df_dst, col_name),
            "f64"   => <f64   as CType>::copy_col(df_src, df_dst, col_name),
            "bool"  => <bool  as CType>::copy_col(df_src, df_dst, col_name),
            "usize" => <usize as CType>::copy_col(df_src, df_dst, col_name),
            "u16" => {
                <u16 as CType>::copy_col(df_src, df_dst, col_name);
                df_dst.copy_labels(df_src, col_name);
            },
            "alloc::string::String" => <String as CType>::copy_col(df_src, df_dst, col_name),
            _ => unknown_type!(col_type, "copy_col")
        };
    }
    /// Copy all rows of a column from one DataFrameSlice to a DataFrame without modification.
    pub fn copy_slice_rows(ds_src: &DataFrameSlice, df_dst: &mut DataFrame, col_name: &str) {
        let col_type = ds_src.col_types()[col_name].as_str();
        match col_type {
            "i32"   => <i32   as CType>::copy_slice_rows(ds_src, df_dst, col_name),
            "f64"   => <f64   as CType>::copy_slice_rows(ds_src, df_dst, col_name),
            "bool"  => <bool  as CType>::copy_slice_rows(ds_src, df_dst, col_name),
            "usize" => <usize as CType>::copy_slice_rows(ds_src, df_dst, col_name),
            "u16" => {
                <u16 as CType>::copy_slice_rows(ds_src, df_dst, col_name);
                // df_dst.copy_labels(ds_src, col_name);
            },
            "alloc::string::String" => <String as CType>::copy_slice_rows(ds_src, df_dst, col_name),
            _ => unknown_type!(col_type, "copy_slice_rows")
        };
    }
    /// Copy specified rows of a column from one DataFrame to another without modification.
    /// Will panic if any row row index is out of bounds. The output will be (re)ordered
    /// according the order that input row indices are encountered in argument `i`.
    pub fn copy_col_rows(df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str, row_i: &Vec<usize>) {
        let col_type = df_src.col_types()[col_name].as_str();
        match col_type {
            "i32"   => <i32   as CType>::copy_col_rows(df_src, df_dst, col_name, row_i),
            "f64"   => <f64   as CType>::copy_col_rows(df_src, df_dst, col_name, row_i),
            "bool"  => <bool  as CType>::copy_col_rows(df_src, df_dst, col_name, row_i),
            "usize" => <usize as CType>::copy_col_rows(df_src, df_dst, col_name, row_i),
            "u16" => {
                <u16 as CType>::copy_col_rows(df_src, df_dst, col_name, row_i);
                df_dst.copy_labels(df_src, col_name);
            },
            "alloc::string::String" => <String as CType>::copy_col_rows(df_src, df_dst, col_name, row_i),
            _ => unknown_type!(col_type, "copy_col_rows")
        };
    }
    /* =============================================================================
    Handle cell and row keys
    ============================================================================= */
    /// Get a vector of CellKeys for use when sorting or grouping a single non-String column.
    pub fn get_cell_keys_col(df_src: &DataFrame, qry: &Query, col_name: &str, negate: bool) -> Vec<CellKey> {
        let col_type = df_src.col_types[col_name].as_str();
        match col_type {
            "i32"   => <i32   as CType>::get_cell_keys_col(df_src, qry, col_name, negate),
            "f64"   => <f64   as CType>::get_cell_keys_col(df_src, qry, col_name, negate),
            "bool"  => <bool  as CType>::get_cell_keys_col(df_src, qry, col_name, negate),
            "usize" => <usize as CType>::get_cell_keys_col(df_src, qry, col_name, negate),
            "u16"   => <u16   as CType>::get_cell_keys_col(df_src, qry, col_name, negate), // cascades to ColVec::to_col which handles RFactor
            "alloc::string::String" => <String as CType>::get_cell_keys_col(df_src, qry, col_name, negate),
            _ => unknown_type!(col_type, "get_cell_keys_col")
        }
    }
    // Fill slot in a Vec<RowKeyN> with the CellKeys from a single column for sorting and grouping.
    set_row_keys_n!(set_row_keys_2, row_keys_2);
    set_row_keys_n!(set_row_keys_4, row_keys_4);
    set_row_keys_n!(set_row_keys_8, row_keys_8);
    // -------------------------------------------------------------------------------
    /// Get a vector of CellKeys for use when sorting or grouping a single non-String column.
    pub fn get_grp_keys_col(df_src: &DataFrame, col_name: &str) -> Vec<CellKey> {
        let col_type = df_src.col_types[col_name].as_str();
        match col_type {
            "i32"   => <i32   as CType>::get_grp_keys_col(df_src, col_name),
            "f64"   => <f64   as CType>::get_grp_keys_col(df_src, col_name),
            "bool"  => <bool  as CType>::get_grp_keys_col(df_src, col_name),
            "usize" => <usize as CType>::get_grp_keys_col(df_src, col_name),
            "u16"   => <u16   as CType>::get_grp_keys_col(df_src, col_name), // cascades to ColVec::to_col which handles RFactor
            "alloc::string::String" => <String as CType>::get_grp_keys_col(df_src, col_name),
            _ => unknown_type!(col_type, "get_grp_keys_col")
        }
    }
    set_grp_keys_n!(set_grp_keys_2, grp_keys_2);
    set_grp_keys_n!(set_grp_keys_4, grp_keys_4);
    set_grp_keys_n!(set_grp_keys_8, grp_keys_8);
    /* =============================================================================
    Handle grouping actions
    ============================================================================= */
    /// Add a grouping column value to a new output grouped DataFrame using a group map.
    /// The value is taken from the first source row index of each group (since all group row values are identical).
    pub fn get_group_val_sorted(
        df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str, group_map: &Vec<&[(usize, usize, usize)]>
    ){
        let col_type = df_src.col_types[col_name].as_str();
        match col_type {
            "i32"   => <i32   as CType>::get_group_val_sorted(df_src, df_dst, col_name, group_map),
            "f64"   => <f64   as CType>::get_group_val_sorted(df_src, df_dst, col_name, group_map),
            "bool"  => <bool  as CType>::get_group_val_sorted(df_src, df_dst, col_name, group_map),
            "usize" => <usize as CType>::get_group_val_sorted(df_src, df_dst, col_name, group_map),
            "u16"   => {
                <u16 as CType>::get_group_val_sorted(df_src, df_dst, col_name, group_map);
                df_dst.copy_labels(df_src, col_name);
            },
            "alloc::string::String" => <String as CType>::get_group_val_sorted(df_src, df_dst, col_name, group_map),
            _ => unknown_type!(col_type, "get_group_val_sorted")
        }
    }
    /// Push one cell in a source DataFrame onto the same column in a destination DataFrame.
    pub fn push_col_val(df_src: &DataFrame, df_dst: &mut DataFrame, col_name: &str, src_row_i: Option<usize>) {
        let col_type = df_src.col_types[col_name].as_str();
        match col_type {
            "i32"   => <i32   as CType>::push_col_val(df_src, df_dst, col_name, src_row_i),
            "f64"   => <f64   as CType>::push_col_val(df_src, df_dst, col_name, src_row_i),
            "bool"  => <bool  as CType>::push_col_val(df_src, df_dst, col_name, src_row_i),
            "usize" => <usize as CType>::push_col_val(df_src, df_dst, col_name, src_row_i),
            "u16"   => <u16   as CType>::push_col_val(df_src, df_dst, col_name, src_row_i),
            "alloc::string::String" => <String as CType>::push_col_val(df_src, df_dst, col_name, src_row_i),
            _ => unknown_type!(col_type, "push_col_val")
        }
    }
}
