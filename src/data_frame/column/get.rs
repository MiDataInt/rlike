//! Implement general column getters and setters.
//! 
//! Most methods defined in this module perform unsafe transmute operations to get
//! or set column values. All such operations perform upstream cross-validation of the
//! column type and the requested type to ensure transmute safety. 

// dependencies
use std::mem::{transmute, transmute_copy};
use std::collections::HashMap;
use super::{Column, RFactorColumn};
use crate::throw;
pub const STRING: &str = "alloc::string::String";

/* -----------------------------------------------------------------------------
error handling
----------------------------------------------------------------------------- */
macro_rules! col_type_mismatch {
    ($col_name:expr, $col_type:expr, $caller:expr) => {
        throw!("DataFrame::{} error: column {} is not of type {}.", $caller, $col_name, $col_type)
    };
}

/* -----------------------------------------------------------------------------
dispatch methods that apply to Vec<Option<T>>, i.e., an entire Column
----------------------------------------------------------------------------- */
/// Trait `ColVec` supports Column get operations that apply to Vec<Option<T>>,
/// i.e., that get or set data of entire columns.
pub trait ColVec {
    /// Convert a Vec<Option<T>> to a Column of matching type.
    fn to_col<'a>(self) -> (&'a str, Column);
    
    /// Get a reference to the Vec<Option<T>> data in a DataFrame Column.
    fn get_ref<'a>(col: &'a Column, col_name: &str) -> &'a Self;

    /// Get a mutable reference to the Vec<Option<T>> data in a DataFrame Column.
    fn get_ref_mut<'a>(col: &'a mut Column, col_name: &str) -> &'a mut Self;
}
// -----------------------------------------------------------------------------
// implement ColVec for types with default handling
macro_rules! impl_col_vec {
    ($($type_name:expr, $prim_type:ty, $col_type:ident),+) => {
        $(
            impl ColVec for Vec<Option<$prim_type>> {
                fn to_col<'a>(self) -> (&'a str, Column) {
                    ($type_name, Column::$col_type(unsafe {transmute(self)}))
                }
                fn get_ref<'a>(col: &'a Column, col_name: &str) -> &'a Self {
                    match col {
                        Column::$col_type(v) => unsafe { transmute(v) },
                        _ => col_type_mismatch!(col_name, $type_name, "get_ref")
                    }
                }
                fn get_ref_mut<'a>(col: &'a mut Column, col_name: &str) -> &'a mut Self {
                    match col {
                        Column::$col_type(v) => unsafe { transmute(v) },
                        _ => col_type_mismatch!(col_name, $type_name, "get_ref_mut")
                    }
                }
            }
        )+
    };
}
impl_col_vec!{
    "i32",   i32,    RInteger,
    "f64",   f64,    RNumeric,
    "bool",  bool,   RLogical,
    "usize", usize,  RSize,
    STRING,  String, RString
}
// -----------------------------------------------------------------------------
// implement ColVec for complex types with nested data structures
impl ColVec for Vec<Option<u16>> {
    fn to_col<'a>(self) -> (&'a str, Column) {
        ("u16", Column::RFactor(RFactorColumn {
            levels: HashMap::new(),
            labels: Vec::new(),
            auto_label: false,
            data: unsafe {transmute(self)}
        }))
    }
    fn get_ref<'a>(col: &'a Column, col_name: &str) -> &'a Self {
        match col {
            Column::RFactor(f) => unsafe { transmute(&f.data) },
            _ => col_type_mismatch!(col_name, "u16", "get_ref")
        }
    }
    fn get_ref_mut<'a>(col: &'a mut Column, col_name: &str) -> &'a mut Self {
        match col {
            Column::RFactor(f) => unsafe { transmute(&mut f.data) },
            _ => col_type_mismatch!(col_name, "u16", "get_ref_mut")
        }
    }
}
// -----------------------------------------------------------------------------
// implement Column for ColVec methods 
// to_col is called directly on Vec<Option<T>> since Column doesn't exist yet
impl Column {
    /// Get a reference to the Vec<Option<T>> data in a DataFrame Column.
    pub fn get_ref<'a, T>(&'a self, col_name: &str) -> &'a Vec<Option<T>> 
    where Vec<Option<T>>: ColVec {
        <Vec<Option<T>>>::get_ref(self, col_name)
    }
    /// Get a mutable reference to the Vec<Option<T>> data in a DataFrame Column.
    pub fn get_ref_mut<'a, T>(&'a mut self, col_name: &str) -> &'a mut Vec<Option<T>> 
    where Vec<Option<T>>: ColVec {
        <Vec<Option<T>>>::get_ref_mut(self, col_name)
    }
}

/* -----------------------------------------------------------------------------
dispatch methods that apply to Option<T>, i.e. a single cell
----------------------------------------------------------------------------- */
/// Trait `ColVec` supports Column get operations that apply to Vec<Option<T>>,
/// i.e., that get or set data of entire columns.
pub trait ColCell {
    /// Return a single Option<T> from a specific row in a Column, i.e., a cell value.
    fn cell<T: 'static>(col: &Column, col_name: &str, row_i: usize) -> Option<T>;

    /// Set the value of a specific cell in a Column by row index and value.
    fn set<T: 'static + Clone>(col: &mut Column, col_name: &str, row_i: usize, val: Option<T>);
}
// -----------------------------------------------------------------------------
// implement ColCell for types with default handling
macro_rules! impl_col_cell {
    ($($type_name:expr, $prim_type:ty, $col_type:ident),+) => {
        $(
            impl ColCell for Option<$prim_type> {
                fn cell<T: 'static>(col: &Column, col_name: &str, row_i: usize) -> Option<T> {
                    match col {
                        Column::$col_type(v) => unsafe { transmute_copy(&v[row_i]) },
                        _ => col_type_mismatch!(col_name, $type_name, "cell")
                    }
                }
                fn set<T: 'static + Clone>(col: &mut Column, col_name: &str, row_i: usize, val: Option<T>) {
                    match col {
                        Column::$col_type(v) => v[row_i] = unsafe { transmute_copy(&val) },
                        _ => col_type_mismatch!(col_name, $type_name, "set")
                    }
                }
            }
        )+
    };
}
impl_col_cell!{
    "i32",   i32,    RInteger,
    "f64",   f64,    RNumeric,
    "bool",  bool,   RLogical,
    "usize", usize,  RSize
}
// -----------------------------------------------------------------------------
// implement ColCell for complex types with nested data structures
impl ColCell for Option<u16> {
    fn cell<T: 'static>(col: &Column, col_name: &str, row_i: usize) -> Option<T> {
        match col {
            Column::RFactor(f) => unsafe { transmute_copy(&f.data[row_i]) },
            _ => col_type_mismatch!(col_name, "u16", "cell")
        }
    }
    fn set<T: 'static + Clone>(col: &mut Column, col_name: &str, row_i: usize, val: Option<T>) {
        match col {
            Column::RFactor(f) => f.data[row_i] = unsafe { transmute_copy(&val) },
            _ => col_type_mismatch!(col_name, "u16", "set")
        }
    }
}
// -----------------------------------------------------------------------------
// implement ColCell for String
impl ColCell for Option<String> {
    fn cell<T: 'static>(_: &Column, _: &str, _: usize) -> Option<T> {
        // transmute leads to double free error by all methods tried
        throw!("DataFrame error: cell() method not supported for RString columns, use cell_string().")
    }
    fn set<T: 'static + Clone>(col: &mut Column, col_name: &str, row_i: usize, val: Option<T>) {
        match col {
            Column::RString(v) => v[row_i] = unsafe { transmute_copy(&val) },
            _ => col_type_mismatch!(col_name, "String", "set")
        }
    }
}
// -----------------------------------------------------------------------------
// implement Column for ColVec methods 
impl Column {

    /// Return a single Option<T> from a specific row in a Column, i.e., a cell value.
    pub fn cell<T: 'static>(&self, col_name: &str, row_i: usize) -> Option<T> 
    where Option<T>: ColCell {
        <Option<T>>::cell(self, col_name, row_i)
    }

    /// Return the string representation of a specific cell in a Column.
    pub fn cell_string(&self, row_i: usize) -> String {
        // unlike other get methods, cell_string() is not generic over T
        macro_rules! __cell_string {
            ($v:expr) => {
                if let Some(x) = &$v[row_i] { x.to_string() } else { "NA".to_string() }
            };
        }
        match self { 
            Column::RInteger(v)  => __cell_string!(v),
            Column::RNumeric(v)  => __cell_string!(v),
            Column::RLogical(v) => __cell_string!(v),
            Column::RSize(v)   => __cell_string!(v),
            Column::RFactor(f) => {
                if let Some(j) = f.data[row_i] { 
                    let level = j.to_string();
                    if let Some(label) = f.labels.get(j as usize) { 
                        level + " (" + label + ")"
                    } else {
                        level + " (NA)"
                    }
                } else { "NA".to_string() }
            },
            Column::RString(v) => __cell_string!(v),
        }
    }

    /// Set the value of a specific cell in a Column by row index and value.
    pub fn set<T: 'static + Clone>(&mut self, row_i: usize, val: Option<T>, col_name: &str)
    where Option<T>: ColCell {
        <Option<T>>::set(self, col_name, row_i, val);
    }
}
