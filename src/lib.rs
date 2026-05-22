//! `rlike` is a Rust crate with a DataFrame and other data types that  
//! combine expressive Rust-like statements with R-like data objects.
//! 
//! The primary structure of interest in `rlike` is its `DataFrame`, a 
//! column-oriented data type for table-based data manipulations.

pub mod types;
pub mod data_frame; 

/* -----------------------------------------------------------------------------
exported macros
----------------------------------------------------------------------------- */
/// Create a vector of column filter operations for use with DataFrame::filter().
#[macro_export]
macro_rules! throw {
    ($($arg:tt)*) => {
        {
            eprintln!("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            eprintln!($($arg)*);
            eprintln!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
            std::process::exit(1)
        }
    };
}
