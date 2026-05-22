// construct and fill DataFrames
pub mod new;
pub mod column;
pub mod io;
pub mod display;

// getters and setters
pub mod get;
pub mod set;
pub mod index;

// create or fill DataFrames with data from existing DataFrames
pub mod inject;
pub mod bind;
pub mod query;
pub mod join;
