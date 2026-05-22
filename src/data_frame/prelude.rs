//! RLike DataFrame re-exports to support `use mdi::data_frame::prelude::*;`

/*-----------------------------------------------------------------------
RLike data types and associated traits
---------------------------------------------------------------------- */
// RLike data types and associated traits
pub use crate::types::*;
/*-----------------------------------------------------------------------
DataFrame and associated types and traits
---------------------------------------------------------------------- */
// DataFrame and associated types and traits
pub use crate::data_frame::DataFrame;
pub use crate::data_frame::r#trait::DataFrameTrait;
// DataFrame queries
pub use crate::data_frame::query::{QueryStatus, Query};
// DataFrame joins
pub use crate::data_frame::join::{Join, JoinType};
/*-----------------------------------------------------------------------
re-export macros flagged with #[macro_export]
macro export always happens in the crate root, so we re-export them here
---------------------------------------------------------------------- */
// from new.rs
pub use crate::df_new;
/*-------------------------------------------------------------------- */
// from io.rs
pub use crate::df_read;
pub use crate::df_write;
pub use crate::df_load;
pub use crate::df_save;
/*-------------------------------------------------------------------- */
// from get.rs
pub use crate::df_get;
/*-------------------------------------------------------------------- */
// from set.rs
// user macros
pub use crate::df_set;
//internal macros
pub use crate::__df_set;
/*-------------------------------------------------------------------- */
// from inject.rs
// user macros
pub use crate::df_inject;
//internal macros
pub use crate::__df_inject;
/*-------------------------------------------------------------------- */
// from column.rs
pub use crate::df_set_labels;
pub use crate::df_retain;
pub use crate::df_drop;
/*-------------------------------------------------------------------- */
// from query.rs
// user macros
pub use crate::df_query;
pub use crate::df_select;
pub use crate::df_pivot;
//internal macros
pub use crate::__qry_filter;
pub use crate::__qry_sort;
pub use crate::__qry_select;
pub use crate::__qry_drop;
pub use crate::__qry_group;
pub use crate::__qry_aggregate_col;
pub use crate::__qry_aggregate;
pub use crate::__qry_pivot;
pub use crate::__df_query;
pub use crate::__df_query_chain;
/*-------------------------------------------------------------------- */
// from index.rs
pub use crate::df_index;
pub use crate::df_get_indexed;
/*-------------------------------------------------------------------- */
// from bind.rs
pub use crate::df_cbind;
pub use crate::df_cbind_ref;
pub use crate::df_cbind_slice;
pub use crate::df_rbind;
pub use crate::df_rbind_ref;
/*-------------------------------------------------------------------- */
// from join.rs
// user macros
pub use crate::df_join;
pub use crate::df_inner;
pub use crate::df_left;
pub use crate::df_outer;
//internal macros
pub use crate::__df_join3;
pub use crate::__df_join2;
pub use crate::__df_join1;
