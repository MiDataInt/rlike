//! Implement special column data types: RFactor, RTime

// dependencies
use std::collections::HashMap;
use super::Column;
use crate::throw;

/* -----------------------------------------------------------------------------
Special column data types: RFactor
----------------------------------------------------------------------------- */
/// `RFactorColumn` defines the RFactor column type and methods for handling 
/// factor data. New instances can be populated with a vector of pre-defined 
/// factor labels, or with auto-labeling enabled.
#[derive(Clone)]
pub struct RFactorColumn {
    pub levels:      HashMap<String, u16>,
    pub labels:      Vec<String>,
    pub auto_label:  bool,
    pub data:        Vec<Option<u16>>,
}
macro_rules! factor_type_mismatch {
    ($caller:expr) => {
        throw!("DataFrame::{} error: column is not of type RFactor/u16.", $caller)
    };
}
impl Column {
    /// Set the labels and levels for an RFactor column of u16 values, e.g., after a file is read from disk.
    pub fn set_labels(&mut self, labels: Vec<&str>) {
        match self {
            Column::RFactor(col_factor) => {
                col_factor.levels.clear();
                col_factor.labels.clear();
                labels.iter().enumerate().for_each(|(i, label)| {
                    col_factor.levels.insert(label.to_string(), i as u16);
                    col_factor.labels.push(label.to_string());
                });
                col_factor.auto_label = false;
            },
            _ => factor_type_mismatch!("set_labels"),
        }
    }
    /// Return the Vec<String> of labels for an RFactor column.
    pub fn get_labels(&self) -> Vec<String> {
        match self {
            Column::RFactor(col_factor) => col_factor.labels.clone(),
            _ => factor_type_mismatch!("get_labels"),
        }
    }
    /// Return the HashMap<String, u16> of levels for an RFactor column.
    pub fn get_levels(&self) -> HashMap<String, u16> {
        match self {
            Column::RFactor(col_factor) => col_factor.levels.clone(),
            _ => factor_type_mismatch!("get_levels"),
        }
    }
    /// Return a new RFactor Column by factorizing another Column into observed categories.
    /// Only RLogical, RInteger, RSize, and RString Columns can be factorized.
    /// RLogical factorizes to false/true levels regardless of data.
    /// RInteger, RSize, and RString factorize to unique observed levels in the order encountered in the data.
    pub fn factorize(&self, col_name: &str, capacity: usize) -> Column {
        let mut col_out = RFactorColumn { // initialize RFactorColumn
            levels:     HashMap::new(),
            labels:     Vec::new(),
            auto_label: false,
            data:       Vec::with_capacity(capacity)
        };
        if let Column::RLogical(_) = self {
            col_out.levels = HashMap::from([
                ("false".to_string(), 0),
                ("true".to_string(), 1) // NA category added only if present in data
            ]);
            col_out.labels = vec![
                "false".to_string(),
                "true".to_string()
            ];
        } 
        let mut push_opt_level = |label: String|{
            if !col_out.levels.contains_key(&label) {
                col_out.levels.insert(label.clone(), col_out.labels.len() as u16);
                col_out.labels.push(label.clone());
            } 
            col_out.data.push(Some(col_out.levels[&label])) // _all_ col_data rows create factor labels; input None becomes label _NA
        };
        match self {
            Column::RInteger(col_data) => { // i32 can be categorized but levels are not predictable
                col_data.iter().for_each(|opt_val| push_opt_level (
                    if let Some(val) = opt_val { val.to_string() } else { "NA".to_string() }
                ));
            },
            Column::RNumeric(_col_data) => { // f64 cannot be categorized
                throw!("DataFrame::factorize error: column {col_name} of type RNumeric(f64) is not suitable for parsing to categorical values.")
            },
            Column::RLogical(col_data) => { // bool is categorized to true/false even if both values are not present
                col_data.iter().for_each(|opt_val| push_opt_level (
                    if let Some(val) = opt_val { val.to_string() } else { "NA".to_string() }
                ));
            },
            Column::RSize(col_data) => { // usize can be categorized but levels are not predictable
                col_data.iter().for_each(|opt_val| push_opt_level (
                    if let Some(val) = opt_val { val.to_string() } else { "NA".to_string() }
                ));
            },
            Column::RFactor(_col_data) => { // expect caller to parse whether a column is already factorized
                throw!("DataFrame::factorize error: column {col_name} is already an RFactor column.")
            },
            Column::RString(col_data) => { // strings can be categorized but levels are not predictable
                col_data.iter().for_each(|opt_val| push_opt_level (
                    if let Some(val) = opt_val { val.to_string() } else { "NA".to_string() }
                ));
            }
        }
        Column::RFactor(col_out)
    }
}

/* -----------------------------------------------------------------------------
Special column data types: RTime
----------------------------------------------------------------------------- */
// /// The `RTimeColumn` struct defines the RTime column type and methods for 
// /// handling DateTime and Duration data. 
// // #[derive(Clone)]
// pub struct RTimeColumn {
//     time_type: RTimeType,
//     data:      Vec<Option<i64>>,
// }
// /// The `RTimeType` enum defines the allowable RTime column types.
// // #[derive(Clone)]
// pub enum RTimeType {
//     DateUtc,
//     DateTimeUtc,
//     TimeSpan,
//     DateLocal(i64),
//     DateTimeLocal(i64),
//     TimeSpanSince(i64),
// }
