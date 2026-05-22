// colummn deserialization to support reading from CSV files

// dependencies
use std::str::FromStr;
use std::any::type_name;
use super::{Column, throw, RFactorColumn};

/* -----------------------------------------------------------------------------
error handling
----------------------------------------------------------------------------- */
macro_rules! parse_failure {
    ($col_type:expr, $str:expr) => {
        throw!("DataFrame::deserialize error: failed to parse {} from string {}.", $col_type, $str)
    };
}

/* -----------------------------------------------------------------------------
private trait with a default deserialization method
----------------------------------------------------------------------------- */
trait Deserialize {
    fn parse(v: &mut Vec<Option<Self>>, strs: Vec<&str>)
    where Self: Sized + FromStr {
        let new_data: Vec<Option<Self>> = strs.into_iter().map(|str|
            match str {
                "NA" => None, // all column types interpret "NA" as NA/None
                _ => match str.parse::<Self>() {
                    Ok(x)  => Some(x),
                    Err(_) => parse_failure!(type_name::<Self>(), str)
                }
            }
        ).collect();
        v.extend(new_data);
    }
}

// implement the default method for most types
macro_rules! impl_deserialize { (
    $($t:ty),+) => { $(impl Deserialize for $t {})+ }; 
}
impl_deserialize!(i32, f64, usize, String);

// implement customized logic for bool deserialization to support various character formats
impl Deserialize for bool {
    fn parse(v: &mut Vec<Option<Self>>, strs: Vec<&str>) {
        let new_data: Vec<Option<bool>> = strs.into_iter().map(|str|
            match str.to_uppercase().as_str() {
                "NA"    => None,
                "T"     => Some(true), // thus supports: t, f, T, F, true, false, TRUE, FALSE
                "F"     => Some(false),
                "TRUE"  => Some(true),
                "FALSE" => Some(false),
                _ => parse_failure!("bool", str)
            }
        ).collect();
        v.extend(new_data);
    }
}

/* -----------------------------------------------------------------------------
// customized logic/signature for u16 deserialization to read strings as factor labels
----------------------------------------------------------------------------- */
fn deserialize_u16(f: &mut RFactorColumn, strs: Vec<&str>) {
    let new_data: Vec<Option<u16>> = strs.into_iter().map(|str|
        match str {
            "NA" => None,
            _ => match f.levels.get(str) {
                Some(x) => Some(*x),
                _ => if f.auto_label { // support auto-labeling of new factor levels on the fly
                    let new_level = f.levels.len() as u16;
                    f.levels.insert(str.to_string(), new_level);
                    f.labels.push(str.to_string());
                    Some(new_level)
                } else {
                    throw!("DataFrame::deserialize error: unknown factor label: {str}.")
                }
            }
        }
    ).collect();
    f.data.extend(new_data);
}

/* -----------------------------------------------------------------------------
// implement deserialization on Column with matching to column type
----------------------------------------------------------------------------- */
impl Column {
    /// Deserialize a buffered chunk of incoming string references and extend them onto a Column.
    pub fn deserialize(&mut self, strs: Vec<&str>){
        match self {
            Column::RInteger(v)   => <i32   as Deserialize>::parse(v, strs),
            Column::RNumeric(v)   => <f64   as Deserialize>::parse(v, strs),
            Column::RLogical(v)  => <bool  as Deserialize>::parse(v, strs),
            Column::RSize(v)    => <usize as Deserialize>::parse(v, strs),
            Column::RFactor(f)       => deserialize_u16(f, strs),
            Column::RString(v) => <String as Deserialize>::parse(v, strs),
        }   
    }
}
