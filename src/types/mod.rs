//! The `rlike::types` module provides a restricted set of data types
//! for use in RLike data objects.
//! - RInteger: `Option<i32>`
//! - RNumeric: `Option<f64>`
//! - RLogical: `Option<bool>`
//! - RSize:    `Option<usize>`
//! - RFactor:  user-defined categorical data, stored as level=Option<u16>
//! - RString:  `Option<String>` (discouraged for performance reasons; use RFactor if possible)
//! - TODO: RTime:    different manifestation of dates and times, stored as timestamp=Option<i64>
//! - TODO: RSized:   `Option<T>` to support custom user-defined data structures with fized memory size
//! 
//! All types are wrapped in `Option<T>` to support R-like NA values, i.e., 
//! missing/null values per cell. 
//! 
//! For simplicity and to reflect the R-like nature of this library,
//! only a single RInteger (i32-based) and a single RNumeric (f64-based) 
//! type are supported, in addition to RLogical (bool), RFactor and RString.
//! 
//! One Rust-like type, `RSize`, is provided for use as an index or counter
//! with 64-bit support on most platforms.
//! 
//! Type `RTime` stores millisecond timestamps as i64 with custom handling that is 
//! more like Rust `chrono` (on which RLike depends) than R, but not exactly like either. 
//! 
//! Importantly, all standard R-like types are stored using distinct Rust primitives,
//! a design choice that makes it possible to refer to column types in different ways.
//! You will typically use the primitive type names in your code, e.g., `col1:i32 = 12543`
//! fills a DataFrame column of the RInteger type.
//! 
//! Finally, the `RSized` type allows calling code to create a column based on any 
//! arbitrary Rust type that implements the `Sized` trait (and other common traits),
//! i.e., that does not have embedded `String`s or other variable-length data.
//! The RSized type is similar to an R named list, but with fixed fields and size.

// modules
mod agg;
mod r#do;

// dependencies
use paste::paste;
pub use agg::Agg;
pub use r#do::Do;

/* -----------------------------------------------------------------------------
RLike and associated trait definitions
----------------------------------------------------------------------------- */
/// Trait RLike is implemented for all data types supported by R-like data objects.
pub trait RLike: Default + Send + Sync {

    // Inner is a Rust primitive type
    type Inner: Default + Clone + PartialOrd; 
    fn default() -> Self;

    // Type-dependent get getter methods (since not every type is simple Option<T>)
    // Macros below define standardized method implementations for primitive types.
    fn from_inner(inner: Self::Inner) -> Self;
    fn to_inner(&self) -> Self::Inner;
    fn to_string(&self) -> String;
    fn is_na(&self) -> bool;
}

/// Trait ToRL converts T into Option<T>.
/// Example usage: `(15).to_rl()`, equivalent to `Some(15)`.
pub trait ToRL {
    type Inner; // an R-like type
    fn to_rl(self) -> Self::Inner;
}

/// Trait ToRLVec converts a Vec<T> to a Vec<Option<T>>.
/// Example usage: `vec![1, 2, 3].to_rl()`.
pub trait ToRLVec {
    type Inner; // an R-like type
    fn to_rl(&self) -> Vec<Self::Inner>;
}
fn wrap_option_vec<T: Copy>(x: &Vec<T>) -> Vec<Option<T>> {
    x.iter().map(|&x| Some(x)).collect() // execute vectorized option wrapping
}

/* -----------------------------------------------------------------------------
RLike trait implementation for Rust primitive types
----------------------------------------------------------------------------- */
macro_rules! impl_rlike_primitive {
    ($rlike_type:ident, $primitive:ty) => {
        paste!{ 
            /// [<$rlike_type Prim>] data type is $primitive.
            pub type [<$rlike_type Prim>] = $primitive; 
            /// $rlike_type data type is Option<$primitive>, i.e., Option<[<$rlike_type Prim>]>.
            pub type $rlike_type = Option<$primitive>;            
        }
        impl RLike for $rlike_type {
            type Inner = $primitive;
            fn default() -> Self { None }
            fn from_inner(inner: Self::Inner) -> Self { Some(inner) }
            fn to_inner(&self) -> Self::Inner { self.unwrap_or(Self::Inner::default()) } // thus, enforces na.rm = TRUE
            fn to_string(&self) -> String { 
                self.map(|x| x.to_string()).unwrap_or("NA".to_string()) 
            }
            fn is_na(&self) -> bool { self.is_none() }
        }
        impl ToRL for $primitive {
            type Inner = $rlike_type;
            fn to_rl(self) -> Self::Inner { Some(self) }
        }
        impl ToRLVec for Vec<$primitive> {
            type Inner = $rlike_type;
            fn to_rl(&self) -> Vec<Self::Inner> { wrap_option_vec(self) }
        }
    };
}
impl_rlike_primitive!(RInteger, i32);
impl_rlike_primitive!(RNumeric, f64);
impl_rlike_primitive!(RLogical, bool);
impl_rlike_primitive!(RSize,    usize);
impl_rlike_primitive!(RFactor,  u16);

/* -----------------------------------------------------------------------------
RLike trait implementation for String data type
----------------------------------------------------------------------------- */
/// RStringPrim data type is String.
pub type RStringPrim = String;
/// RString data type is Option<String>, i.e., Option<RStringPrim>.
pub type RString = Option<String>;
impl RLike for RString {
    type Inner = String;
    fn default() -> Self { None }
    fn from_inner(inner: Self::Inner) -> Self { Some(inner) }
    fn to_inner(&self) -> Self::Inner {
        self.as_ref().map_or_else(|| "NA".to_string(), |s| s.clone())
    }
    fn to_string(&self) -> String { self.to_inner() }
    fn is_na(&self) -> bool { self.is_none() }
}
impl ToRL for String {
    type Inner = RString;
    fn to_rl(self) -> Self::Inner { Some(self) }
}
impl ToRLVec for Vec<String> {
    type Inner = RString;
    fn to_rl(&self) -> Vec<Self::Inner> {
        self.iter().map(|x| Some(x.clone())).collect()
    }
}
impl ToRL for &str {
    type Inner = RString;
    fn to_rl(self) -> Self::Inner { Some(self.to_string()) }
}
impl ToRLVec for Vec<&str> {
    type Inner = RString;
    fn to_rl(&self) -> Vec<Self::Inner> {
        self.iter().map(|x| Some(x.to_string())).collect()
    }
}
