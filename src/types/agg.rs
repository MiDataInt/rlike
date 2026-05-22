/* -----------------------------------------------------------------------------
Aggregation functions for RLike primitive (not Option-wrapped) types
----------------------------------------------------------------------------- */

// dependencies
use std::iter::Sum;
// use std::ops::Add;

/// The `Agg` struct provides a set of aggregation functions for RLike data types
/// for use in grouping queries.
/// 
/// The input is Vec<T>, so NA/None values must already have been removed.
/// Output values are calculated from the supplied values only, so are 
/// equivalent to using `na.rm = TRUE` in R.
/// 
/// See `df_query!()` for usage details.
pub struct Agg {}
impl Agg {
    /// Return a boolean indicating whether the input vector has any data.
    pub fn has_data<T>(x: Vec<T>) -> bool { 
        x.len() > 0
    }
    /// Return a usize indicating the number of elements in the input vector.
    pub fn count<T>(x: Vec<T>) -> usize { 
        x.len() 
    }
    /// Return a usize indicating the number of TRUE elements in an input Vec<bool>.
    pub fn true_count(x: Vec<bool>) -> usize {
        x.iter().filter(|&x| *x).count() 
    }
    /// Return the sum of the elements in the input vector as the same data type.
    /// This sum method may overflow depending the nature the input data.
    /// See `sum_into()` for a method that can prevent overflow.
    pub fn sum<T: Sum>(x: Vec<T>) -> T {
        x.into_iter().sum::<T>() 
    }
    /// Return the sum of the elements in the input vector as a different data type.
    /// The method can prevent overflow by converting input values to a different data 
    /// type before summing, e.g., summing i32 values into f64.
    pub fn sum_into<T: Into<U>, U: Sum>(x: Vec<T>) -> U {
        x.into_iter().map(|val| val.into()).sum::<U>() 
    }
    /// Return the mean of the elements in the input vector.
    pub fn mean<T: Sum + Into<f64> + Copy>(x: Vec<T>) -> f64 { 
        let n = x.len() as f64;
        Self::sum_into(x) / n // sum as f64 to prevent overflow
    }
    /// Return the fraction of TRUE elements in an input Vec<bool>.
    pub fn true_freq(x: Vec<bool>) -> f64 {
        let n = x.len() as f64;
        x.iter().filter(|&x| *x).count() as f64 / n
    }
    /// Return the first element of the input vector.
    pub fn first<T: Copy>(x: Vec<T>) -> T { 
        x[0] 
    }
    /// Return the last element of the input vector.
    /// Note: this is not the same as `x.last()`, which returns an Option<T>.
    pub fn last<T: Copy>(x: Vec<T>) -> T { 
        x[x.len() - 1] 
    }
    /// Return the minimum element value of the input vector.
    pub fn min<T: Ord + Copy>(x: Vec<T>) -> T {
        x.iter().fold(x[0], |a, &b| a.min(b))
    }
    /// Return the maximum element value of the input vector.
    pub fn max<T: Ord + Copy>(x: Vec<T>) -> T {
        x.iter().fold(x[0], |a, &b| a.max(b))
    }
    /// Return the minimum element value of an f64 input vector.
    pub fn min_f64(x: Vec<f64>) -> f64 {
        x.iter().fold(f64::INFINITY, |a, &b| a.min(b))
    }
    /// Return the maximum element value of an f64 input vector.
    pub fn max_f64(x: Vec<f64>) -> f64 {
        x.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b))
    }
    // TODO: median, SD, var, etc.
}
