/* -----------------------------------------------------------------------------
Aggregation functions for RLike (Option-wrapped) types, as used in do() queries
----------------------------------------------------------------------------- */

// dependencies
use rayon::prelude::*;
use std::iter::Sum;
use std::ops::Add;

/// The `Do` struct provides a set of aggregation functions for RLike data types
/// mainly for use in do queries. They take Vec<Option<T>> as input and return
/// Vec<Option<U>> as output to simplify assembly of grouped DataFrames. 
/// 
/// The presumed input is a vector of column values in a set of grouped DataFrame rows. 
/// 
/// Outputs are either single aggregated values or vectors of the same length as the input
/// vector, in each case wrapped for direct usage in a `df_new!()`or `df_inject!()` statement.
/// 
/// While the primary utility of `Do` methods is in do queries, the methods are not tied
/// to DataFrame objects and can be used for any RLike data manipulation.
/// 
/// Implicit row-wise parallelization is used for all operations whenever possible.
/// 
/// See `df_query!()` for usage details.
pub struct Do {}
impl Do {
    /* =============================================================================
    helper functions
    ============================================================================= */
    // Remove NA values from a vector of Option<T> values in preparation for aggregation.
    pub fn na_rm<T: Copy + Send + Sync>(x: &[Option<T>]) -> Vec<T> {
        x.par_iter().filter_map(|&x| x).collect()
    }
    // Replace NA/None values in a vector of Option<T> values to Some(T) in preparation for aggregation.
    pub fn na_replace<T: Copy + Send + Sync>(x: &mut Vec<Option<T>>, na_replace: T) {
        x.par_iter_mut().for_each(|x| {
            if x.is_none() { *x = Some(na_replace) }
        });
    }
    /* =============================================================================
    single-column functions
    ============================================================================= */
    /* -----------------------------------------------------------------------------
    Functions that return a single aggregate value, i.e., Vec<Option<U>> with length 1,
    calculated from non-NA values in the input vector, where a value is reported even 
    if all input values are NA. 
    ----------------------------------------------------------------------------- */
    /// Return a boolean indicating whether the input vector has any non-NA data.
    pub fn has_data<T: Copy + Send + Sync>(x: &[Option<T>]) -> Vec<Option<bool>> { 
        vec![Some(Do::na_rm(x).len() > 0)]
    }
    /// Return a usize indicating the number of non-NA elements in the input vector.
    pub fn count<T: Copy + Send + Sync>(x: &[Option<T>]) -> Vec<Option<usize>> { 
        vec![Some(Do::na_rm(x).len())]
    }
    /* -----------------------------------------------------------------------------
    Functions that return a single aggregate value, i.e., Vec<Option<U>> with length 1,
    calculated from non-NA values in the input vector, where NA is reported if all input 
    values are NA. 
    ----------------------------------------------------------------------------- */
    /// Return a usize indicating the number of TRUE elements in an input Vec<Option<bool>.
    pub fn true_count(x: &[Option<bool>]) -> Vec<Option<usize>> { // ==sum(bool)
        let na_rm = Do::na_rm(x);
        if na_rm.len() == 0 { return vec![None]; }
        vec![Some(na_rm.into_par_iter().filter(|&x| x).count())]
    }
    /// Return the sum of the non-NA elements in the input vector.
    /// This sum method may overflow depending the nature the input data.
    /// See `sum_into()` for a method that can prevent overflow.
    pub fn sum<T: Copy + Sum + Send + Sync>(x: &[Option<T>]) -> Vec<Option<T>> {
        let na_rm = Do::na_rm(x);
        if na_rm.len() == 0 { return vec![None]; }
        vec![Some(na_rm.into_par_iter().sum::<T>())]
    }
    /// Return the sum of the non-NA elements in the input vector as a different data type.
    /// The method can prevent overflow by converting input values to a different data 
    /// type before summing, e.g., summing i32 values into f64.
    pub fn sum_into<T: Copy + Into<U> + Send + Sync, U: Sum + Send + Sync>(x: &[Option<T>]) -> Vec<Option<U>> {
        let na_rm = Do::na_rm(x);
        if na_rm.len() == 0 { return vec![None]; }
        let sum = na_rm.into_par_iter().map(|val| val.into()).sum::<U>();
        vec![Some(sum)]
    }
    /// Return the mean of the non-NA elements in the input vector.
    pub fn mean<T: Copy + Into<f64> + Send + Sync>(x: &[Option<T>]) -> Vec<Option<f64>> { 
        let na_rm = Do::na_rm(x);
        let n = na_rm.len() as f64;
        if n == 0.0 { return vec![None]; }
        vec![Some(na_rm.into_par_iter().map(|val| val.into()).sum::<f64>() / n)]
    }
    /// Return the fraction of TRUE elements among the non-NA elements of an input Vec<Option<bool>.
    pub fn true_freq(x: &[Option<bool>]) -> Vec<Option<f64>> { // ==mean(bool)
        let na_rm = Do::na_rm(x);
        let n = na_rm.len() as f64;
        if n == 0.0 { return vec![None]; }
        vec![Some(na_rm.into_par_iter().filter(|&x| x).count() as f64 / n)]
    }
    /* -----------------------------------------------------------------------------
    Functions that return Vec<Option<U>> of same length as input, where output values
    are unique to each element but depend on a value aggregated over all elements.
    NA input values result in NA output values.
    ----------------------------------------------------------------------------- */
    /// Return the frequency of each value among the non-NA values in a vector.
    pub fn freq<T: Copy + Default + Add<Output = T> + Sum + Into<f64> + Send + Sync>(x: &[Option<T>]) -> Vec<Option<f64>> {
        let sum = Self::sum_into::<T, f64>(x)[0];
        if sum.is_none() { return vec![None]; }
        let sum = sum.unwrap();
        x.par_iter().map(|&x| { // iterate over x, not na_rm
            if let Some(val) = x { Some(val.into() / sum) } else { None } }
        ).collect()
    }
    /// Return running sum of the non-NA values in a vector.
    /// This sum method may overflow depending the nature the input data.
    /// See `cumsum_into()` for a method that can prevent overflow.
    pub fn cumsum<T: Copy + Default + Add<Output = T>>(x: &[Option<T>]) -> Vec<Option<T>> {
        // cannot be parallelized because the sum depends on the previous ordered value
        x.iter().scan(T::default(), |state, &x| {
            if let Some(val) = x { // iterate over x, not na_rm
                *state = *state + val; 
                Some(Some(*state))
            } else { 
                Some(None) 
            }
        }).collect()
    }
    /// Return running sum of the non-NA values in a vector as a different data type.
    /// The method can prevent overflow by converting input values to a different data 
    /// type before summing, e.g., summing i32 values into f64.
    pub fn cumsum_into<T: Copy + Into<U>, U: Copy + Default + Add<Output = U>>(x: &[Option<T>]) -> Vec<Option<U>> {
        // cannot be parallelized because the sum depends on the previous ordered value
        x.iter().scan(U::default(), |state, &x| {
            if let Some(val) = x { // iterate over x, not na_rm
                *state = *state + val.into(); 
                Some(Some(*state))
            } else { 
                Some(None) 
            }
        }).collect()
    }
    /* =============================================================================
    two-column functions
    ============================================================================= */
    pub fn add<T: Copy + Default + Add<Output = T> + Send + Sync>(x: &[Option<T>], y: &[Option<T>]) -> Vec<Option<T>> {
        x.par_iter().zip(y.par_iter()).map(|(&x, &y)| {
            if let (Some(x), Some(y)) = (x, y) { Some(x + y) } else { None }
        }).collect()
    }
}
