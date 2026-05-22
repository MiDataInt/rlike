---
published: false
---

# RLike Rust crate

`rlike` is a Rust crate with a DataFrame and other data types that 
combine expressive Rust-like statements with R-like data objects.

## History of the name

`rlike` supports data analysis pipelines by providing fast, 
Rust-native data interfaces. The name `rlike` reflects an initial 
goal to provide a naming system reminiscent of R data objects, which 
is sometimes true as implemented, sometimes not. It grew to also
refer to the Rust-like statement syntax deployed by `rlike`.

## RLike DataFrame vs. common alternatives

The primary structure of interest in `rlike` is its
[DataFrame](https://github.com/MiDataInt/mdi-pipelines-framework/tree/main/crates/mdi/src/rlike/data_frame),
a column-oriented data type for table-based data manipulation.

Other Rust packages, such as 
[polars](https://docs.rs/polars/latest/polars/)
 and 
 [nalgebra](https://docs.rs/nalgebra/latest/nalgebra/), 
provide overlapping functionality with DataFrame. You will want to compare 
them to determine which is best for your use case. Crates like `polars` have 
richer feature sets but `rlike` offers a distinct, concise, and expressive 
**Rust-like statement syntax** for column definition, data updates, and queries 
via a powerful set of macros. This statement syntax allows for 
streamlined code with much less quoting, fewer long method chains, 
compile-time type and borrow validation, etc. See the DataFrame
[documentation](https://github.com/MiDataInt/mdi-pipelines-framework/tree/main/crates/mdi/src/rlike/data_frame)
and
[examples](https://github.com/wilsonte-umich/mdi-pipelines-framework/tree/main/crates/mdi/examples) for details.

Differences aside, `rlike` adopts many of the same best practices
for efficient column-based data manipulation as `polars`, although 
without using the Apache Arrow format. Most notably, `rlike` encodes 
null/NA values using None elements in Rust `Option<T>` values. 
