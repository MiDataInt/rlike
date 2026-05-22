---
published: false
---

# RLike DataFrame

`rlike::DataFrame` offers a columnar data structure stored 
as HashMaps of named columns, where data are stored as one vector per column. 
Different columns can have different enumerated RLike data types. Thus, columns 
take the form `HashMap<String, Column>`, where a Column is `Vec<RLike>`, e.g., 
`Vec<Option<i32>>`, etc.

## Macros vs. methods

You are encouraged to use a series of powerful macros to create and manipulate
DataFrames, which offer an **expressive statement syntax**. However, many actions
can be accomplished using Rust-like method chaining, albeit with more verbose 
code and a greater need to understand the underlying data structures.

## Data types

`rlike::DataFrame` columns must conform to one of a set of RLike data types
[described here](https://github.com/wilsonte-umich/mdi-pipelines-framework/blob/main/crates/mdi/src/rlike/types/mod.rs),
which include standard integer, numeric, boolean, factor and string types,
as well as support for custom data types created by users that implement `Sized`.
Throughout, column values are `Vec<Option<T>>` to allow NA/null values to 
be encoded using Rust `None`.

## Detailed examples

The following is an organizational overview of DataFrame macros and methods. 
Many **code snippets below are pseudo-code**, they are a guide to the main 
DataFrame features, not runnable examples. See the documentation of the 
individual macros and methods as well as the 
[complete examples](https://github.com/wilsonte-umich/mdi-pipelines-framework/tree/main/crates/mdi/examples) 
for details on usage and statement syntax.

## Loading DataFrame for use

The create includes a prelude module to allow all DataFrame features and macros
to be enabled in your script with command:

```rust
use mdi::data_frame::prelude::*;
```

## Creating a DataFrame anew

The `df_new!()` macro is used to create a new DataFrame without using 
other DataFrames as input.

- `let mut df = df_new!();` = create a new empty DataFrame
- `let mut df = df_new!(statement, ...);` = create and fill a new DataFrame

The following are related methods used to create empty DataFrames 
with no columns or with empty columns matching an existing DataFrame's schema.

- `let mut df = DataFrame::new();` = create an empty DataFrame 
- `let mut df = DataFrame::from_schema(&df);` = create an empty DataFrame matching another DataFrame's columns

## Filling a DataFrame from a file or stream

After establising an empty DataFrame with a column schema, you can fill it with
row-major data, e.g., a CSV file, read from a file or STDIN using the `df_read!()` 
macro, and write it again with `df_write!()`.

- TODO

Alternatively, RLike offers a binary column-major storage format for DataFrames, 
which loads faster using the analogous `df_load!()` and `df_save!()` macros.

- TODO

## Extracting data from DataFrames

The following are the main DataFrame getter and setter macros when you know the
column name and/or integer row index.

- `let vals: Vec<Option<T>> = df_get!(&df, col [, ...]);` = get [and modify] data from col

These are the related getter and setter public methods.

- `let vals: Vec<Option<T>> = df.get(col)` = copy the data from col
- `let vals: &Vec<Option<T>> = df.get_ref(col)` = return a reference to the data in col
- `let vals: &mut Vec<Option<T>> = df.get_ref(col)` = return a mutable reference to the data in col
- `let val: String = df.cell_string(col, row_i)` = retrieve a single cell value from col as a String
- `let val: Option<T> = df.cell(col, row_i)` = retrieve a single cell value from col
- `df.set(col, row_i, val)` = update a single cell value in df by mutable reference

In addition, DataFrames support data retrieval keyed by the values of one to eight columns.

- 'let df = df_index!(df, col, ...)' = set an index on df using the named columns as keys
- 'let ds = df_get_indexed!(df, col = val, ...)' = get a DataFrameSlice of rows matching specific key value(s)

## Modifying DataFrames in place

The following macros add or update columns of a DataFrame instance in place, 
i.e., the df is permanently modified by mutable reference with only as much copying 
as needed to fill new rows or columns.

- `df_retain!(&mut df, col, ...)` = keep only named columns in df, in order; drop all others
- `df_drop!(&mut df, col, ...)` = remove one or more named columns from df; retain all others
- `df_set!(&mut df, statement; ...)` = add one or more columns or update existing column(s) by column-wise operations

The following are related chainable methods used to modify DataFrames in place.

- `df.add_col(col, data)` = add one new column to df
- `df.replace_or_add_col(col, data)` = add or replace one column in df
- `df.retain_cols(cols)` = remove all but the named columns from df
- `df.drop_col(col)` = remove one column from df
- 'df.reserve(additional)' = reserve space for additional rows to minimize reallocation

## Creating simple combinations of existing DataFrames

Two or more DataFrames can be combined in column-wise or row-wise fashion using 
functions with the R-like names `cbind` and `rbind`, respectively. Binding functions 
(like queries below) always return a new DataFrame.

The simplest named macros consume all input DataFrames. Use the same input and output
variable names to replace one of the input DataFrames with the result.

- `let mut df1 = df_cbind!(df1, df2, ...)` = consume and combine two or more DataFrames by column
- `let mut df3 = df_rbind!(df1, df2, ...)` = consume and combine two or more DataFrames by row

The following are equivalent methods for consuming and binding just two DataFrames.

- `let mut df1 = df1.cbind(df2)` = consume and combine two DataFrames by column
- `let mut df3 = df1.rbind(df2)` = consume and combine two DataFrames by row

Use the equivalent macros and methods with a `_ref` suffix to combine two or more 
DataFrames by reference without consuming them. Especially for `cbind_ref()`, this
requires more allocation and copying so is slower than the non-ref version (due to the
column-wise data model, even `rbind()` requires copying from df2 to df1, whereas
`cbind()` allows the output DataFrame to simply take ownership of df2 columns).

- `let mut df1 = df_cbind_ref!(&df1, &df2, ...)` = copy and combine two or more DataFrames by column
- `let mut df3 = df_rbind_ref!(&df1, &df2, ...)` = copy and combine two or more DataFrames by row
- `let mut df1 = df1.cbind_ref(&df2)` = copy and combine two DataFrames by column
- `let mut df3 = df1.rbind_ref(&df2)` = copy and combine two DataFrames by row

The bind macros enforce data integrity by ensuring a compatible number of rows
for `cbind()` and a matching column schema for `rbind()`, noting that `cbind()`
will recycle empty and single-row DataFrames as needed to match another DataFrame.

## Queries that act on a single DataFrame

The `df_query!()` macro supports queries on a single DataFrame in SQL-like patterns. 
Queries return a newly allocated DataFrame, copying data from the input DataFrame by 
reference as needed, i.e., input DataFrames are neither altered nor consumed. If you 
want the query to replace the input DataFrame, simply use the same variable name. 
Query actions can be chained together within a single call to `df_query!()` as 
illustrated below.

### Query Actions

Queries are constructed by a series of sequential calls to query 'actions'.

The first non-exclusive actions collect and pre-calculate parameters needed for query execution:

- `filter(statement; ...)` = calculate one or more row filters prior to query execution
- `sort()` or `sort(col, ...)` = sort the output by one or more columns
- `group(statement)` = list the grouping and aggregation column(s), in formula syntax

The last exclusive actions trigger query execution toward different goals and output types.
They return a new DataFrame (or Vec<T> in the case of `do::<T>()`).

- `select()` or `select(col, ...)` = list columns to include in the output; execute query
- `drop(col, ...)` = list columns to omit from the output; execute query
- `aggregate(statement; ...)` = calculate zero to many group aggregates as one value per group
- `do(op)` = execute a custom operation on grouped data to return a new DataFrame by `rbind`ing groups
- `do::<T>(op)` = execute a custom operation on grouped data to return a new Vec<T> by `flatten`ing groups
- `pivot(statement)` = define a formula and fill operation for long-to-wide reshaping via column pivot

One additional action is not a querying action per se, but provides a way to call `df_set!()` inline 
between two chained queries, e.g., allowing compound queries that filter data prior to calculating new 
columns and returning them.

- `set(statement; ...)` = create or update (filtered) columns prior to next query execution

A few intuitive guides helps remember the supported order and combinations of query actions:

- when needed, `filter()` comes first to reduce the number of rows that must be sorted, etc.
- when needed, `sort()` dictates that the output will be sorted by columns specified in either 
  `sort()` itself or other actions; if not sorted, the row order of the input DataFrame is preserved
- `group()` and `pivot()` specify grouping columns in formula syntax
- `select()`, `drop()`, `aggregate()`, `do()`, `do::<T>()`, and `pivot()` trigger
  query execution so are mutually exclusive and always last in a query sequence
- `set()` is not a querying action per se and is only used as a shortcut to `df_set!()` within query chains

See the `df_query!()` macro documentation for details on statement syntax 
and closure arguments for various actions.

### Query macro synonyms

A few query execution actions have synonyms that can simplify code when no other 
prepratory actions are needed. Grouping queries always require multiple actions, so do
not have synonyms.

- `df_select!(&df, col, ...)` is a synonym for `df_query!(&df, select(col, ...))`
- `df_pivot!(&df, statement)` is a synonym for `df_query!(&df, pivot(statement))`

### Select/Drop Queries

The  `select()` and `drop()` actions trigger SQL-like Select query execution without data grouping.

```rust
// copy all rows and all columns
let df_qry = df_query!(&df, select());
let df_qry = df_select!(&df);

// copy all rows of specified columns
let df_qry = df_query!(&df, select(col, ...));
let df_qry = df_select!(&df, col, ...);

// copy filtered rows of all columns
let df_qry = df_query!(&df, filter(...), select());

// copy filtered rows of specified columns
let df_qry = df_query!(&df, filter(...), select(col, ...));

// add output sorting to the above patterns to yield a complete Filter/Sort/Select (FSS) pattern
let df_qry = df_query!(&df, filter(...), sort(col, ...), select(col, ...));
// etc.

// specify the columns to omit from the output, rather than the columns to include
let df_qry = df_query!(&df, filter(...), sort(col, ...), drop(col, ...));
// etc.

// run multiple sequential queries by chaining them in a single call to df_query!()
let df_qry = df_query!(
    &df,
    filter(...), select(col, ...), // pass a temporary df from first query to subsequent actions
    set(...), // perform complex column-wise operations on only the filtered data rows
    sort(), group(...), aggregate(...) // do something with the new columns in a second query
);
```

### Group and Aggregate Queries

The `group()` action specifies that query operations are performed on keyed
groups of the input data rows. The first and simplest pattern calculates
one or more column aggregates for each group.

```rust
// group and aggregate over all rows; report groups in the order they are encountered in df
let df_agg = df_query!(&df, group(...), aggregate(...));

// filter rows before aggregating (only illustrated once, available for all grouping queries)
let df_agg = df_query!(&df, filter(...), group(...), aggregate(...));

// report groups sorted by `group()` key columns; sorting is triggered by the empty `sort()`
let df_agg = df_query!(&df, sort(), group(...), aggregate(...));

// report groups sorted by `sort(...)` columns; `sort(...)` column list may be longer than `group(...)` keys
let df_agg = df_query!(&df, sort(col, ...), group(...), aggregate(...));
```

### Group and Do Queries

More complex grouped outputs can be generated using Do queries, which apply
a user-defined closure operation to each group of rows, resulting in an ouput 
DataFrame or Vec<T> constructed by `rbind` and `flatten`, respectively.

```rust
// group and do over all rows; generate a DataFrame; report groups in the order encountered in df
let df_agg = df_query!(&df, group(...), do(op));

// group and do over all rows; generate a Vec<T>; report groups in the order encountered in df
let df_agg = df_query!(&df, group(...), do<T>(op));

// add filtering and sorting to the above patterns as needed (the same as for Aggregate queries)
let df_agg = df_query!(&df, filter(...), sort(), group(...), do(op));
// etc.
```

### Pivot Queries

Pivot queries are a special case of grouping queries that reshape long data into wide data.
The net effect is similar to Group and Aggregate queries, but the aggregated output columns
are determined by category values in a pivot column. 

```rust
// perform a long-to-wide pivot on all rows; report groups in the order encountered in df
let df_pvt = df_query!(&df, pivot(...));
let df_pvt = df_pivot!(&df, ...);

// perform a long-to-wide pivot on all rows; report groups sorted by the `pivot(...)` key columns
let df_pvt = df_query!(&df, sort(), pivot(...));

// filter rows before pivoting
let df_pvt = df_query!(&df, filter(...), pivot(...));
let df_pvt = df_query!(&df, filter(...), sort(), pivot(...));

// select after pivot by query chaining; will fail if select() columns are missing from pivot categories
let df_pvt = df_query!(&df, pivot(...), select(col, ...));
```

## Joining/merging two DataFrames on key columns

Whereas queries take a single DataFrame as input, the `df_join!()` macro 
performs SQL-like join (R-like merge) operations on two or more DataFrames using
a partially shared statement syntax.

The following preparative actions operate the same as for single DataFrame queries:

- `filter(...)` = calculate one or more row filters prior to executing the join
- `sort()` = report results sorted by the join key columns (only empty sort() allowed for joins)

The following actions trigger execution of their respective join types 
(note that right joins are rarely needed and not supported):

- `inner(...)` = inner join, i.e., only return rows with matching key values
- `left(...)`  = left join,  i.e., return all rows from df1 and matching rows from df2
- `outer(...)` = outer join, i.e., return all rows from both DataFrames

As with queries, the following macros are simplifying synonyms for `df_join!()`
when no preparative actions are needed prior to inner, left, or outer joins
of exactly two DataFrames. Note the difference that `df_inner!()` and `df_left!()`
yield unsorted output, whereas `df_outer!()` yields sorted output.

- `df_inner!(&df1, &df2, ...)` is a synonym for `df_join!(&df1, &df2, inner(...))`
- `df_left!(&df1, &df2, ...)`  is a synonym for `df_join!(&df1, &df2, left(...))`
- `df_outer!(&df1, &df2, ...)` is a synonym for `df_join!(&df1, &df2, sort(), outer(...))`

See the `df_join!()` macro documentation for details on join statement syntax.

```rust
// unfiltered, unsorted joins of two DataFrames by the hash join method
let df_join = df_join!(&df1, &df2, inner(...));
let df_join = df_join!(&df1, &df2, left(...));
let df_join = df_inner!(&df1, &df2, ...);
let df_join = df_left!(&df1, &df2, ...);

// unfiltered, sorted joins of two DataFrames by the merge-sort join method
let df_join = df_join!(&df1, &df2, sort(), inner(...));
let df_join = df_join!(&df1, &df2, sort(), left(...));
let df_join = df_join!(&df1, &df2, sort(), outer(...));
let df_join = df_outer!(&df1, &df2, ...);

// add pre-join filtering to joins
let df_join = df_join!(&df1, &df2, filter(...), inner(...));
let df_join = df_join!(&df1, &df2, filter(...), sort(), inner(...));
// etc.

// join multiple DataFrames in a left-associative series
let df_join = df_join!(&df1, &df2, &df3, ..., inner(...));
// etc.

// apply join to temporary DataFrames calculated on the fly
let df_join = df_join!(
    &df_query!(&df1, filter(...), select(col, ...)),
    &df_query!(&df2, filter(...), select(col, ...)),
    inner(...)
);
```
