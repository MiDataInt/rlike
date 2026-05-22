---
published: false
---

# Overview of DataFrame query and join manipulations

Many query-related DataFrame manipulations are built on:

- a lazy approach in which most operations aren't performed until they can be optimized
- a row key assembly for efficient row matching in sort, group, join, and index operations
- a strategy of row index mapping to streamline operations while minimizing copying and allocation

## Filtering strategy

Row filtering uses a "half-lazy" approach enforced by the `df_query!()` macro that processes 
query `filter()` actions. 

As soon as a `filter()` action is encountered, 
the `Query::kept_rows` structure field is filled with a `Vec<bool>` indicating whether or 
not each row passed the filter. There is no penalty for doing this immediately as
row filters must always be calculated on every row and they can be done efficiently at this 
stage in row order using implicit parallel processing.

When multiple filters are requested, they are applied sequentially by `and`ing each new 
filter to `Query::kept_rows` in the equivalent of a `fold`. This avoids the need to 
store filter operations in order to execute them later, simplifying the codebase.

Filters are applied to the DataFrame later, as described below.

## Cell/row key configuration

DataFrames use cell/row keys that are universally applicable abstractions of row data. 
The assembly process reduces one to eight key columns to single key values per row that 
can be used interchangeably for row comparison during sorting, grouping, joining, and indexing. 

Cell/row keys are represented individually in memory as 
elements of a `Vec<[u8; n_bytes]>` vector over all DataFrame rows. Such vectors 
maintain an alignment of 1 regardless of the number of bytes in the key or the number 
of keys/rows in the DataFrame. This configuration supports efficient comparison 
by examining PartialOrd or PartialEq on two `[u8; n_bytes]` keys.

The strategy for constructing keys is very similar to that used for `polars` 
data frames and we refer readers to their excellent description here:

<https://arrow.apache.org/blog/2022/11/07/multi-column-sorts-in-arrow-rust-part-2/>

A difference is that `rlike` DataFrames encode NA status using Some/None within a data
element itself. This NA status accounts for the first byte of an assembled single-column key.
The remaining bytes of a column's key are sized to the largest possible data type (excluding 
Strings, which cannot be indexed by keys), i.e., 8 bytes = 64-bit.

Single-column keys have the same memory size of 1 + 8 = 9 bytes regardless of data type to allow 
them to be combined into predictably sized multi-column keys. Multi-column keys are calculated 
in increments of 2, 4, or 8 columns, such that three and four-column keys each have the size of 
a four-column key, i.e., 4 columns * 9 bytes per column = 36 bytes, i.e., `[u8; 36]`, etc.

For efficiency, row keys are always calculated after any requested filtering has been applied, 
but may be calculated against either `df_flt` or `df_fss` as described below.

## Query index map (i_map)

An index map, or `i_map` in code, applies to a set of processing stages of a single 
DataFrame to represent the consequences of &ndash; or, really, the plan for &ndash; 
a set of table operations.

### Logical sequence of processed DataFrames

The mapping strategy is built on a series of DataFrames named in code as:

- `df_src` = the initial source DataFrame for the query, join, etc.
- `df_flt` = a conceptual DataFrame in which filter operations have been applied to `df_src`
- `df_fss` = the result of applying row sorting and column selection to `df_flt`, where 
            "fss" stands for "filter/sort/select"; `df_fss` only carries non-key columns
- `df_grp` = a DataFrame that initially contains just one row per key column group;
             `df_grp` only carries key columns and only applies to grouping queries

For a select query, `df_fss` is the product DataFrame that is returned to the caller.

For a grouping query, `df_fss` and `df_grp` are constructed in memory and passed to the 
downstream actions that use them to build the final output DataFrame (see below).

The `df_flt` DataFrame is always conceptual in that it never exists in
memory, but it is represented in the correspondingly named indices below.

The `df_fss` DataFrame may also be conceptual if no rows changed during a grouping query,
in which case `df_src` is passed and used in its place.

Join and bind operations use two DataFrames suffixed with `_l` for the 
left-hand side (LHS) and `_r` for the right-hand side (RHS), i.e., `df_src_l` and 
`df_src_r`, etc. Otherwise, maps and keys are similar to queries.

### Index map (i_map) data structure

An `i_map` is a `Vec<(src_i: usize, flt_i: usize, fss_i: usize)>`, i.e., a vector of tuples, 
where each tuple corresponds to one row of `df_src` and carries three row indices:

- `src_i` = row index in `df_src`
- `flt_i` = row index in `df_flt` (as if it had actually existed)
- `fss_i` = row index in `df_fss` (once it is created)

We build the `i_map` and apply filter/sort/select operations to it based on the data in `df_src`,
rather than performing interim operations on the DataFrame itself. Thus, the `i_map` may shrink 
to have fewer elements than `df_src.n_row()` after filters are applied, and its elements may be 
re-ordered after sorting is applied. The `i_map` indices of the prior DataFrame, and `df_src`
itself, remain unchanged as this happens.

Here is an example of an input `df_src` - we will filter against `int_col` and sort and group on 
`num_col` below:

| record_i | int_col | num_col |
|-------|--------|-------|
| 10 | 99 | 0.0 |
| 11 | 99 | 1.1 | 
| 12 | 0 | NA |
| 13 | 99 | 3.3 |
| 14 | 99 | 1.1 |
| 15 | 99 | 2.2 |
| 16 | 0 | NA |
| 17 | 99 | 3.3 |
| 18 | 99 | 4.4 |
| 19 | 99 | 3.3 |

This is the `i_map` for `df_src` - notice that:

- all three indices are numbered sequentially, one for each row in `df_src`
- all indices are the same at this stage
- the `i_map` only carries indices, not data

| src_i | flt_i | fss_i |
|-------|--------|-------|
| 0 | 0 | 0 |
| 1 | 1 | 1 | 
| 2 | 2 | 2 |
| 3 | 3 | 3 |
| 4 | 4 | 4 |
| 5 | 5 | 5 |
| 6 | 6 | 6 |
| 7 | 7 | 7 |
| 8 | 8 | 8 |
| 9 | 9 | 9 |

This is what the `i_map` may look like for `df_flt`, i.e., after filtering
has been applied. Filter application is a simple matter of zipping the initial
`i_map` and `Query::kept_rows` and applying the `filter()` method. Notice that:

- rows 2 and 6, where `int_col == 0`, are gone
- `scr_i` values are unchanged
- `flt_i` and `fss_i` have been renumbered sequentially

| src_i | flt_i | fss_i |
|-------|--------|-------|
| 0 | 0 | 0 |
| 1 | 1 | 1 | 
| 3 | 2 | 2 |
| 4 | 3 | 3 |
| 5 | 4 | 4 |
| 7 | 5 | 5 |
| 8 | 6 | 6 |
| 9 | 7 | 7 |

This is what the `i_map` may look like after sorting - notice that:

- the `i_map` element order has changed to reflect a `num_col` sort
- `scr_i` and `flt_i` values are unchanged and are no longer in numeric order
- `fss_i` has been renumbered sequentially

| src_i | flt_i | fss_i |
|-------|--------|-------|
| 0 | 0 | 0 |
| 1 | 1 | 1 | 
| 4 | 3 | 2 |
| 5 | 4 | 3 |
| 3 | 2 | 4 |
| 9 | 7 | 5 |
| 7 | 5 | 6 |
| 8 | 6 | 7 |

The `i_map` is then used to assemble `df_fss` - the product of a select query.
We can do this using any of the indices stored in the `i_map` as long as we apply
them to a DataFrame or other object with the same filter and sort status as the index.
For `df_fss` assembly, we use `src_i` to index into `df_src` as the copy source.

| record_i | int_col | num_col |
|-------|--------|-------|
| 10 | 99 | 0.0 |
| 11 | 99 | 1.1 |
| 14 | 99 | 1.1 |
| 15 | 99 | 2.2 |
| 13 | 99 | 3.3 |
| 19 | 99 | 3.3 | 
| 17 | 99 | 3.3 |
| 18 | 99 | 4.4 |

## Group map (group_map)

For grouping queries, we next create an extension of an `i_map` called a `group_map`,
which has type `Vec<&[(usize, usize, usize)]>`. A `group_map` is a vector of contiguous 
slices of the sorted (or merely grouped, see below) `i_map`. Thus, a `group_map` is 
an organizational representation of an `i_map` - the indices are not copied from the `i_map`
into the `group_map`, we simply maintain pointers into the grouped `i_map`.  There is one 
element in the `group_map` vector for each group in the data.

The following is a conceptual representaion of the `group_map` resulting from the `i_map` above, 
after grouping for the previously sorted `num_col` - again, this is just for visualization, 
each of the five groups in the `group_map` are stored as slice pointers into the `i_map`:

| src_i | flt_i | fss_i |
|-------|--------|-------|
group 0, num_col == 0.0
| 0 | 0 | 0 |
group 1, num_col == 1.1
| 1 | 1 | 1 | 
| 4 | 3 | 2 |
group 2, num_col == 2.2
| 5 | 4 | 3 |
group 3, num_col == 3.3
| 3 | 2 | 4 |
| 9 | 7 | 5 |
| 7 | 5 | 6 |
group 4, num_col == 4.4
| 8 | 6 | 7 |

From this `group_map`, we can construct `df_grp`, with one row of key columns per group:

| num_col |
|-------|
| 0.0 |
| 1.1 |
| 2.2 |
| 3.3 |
| 4.4 |

and `df_fss`, with the non-key columns and one row per `i_map` element:

| record_i | int_col |
|-------|--------|
group 0, num_col == 0.0
| 10 | 99 |
group 1, num_col == 1.1
| 11 | 99 |
| 14 | 99 |
group 2, num_col == 2.2
| 15 | 99 |
group 3, num_col == 3.3
| 13 | 99 |
| 19 | 99 |
| 17 | 99 |
group 4, num_col == 4.4
| 18 | 99 |

If the `Agg::first()` aggregation function was applied to the non-key columns, we'd get this DataFrame:

| num_col | record_i | int_col |
|-------|-------|--------|
| 0.0 | 10 | 99 |
| 1.1 | 11 | 99 |
| 2.2 | 15 | 99 |
| 3.3 | 13 | 99 |
| 4.4 | 18 | 99 |

### Sorted grouping queries

If we follow the sequence described above in its entirety, we will have performed a sorted grouping 
query. For such a query, the most typical code path would calculate the sort keys from
the conceptual `df_flt`, such that sorting is applied to the reduced data rows after filtering.
Therefore, our vector of sort keys of type `Vec<[u8; n_bytes]>`, would be ordered in a manner suitable
for indexing with `flt_i`. 

For grouping, we may not need to recalculate keys after sorting since they are the same as the sort keys.
However, sometimes a DataFrame comes to us pre-sorted, or the sort keys encompass more columns
than the grouping keys, such that `df_fss` does not carry the proper (or any) grouping keys.
In such cases, we calculate grouping keys from `df_fss` (not from `df_flt` as above), so that they
are instead indexed using `fss_i`.

### Unsorted grouping queries

The process for performing an unsorted query follows the same path as above through 
filtering to construct the `i_map` corresponding to `df_flt`. From there, of course no
sorting is performed. Instead, grouping keys are calculated on `df_flt`, indexed with 
`flt_i`, and hashing is used to establish groups in the order they are encountered
in `df_flt`.

So, here is the unsorted `group_map`:

| src_i | flt_i | fss_i |
|-------|--------|-------|
group 0, num_col == 0.0
| 0 | 0 | 0 |
group 1, num_col == 1.1
| 1 | 1 | 1 | 
| 4 | 3 | 2 |
group 3, num_col == 3.3
| 3 | 2 | 3 |
| 7 | 5 | 4 |
| 9 | 7 | 5 |
group 2, num_col == 2.2
| 5 | 4 | 6 |
group 4, num_col == 4.4
| 8 | 6 | 7 |

and the corresponding `df_grp` and `df_fss`:

| num_col |
|-------|
| 0.0 |
| 1.1 |
| 3.3 |
| 2.2 |
| 4.4 |

| record_i | int_col |
|-------|--------|
group 0, num_col == 0.0
| 10 | 99 |
group 1, num_col == 1.1
| 11 | 99 |
| 14 | 99 |
group 3, num_col == 3.3
| 13 | 99 |
| 17 | 99 |
| 19 | 99 |
group 2, num_col == 2.2
| 15 | 99 |
group 4, num_col == 4.4
| 18 | 99 |

Notice that the resulting groups are the same as above, but their order in the final table is different.

| num_col | record_i | int_col |
|-------|-------|--------|
| 0.0 | 10 | 99 |
| 1.1 | 11 | 99 |
| 3.3 | 13 | 99 |
| 2.2 | 15 | 99 |
| 4.4 | 18 | 99 |

## Indexed row retrieval

PENDING.

