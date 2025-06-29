Analysis the layout of filters from the clickbench

Related ticket: https://github.com/apache/datafusion/pull/16562


Background:

Filter representation is very important for query performance -- and depending on how many
rows are selected and what pattern are selected, means that different filter representations 
might be better than others

arrow-rs parquet represents filter results as a `BooleanArray` (a classic bitmask representation in the literature),
which are then converted to `RowSelection` for evaluation during evaluation (TODO document link)

The `RowSelection` is then used to skip/scan ranges of the rows during decoding

It turns out that for certain patterns of filters, it is faster to decode all rows and then apply the `filter` kernel
rather than apply the skip/scan logic from `RowSelection`. See the ticket
https://github.com/apache/arrow-rs/issues/7456
 for more details.



This crate is a tool that helps analyze the layout of filters from the clickbench dataset
to help visualize and design heuristics for the best filter representation.
# Usage

`BooleanArray` 


TODO:
1. writeup how the data was gathered
1. Visualize the layout of filters from the clickbench dataset with an impage