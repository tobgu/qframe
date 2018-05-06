[![Build Status](https://travis-ci.org/tobgu/qframe.png)](https://travis-ci.org/tobgu/qframe)

QFrame is an immutable data frame. Any operations on a QFrame results in
a new QFrame, the original QFrame remains unchanged. This can be done
fairly efficiently since much of the underlying data will be shared
between the two frames.

The design of QFrame has mainly be driven by the requirements from
[qocache](https://github.com/tobgu/qocache) but it is in many aspects
a general purpose data frame. Any suggestions for added/improved
functionality to support a wider scope is always of interest as long
as they don't conflict with the requirements from qocache!

## Design goals
* Performance
  - Speed should be on par with, or better than, Python Pandas for corresponding operations.
  - No or very little memory overhead per data element.
  - Performance impact of operations should be straight forward to reason about.
* API
  - Should be reasonably small.
  - Should allow custom, user provided, functions to be used for data processing
  - Should provide built in functions for most common operations

## General design
A QFrame is a collection of columns which can be of type int, float,
string, bool or enum. For more information about the data types see the
[docs](https://godoc.org/github.com/tobgu/qframe).

In addition to the columns there is also an index which controls
which rows in the columns that are part of the QFrame and the
sort order of these columns.
Many operations on QFrames only affect the index, the underlying
data remains the same.

## Functionality
TODO

### IO
TODO

### Filtering
TODO

### Grouping and aggregation
TODO

### Data manipulation
TODO

## Examples
Examples of the most common operations are available in the
[docs](https://godoc.org/github.com/tobgu/qframe).

## Limitations
* The API can still not be considered stable.
* The maximum number of rows in a QFrame is 4294967296 (2^32).
* The CSV parser only handles ASCII characters as separators.
* Individual strings cannot be longer than 268 Mb (2^28 byte).
* A string column cannot contain more than a total of 34 Gb (2^35 byte).

## Performance
There are a number of benchmarks in [qbench](https://github.com/tobgu/qbench)
comparing qframe to Pandas and Gota where applicable.

## Other data frames
Pandas, Gota

## Contribute
How to build, run tests, etc.