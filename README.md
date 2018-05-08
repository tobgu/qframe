[![Build Status](https://travis-ci.org/tobgu/qframe.png)](https://travis-ci.org/tobgu/qframe) [![Go Report Card](https://goreportcard.com/badge/github.com/tobgu/qframe)](https://goreportcard.com/report/github.com/tobgu/qframe)

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

## High level design
A QFrame is a collection of columns which can be of type int, float,
string, bool or enum. For more information about the data types see the
[types docs](https://godoc.org/github.com/tobgu/qframe/types).

In addition to the columns there is also an index which controls
which rows in the columns that are part of the QFrame and the
sort order of these columns.
Many operations on QFrames only affect the index, the underlying
data remains the same.

API functions that require configuration parameters make use of
[functional options](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis)
to allow more options to be easily added in the future in a backwards
compatible way.

## Usage
Below are some examples of common use cases. The list is not exhaustive
in any way. For a complete description including more examples see the
[docs](https://godoc.org/github.com/tobgu/qframe).

### IO
QFrames can currently be read from and written to CSV and record
oriented JSON.

Read CSV data:
```go
input := `COL1,COL2
a,1.5
b,2.25
c,3.0`

f := qframe.ReadCsv(strings.NewReader(input))
fmt.Println(f)
```
Output:
```
COL1(s) COL2(f)
------- -------
      a     1.5
      b    2.25
      c       3

Dims = 2 x 3
```

### Filtering
Filtering can be done either by applying individual filters
to the QFrame or by combining filters using AND and OR.

Filter with OR-clause:
```go
f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []string{"a", "b", "c"}})
newF := f.Filter(qframe.Or(
    qframe.Filter{Column: "COL1", Comparator: ">", Arg: 2},
    qframe.Filter{Column: "COL2", Comparator: "=", Arg: "a"}))
fmt.Println(newF)
```

Output:
```
COL1(i) COL2(s)
------- -------
      1       a
      3       c

Dims = 2 x 2
```

### Grouping and aggregation
Grouping and aggregation are two different steps. The function
used in the aggregation step takes a slice of elements and
returns an elements. For floats this function signature matches
many of thestatistical functions in [Gonum](https://github.com/gonum/gonum)
which can hence be applied directly.

```go
intSum := func(xx []int) int {
    result := 0
    for _, x := range xx {
        result += x
    }
    return result
}

f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 2, 3, 3}, "COL2": []string{"a", "b", "c", "a", "b"}})
f = f.GroupBy(groupby.Columns("COL2")).Aggregate(qframe.Aggregation{Fn: intSum, Column: "COL1"})
fmt.Println(f.Sort(qframe.Order{Column: "COL2"}))
```

Output:
```
COL2(s) COL1(i)
------- -------
      a       4
      b       5
      c       2

Dims = 2 x 3
```

### Data manipulation
There are two different functions by which data can be manipulated,
`QFrame.Apply` and `QFrame.Eval`.
`Eval` is slightly more high level and takes a more data driven approach
but basically boils down to a bunch of `Apply` in the end.

Example using `Apply` to string concatenate two columns:
```go
f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []string{"a", "b", "c"}})
f = f.Apply(
    qframe.Instruction{Fn: function.StrI, DstCol: "COL1", SrcCol1: "COL1"},
    qframe.Instruction{Fn: function.ConcatS, DstCol: "COL3", SrcCol1: "COL1", SrcCol2: "COL2"})
fmt.Println(f.Select("COL3"))
```

Output:
```
COL3(s)
-------
     1a
     2b
     3c

Dims = 1 x 3
```

The same example using `Eval` instead:
```go
f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []string{"a", "b", "c"}})
f = f.Eval("COL3", qframe.Expr2("+", qframe.Expr1("str", types.ColumnName("COL1")), types.ColumnName("COL2")))
fmt.Println(f.Select("COL3"))
```

## More usage examples
Examples of the most common operations are available in the
[docs](https://godoc.org/github.com/tobgu/qframe).

## Limitations
* The API can still not be considered stable.
* The maximum number of rows in a QFrame is 4294967296 (2^32).
* The CSV parser only handles ASCII characters as separators.
* Individual strings cannot be longer than 268 Mb (2^28 byte).
* A string column cannot contain more than a total of 34 Gb (2^35 byte).

## Performance/benchmarks
There are a number of benchmarks in [qbench](https://github.com/tobgu/qbench)
comparing qframe to Pandas and Gota where applicable.

## Other data frames
The work on QFrame has been inspired by [Python Pandas](https://pandas.pydata.org/)
and [Gota](https://github.com/kniren/gota).

## Contribute
Want to contribute? Great! Open an issue on Github and let the discussions
begin! Below are some instructions for working with QFrame.

### Install dependencies
`make dev-deps`

### Run tests
`make test`

This will also trigger code to be regenerated.

### Code generation
The codebase contains some generated code to reduce the amount of
duplication required for similar functionality across different column
types. Generated code is recognized by file names ending with `_gen.go`.
These files must never be edited directly.

To trigger code generation:
`make generate`