[![Build Status](https://travis-ci.org/tobgu/qframe.png)](https://travis-ci.org/tobgu/qframe)
[![gocover.run](https://gocover.run/github.com/tobgu/qframe.svg?style=flat&tag=1.10)](https://gocover.run?tag=1.10&repo=github.com%2Ftobgu%2Fqframe)
[![Go Report Card](https://goreportcard.com/badge/github.com/tobgu/qframe)](https://goreportcard.com/report/github.com/tobgu/qframe)
[![GoDoc](https://godoc.org/github.com/tobgu/qframe?status.svg)](https://godoc.org/github.com/tobgu/qframe)

QFrame is an immutable data frame that support filtering, aggregation
and data manipulation. Any operation on a QFrame results in
a new QFrame, the original QFrame remains unchanged. This can be done
fairly efficiently since much of the underlying data will be shared
between the two frames.

The design of QFrame has mainly been driven by the requirements from
[qocache](https://github.com/tobgu/qocache) but it is in many aspects
a general purpose data frame. Any suggestions for added/improved
functionality to support a wider scope is always of interest as long
as they don't conflict with the requirements from qocache!
See [Contribute](#contribute).

## Installation
`go get github.com/tobgu/qframe`

## Usage
Below are some examples of common use cases. The list is not exhaustive
in any way. For a complete description of all operations including more
examples see the [docs](https://godoc.org/github.com/tobgu/qframe).

### IO
QFrames can currently be read from and written to CSV, record
oriented JSON, and any SQL database supported by the go `database/sql`
driver.

#### CSV Data

Read CSV data:
```go
input := `COL1,COL2
a,1.5
b,2.25
c,3.0`

f := qframe.ReadCSV(strings.NewReader(input))
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

#### SQL Data

QFrame supports reading and writing data from the standard library `database/sql`
drivers. It has been tested with [SQLite](github.com/mattn/go-sqlite3), [Postgres](github.com/lib/pq), and [MariaDB](github.com/go-sql-driver/mysql).

##### SQLite Example

Load data to and from an in-memory SQLite database. Note
that this example requires you to have [go-sqlite3](https://github.com/mattn/go-sqlite3) installed
prior to running.

```go
package main

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"github.com/tobgu/qframe"
	qsql "github.com/tobgu/qframe/config/sql"
)

func main() {
	// Create a new in-memory SQLite database.
	db, _ := sql.Open("sqlite3", ":memory:")
	// Add a new table.
	db.Exec(`
	CREATE TABLE test (
		COL1 INT,
		COL2 REAL,
		COL3 TEXT,
		COL4 BOOL
	);`)
	// Create a new QFrame to populate our table with.
	qf := qframe.New(map[string]interface{}{
		"COL1": []int{1, 2, 3},
		"COL2": []float64{1.1, 2.2, 3.3},
		"COL3": []string{"one", "two", "three"},
		"COL4": []bool{true, true, true},
	})
	fmt.Println(qf)
	// Start a new SQL Transaction.
	tx, _ := db.Begin()
	// Write the QFrame to the database.
	qf.ToSQL(tx,
		// Write only to the test table
		qsql.Table("test"),
		// Explicitly set SQLite compatibility.
		qsql.SQLite(),
	)
	// Create a new QFrame from SQL.
	newQf := qframe.ReadSQL(tx,
		// A query must return at least one column. In this 
		// case it will return all of the columns we created above.
		qsql.Query("SELECT * FROM test"),
		// SQLite stores boolean values as integers, so we
		// can coerce them back to bools with the CoercePair option.
		qsql.Coerce(qsql.CoercePair{Column: "COL4", Type: qsql.Int64ToBool}),
		qsql.SQLite(),
	)
	fmt.Println(newQf)
	fmt.Println(newQf.Equals(qf))
}
```

Output:

```
COL1(i) COL2(f) COL3(s) COL4(b)
------- ------- ------- -------
      1     1.1     one    true
      2     2.2     two    true
      3     3.3   three    true

Dims = 4 x 3
true 
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
Grouping and aggregation is done in two distinct steps. The function
used in the aggregation step takes a slice of elements and
returns an element. For floats this function signature matches
many of the statistical functions in [Gonum](https://github.com/gonum/gonum),
these can hence be applied directly.

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
`Apply` and `Eval`.
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
f = f.Eval("COL3", qframe.Expr("+", qframe.Expr("str", types.ColumnName("COL1")), types.ColumnName("COL2")))
fmt.Println(f.Select("COL3"))
```

## More usage examples
Examples of the most common operations are available in the
[docs](https://godoc.org/github.com/tobgu/qframe).

## Error handling
All operations that may result in errors will set the `Err` variable
on the returned QFrame to indicate that an error occurred.
The presence of an error on the QFrame will prevent any future operations
from being executed on the frame (eg. it follows a monad-like pattern).
This allows for smooth chaining of multiple operations without having
to explicitly check errors between each operation.

## Configuration parameters
API functions that require configuration parameters make use of
[functional options](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis)
to allow more options to be easily added in the future in a backwards
compatible way.

## Design goals
* Performance
  - Speed should be on par with, or better than, Python Pandas for corresponding operations.
  - No or very little memory overhead per data element.
  - Performance impact of operations should be straight forward to reason about.
* API
  - Should be reasonably small and low ceremony.
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

Many functions and methods in qframe take the empty interface as parameter,
for functions to be applied or string references to internal functions
for example.
These always correspond to a union/sum type with a fixed set of valid types
that are checked in runtime through type switches (there's hardly any
reflection applied in QFrame for performance reasons).
Which types are valid depends on the function called and the column type
that is affected. Modelling this statically is hard/impossible in Go,
hence the dynamic approach. If you plan to use QFrame with datasets
with fixed layout and types it should be a small task to write tiny
wrappers for the types you are using to regain static type safety.

## Limitations
* The API can still not be considered stable.
* The maximum number of rows in a QFrame is 4294967296 (2^32).
* The CSV parser only handles ASCII characters as separators.
* Individual strings cannot be longer than 268 Mb (2^28 byte).
* A string column cannot contain more than a total of 34 Gb (2^35 byte).
* At the moment you cannot rely on any of the errors returned to
  fulfill anything else than the `Error` interface. In the future
  this will hopefully be improved to provide more help in identifying
  the root cause of errors.

## Performance/benchmarks
There are a number of benchmarks in [qbench](https://github.com/tobgu/qbench)
comparing QFrame to Pandas and Gota where applicable.

## Other data frames
The work on QFrame has been inspired by [Python Pandas](https://pandas.pydata.org/)
and [Gota](https://github.com/kniren/gota).

## Contribute
Want to contribute? Great! Open an issue on Github and let the discussions
begin! Below are some instructions for working with the QFrame repo.

### Ideas for further work
Below are some ideas of areas where contributions would be welcome.

* Support for more input and output formats.
* Support for additional column formats.
* Support for using the [Arrow](https://github.com/apache/arrow) format for columns.
* General CPU and memory optimizations.
* Improve documentation.
* More analytical functionality.
* Dataset joins.
* Improved interoperability with other libraries in the Go data science eco system.
* Improve string representation of QFrames.

### Install dependencies
`make dev-deps`

### Tests
Please contribute tests together with any code. The tests should be
written against the public API to avoid lockdown of the implementation
and internal structure which would make it more difficult to change in
the future.

Run tests:
`make test`

This will also trigger code to be regenerated.

### Code generation
The codebase contains some generated code to reduce the amount of
duplication required for similar functionality across different column
types. Generated code is recognized by file names ending with `_gen.go`.
These files must never be edited directly.

To trigger code generation:
`make generate`
