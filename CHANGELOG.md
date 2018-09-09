### 2018-09-09 v0.2.0
SQL and plotting support! Thanks a lot to @kevinshoon for adding this!
* Add support for reading from/writing to SQL databases. Thanks @kevinschoon for this!
* Add support for plotting using Gonum plot. Thanks @kevinschoon for this!
* Rename `ToJson/FromJson/ToCsv/FromCsv` -> `ToJSON/FromJSON/ToCSV/FromCSV` for 
  consistency with stdlib. Thanks @sbinet for this!
* qframe.Expr1 and qframe.Expr2 have been merged to one function qframe.Expr.
* Minor bug fix in CSV reading where reads that return io.EOF together with
  data previously did not work.

### 2018-05-23 v0.1.0
* Initial release
