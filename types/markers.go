package types

// ColumnName is used to separate a column identifier from a string constant.
// It is used when filtering and evaluating expressions where ambiguities
// would otherwise arise.
type ColumnName string
