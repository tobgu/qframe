package qframe

import (
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/internal/grouper"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/types"
)

// Internal statistics for grouping. Clients should not depend on this for any
// type of decision making. It is strictly "for info". The layout may change
// if the underlying grouping mechanisms change.
type GroupStats grouper.GroupStats

type Grouper struct {
	indices        []index.Int
	groupedColumns []string
	columns        []namedColumn
	columnsByName  map[string]namedColumn
	Err            error
	Stats          GroupStats
}

type GroupByConfig struct {
	columns     []string
	groupByNull bool
	// dropNulls?
}

type GroupByConfigFn func(c *GroupByConfig)

func GroupBy(columns ...string) GroupByConfigFn {
	return func(c *GroupByConfig) {
		c.columns = columns
	}
}

// Setting this to "true" means that nil/NaN values are grouped
// together. Default "false".
func GroupByNull(b bool) GroupByConfigFn {
	return func(c *GroupByConfig) {
		c.groupByNull = b
	}
}

func newGroupByConfig(configFns []GroupByConfigFn) GroupByConfig {
	var config GroupByConfig
	for _, f := range configFns {
		f(&config)
	}

	return config
}

// TODO-C
type Aggregation struct {
	Fn     types.SliceFuncOrBuiltInId
	Column string
}

func (g Grouper) Aggregate(aggs ...Aggregation) QFrame {
	if g.Err != nil {
		return QFrame{Err: g.Err}
	}

	// TODO: Check that columns exist but are not part of groupedColumns

	// Loop over all groups and pick the first row in each of the groups.
	// This index will be used to populate the grouped by columns below.
	firstElementIx := make(index.Int, len(g.indices))
	for i, ix := range g.indices {
		firstElementIx[i] = ix[0]
	}

	newColumnsByName := make(map[string]namedColumn, len(g.groupedColumns)+len(aggs))
	newColumns := make([]namedColumn, 0, len(g.groupedColumns)+len(aggs))
	for i, colName := range g.groupedColumns {
		col := g.columnsByName[colName]
		col.pos = i
		col.Column = col.Subset(firstElementIx)
		newColumnsByName[colName] = col
		newColumns = append(newColumns, col)
	}

	var err error
	for _, agg := range aggs {
		col := g.columnsByName[agg.Column]
		col.Column, err = col.Aggregate(g.indices, agg.Fn)
		if err != nil {
			return QFrame{Err: errors.Propagate("Aggregate", err)}
		}

		newColumnsByName[agg.Column] = col
		newColumns = append(newColumns, col)
	}

	return QFrame{columns: newColumns, columnsByName: newColumnsByName, index: index.NewAscending(uint32(len(g.indices)))}
}
