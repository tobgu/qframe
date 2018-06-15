package qframe_test

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/tobgu/qframe"
)

// MockDriver implements a fake SQL driver for testing.
type MockDriver struct {
	t *testing.T
	// expected SQL query
	query string
	// results holds values that are
	// returned from a database query
	results struct {
		// column names for each row of values
		columns []string
		// each value for each row
		values [][]driver.Value
	}
	// args holds expected values
	args struct {
		// values we expect to be given
		// to the database
		values [][]driver.Value
	}
}

func (m MockDriver) Open(name string) (driver.Conn, error) {
	stmt := &MockStmt{
		t:      m.t,
		values: m.args.values,
		rows: &MockRows{
			t:       m.t,
			columns: m.results.columns,
			values:  m.results.values,
		},
	}
	return &MockConn{
		t:     m.t,
		stmt:  stmt,
		query: m.query,
	}, nil
}

type MockRows struct {
	t       *testing.T
	idx     int
	columns []string
	values  [][]driver.Value
}

func (m *MockRows) Next(dest []driver.Value) error {
	if m.idx == len(m.values) {
		return io.EOF
	}
	for i := 0; i < len(dest); i++ {
		dest[i] = m.values[m.idx][i]
	}
	m.idx++
	return nil
}

func (m MockRows) Close() error { return nil }

func (m MockRows) Columns() []string { return m.columns }

type MockTx struct{}

func (m MockTx) Commit() error { return nil }

func (m MockTx) Rollback() error { return nil }

type MockStmt struct {
	t      *testing.T
	rows   *MockRows
	idx    int
	values [][]driver.Value
}

func (s MockStmt) Close() error { return nil }

func (s MockStmt) NumInput() int { return len(s.values) }

func (s *MockStmt) Exec(args []driver.Value) (driver.Result, error) {
	for i, arg := range args {
		if s.values[s.idx][i] != arg {
			s.t.Errorf("arg %t != %t", arg, s.values[s.idx][i])
		}
	}
	s.idx++
	return nil, nil
}

func (s MockStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.rows, nil
}

type MockConn struct {
	t     *testing.T
	query string
	stmt  *MockStmt
}

func (m MockConn) Prepare(query string) (driver.Stmt, error) {
	if query != m.query {
		m.t.Errorf("invalid query: %s != %s", query, m.query)
	}
	return m.stmt, nil
}

func (c MockConn) Close() error { return nil }
func (c MockConn) Begin() (driver.Tx, error) {
	return &MockTx{}, nil
}

func TestQFrame_ToSQL(t *testing.T) {
	dvr := MockDriver{t: t}
	dvr.query = "INSERT INTO \"test\" (COL1,COL2,COL3) VALUES (?,?,?);"
	dvr.args.values = [][]driver.Value{
		[]driver.Value{int64(1), 1.1, "one"},
		[]driver.Value{int64(2), 2.2, "two"},
		[]driver.Value{int64(3), 3.3, "three"},
	}
	sql.Register("TestToSQL", dvr)
	db, _ := sql.Open("TestToSQL", "")
	tx, _ := db.Begin()
	qf := qframe.New(map[string]interface{}{
		"COL1": []int{1, 2, 3},
		"COL2": []float64{1.1, 2.2, 3.3},
		"COL3": []string{"one", "two", "three"},
	})
	assertNotErr(t, qf.ToSQL(tx, "test"))
}

func TestQFrame_ReadSQL(t *testing.T) {
	dvr := MockDriver{t: t}
	dvr.results.columns = []string{"COL1", "COL2", "COL3"}
	dvr.results.values = [][]driver.Value{
		[]driver.Value{int64(1), 1.1, "one"},
		[]driver.Value{int64(2), 2.2, "two"},
		[]driver.Value{int64(3), 3.3, "three"},
	}
	sql.Register("TestReadSQL", dvr)
	db, _ := sql.Open("TestReadSQL", "")
	tx, _ := db.Begin()
	qf := qframe.ReadSQL(tx)
	assertNotErr(t, qf.Err)
	expected := qframe.New(map[string]interface{}{
		"COL1": []int{1, 2, 3},
		"COL2": []float64{1.1, 2.2, 3.3},
		"COL3": []string{"one", "two", "three"},
	})
	assertEquals(t, expected, qf)
}
