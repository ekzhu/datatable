package datatable

import (
	"encoding/json"
	"errors"
)

var (
	IndexError  = errors.New("Index out of range")
	ValueError  = errors.New("Incorrect argument value")
	NumColError = errors.New("Incorrect number of columns")
)

// DataTable is an in-memory relational table.
// The data values are immutable.
type DataTable struct {
	columns [][]string
	nrow    int
	ncol    int
}

// NewDataTable creates a new data table with a given number of columns.
func NewDataTable(ncol int) *DataTable {
	columns := make([][]string, ncol)
	for y := range columns {
		columns[y] = make([]string, 0)
	}
	return &DataTable{
		columns: columns,
		nrow:    0,
		ncol:    ncol,
	}
}

func (dt *DataTable) NumRow() int {
	return dt.nrow
}

func (dt *DataTable) NumCol() int {
	return dt.ncol
}

// CheckRow returns an error if row x does not exist.
func (dt *DataTable) CheckRow(x int) error {
	if x < 0 || x >= dt.nrow {
		return IndexError
	}
	return nil
}

// CheckCol returns an error if column y does not exist.
func (dt *DataTable) CheckCol(y int) error {
	if y < 0 || y > dt.ncol {
		return IndexError
	}
	return nil
}

// AppendRow appends a new row at the bottom of the table.
func (dt *DataTable) AppendRow(row []string) error {
	if len(row) != dt.ncol {
		return NumColError
	}
	for x := range row {
		dt.columns[x] = append(dt.columns[x], row[x])
	}
	dt.nrow++
	return nil
}

// GetRow returns the row at index x.
func (dt *DataTable) GetRow(x int) ([]string, error) {
	if x < 0 || x >= dt.nrow {
		return nil, IndexError
	}
	row := make([]string, dt.ncol)
	for y := 0; y < dt.ncol; y++ {
		row[y] = dt.columns[y][x]
	}
	return row, nil
}

// ApplyColumn calls the function fn using all values in column y
// from top to bottom.
// fn takes two argument: the first is the row index and the second
// is the corresponding value.
// Error is returned immediately if encountered.
func (dt *DataTable) ApplyColumn(y int, fn func(int, string) error) error {
	if y < 0 || y > dt.ncol {
		return IndexError
	}
	for x, v := range dt.columns[y] {
		if err := fn(x, v); err != nil {
			return err
		}
	}
	return nil
}

// Slice take a contiguous subset of at most n rows, starting at index x,
// and make a new data table from them.
func (dt *DataTable) Slice(x, n int) (*DataTable, error) {
	if x < 0 || x >= dt.nrow {
		return nil, IndexError
	}
	if n < 0 {
		return nil, ValueError
	}
	end := x + n
	if end >= dt.nrow {
		end = dt.nrow
	}
	columns := make([][]string, dt.ncol)
	for y := range columns {
		columns[y] = dt.columns[y][x:end]
	}
	nrow := end - x
	return &DataTable{
		columns: columns,
		nrow:    nrow,
		ncol:    len(columns),
	}, nil
}

// Join performs relational join between the left and right tables.
// The join condition is defined by the function fn, which takes two rows,
// l and r, from the left and right tables respectively, and returns
// whether the two rows should be joined.
// The join result is returned as a new data table.
// Each joined rows contains all the fields from the input tables,
// in the order of [left table fields ... right table fields ...].
func Join(left, right *DataTable, fn func(l, r []string) bool) *DataTable {
	return joinTables(left, right, fn, false)
}

// LeftJoin is similar to Join, execpt that every row from the left table
// will be part of the join result even it doesn't join with any row from
// the right table.
// e.g., [left table fields ... empty fields]
// where the empty fields have the same number of columns as the right table.
func LeftJoin(left, right *DataTable, fn func(l, r []string) bool) *DataTable {
	return joinTables(left, right, fn, true)
}

func joinTables(left, right *DataTable, fn func(l, r []string) bool, leftJoin bool) *DataTable {
	out := make(chan []string)
	go func() {
		for i := 0; i < left.NumRow(); i++ {
			l, _ := left.GetRow(i)
			for j := 0; j < right.NumRow(); j++ {
				r, _ := right.GetRow(j)
				if fn(l, r) {
					out <- append(l, r...)
				}
			}
			if leftJoin {
				r := make([]string, right.NumCol())
				out <- append(l, r...)
			}
		}
		close(out)
	}()
	joined := NewDataTable(left.NumCol() + right.NumCol())
	for row := range out {
		joined.AppendRow(row)
	}
	return joined
}

// Marshal data table into JSON.
func (dt *DataTable) MarshalJSON() ([]byte, error) {
	return json.Marshal(dt.columns)
}

// Unmarshal data table from JSON.
func (dt *DataTable) UnmarshalJSON(data []byte) error {
	var columns [][]string
	if err := json.Unmarshal(data, &columns); err != nil {
		return err
	}
	var nrow, ncol int
	ncol = len(columns)
	if ncol > 0 {
		nrow = len(columns[0])
		for y := 0; y < ncol; y++ {
			if len(columns[y]) != nrow {
				return NumColError
			}
		}
	}
	dt = &DataTable{
		columns: columns,
		nrow:    nrow,
		ncol:    ncol,
	}
	return nil
}
