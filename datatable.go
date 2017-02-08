package datatable

import (
	"encoding/json"
	"errors"
)

var (
	ColIndexError = errors.New("Column index out of range")
	RowIndexError = errors.New("Row index out of range")
	ValueError    = errors.New("Incorrect argument value")
	NumColError   = errors.New("Incorrect number of columns")
)

// DataTable is an in-memory relational table.
// The data values are immutable.
type DataTable struct {
	rows [][]string
	nrow int
	ncol int
}

// NewDataTable creates a new data table with a given number of columns.
func NewDataTable(ncol int) *DataTable {
	rows := make([][]string, 0)
	return &DataTable{
		rows: rows,
		nrow: 0,
		ncol: ncol,
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
		return RowIndexError
	}
	return nil
}

// CheckCol returns an error if column y does not exist.
func (dt *DataTable) CheckCol(y int) error {
	if y < 0 || y >= dt.ncol {
		return ColIndexError
	}
	return nil
}

// AppendRow appends a new row at the bottom of the table.
func (dt *DataTable) AppendRow(row []string) error {
	if len(row) != dt.ncol {
		return NumColError
	}
	dt.rows = append(dt.rows, row)
	dt.nrow++
	return nil
}

// GetRow returns the row at index x.
func (dt *DataTable) GetRow(x int) ([]string, error) {
	if err := dt.CheckRow(x); err != nil {
		return nil, err
	}
	row := make([]string, dt.ncol)
	copy(row, dt.rows[x])
	return row, nil
}

// ApplyColumn calls the function fn using all values in column y
// from top to bottom.
// fn takes two argument: the first is the row index and the second
// is the corresponding value.
// Error is returned immediately if encountered.
func (dt *DataTable) ApplyColumn(y int, fn func(int, string) error) error {
	if err := dt.CheckCol(y); err != nil {
		return err
	}
	for x, row := range dt.rows {
		if err := fn(x, row[y]); err != nil {
			return err
		}
	}
	return nil
}

// Slice take a contiguous subset of at most n rows, starting at index x,
// and make a new data table from them.
func (dt *DataTable) Slice(x, n int) (*DataTable, error) {
	if x < 0 || x >= dt.nrow {
		return nil, RowIndexError
	}
	if n < 0 {
		return nil, ValueError
	}
	end := x + n
	if end >= dt.nrow {
		end = dt.nrow
	}
	rows := dt.rows[x:end]
	return &DataTable{
		rows: rows,
		nrow: len(rows),
		ncol: dt.ncol,
	}, nil
}

// Marshal data table into JSON.
func (dt *DataTable) MarshalJSON() ([]byte, error) {
	return json.Marshal(dt.rows)
}

// Unmarshal data table from JSON.
func (dt *DataTable) UnmarshalJSON(data []byte) error {
	var rows [][]string
	if err := json.Unmarshal(data, &rows); err != nil {
		return err
	}
	var nrow, ncol int
	nrow = len(rows)
	if nrow > 0 {
		ncol = len(rows[0])
		for x := range rows {
			if len(rows[x]) != ncol {
				return NumColError
			}
		}
	}
	dt = &DataTable{
		rows: rows,
		nrow: nrow,
		ncol: ncol,
	}
	return nil
}

// Join performs relational join between the left and right tables.
// The join condition is defined by the function fn, which takes two rows,
// l and r, from the left and right tables respectively, and returns
// whether the two rows should be joined.
// The join result is returned as a new data table.
// Each joined rows contains all the fields from the input tables,
// in the order of [left table fields ... right table fields ...].
func Join(left, right *DataTable, fn func(l, r []string) bool) *DataTable {
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
		}
		close(out)
	}()
	joined := NewDataTable(left.NumCol() + right.NumCol())
	for row := range out {
		joined.AppendRow(row)
	}
	return joined
}

// LeftJoin is similar to Join, execpt that every row from the left table
// will be part of the join result even it doesn't join with any row from
// the right table.
// e.g., [left table fields ... empty fields]
// where the empty fields have the same number of columns as the right table.
func LeftJoin(left, right *DataTable, fn func(l, r []string) bool) *DataTable {
	out := make(chan []string)
	go func() {
		for i := 0; i < left.NumRow(); i++ {
			l, _ := left.GetRow(i)
			var rowsJoined int
			for j := 0; j < right.NumRow(); j++ {
				r, _ := right.GetRow(j)
				if fn(l, r) {
					out <- append(l, r...)
					rowsJoined++
				}
			}
			if rowsJoined == 0 {
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

func HashJoin(left, right *DataTable, fnLeft, fnRight func([]string) string) *DataTable {
	out := make(chan []string)
	go func() {
		defer close(out)
		// Find relative sizes
		var smaller, larger *DataTable
		var fnSmall, fnLarge func([]string) string
		var fnJoin func([]string, []string) []string
		if left.NumRow() > right.NumRow() {
			smaller, larger = right, left
			fnSmall, fnLarge = fnRight, fnLeft
			fnJoin = func(s, l []string) []string {
				return append(l, s...)
			}
		} else {
			smaller, larger = left, right
			fnSmall, fnLarge = fnLeft, fnRight
			fnJoin = func(s, l []string) []string {
				return append(s, l...)
			}
		}
		// Build map for the larger
		ht := make(map[string][][]string)
		for i := 0; i < larger.NumRow(); i++ {
			row, _ := larger.GetRow(i)
			key := fnLarge(row)
			if _, exists := ht[key]; !exists {
				ht[key] = make([][]string, 0)
			}
			ht[key] = append(ht[key], row)
		}
		// Perform join
		for i := 0; i < smaller.NumRow(); i++ {
			rowSmall, _ := smaller.GetRow(i)
			key := fnSmall(rowSmall)
			if bucket, exists := ht[key]; exists {
				for _, rowLarge := range bucket {
					out <- fnJoin(rowSmall, rowLarge)
				}
			}
		}
	}()
	joined := NewDataTable(left.NumCol() + right.NumCol())
	for row := range out {
		joined.AppendRow(row)
	}
	return joined
}
