package datatable

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
)

var (
	NumColError    = errors.New("Incorrect number of columns")
	SingleColError = errors.New("Refuse to remove the last column")
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

// NumRow returns the number of rows in the table
func (dt *DataTable) NumRow() int {
	return dt.nrow
}

// NumCol returns the number of columns in the table
func (dt *DataTable) NumCol() int {
	return dt.ncol
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

// Get returns the value at row x and column y.
func (dt *DataTable) Get(x, y int) string {
	return dt.rows[x][y]
}

// GetRow returns the row at index x.
func (dt *DataTable) GetRow(x int) []string {
	row := make([]string, dt.ncol)
	copy(row, dt.rows[x])
	return row
}

// ApplyColumn calls the function fn using all values in column y
// from the first to the last row.
// fn takes two arguments: the first is the row index and the second
// is the corresponding value.
// Error is returned immediately if encountered.
func (dt *DataTable) ApplyColumn(fn func(int, string) error, y int) error {
	for x, row := range dt.rows {
		if err := fn(x, row[y]); err != nil {
			return err
		}
	}
	return nil
}

// ApplyColumns calls the function fn using all values in multiple columns
// given by their indexes, from the first to the last row.
// fn takes two arguments: the first is the row index and the second
// is the corresponding row projected on the given columns.
// Error is returned immediately if encountered.
func (dt *DataTable) ApplyColumns(fn func(int, []string) error, ys ...int) error {
	for x, row := range dt.rows {
		row2 := make([]string, len(ys))
		for y2, y := range ys {
			row2[y2] = row[y]
		}
		if err := fn(x, row2); err != nil {
			return err
		}
	}
	return nil
}

// RemoveColumn deletes the column at index y
func (dt *DataTable) RemoveColumn(y int) error {
	if dt.NumCol() == 1 {
		return SingleColError
	}
	for x, row := range dt.rows {
		dt.rows[x] = append(row[:y], row[y+1:]...)
	}
	dt.ncol--
	return nil
}

// RemoveRow deletes the row at index x
func (dt *DataTable) RemoveRow(x int) {
	dt.rows = append(dt.rows[:x], dt.rows[x+1:]...)
	dt.nrow--
}

// Project creates a new DataTable that has only a subset of
// the columns, which are indicated by the given column indexes.
func (dt *DataTable) Project(ys ...int) *DataTable {
	dt2 := NewDataTable(len(ys))
	for _, row := range dt.rows {
		row2 := make([]string, len(ys))
		for y2, y := range ys {
			row2[y2] = row[y]
		}
		if err := dt2.AppendRow(row2); err != nil {
			panic("Row data corrupted")
		}
	}
	return dt2
}

// Slice take a contiguous subset of at most n rows, starting at index x,
// and make a new DataTable from them.
// Note that different from Project, the new DataTable uses the underlying rows
// of the original DataTable, and changes to the new table may affect
// the original.
func (dt *DataTable) Slice(x, n int) *DataTable {
	end := x + n
	if end >= dt.nrow {
		end = dt.nrow
	}
	rows := dt.rows[x:end]
	return &DataTable{
		rows: rows,
		nrow: len(rows),
		ncol: dt.ncol,
	}
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

// ToCsv writes the table in standard CSV format to a file
func (dt *DataTable) ToCsv(file io.Writer) error {
	writer := csv.NewWriter(file)
	for i := 0; i < dt.NumRow(); i++ {
		row := dt.GetRow(i)
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
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
			l := left.GetRow(i)
			for j := 0; j < right.NumRow(); j++ {
				r := right.GetRow(j)
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
			l := left.GetRow(i)
			var rowsJoined int
			for j := 0; j < right.NumRow(); j++ {
				r := right.GetRow(j)
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

// HashJoin performs equal join on the two tables, and returns the result as
// a new DataTable.
// fnLeft and fnRight are functions that take a row as the input and return
// the value used for equality condition in join.
// HashJoin is generally faster than Join, which does nested loop join, but uses more
// memory due to the temporary hash table.
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
			row := larger.GetRow(i)
			key := fnLarge(row)
			if _, exists := ht[key]; !exists {
				ht[key] = make([][]string, 0)
			}
			ht[key] = append(ht[key], row)
		}
		// Perform join
		for i := 0; i < smaller.NumRow(); i++ {
			rowSmall := smaller.GetRow(i)
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
