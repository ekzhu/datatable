package datatable

import (
	"encoding/json"
	"os"
	"testing"
)

func printTable(dt *DataTable, t *testing.T) {
	for i := 0; i < dt.NumRow(); i++ {
		row := dt.GetRow(i)
		t.Log(row)
	}
}

func Test_Create(t *testing.T) {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	if dt.NumCol() != 3 {
		t.Error(dt.NumRow())
	}
	if dt.NumRow() != 2 {
		t.Error(dt.NumRow())
	}
	if dt.Get(0, 2) != "c" {
		t.Fail()
	}
	if err := dt.AppendRow([]string{"1", "2"}); err != NumColError {
		t.Error(err)
	}
	if row := dt.GetRow(1); row[0] != "e" {
		t.Fail()
	}
	if col := dt.GetColumn(1); col[0] != "b" {
		t.Fail()
	}
}

func Test_Slice(t *testing.T) {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	dt2 := dt.Slice(0, 10)
	if dt2.NumRow() != 4 {
		t.Error(dt2.NumRow())
	}
	if dt2.NumCol() != 3 {
		t.Error(dt2.NumCol())
	}

	dt3 := dt.Slice(2, 2)
	if row := dt3.GetRow(0); row[0] != "f" {
		t.Error(row)
	}
	if row := dt3.GetRow(1); row[0] != "g" {
		t.Error(row)
	}

	dt4 := dt.Slice(1, 0)
	if dt4.NumRow() != 0 {
		t.Error(dt2.NumRow())
	}
	dt4.AppendRow([]string{"x", "y", "z"})
	if dt4.NumRow() != 1 {
		t.Error(dt4.NumRow())
	}
}

func Test_Project(t *testing.T) {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	dt2 := dt.Project(0, 2)
	if dt2.NumRow() != 4 {
		t.Error(dt2.NumRow())
	}
	if dt2.NumCol() != 2 {
		t.Error(dt2.NumCol())
	}
}

func Test_RemoveRow(t *testing.T) {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	dt.RemoveRow(1)
	if dt.NumRow() != 3 {
		t.Fail()
	}
	printTable(dt, t)

	dt.RemoveRow(0)
	if dt.NumRow() != 2 {
		t.Fail()
	}
	printTable(dt, t)

	dt = NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	dt.RemoveRow(3)
	if dt.NumRow() != 3 {
		t.Fail()
	}
	printTable(dt, t)
}

func Test_RemoveColumn(t *testing.T) {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	err := dt.RemoveColumn(1)
	if err != nil {
		t.Error(err)
	}
	if dt.NumCol() != 2 {
		t.Fail()
	}
	printTable(dt, t)

	err = dt.RemoveColumn(0)
	if err != nil {
		t.Error(err)
	}
	if dt.NumCol() != 1 {
		t.Fail()
	}
	printTable(dt, t)

	dt = NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	err = dt.RemoveColumn(2)
	if err != nil {
		t.Error(err)
	}
	printTable(dt, t)
}

func Test_Join(t *testing.T) {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	dt2 := NewDataTable(2)
	dt2.AppendRow([]string{"a", "1"})
	dt2.AppendRow([]string{"f", "2"})
	dt2.AppendRow([]string{"k", "3"})

	dt3 := Join(dt, dt2, func(l, r []string) bool {
		return l[0] == r[0]
	})
	if n := dt3.NumCol(); n != 5 {
		t.Error(n)
	}
	if n := dt3.NumRow(); n != 2 {
		t.Error(n)
	}
	for i := 0; i < dt3.NumRow(); i++ {
		row := dt3.GetRow(i)
		t.Log(row)
	}
}

func Test_HashJoin(t *testing.T) {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	dt2 := NewDataTable(2)
	dt2.AppendRow([]string{"a", "1"})
	dt2.AppendRow([]string{"f", "2"})
	dt2.AppendRow([]string{"k", "3"})

	dt3 := HashJoin(dt, dt2,
		func(l []string) string {
			return l[0]
		}, func(r []string) string {
			return r[0]
		})
	if n := dt3.NumCol(); n != 5 {
		t.Error(n)
	}
	if n := dt3.NumRow(); n != 2 {
		t.Error(n)
	}
	for i := 0; i < dt3.NumRow(); i++ {
		row := dt3.GetRow(i)
		t.Log(row)
	}
}

func Test_MarshalJSON(t *testing.T) {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	data, err := json.Marshal(dt)
	if err != nil {
		t.Error(err)
	}
	t.Log(string(data))
	var dt2 DataTable
	if err := json.Unmarshal(data, &dt2); err != nil {
		t.Error(err)
	}
	dt2.ToCSV(os.Stdout)

	type testStruct struct {
		Table *DataTable `json:"table"`
	}
	s := &testStruct{
		Table: dt,
	}
	data, err = json.Marshal(s)
	if err != nil {
		t.Error(err)
	}
	t.Log(string(data))
	var s2 testStruct
	if err := json.Unmarshal(data, &s2); err != nil {
		t.Error(err)
	}
	t.Log(s2.Table)
}
