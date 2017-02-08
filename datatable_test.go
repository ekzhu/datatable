package datatable

import (
	"encoding/json"
	"testing"
)

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
	if err := dt.AppendRow([]string{"1", "2"}); err != NumColError {
		t.Error(err)
	}
	if row, err := dt.GetRow(1); err != nil || row[0] != "e" {
		t.Fail()
	}
}

func Test_Slice(t *testing.T) {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	dt2, _ := dt.Slice(0, 10)
	if dt2.NumRow() != 4 {
		t.Error(dt2.NumRow())
	}
	if dt2.NumCol() != 3 {
		t.Error(dt2.NumCol())
	}

	dt3, _ := dt.Slice(2, 2)
	if row, _ := dt3.GetRow(0); row[0] != "f" {
		t.Error(row)
	}
	if row, _ := dt3.GetRow(1); row[0] != "g" {
		t.Error(row)
	}

	dt4, _ := dt.Slice(1, 0)
	if dt4.NumRow() != 0 {
		t.Error(dt2.NumRow())
	}
	dt4.AppendRow([]string{"x", "y", "z"})
	if dt4.NumRow() != 1 {
		t.Error(dt4.NumRow())
	}
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
		row, _ := dt3.GetRow(i)
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
		row, _ := dt3.GetRow(i)
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
}
