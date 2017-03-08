package datatable

import (
	"fmt"
	"os"
	"strings"
)

func ExampleDataTable_ApplyColumn() {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	// Concatenate all values in the first column
	s := ""
	dt.ApplyColumn(func(x int, v string) error {
		s += v
		return nil
	}, 0)

	fmt.Println(s)
	// Output: aefg
}

func ExampleDataTable_ApplyColumns() {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"a", "b", "x"})
	dt.AppendRow([]string{"e", "h", "l"})

	// Count the number of unique pairs in the first two
	// columns
	s := make(map[string]bool)
	dt.ApplyColumns(func(x int, vs []string) error {
		pair := strings.Join(vs, ",")
		s[pair] = true
		return nil
	}, 0, 1)

	fmt.Println(len(s))
	// Output: 3
}

func ExampleDataTable_Project() {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	// Project on the first and the third column
	dt2 := dt.Project(0, 2)

	dt2.ToCsv(os.Stdout)
	// Output:
	// a,c
	// e,g
	// f,x
	// g,l
}

func ExampleDataTable_Slice() {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	// Take 2 rows starting at the row index 1
	dt2 := dt.Slice(1, 2)

	dt2.ToCsv(os.Stdout)
	// Output:
	// e,f,g
	// f,k,x
}

func ExampleJoin() {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	dt2 := NewDataTable(2)
	dt2.AppendRow([]string{"a", "1"})
	dt2.AppendRow([]string{"f", "2"})
	dt2.AppendRow([]string{"k", "3"})

	// Join dt1 and dt2 on their first columns..
	dt3 := Join(dt, dt2, func(l, r []string) bool {
		return l[0] == r[0]
	})

	dt3.ToCsv(os.Stdout)
	// Output:
	// a,b,c,a,1
	// f,k,x,f,2
}

func ExampleHashJoin() {
	dt := NewDataTable(3)
	dt.AppendRow([]string{"a", "b", "c"})
	dt.AppendRow([]string{"e", "f", "g"})
	dt.AppendRow([]string{"f", "k", "x"})
	dt.AppendRow([]string{"g", "h", "l"})

	dt2 := NewDataTable(2)
	dt2.AppendRow([]string{"a", "1"})
	dt2.AppendRow([]string{"f", "2"})
	dt2.AppendRow([]string{"k", "3"})

	// Join dt1 and dt2 on their first columns.
	dt3 := HashJoin(dt, dt2,
		func(l []string) string {
			return l[0]
		}, func(r []string) string {
			return r[0]
		})

	dt3.ToCsv(os.Stdout)
	// Output:
	// a,b,c,a,1
	// f,k,x,f,2
}
