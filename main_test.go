package main

import (
	"fmt"
	"mytest/internal/dataframe"
	"mytest/internal/gdataframe"
	"mytest/internal/idataframe"
	"testing"
)

type Row struct {
	cols []int
}

func BenchmarkSum(b *testing.B) {
	df := dataframe.NewDataframe()
	idf := idataframe.NewDataframe()
	gdf := gdataframe.NewDataframe[int]()

	const size = 1024 * 1024
	fmt.Println("size -->", size/1024/1024, "MB")

	rows := make([]Row, size)

	s := make([]int, size)
	si := make([]interface{}, size)
	for i := 0; i < size; i++ {
		s[i] = i
		si[i] = i
		rows[i].cols = make([]int, 3)
		rows[i].cols[2] = i
	}

	fmt.Println("# Generation finished")
	df.AddCol("mycol", s...)
	idf.AddCol("mycol", si...)
	gdf.AddCol("mycol", s...)

	b.Run("int dataframe -> sum col", func(b *testing.B) {
		_ = df.SumCol("mycol")
	})

	b.Run("int dataframe -> sum col goroutines", func(b *testing.B) {
		_ = df.SumColGoroutine("mycol")
	})

	b.Run("interface dataframe -> sum col", func(b *testing.B) {
		_ = idf.SumCol("mycol")
	})

	b.Run("interface dataframe -> sum col goroutines", func(b *testing.B) {
		_ = idf.SumColGoroutine("mycol")
	})

	b.Run("generics dataframe -> sum col goroutines", func(b *testing.B) {
		_ = gdf.SumColGoroutine("mycol")
	})

	b.Run("generics dataframe -> sum col", func(b *testing.B) {
		_ = gdf.SumCol("mycol")
	})

	b.Run("sum cols stored in rows", func(b *testing.B) {
		var sum int
		for _, row := range rows {
			sum += row.cols[2]
		}
		_ = sum
	})
}
