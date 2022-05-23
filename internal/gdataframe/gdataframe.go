package gdataframe

type Number interface {
	int64 | float64 | int
}

type Dataframe[T Number] struct {
	cols map[string][]T
}

func NewDataframe[T Number]() *Dataframe[T] {
	return &Dataframe[T]{
		cols: make(map[string][]T),
	}
}

func (df *Dataframe[T]) AddCol(name string, data ...T) {
	df.cols[name] = data
}

func (df *Dataframe[T]) SumCol(name string) T {
	var sum T
	for _, v := range df.cols[name] {
		sum += v
	}
	return sum
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func (df *Dataframe[T]) SumColGoroutine(name string) T {

	goroutines := 2
	length := len(df.cols[name])
	chunkSize := length / goroutines

	ch := make(chan T, goroutines)

	for i := 0; i < goroutines; i++ {
		start := chunkSize * i
		end := Min(chunkSize*(i+1), length)

		go func(start, end int) {
			var sum T
			for i := start; i < end; i++ {
				sum += df.cols[name][i]
			}
			ch <- sum
		}(start, end)
	}

	var sum T
	for i := 0; i < goroutines; i++ {
		sum += <-ch
	}

	return sum
}
