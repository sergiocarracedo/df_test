package dataframe

type Data = []int

type Dataframe struct {
	cols map[string]Data
}

func NewDataframe() *Dataframe {
	return &Dataframe{
		cols: make(map[string]Data),
	}
}

func (df *Dataframe) AddCol(name string, data ...int) {
	df.cols[name] = data
}

func (df *Dataframe) SumCol(name string) int {
	var sum int
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

func (df *Dataframe) SumColGoroutine(name string) int {

	goroutines := 2
	length := len(df.cols[name])
	chunkSize := length / goroutines

	ch := make(chan int, goroutines)

	for i := 0; i < goroutines; i++ {
		start := chunkSize * i
		end := Min(chunkSize*(i+1), length)

		go func(start, end int) {
			var sum int
			for i := start; i < end; i++ {
				sum += df.cols[name][i]
			}
			ch <- sum
		}(start, end)
	}

	var sum int
	for i := 0; i < goroutines; i++ {
		sum += <-ch
	}

	return sum
}
