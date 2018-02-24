package bseries

var aggregations = map[string]func([]bool) bool{
	"majority": majority,
}

func majority(b []bool) bool {
	tCount, fCount := 0, 0
	for _, x := range b {
		if x {
			tCount++
		} else {
			fCount++
		}
	}

	return tCount > fCount
}
