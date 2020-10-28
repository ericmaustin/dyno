package distribution

import (
	"fmt"
	"math"
	"sort"
)

func Linear(count, units, min, alpha int) []int {
	distribution := make([]int, count)

	total := 0.0
	segments := make([]float64, count)

	for i := 0; i < count; i++ {
		segments[i] = float64(i * alpha)
		total = total + float64(segments[i])
	}

	for i, seg := range segments {
		cu := int(math.Ceil(seg / total * float64(units)))
		fmt.Println(cu)
		if min > 0 && cu < min {
			units = units - (min - cu)
			cu = min
		}
		distribution[i] = cu
	}

	sort.Ints(distribution)

	for i, j := 0, len(distribution)-1; i < j; i, j = i+1, j-1 {
		distribution[i], distribution[j] = distribution[j], distribution[i]
	}

	return distribution
}

func Exponential(count, units, min, alpha int) []int {

	distribution := make([]int, count)

	total := 0.0
	segments := make([]float64, count)

	for i := 0; i < count; i++ {
		segments[i] = float64(math.Pow(float64(i+1), float64(alpha)))
		total = total + float64(segments[i])
	}

	for i, seg := range segments {
		cu := int(math.Ceil(seg / total * float64(units)))
		fmt.Println(cu)
		if min > 0 && cu < min {
			units = units - (min - cu)
			cu = min
		}
		distribution[i] = cu
	}
	sort.Ints(distribution)

	for i, j := 0, len(distribution)-1; i < j; i, j = i+1, j-1 {
		distribution[i], distribution[j] = distribution[j], distribution[i]
	}

	return distribution
}
