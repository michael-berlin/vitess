package pkg2

import (
	"math"
	"testing"
)

func TestFile2(t *testing.T) {
	sum := 0.0
	for i := 0; i < 100000000; i++ {
		sum += math.Sqrt(float64(i))
	}
}
