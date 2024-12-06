package rand

import (
	"math/rand/v2"
)

func Bool() bool {
	v := rand.IntN(2)
	return v == 1
}

func Range(min, max int) int {
	return rand.IntN(max-min) + min
}
