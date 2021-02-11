package gohalt

import (
	"crypto/rand"
	"math"
	"math/big"
)

func rndf64(fallback float64) float64 {
	rnd, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		return fallback
	}
	return float64(rnd.Int64()) / math.MaxInt64
}
