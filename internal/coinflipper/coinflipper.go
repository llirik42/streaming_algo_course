package coinflipper

import "math/rand"

type CoinFlipper struct {
	random      *rand.Rand
	probability float64
}

func New(seed int64, probability float64) *CoinFlipper {
	return &CoinFlipper{
		random:      rand.New(rand.NewSource(seed)),
		probability: probability,
	}
}

func (cf *CoinFlipper) Flip() bool {
	return cf.random.Float64() < cf.probability
}
