package stream

import (
	"fmt"
	"hash"
	. "kvschool/internal/helpers"

	"github.com/cespare/xxhash"
	"github.com/spaolacci/murmur3"
)

type CountMinSketch struct {
	counters [][]uint64
	width    uint64
	depth    uint32

	hf1 hash.Hash64
	hf2 hash.Hash64
}

func NewCountMinSketch(width, depth uint32) *CountMinSketch {
	counters := make([][]uint64, depth)

	var i uint32
	for i = 0; i < depth; i++ {
		counters[i] = make([]uint64, width)
	}

	return &CountMinSketch{
		counters: counters,
		width:    uint64(width),
		depth:    depth,
		hf1:      xxhash.New(),
		hf2:      murmur3.New64(),
	}
}

func (c *CountMinSketch) Add(key []byte) error {
	var i uint32
	for i = 0; i < c.depth; i++ {
		counterIndex, err := c.calculateCounterIndex(key, i)
		if err != nil {
			return fmt.Errorf("cms Add: calculate counter index: %w", err)
		}

		c.counters[i][counterIndex]++
	}

	return nil
}

// Estimate возвращает примерную частоту ключа.
// Гарантия: Estimate >= TrueCount (никогда не занижает).
func (c *CountMinSketch) Estimate(key []byte) (uint64, error) {
	var minCounter uint64 = 1<<64 - 1

	var i uint32
	for i = 0; i < c.depth; i++ {
		counterIndex, err := c.calculateCounterIndex(key, i)
		if err != nil {
			return 0, fmt.Errorf("cms Estimate: calculate counter index: %w", err)
		}

		currentCounter := c.counters[i][counterIndex]
		minCounter = min(minCounter, currentCounter)
	}

	return minCounter, nil
}

func (c *CountMinSketch) calculateCounterIndex(key []byte, hashIndex uint32) (uint64, error) {
	h1, err := CalculateKeyHash(key, c.hf1)
	if err != nil {
		return 0, fmt.Errorf("cms calculateCounterIndex(): hash 1: %w", err)
	}

	h2, err := CalculateKeyHash(key, c.hf2)
	if err != nil {
		return 0, fmt.Errorf("cms calculateCounterIndex(): hash 2: %w", err)
	}

	return (h1 + (uint64(hashIndex)+1)*h2) % c.width, nil
}
