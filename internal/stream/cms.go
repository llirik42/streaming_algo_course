package stream

import (
	"fmt"
	"hash"
	"hash/fnv"
	. "kvschool/internal/helpers"
)

type CountMinSketch struct {
	counters [][]uint64
	width    uint64
	depth    uint32

	hashFunctions []hash.Hash64
}

func NewCountMinSketch(width, depth uint32) *CountMinSketch {
	counters := make([][]uint64, depth)
	hashFunctions := make([]hash.Hash64, depth)

	var i uint32
	for i = 0; i < depth; i++ {
		counters[i] = make([]uint64, width)
		hashFunctions[i] = fnv.New64()
	}

	return &CountMinSketch{
		counters:      counters,
		width:         uint64(width),
		depth:         depth,
		hashFunctions: hashFunctions,
	}
}

func (c *CountMinSketch) Add(key []byte) error {
	var i uint32
	for i = 0; i < c.depth; i++ {
		hashFunction := c.hashFunctions[i]
		counterIndex, err := c.calculateCounterIndex(key, hashFunction)
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
		hashFunction := c.hashFunctions[i]
		counterIndex, err := c.calculateCounterIndex(key, hashFunction)
		if err != nil {
			return 0, fmt.Errorf("cms Estimate: calculate counter index: %w", err)
		}

		currentCounter := c.counters[i][counterIndex]
		minCounter = min(minCounter, currentCounter)
	}

	return minCounter, nil
}

func (c *CountMinSketch) calculateCounterIndex(key []byte, h hash.Hash64) (uint64, error) {
	hashValue, err := CalculateKeyHash(key, h)
	if err != nil {
		return 0, fmt.Errorf("cms: calculateCounterIndex: %w", err)
	}
	return hashValue % c.width, nil
}
