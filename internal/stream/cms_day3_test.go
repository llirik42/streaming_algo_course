//go:build day3

package stream

import (
	"fmt"
	. "kvschool/internal/helpers"
	"math/rand"
	"testing"
)

func addKey(t *testing.T, cms *CountMinSketch, key []byte) {
	if err := cms.Add(key); err != nil {
		t.Errorf("Adding key error = %v", err)
	}
}

func estimate(t *testing.T, cms *CountMinSketch, key []byte, bottom uint64) {
	est, err := cms.Estimate(key)
	if err != nil {
		t.Errorf("Estimate error = %v", err)
	}

	if est < bottom {
		t.Errorf("Underestimate = %v < %v", est, bottom)
	}
}

func TestCountMinSketch_EstimateMonotone(t *testing.T) {
	cms := NewCountMinSketch(64, 4)

	for i := 0; i < 10; i++ {
		if err := cms.Add([]byte("hot")); err != nil {
			t.Fatalf("Add hot: %v", err)
		}
	}
	est, err := cms.Estimate([]byte("hot"))
	if err != nil {
		t.Fatalf("Estimate hot: %v", err)
	}
	// Для CMS типично: оценка >= истинного значения (overestimate допустим),
	// но undercount — индикатор ошибки.
	if est < 10 {
		t.Fatalf("estimate too small: %d", est)
	}
}

func TestCountMinSketch_Empty(t *testing.T) {
	cmsList := []*CountMinSketch{
		NewCountMinSketch(1, 1),
		NewCountMinSketch(1, 10),
		NewCountMinSketch(10, 1),
		NewCountMinSketch(10, 10),
	}

	keys := [][]byte{
		StringToBytes(""),
		StringToBytes("key-1"),
		StringToBytes("123456789"),
		StringToBytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}

	for _, k := range keys {
		for _, cms := range cmsList {
			estimate(t, cms, k, 0)
		}
	}
}

func TestCountMinSketch_SingleKey(t *testing.T) {
	cmsList := []*CountMinSketch{
		NewCountMinSketch(1, 1),
		NewCountMinSketch(1, 10),
		NewCountMinSketch(10, 1),
		NewCountMinSketch(10, 10),
	}

	key := StringToBytes("key")
	for _, cms := range cmsList {
		estimate(t, cms, key, 0)
	}

	for _, cms := range cmsList {
		addKey(t, cms, key)
		estimate(t, cms, key, 1)
	}

	rng := rand.New(rand.NewSource(0))
	for _, cms := range cmsList {
		additionsNumber := rng.Int() % 1000
		for i := 0; i < additionsNumber; i++ {
			addKey(t, cms, key)
		}
		estimate(t, cms, key, uint64(additionsNumber)+1)
	}
}

func TestCountMinSketch_MultipleKeysSequential(t *testing.T) {
	cmsList := []*CountMinSketch{
		NewCountMinSketch(1, 1),
		NewCountMinSketch(1, 10),
		NewCountMinSketch(10, 1),
		NewCountMinSketch(10, 10),
		NewCountMinSketch(100, 1),
		NewCountMinSketch(1000, 1),
	}

	keysNumber := 10000
	keys := make([][]byte, keysNumber)
	for i := 0; i < keysNumber; i++ {
		keys[i] = StringToBytes(fmt.Sprintf("key-%d", i+1))
	}

	additions := make([]uint64, keysNumber)
	rng := rand.New(rand.NewSource(0))

	for i, key := range keys {
		for _, cms := range cmsList {
			additionsNumber := uint64(rng.Int() % 1000)
			additions[i] = additionsNumber
			var i uint64
			for i = 0; i < additionsNumber; i++ {
				addKey(t, cms, key)
			}
			estimate(t, cms, key, additionsNumber)
		}
	}
}

func TestCountMinSketch_MultipleKeysRandom(t *testing.T) {
	cmsList := []*CountMinSketch{
		NewCountMinSketch(1, 1),
		NewCountMinSketch(1, 10),
		NewCountMinSketch(10, 1),
		NewCountMinSketch(10, 10),
		NewCountMinSketch(100, 1),
		NewCountMinSketch(1000, 1),
	}

	keysNumber := 100000
	keys := make([][]byte, keysNumber)
	for i := 0; i < keysNumber; i++ {
		keys[i] = StringToBytes(fmt.Sprintf("key-%d", i+1))
	}

	totalAdditionsNumber := keysNumber * 10
	additions := make([]uint64, keysNumber)
	rng := rand.New(rand.NewSource(0))

	for i := 0; i < totalAdditionsNumber; i++ {
		keyIndex := rng.Int() % keysNumber
		key := keys[keyIndex]
		additions[keyIndex]++
		for _, cms := range cmsList {
			addKey(t, cms, key)
		}
	}

	for i, key := range keys {
		for _, cms := range cmsList {
			estimate(t, cms, key, additions[i])
		}
	}
}
