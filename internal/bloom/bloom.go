package bloom

import (
	"fmt"
	"hash"
	. "kvschool/internal/helpers"

	"github.com/cespare/xxhash"
	"github.com/spaolacci/murmur3"
)

type Filter struct {
	masks      []*bitBuffer
	masksCount uint8
	maskSize   uint64

	hf1 hash.Hash64
	hf2 hash.Hash64
}

func New(size uint64, hashes uint8) *Filter {
	masksCount := hashes
	maskSize := size
	masks := make([]*bitBuffer, masksCount)

	var i uint8
	for i = 0; i < masksCount; i++ {
		masks[i] = newBitBuffer(maskSize)
	}

	return &Filter{
		masks:      masks,
		masksCount: masksCount,
		maskSize:   maskSize,
		hf1:        xxhash.New(),
		hf2:        murmur3.New64(),
	}
}

func (f *Filter) Add(key []byte) error {
	var i uint8
	for i = 0; i < f.masksCount; i++ {
		bitIndex, err := f.calculateBitIndex(key, i)
		if err != nil {
			return fmt.Errorf("bloom Add: calculate bit index: %w", err)
		}

		currentMask := f.masks[i]
		currentMask.setBit(bitIndex, 1)
	}

	return nil
}

func (f *Filter) MayContain(key []byte) (bool, error) {
	var i uint8
	for i = 0; i < f.masksCount; i++ {
		bitIndex, err := f.calculateBitIndex(key, i)
		if err != nil {
			return false, fmt.Errorf("bloom MayContain: calculate bit index: %w", err)
		}

		currentMask := f.masks[i]
		if currentMask.getBit(bitIndex) == 0 {
			return false, nil
		}
	}

	return true, nil
}

func (f *Filter) calculateBitIndex(key []byte, hashIndex uint8) (uint64, error) {
	h1, err := CalculateKeyHash(key, f.hf1)
	if err != nil {
		return 0, fmt.Errorf("bloom calculateBitIndex(): hash 1: %w", err)
	}

	h2, err := CalculateKeyHash(key, f.hf2)
	if err != nil {
		return 0, fmt.Errorf("bloom calculateBitIndex(): hash 2: %w", err)
	}

	return (h1 + (uint64(hashIndex)+1)*h2) % f.maskSize, nil
}
