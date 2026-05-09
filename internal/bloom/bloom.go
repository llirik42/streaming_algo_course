package bloom

import (
	"fmt"
	"hash"
	"hash/fnv"
)

type Filter struct {
	masks      []*bitBuffer
	masksCount int
	maskSize   uint64

	hashFunctions []hash.Hash64
}

func New(size uint64, hashes uint8) *Filter {
	masksCount := int(hashes)
	maskSize := size

	masks := make([]*bitBuffer, masksCount)
	hashFunctions := make([]hash.Hash64, masksCount)

	for i := 0; i < masksCount; i++ {
		masks[i] = newBitBuffer(maskSize)
		hashFunctions[i] = fnv.New64()
	}

	return &Filter{
		masks:         masks,
		masksCount:    masksCount,
		maskSize:      maskSize,
		hashFunctions: hashFunctions,
	}
}

func (f *Filter) Add(key []byte) error {
	for i := 0; i < f.masksCount; i++ {
		hashFunction := f.hashFunctions[i]
		bitIndex, err := f.calculateBitIndex(key, hashFunction)
		if err != nil {
			return fmt.Errorf("bloom Add: calculate bucket index: %w", err)
		}

		currentMask := f.masks[i]
		currentMask.setBit(bitIndex, 1)
	}

	return nil
}

func (f *Filter) MayContain(key []byte) (bool, error) {
	for i := 0; i < f.masksCount; i++ {
		hashFunction := f.hashFunctions[i]
		bitIndex, err := f.calculateBitIndex(key, hashFunction)
		if err != nil {
			return false, fmt.Errorf("bloom MayContain: calculate bucket index: %w", err)
		}

		currentMask := f.masks[i]
		if currentMask.getBit(bitIndex) == 0 {
			return false, nil
		}
	}

	return true, nil
}

func (f *Filter) calculateBitIndex(key []byte, h hash.Hash64) (uint64, error) {
	hashValue, err := calculateHash(key, h)
	if err != nil {
		return 0, fmt.Errorf("calculateBucketIndex: %w", err)
	}
	return hashValue % f.maskSize, nil
}

func calculateHash(key []byte, h hash.Hash64) (uint64, error) {
	n, err := h.Write(key)

	if n < len(key) {
		return 0, fmt.Errorf("bloom calculateHash: %d < %d", n, len(key))
	}
	if err != nil {
		return 0, fmt.Errorf("bloom calculateHash: %w", err)
	}

	result := h.Sum64()
	h.Reset()
	return result, nil
}
