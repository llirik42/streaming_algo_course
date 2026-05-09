package bloom

import (
	"fmt"
	"hash"
	"hash/fnv"
	. "kvschool/internal/helpers"
)

const (
	saltSize = 128
)

type Filter struct {
	masks      []*bitBuffer
	masksCount uint8
	maskSize   uint64

	salt          [][]byte
	hashFunctions []hash.Hash64
}

func New(size uint64, hashes uint8) *Filter {
	masksCount := hashes
	maskSize := size

	masks := make([]*bitBuffer, masksCount)
	hashFunctions := make([]hash.Hash64, masksCount)
	salt := make([][]byte, masksCount)

	var i uint8
	for i = 0; i < masksCount; i++ {
		masks[i] = newBitBuffer(maskSize)
		hashFunctions[i] = fnv.New64()

		newSalt, err := CreateSalt(saltSize)
		if err != nil {
			panic(err)
		}
		salt[i] = newSalt
	}

	return &Filter{
		masks:         masks,
		masksCount:    masksCount,
		maskSize:      maskSize,
		salt:          salt,
		hashFunctions: hashFunctions,
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
	h := f.hashFunctions[hashIndex]
	hashValue, err := CalculateKeyHash(key, h, f.salt[hashIndex])
	if err != nil {
		return 0, fmt.Errorf("bloom calculateBitIndex: %w", err)
	}
	return hashValue % f.maskSize, nil
}
