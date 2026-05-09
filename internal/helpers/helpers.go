package helpers

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"hash"
)

func StringToBytes(s string) []byte {
	return []byte(s)
}

func CloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func KeysEqual(key1, key2 []byte) bool {
	return bytes.Equal(key1, key2)
}

func ValuesEqual(v1, v2 []byte) bool {
	return bytes.Equal(v1, v2)
}

func CompareKeys(key1, key2 []byte) int {
	return bytes.Compare(key1, key2)
}

func CalculateKeyHash(key []byte, h hash.Hash64, additionalData []byte) (uint64, error) {
	var n int
	var err error

	n, err = h.Write(key)
	if n < len(key) {
		return 0, fmt.Errorf("CalculateHash: key: %d < %d", n, len(key))
	}
	if err != nil {
		return 0, fmt.Errorf("CalculateHash: key: %w", err)
	}

	n, err = h.Write(additionalData)
	if n < len(additionalData) {
		return 0, fmt.Errorf("CalculateHash: data: %d < %d", n, len(key))
	}
	if err != nil {
		return 0, fmt.Errorf("CalculateHash: data: %w", err)
	}

	result := h.Sum64()
	h.Reset()
	return result, nil
}

func CreateSalt(size int) ([]byte, error) {
	result := make([]byte, size)

	n, err := rand.Read(result)
	if err != nil {
		return nil, fmt.Errorf("CreateSalt: %w", err)
	}

	if n != size {
		return nil, fmt.Errorf("CreateSalt: salt: %d < %d", n, size)
	}

	return result, nil
}
