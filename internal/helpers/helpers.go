package helpers

import (
	"bytes"
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

func CalculateKeyHash(key []byte, h hash.Hash64) (uint64, error) {
	n, err := h.Write(key)

	if n < len(key) {
		return 0, fmt.Errorf("CalculateHash: %d < %d", n, len(key))
	}
	if err != nil {
		return 0, fmt.Errorf("CalculateHash: %w", err)
	}

	result := h.Sum64()
	h.Reset()
	return result, nil
}
