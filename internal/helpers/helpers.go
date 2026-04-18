package helpers

import (
	"bytes"
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

func CompareKeys(key1, key2 []byte) int {
	return bytes.Compare(key1, key2)
}
