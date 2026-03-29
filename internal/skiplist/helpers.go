package skiplist

import "bytes"

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func keysEqual(key1, key2 []byte) bool {
	return bytes.Equal(key1, key2)
}

func keysCompare(key1, key2 []byte) int {
	return bytes.Compare(key1, key2)
}
