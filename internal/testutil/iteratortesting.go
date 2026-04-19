package testutil

import (
	"bytes"
	"fmt"
	"kvschool/internal/iterator"
	"testing"

	. "kvschool/internal/helpers"
)

type TestCase struct {
	StartKey    []byte
	EndKey      []byte
	StartNumber int
	EndNumber   int
}

func TestIterator(t *testing.T, it iterator.Iterator, start, end, step int) {
	for i := start; i < end; i += step {
		expectedKey := StringToBytes(fmt.Sprintf("key-%d", i))
		expectedValue := StringToBytes(fmt.Sprintf("value-%d", i))
		k, v, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			t.Fatalf("Next: expected ok, got not ok on %d", i)
		}
		if !bytes.Equal(k, expectedKey) {
			t.Fatalf("Next: expected key %s, got %s on %d", expectedKey, k, i)
		}
		if !bytes.Equal(v, expectedValue) {
			t.Fatalf("Next: expected value %s, got %s on %d", expectedValue, v, i)
		}
	}

	t.Errorf("%v\n", it)

	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		_, _, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if ok {
			t.Fatalf("Next expected not ok, got ok on %d", i)
		}
	}
}
