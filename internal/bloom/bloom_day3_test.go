//go:build day3

package bloom

import (
	"fmt"
	. "kvschool/internal/helpers"
	"testing"
)

func addKey(t *testing.T, filter *Filter, key []byte) {
	if err := filter.Add(key); err != nil {
		t.Errorf("Error adding key: %v", err)
	}
}

func checkDoesntContain(t *testing.T, filter *Filter, key []byte) {
	mayContain, err := filter.MayContain(key)
	if err != nil {
		t.Errorf("Error checking key: %v", err)
	}
	if mayContain {
		t.Fatal("filter must not contain key")
	}
}

func checkMayContain(t *testing.T, filter *Filter, key []byte) {
	mayContain, err := filter.MayContain(key)
	if err != nil {
		t.Errorf("Error checking key: %v", err)
	}
	if !mayContain {
		t.Fatal("filter returned that it doesn't contain key")
	}
}

func TestBloom_NoFalseNegatives(t *testing.T) {
	// Параметры маленькие намеренно: цель теста — свойство "нет false negative",
	// а не качество false positive.
	f := New(1024, 3)

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for _, k := range keys {
		if err := f.Add(k); err != nil {
			t.Fatalf("Add(%q): %v", string(k), err)
		}
	}
	for _, k := range keys {
		ok, err := f.MayContain(k)
		if err != nil {
			t.Fatalf("MayContain(%q): %v", string(k), err)
		}
		if !ok {
			t.Fatalf("false negative for key=%q", string(k))
		}
	}
}

func TestBloom_Empty(t *testing.T) {
	f1 := New(1, 1)
	f2 := New(1024, 3)
	f3 := New(1024*1024, 10)

	keys := [][]byte{
		StringToBytes(""),
		StringToBytes("key-1"),
		StringToBytes("123456789"),
		StringToBytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}

	for _, k := range keys {
		checkDoesntContain(t, f1, k)
		checkDoesntContain(t, f2, k)
		checkDoesntContain(t, f3, k)
	}
}

func TestBloom_SingleKey(t *testing.T) {
	f1 := New(1, 1)
	f2 := New(1024, 3)
	f3 := New(1024*1024, 10)

	key := StringToBytes("key")

	addKey(t, f1, key)
	checkMayContain(t, f1, key)

	addKey(t, f2, key)
	checkMayContain(t, f2, key)

	addKey(t, f3, key)
	checkMayContain(t, f3, key)
}

func TestBloom_MultipleKeysSequential(t *testing.T) {
	filters := []*Filter{
		New(1, 1),
		New(1, 10),
		New(1, 100),
		New(100, 1),
		New(100, 10),
		New(1024, 1),
		New(1024, 3),
		New(1024, 10),
	}

	keysNumber := 1024 * 50
	keys := make([][]byte, keysNumber)
	for i := 0; i < keysNumber; i++ {
		keys[i] = StringToBytes(fmt.Sprintf("key-%d", i+1))
	}

	for _, filter := range filters {
		for _, key := range keys {
			addKey(t, filter, key)
			checkMayContain(t, filter, key)
		}
	}

	// На всякий случай
	for _, filter := range filters {
		for _, key := range keys {
			checkMayContain(t, filter, key)
		}
	}
}
