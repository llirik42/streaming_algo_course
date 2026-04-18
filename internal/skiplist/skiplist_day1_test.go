//go:build day1

package skiplist

import (
	"bytes"
	"fmt"
	"kvschool/internal/iterator"
	"testing"
)

func stringToBytes(s string) []byte {
	return []byte(s)
}

func TestSkipList_BasicCRUD(t *testing.T) {
	sl := New(1)

	if err := sl.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if err := sl.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := sl.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("Put c: %v", err)
	}

	v, err := sl.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get a: %v", err)
	}
	if !bytes.Equal(v, []byte("1")) {
		t.Fatalf("Get a mismatch: %q", string(v))
	}

	if err := sl.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete b: %v", err)
	}
	_, err = sl.Get([]byte("b"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestSkipList_ScanOrderAndRange(t *testing.T) {
	sl := New(1)
	_ = sl.Put([]byte("a"), []byte("1"))
	_ = sl.Put([]byte("b"), []byte("2"))
	_ = sl.Put([]byte("c"), []byte("3"))

	it, err := sl.Scan([]byte("b"), []byte("d"))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer it.Close()

	var keys []string
	for {
		k, _, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		keys = append(keys, string(k))
	}
	if len(keys) != 2 || keys[0] != "b" || keys[1] != "c" {
		t.Fatalf("unexpected keys: %#v", keys)
	}
}

func TestSkipList_BasicOperations(t *testing.T) {
	sl := New(1)

	for i := 10; i <= 99; i++ {
		key := stringToBytes(fmt.Sprintf("key-%d", i))
		value := stringToBytes(fmt.Sprintf("value-%d", i))
		if err := sl.Put(key, value); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	for i := 10; i <= 99; i++ {
		key := stringToBytes(fmt.Sprintf("key-%d", i))
		expectedValue := stringToBytes(fmt.Sprintf("value-%d", i))
		realValue, err := sl.Get(key)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if !bytes.Equal(realValue, expectedValue) {
			t.Fatalf("Get %d: expected %s, got %s", i, expectedValue, realValue)
		}
	}

	keyToCheck := stringToBytes("key-18")
	keyToCheckNewValue := stringToBytes("new-value")
	if err := sl.Put(keyToCheck, keyToCheckNewValue); err != nil {
		t.Fatalf("Put %s: %v", keyToCheck, err)
	}

	keyToCheckRealValue, err := sl.Get(keyToCheck)
	if err != nil {
		t.Fatalf("Get %s: %v", keyToCheck, err)
	}
	if !bytes.Equal(keyToCheckRealValue, keyToCheckNewValue) {
		t.Fatalf("Get %s: expected %s, got %s", keyToCheck, keyToCheckNewValue, keyToCheckRealValue)
	}

	for i := 10; i <= 99; i++ {
		key := stringToBytes(fmt.Sprintf("key-%d", i))
		if sl.IsEmpty() {
			t.Fatalf("Empty on %d", i)
		}
		if err := sl.Delete(key); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}
	if !sl.IsEmpty() {
		t.Fatalf("Not empty")
	}
	if levelsNumber := sl.GetLevelsNumber(); levelsNumber != 1 {
		t.Fatalf("exptected 1 level, got %d", levelsNumber)
	}
}

func TestSkipList_UnknownKey(t *testing.T) {
	sl := New(1)

	key1 := stringToBytes("key")
	if _, err := sl.Get(key1); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound on Get(%s), got %v", key1, err)
	}
	if err := sl.Delete(key1); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound on Delete(%s), got %v", key1, err)
	}
	if err := sl.Put(key1, []byte("value")); err != nil {
		t.Fatalf("Put %s: %v", key1, err)
	}

	key2 := stringToBytes("another-key")
	if _, err := sl.Get(key2); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound on Get(%s), got %v", key2, err)
	}
	if err := sl.Delete(key2); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound on Delete(%s), got %v", key2, err)
	}
}

func TestSkipList_ScanEmpty(t *testing.T) {
	sl := New(1)

	it1, err1 := sl.Scan(nil, nil)
	it2, err2 := sl.Scan(stringToBytes("key-1"), nil)
	it3, err3 := sl.Scan(stringToBytes("key-1"), stringToBytes("key-2"))
	it4, err4 := sl.Scan(nil, stringToBytes("key-2"))

	iterators := []iterator.Iterator{it1, it2, it3, it4}
	errors := []error{err1, err2, err3, err4}

	for i := 0; i < len(iterators); i++ {
		err := errors[i]
		it := iterators[i]

		if err != nil {
			t.Fatalf("Scan, iterator %d: %v", i+1, err)
		}

		_, _, ok, nextErr := it.Next()
		if nextErr != nil {
			t.Fatalf("Next, iterator %d: %v", i+1, nextErr)
		}
		if ok {
			t.Fatalf("Unexpected ok, iterator %d", i+1)
		}
	}
}

func TestSkipList_ScanSingle(t *testing.T) {
	sl := New(1)

	key := stringToBytes("key1")
	smallerKey := stringToBytes("key0")
	greaterKey := stringToBytes("key2")
	value := stringToBytes("value1")

	if err := sl.Put(key, value); err != nil {
		t.Fatalf("Put %s: %v", key, err)
	}

	it1, err1 := sl.Scan(nil, key)
	it2, err2 := sl.Scan(key, nil)
	it3, err3 := sl.Scan(nil, smallerKey)
	it4, err4 := sl.Scan(smallerKey, nil)
	it5, err5 := sl.Scan(nil, greaterKey)
	it6, err6 := sl.Scan(greaterKey, nil)
	it7, err7 := sl.Scan(key, smallerKey)
	it8, err8 := sl.Scan(smallerKey, key)
	it9, err9 := sl.Scan(key, greaterKey)
	it10, err10 := sl.Scan(greaterKey, key)
	it11, err11 := sl.Scan(smallerKey, greaterKey)
	it12, err12 := sl.Scan(greaterKey, smallerKey)
	it13, err13 := sl.Scan(nil, nil)
	it14, err14 := sl.Scan(key, key)
	it15, err15 := sl.Scan(smallerKey, smallerKey)
	it16, err16 := sl.Scan(greaterKey, greaterKey)

	errors := []error{
		err1, err2, err3, err4, err5, err6, err7, err8,
		err9, err10, err11, err12, err13, err14, err15, err16,
	}
	for i, err := range errors {
		if err != nil {
			t.Fatalf("Scan, iterator %d: %v", i+1, err)
		}
	}

	iterators := []iterator.Iterator{
		it1, it2, it3, it4, it5, it6, it7, it8,
		it9, it10, it11, it12, it13, it14, it15, it16,
	}

	nonEmptyIndexes := []int{1, 3, 4, 8, 10, 12}
	nonEmptyIterators := make([]iterator.Iterator, 0)
	for i := 0; i < 16; i++ {
		for _, index := range nonEmptyIndexes {
			if i == index {
				nonEmptyIterators = append(nonEmptyIterators, iterators[i])
				break
			}
		}
	}

	for i, it := range nonEmptyIterators {
		k, v, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next on %d: %v", i+1, err)
		}
		if !ok {
			t.Fatalf("Next: expected ok, got not ok on %d", i+1)
		}
		if !bytes.Equal(k, key) {
			t.Fatalf("Next: expected key %s, got %s on %d", key, k, i+1)
		}
		if !bytes.Equal(value, v) {
			t.Fatalf("Next: expected value %s, got %s on %d", value, v, i+1)
		}
	}

	// Потому что 13 - несчастливое число
	for j := 0; j < 13; j++ {
		for i, it := range iterators {
			_, _, ok, err := it.Next()
			if err != nil {
				t.Fatalf("Next on %d: %v", i+1, err)
			}
			if ok {
				t.Fatalf("Next expected not ok, got ok on %d", i+1)
			}
		}
	}
}

func TestSkipList_ScanMultiple(t *testing.T) {
	sl := New(1)

	for i := 15; i <= 90; i++ {
		key := stringToBytes(fmt.Sprintf("key-%d", i))
		err := sl.Put(key, stringToBytes(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	type TestCase struct {
		StartKey    []byte
		EndKey      []byte
		StartNumber int
		EndNumber   int
	}

	testIterator := func(it iterator.Iterator, expectedStart, expectedEnd int) {
		for i := expectedStart; i <= expectedEnd; i++ {
			expectedKey := stringToBytes(fmt.Sprintf("key-%d", i))
			expectedValue := stringToBytes(fmt.Sprintf("value-%d", i))
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
		// Потому что 13 - несчастливое число
		for i := 0; i < 13; i++ {
			_, _, ok, err := it.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			if ok {
				t.Fatalf("Next expected not ok, got ok on")
			}
		}
	}

	cases := []TestCase{
		{nil, nil, 15, 90},
		{stringToBytes(fmt.Sprintf("key-15")), nil, 15, 90},
		{stringToBytes(fmt.Sprintf("key-10")), nil, 15, 90},
		{stringToBytes(fmt.Sprintf("key-26")), nil, 26, 90},
		{nil, stringToBytes(fmt.Sprintf("key-90")), 15, 89},
		{nil, stringToBytes(fmt.Sprintf("key-95")), 15, 90},
		{nil, stringToBytes(fmt.Sprintf("key-78")), 15, 77},
		{stringToBytes(fmt.Sprintf("key-15")), stringToBytes(fmt.Sprintf("key-90")), 15, 89},
		{stringToBytes(fmt.Sprintf("key-20")), stringToBytes(fmt.Sprintf("key-76")), 20, 75},
	}

	for i, c := range cases {
		startKey := c.StartKey
		endKey := c.EndKey
		startNumber := c.StartNumber
		endNumber := c.EndNumber

		it, err := sl.Scan(startKey, endKey)
		if err != nil {
			t.Fatalf("Scan %d: %v", err, i)
		}

		testIterator(it, startNumber, endNumber)
	}
}

func BenchmarkPutGetProbability(b *testing.B) {
	for i := 1; i <= 99; i++ {
		probability := float64(i) / 100.0

		b.Run(fmt.Sprintf("probability=%f", probability), func(b *testing.B) {
			size := 100000
			sl := New(1)
			sl.SetProbability(probability)

			keys := make([][]byte, size)
			values := make([][]byte, size)

			for j := 0; j < size; j++ {
				keys[j] = stringToBytes(fmt.Sprintf("key-%d", j))
				values[j] = stringToBytes(fmt.Sprintf("value-%d", j))
			}

			b.ReportAllocs()
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				for k := 0; k < size; k++ {
					key := keys[k]

					if k%2 == 0 {
						if err := sl.Put(key, values[k]); err != nil {
							b.Fatalf("ошибка put: %v", err)
						}
					} else {
						_, _ = sl.Get(key)
					}
				}
			}
		})
	}
}

func BenchmarkPutGetN(b *testing.B) {
	for i := 1; i <= 200; i++ {
		size := i * 10000

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			sl := New(1)
			keys := make([][]byte, size)
			values := make([][]byte, size)

			for j := 0; j < size; j++ {
				keys[j] = stringToBytes(fmt.Sprintf("key-%d", j))
				values[j] = stringToBytes(fmt.Sprintf("value-%d", j))
			}

			b.ReportAllocs()
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				for k := 0; k < size; k++ {
					key := keys[k]

					if k%2 == 0 {
						if err := sl.Put(key, values[k]); err != nil {
							b.Fatalf("ошибка put: %v", err)
						}
					} else {
						_, _ = sl.Get(key)
					}
				}
			}
		})
	}
}

func BenchmarkDeleteN(b *testing.B) {
	for i := 1; i <= 150; i++ {
		size := i * 10000

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			slList := make([]*SkipList, b.N)

			for i := 0; i < b.N; i++ {
				slList[i] = New(1)
			}

			keys := make([][]byte, size)
			values := make([][]byte, size)

			for j := 0; j < size; j++ {
				keys[j] = stringToBytes(fmt.Sprintf("key-%d", j))
				values[j] = stringToBytes(fmt.Sprintf("value-%d", j))

				for i := 0; i < b.N; i++ {
					_ = slList[i].Put(keys[j], values[j])
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				for k := size - 1; k >= 0; k-- {
					key := keys[k]
					if err := slList[j].Delete(key); err != nil {
						b.Fatalf("Delete: %v", err)
					}
				}
			}
		})
	}
}
