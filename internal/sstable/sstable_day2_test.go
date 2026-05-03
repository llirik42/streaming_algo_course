//go:build day2

package sstable

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	. "kvschool/internal/helpers"
	. "kvschool/internal/testutil"
)

func TestSSTable_Empty(t *testing.T) {
	buffer := bytes.NewBuffer(nil)

	writer := NewWriter(buffer)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.ValidateChecksum(); err != nil {
		t.Fatal(err)
	}

	iterator, err := reader.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, _, ok, _ := iterator.Next()
	if ok {
		t.Errorf("iterator.Next() returned ok=%t", ok)
	}

	// На всякий случай
	_, _, ok, _ = iterator.Next()
	if ok {
		t.Errorf("iterator.Next() returned ok=%t", ok)
	}
}

func TestSSTable_Simple(t *testing.T) {
	buffer := bytes.NewBuffer(nil)

	writer := NewWriter(buffer)

	n := 1000
	random := rand.New(rand.NewSource(0))
	expectedKeys := make([][]byte, n)
	expectedValues := make([][]byte, n)
	for i := 0; i < n; i++ {
		expectedKeys[i] = StringToBytes(GenerateRandomString(48, random))
		expectedValues[i] = StringToBytes(GenerateRandomString(128, random))
		if err := writer.Add(expectedKeys[i], expectedValues[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.ValidateChecksum(); err != nil {
		t.Fatal(err)
	}

	iterator, err := reader.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < n; i++ {
		key, value, ok, err := iterator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("iterator.Next() %d returned ok=%t", i, ok)
		}

		if !KeysEqual(key, expectedKeys[i]) {
			t.Errorf("iterator.Next() returned key=%q; want %q", key, expectedKeys[i])
		}
		if !ValuesEqual(value, expectedValues[i]) {
			t.Errorf("iterator.Next() returned value=%q; want %q", value, expectedValues[i])
		}
	}

	_, _, ok, _ := iterator.Next()
	if ok {
		t.Errorf("iterator.Next() returned ok=%t", ok)
	}

	// На всякий случай
	_, _, ok, _ = iterator.Next()
	if ok {
		t.Errorf("iterator.Next() returned ok=%t", ok)
	}
}

func TestSSTable_IteratorClose(t *testing.T) {
	buffer := bytes.NewBuffer(nil)

	writer := NewWriter(buffer)

	n := 100
	random := rand.New(rand.NewSource(0))
	expectedKeys := make([][]byte, n)
	expectedValues := make([][]byte, n)
	for i := 0; i < n; i++ {
		expectedKeys[i] = StringToBytes(GenerateRandomString(48, random))
		expectedValues[i] = StringToBytes(GenerateRandomString(128, random))
		if err := writer.Add(expectedKeys[i], expectedValues[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.ValidateChecksum(); err != nil {
		t.Fatal(err)
	}

	iterator, err := reader.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	nToRead := n / 2
	for i := 0; i < nToRead; i++ {
		key, value, ok, err := iterator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("iterator.Next() returned ok=%t", ok)
		}

		if !KeysEqual(key, expectedKeys[i]) {
			t.Errorf("iterator.Next() returned key=%q; want %q", key, expectedKeys[i])
		}
		if !ValuesEqual(value, expectedValues[i]) {
			t.Errorf("iterator.Next() returned value=%q; want %q", value, expectedValues[i])
		}
	}

	if err := iterator.Close(); err != nil {
		t.Fatal(err)
	}

	_, _, ok, _ := iterator.Next()
	if ok {
		t.Errorf("iterator.Next() returned ok=%t", ok)
	}

	// На всякий случай
	_, _, ok, _ = iterator.Next()
	if ok {
		t.Errorf("iterator.Next() returned ok=%t", ok)
	}
}

func TestSSTable_IteratorRangeSmall(t *testing.T) {
	buffer := bytes.NewBuffer(nil)

	writer := NewWriter(buffer)

	for i := 300; i <= 800; i += 10 {
		key := StringToBytes(fmt.Sprintf("key-%d", i))
		value := StringToBytes(fmt.Sprintf("value-%d", i))
		if err := writer.Add(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.ValidateChecksum(); err != nil {
		t.Fatal(err)
	}

	cases := []TestCase{
		{nil, nil, 300, 800},
		{StringToBytes(fmt.Sprintf("key-300")), nil, 300, 800},
		{StringToBytes(fmt.Sprintf("key-299")), nil, 300, 800},
		{StringToBytes(fmt.Sprintf("key-301")), nil, 310, 800},
		{nil, StringToBytes(fmt.Sprintf("key-800")), 300, 790},
		{nil, StringToBytes(fmt.Sprintf("key-801")), 300, 800},
		{nil, StringToBytes(fmt.Sprintf("key-799")), 300, 790},
		{StringToBytes(fmt.Sprintf("key-300")), StringToBytes(fmt.Sprintf("key-800")), 300, 790},
		{StringToBytes(fmt.Sprintf("key-400")), StringToBytes(fmt.Sprintf("key-700")), 400, 690},
	}

	for i, c := range cases {
		startKey := c.StartKey
		endKey := c.EndKey
		startNumber := c.StartNumber
		endNumber := c.EndNumber

		it, err := reader.Iterator(startKey, endKey)
		if err != nil {
			t.Fatalf("Iterator %d: %v", err, i)
		}

		TestIterator(t, it, startNumber, endNumber+1, 10)
	}
}

func TestSSTable_IteratorRangeLarge(t *testing.T) {
	buffer := bytes.NewBuffer(nil)

	writer := NewWriter(buffer)

	for i := 30000; i <= 80000; i += 10 {
		key := StringToBytes(fmt.Sprintf("key-%d", i))
		value := StringToBytes(fmt.Sprintf("value-%d", i))
		if err := writer.Add(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.ValidateChecksum(); err != nil {
		t.Fatal(err)
	}

	cases := []TestCase{
		{nil, nil, 30000, 80000},
		{StringToBytes(fmt.Sprintf("key-30000")), nil, 30000, 80000},
		{StringToBytes(fmt.Sprintf("key-20099")), nil, 30000, 80000},
		{StringToBytes(fmt.Sprintf("key-30001")), nil, 30010, 80000},
		{nil, StringToBytes(fmt.Sprintf("key-80000")), 30000, 79990},
		{nil, StringToBytes(fmt.Sprintf("key-80001")), 30000, 80000},
		{nil, StringToBytes(fmt.Sprintf("key-70099")), 30000, 70090},
		{StringToBytes(fmt.Sprintf("key-30000")), StringToBytes(fmt.Sprintf("key-80000")), 30000, 79990},
		{StringToBytes(fmt.Sprintf("key-40000")), StringToBytes(fmt.Sprintf("key-70000")), 40000, 69990},
	}

	for i, c := range cases {
		startKey := c.StartKey
		endKey := c.EndKey
		startNumber := c.StartNumber
		endNumber := c.EndNumber

		it, err := reader.Iterator(startKey, endKey)
		if err != nil {
			t.Fatalf("Iterator %d: %v", err, i)
		}

		TestIterator(t, it, startNumber, endNumber+1, 10)
	}
}
