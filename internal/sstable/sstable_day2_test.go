package sstable

import (
	"bytes"
	"math/rand"
	"testing"

	. "kvschool/internal/helpers"
	. "kvschool/internal/testutil"
)

func TestSSTable_EmptyWriterReader(t *testing.T) {
	buffer := bytes.NewBuffer(nil)

	writer := NewWriter(buffer)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
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

func TestSSTable_SimpleWriterReader(t *testing.T) {
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
			t.Errorf("iterator.Next() returned ok=%t", ok)
		}

		if !KeysEqual(key, expectedKeys[i]) {
			t.Errorf("iterator.Next() returned key=%q; want %q", key, expectedKeys[i])
		}
		if !bytes.Equal(value, expectedValues[i]) {
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
		if !bytes.Equal(value, expectedValues[i]) {
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
