//go:build day2

package wal

import (
	"bytes"
	"fmt"
	. "kvschool/internal/helpers"
	"testing"
)

func recordsEqual(r1, r2 *Record) bool {
	if r1.Type != r2.Type {
		return false
	}

	if !KeysEqual(r1.Key, r2.Key) {
		return false
	}

	if r1.Type == OpPut {
		if !bytes.Equal(r1.Value, r2.Value) {
			return false
		}
	}

	return true
}

func TestSSTable_EmptyLog(t *testing.T) {
	buffer := bytes.NewBuffer(nil)

	writer := NewWriter(buffer)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err := reader.ValidateChecksums(); err != nil {
		t.Fatal(err)
	}

	iterator := reader.Iterator()
	defer iterator.Close()

	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		_, ok, _ := iterator.Next()
		if ok {
			t.Fatalf("Expected not ok, got ok")
		}
	}
}

func TestSSTable_SingleRecordDelete(t *testing.T) {
	expectedRecord := Record{
		Type: OpDelete,
		Key:  []byte("key"),
	}

	buffer := bytes.NewBuffer(nil)

	writer := NewWriter(buffer)
	if err := writer.Append(expectedRecord); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err := reader.ValidateChecksums(); err != nil {
		t.Fatal(err)
	}

	iterator := reader.Iterator()
	defer iterator.Close()

	record, ok, err := iterator.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Expected ok, got not ok")
	}
	if !recordsEqual(&record, &expectedRecord) {
		t.Fatalf("Expected %v, got %v", expectedRecord, record)
	}

	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		_, ok, _ := iterator.Next()
		if ok {
			t.Fatalf("Expected not ok, got ok")
		}
	}
}

func TestSSTable_SingleRecordPut(t *testing.T) {
	expectedRecord := Record{
		Type:  OpPut,
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	buffer := bytes.NewBuffer(nil)

	writer := NewWriter(buffer)
	if err := writer.Append(expectedRecord); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err := reader.ValidateChecksums(); err != nil {
		t.Fatal(err)
	}

	iterator := reader.Iterator()
	defer iterator.Close()

	record, ok, err := iterator.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Expected ok, got not ok")
	}
	if !recordsEqual(&record, &expectedRecord) {
		t.Fatalf("Expected %v, got %v", expectedRecord, record)
	}

	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		_, ok, _ := iterator.Next()
		if ok {
			t.Fatalf("Expected not ok, got ok")
		}
	}
}

func TestSSTable_SmallNumberOfRecords(t *testing.T) {
	expectedRecords := []Record{
		{
			Type:  OpPut,
			Key:   []byte("key-1"),
			Value: []byte("value-1"),
		},
		{
			Type:  OpPut,
			Key:   []byte("key-1"),
			Value: []byte("value-1"),
		},
		{
			Type:  OpPut,
			Key:   []byte("key-2"),
			Value: []byte("value-1"),
		},
		{
			Type:  OpPut,
			Key:   []byte("key-1"),
			Value: []byte("value-1"),
		},
		{
			Type: OpDelete,
			Key:  []byte("unknown key"),
		},
		{
			Type: OpDelete,
			Key:  []byte("key-1"),
		},
		{
			Type:  OpPut,
			Key:   []byte("new key"),
			Value: []byte("new key value"),
		},
	}

	buffer := bytes.NewBuffer(nil)

	writer := NewWriter(buffer)
	for _, r := range expectedRecords {
		if err := writer.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err := reader.ValidateChecksums(); err != nil {
		t.Fatal(err)
	}

	iterator := reader.Iterator()
	defer iterator.Close()

	for _, expectedRecord := range expectedRecords {
		readRecord, ok, err := iterator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("Expected ok, got not ok")
		}

		if !recordsEqual(&readRecord, &expectedRecord) {
		}
	}

	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		_, ok, _ := iterator.Next()
		if ok {
			t.Fatalf("Expected not ok, got ok")
		}
	}
}

func TestSSTable_LargeNumberOfRecords(t *testing.T) {
	n := 1000000

	expectedRecords := make([]Record, n)
	for i := 0; i < n; i++ {
		var opType OpType
		if i%10 == 0 {
			opType = OpDelete
		} else {
			opType = OpPut
		}

		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		if opType == OpDelete {
			expectedRecords[i] = Record{Type: opType, Key: key}
		} else {
			expectedRecords[i] = Record{Type: opType, Key: key, Value: value}
		}
	}

	buffer := bytes.NewBuffer(nil)

	writer := NewWriter(buffer)
	for _, r := range expectedRecords {
		if err := writer.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err := reader.ValidateChecksums(); err != nil {
		t.Fatal(err)
	}

	iterator := reader.Iterator()
	defer iterator.Close()

	for _, expectedRecord := range expectedRecords {
		readRecord, ok, err := iterator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("Expected ok, got not ok")
		}

		if !recordsEqual(&readRecord, &expectedRecord) {
		}
	}

	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		_, ok, _ := iterator.Next()
		if ok {
			t.Fatalf("Expected not ok, got ok")
		}
	}
}
