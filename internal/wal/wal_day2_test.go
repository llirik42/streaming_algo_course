package wal

import (
	"bytes"
	"testing"
)

func TestSSTable_EmptyWriterReader(t *testing.T) {
	buffer := bytes.NewBuffer(nil)

	writer := NewWriter(buffer)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err := reader.ValidateChecksums(); err != nil {
		t.Fatal(err)
	}

	//iterator, err := reader.Iterator(nil, nil)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//_, _, ok, _ := iterator.Next()
	//if ok {
	//	t.Errorf("iterator.Next() returned ok=%t", ok)
	//}
	//
	//// На всякий случай
	//_, _, ok, _ = iterator.Next()
	//if ok {
	//	t.Errorf("iterator.Next() returned ok=%t", ok)
	//}
}
