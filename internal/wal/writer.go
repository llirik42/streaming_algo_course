package wal

import (
	"encoding/binary"
	"fmt"
	"hash"
	"io"
)

const (
	NumberOfRecordInChecksum int = 100
)

var ByteOrder binary.ByteOrder = binary.LittleEndian

type Writer struct {
	ioWriter                     io.Writer
	currentChecksum              hash.Hash
	currentChecksumRecordsNumber int
}

func NewWriter(ioWriter io.Writer) *Writer {
	writer := &Writer{ioWriter: ioWriter}
	writer.resetChecksum()
	return writer
}

func (w *Writer) Append(record Record) error {
	if err := w.writeRecord(record); err != nil {
		return fmt.Errorf("wal Append: writing record: %w", err)
	}
	if err := updateChecksum(record, w.currentChecksum); err != nil {
		return fmt.Errorf("wal Append: updating checksum: %w", err)
	}

	w.currentChecksumRecordsNumber++
	if w.currentChecksumRecordsNumber == NumberOfRecordInChecksum {
		if err := w.writeChecksum(); err != nil {
			return fmt.Errorf("wal Append: writing checksum: %w", err)
		}
		w.resetChecksum()
	}

	return nil
}

func (w *Writer) Close() error {
	recordsToAlign := NumberOfRecordInChecksum - w.currentChecksumRecordsNumber
	for i := 0; i < recordsToAlign; i++ {
		if err := w.Append(createGuardRecord()); err != nil {
			return fmt.Errorf("wal Close: writing alignment record: %w", err)
		}
	}

	return nil
}

func (w *Writer) writeRecord(record Record) error {
	typeBytes := make([]byte, 1)
	typeBytes[0] = byte(record.Type)

	if err := w.writeBytes(typeBytes); err != nil {
		return fmt.Errorf("wal writeRecord: write type %w", err)
	}

	if err := w.writeBytesWithLength(record.Key); err != nil {
		return fmt.Errorf("wal writeRecord: write key: %w", err)
	}

	if record.Type == OpPut {
		if err := w.writeBytesWithLength(record.Value); err != nil {
			return fmt.Errorf("wal writeRecord: write value %w", err)
		}
	}

	return nil
}

func (w *Writer) writeChecksum() error {
	checksum := calculateChecksum(w.currentChecksum)

	if err := w.writeBytesWithLength(checksum); err != nil {
		return fmt.Errorf("wal writeChecksum: %w", err)
	}

	return nil
}

func (w *Writer) writeBytesWithLength(buffer []byte) error {
	length := len(buffer)

	if err := w.writeUint32(uint32(length)); err != nil {
		return fmt.Errorf("wal writeBytesWithLength: length: %w", err)
	}
	if err := w.writeBytes(buffer); err != nil {
		return fmt.Errorf("wal writeBytesWithLength: buffer: %w", err)
	}

	return nil
}

func (w *Writer) writeUint32(value uint32) error {
	if err := binary.Write(w.ioWriter, ByteOrder, value); err != nil {
		return fmt.Errorf("wal writeUint32: %w", err)
	}

	return nil
}

func (w *Writer) writeBytes(buffer []byte) error {
	length := len(buffer)
	n, err := w.ioWriter.Write(buffer)

	if err != nil {
		return fmt.Errorf("wal writeBytes: %w", err)
	}

	if n < length {
		return fmt.Errorf("wal writeBytes: %d < %d", n, length)
	}

	return nil
}

func (w *Writer) resetChecksum() {
	w.currentChecksum = createChecksumHash()
	w.currentChecksumRecordsNumber = 0
}
