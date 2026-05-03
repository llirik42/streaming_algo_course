package wal

import (
	"bytes"
	"fmt"
	"io"
)

type Reader struct {
	ioReader  io.ReaderAt
	totalSize int64
}

func NewReader(ioReader io.ReaderAt, totalSize int64) *Reader {
	return &Reader{ioReader: ioReader, totalSize: totalSize}
}

func (r *Reader) Iterator() *Iterator {
	return &Iterator{
		reader:    r,
		endOffset: r.totalSize,
	}
}

func (r *Reader) ValidateChecksums() error {
	var offset int64 = 0

	for offset < r.totalSize {
		checksumHash := createChecksumHash()

		for recordIndex := 0; recordIndex < NumberOfRecordInChecksum; recordIndex++ {
			record, readCount1, err := r.readRecord(offset)
			if err != nil {
				return fmt.Errorf("wal ValidateChecksums: read record: %w", err)
			}
			if err := updateChecksum(record, checksumHash); err != nil {
				return fmt.Errorf("wal ValidateChecksums: update checksum: %w", err)
			}
			offset += readCount1
		}

		realChecksum, readCount2, err := r.readChecksum(offset)
		if err != nil {
			return fmt.Errorf("wal ValidateChecksums: read checksum: %w", err)
		}
		offset += readCount2

		calculatedChecksum := calculateChecksum(checksumHash)

		if !bytes.Equal(calculatedChecksum, realChecksum) {
			return fmt.Errorf("wal ValidateChecksums: mismatch on checksum")
		}
	}

	return nil
}

func (r *Reader) readRecord(offset int64) (Record, int64, error) {
	recordTypeBytes, readCount1, err := r.readBytes(offset, 1)
	if err != nil {
		return Record{}, 0, fmt.Errorf("wal readRecord: reading record type: %v", err)
	}
	recordType := OpType(recordTypeBytes[0])

	key, readCount2, err := r.readBytesWithLength(offset + readCount1)
	if err != nil {
		return Record{}, 0, fmt.Errorf("wal readRecord: reading record key: %v", err)
	}

	readCount := readCount1 + readCount2

	var value []byte
	if recordType == OpPut {
		var readCount3 int64
		value, readCount3, err = r.readBytesWithLength(offset + readCount1 + readCount2)
		if err != nil {
			return Record{}, readCount1 + readCount2 + readCount3, fmt.Errorf("wal readRecord: reading record value: %v", err)
		}
		readCount += readCount3
	}

	return Record{
		Type:  recordType,
		Key:   key,
		Value: value,
	}, readCount, nil
}

func (r *Reader) readChecksum(offset int64) ([]byte, int64, error) {
	checksum, readCount, err := r.readBytesWithLength(offset)
	if err != nil {
		return nil, 0, fmt.Errorf("wal readChecksum: %v", err)
	}

	return checksum, readCount, nil
}

func (r *Reader) readBytesWithLength(offset int64) ([]byte, int64, error) {
	length, readCount1, err := r.readUint32(offset)
	if err != nil {
		return nil, 0, fmt.Errorf("wal readBytesWithLength: reading length: %w", err)
	}

	// offset+4 - учесть прочитанные 4 байта (uint32) длины буфера
	buffer, readCount2, err := r.readBytes(offset+readCount1, int(length))
	if err != nil {
		return nil, 0, fmt.Errorf("wal readBytesWithLength: reading buffer: %w", err)
	}

	return buffer, readCount1 + readCount2, nil
}

func (r *Reader) readUint32(offset int64) (uint32, int64, error) {
	buffer, readCount, err := r.readBytes(offset, 4)
	if err != nil {
		return 0, 0, fmt.Errorf("wal readUint32: reading bytes: %w", err)
	}

	return ByteOrder.Uint32(buffer), readCount, nil
}

func (r *Reader) readBytes(offset int64, count int) ([]byte, int64, error) {
	buffer := make([]byte, count)

	n, err := r.ioReader.ReadAt(buffer, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("wal readBytes: %w", err)
	}
	if n < count {
		return nil, 0, fmt.Errorf("wal readBytes: %d < %d", n, count)
	}

	return buffer, int64(count), nil
}
