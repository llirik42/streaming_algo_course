package wal

import (
	"bytes"
	"fmt"
	"hash"
	"io"
)

type Reader struct {
	ioReader                   io.ReaderAt
	validateChecksum           bool
	offset                     int64
	currentChecksum            hash.Hash
	currentChecksumRecordsRead int
	currentChecksumRecords     []Record
	lastChecksum               []byte
	totalSize                  int64
}

func NewReader(ioReader io.ReaderAt, totalSize int64) *Reader {
	return &Reader{ioReader: ioReader, totalSize: totalSize}
}

func (r *Reader) Next() (Record, bool, error) {
	record, err := r.readRecord()
	if err != nil {
		return Record{}, false, fmt.Errorf("wal Next: read record: %w", err)
	}

	r.currentChecksumRecordsRead++
	if r.currentChecksumRecordsRead == NumberOfRecordInChecksum {
		r.currentChecksumRecordsRead = 0
		if err := r.readChecksum(); err != nil {
			return Record{}, false, fmt.Errorf("wal Next: read checksum: %w", err)
		}
	}

	return record, true, nil
}

func (r *Reader) ValidateChecksums() error {
	previousOffset := r.offset
	r.offset = 0

	for checksumIndex := 0; r.offset < r.totalSize; checksumIndex++ {
		checksumHash := createChecksumHash()

		for recordIndex := 0; recordIndex < NumberOfRecordInChecksum; recordIndex++ {
			record, ok, err := r.Next()
			if err != nil {
				return fmt.Errorf("wal ValidateChecksums: read record %d: %w", recordIndex, err)
			}
			if !ok {
				return fmt.Errorf("wal ValidateChecksums: read record got not ok, expected ok")
			}
			if err := updateChecksum(record, checksumHash); err != nil {
				return fmt.Errorf("wal ValidateChecksums: update checksum: %w", err)
			}
		}

		if !bytes.Equal(calculateChecksum(checksumHash), r.lastChecksum) {
			return fmt.Errorf("wal ValidateChecksums: mismatch on checksum %d", checksumIndex)
		}
	}

	r.offset = previousOffset

	return nil
}

func (r *Reader) readRecord() (Record, error) {
	recordTypeBytes, err := r.readBytes(1)
	if err != nil {
		return Record{}, fmt.Errorf("wal readRecord: reading record type: %v", err)
	}
	recordType := OpType(recordTypeBytes[0])

	key, err := r.readBytesWithLength()
	if err != nil {
		return Record{}, fmt.Errorf("wal readRecord: reading record key: %v", err)
	}

	var value []byte
	if recordType == OpPut {
		value, err = r.readBytesWithLength()
		if err != nil {
			return Record{}, fmt.Errorf("wal readRecord: reading record value: %v", err)
		}
	}

	return Record{
		Type:  recordType,
		Key:   key,
		Value: value,
	}, nil
}

func (r *Reader) readChecksum() error {
	checksum, err := r.readBytesWithLength()
	if err != nil {
		return fmt.Errorf("wal readChecksum: %v", err)
	}

	r.lastChecksum = checksum

	return nil
}

func (r *Reader) readBytesWithLength() ([]byte, error) {
	length, err := r.readUint32()
	if err != nil {
		return nil, fmt.Errorf("wal readBytesWithLength: reading length: %w", err)
	}

	buffer, err := r.readBytes(int(length))
	if err != nil {
		return nil, fmt.Errorf("wal readBytesWithLength: reading buffer: %w", err)
	}

	return buffer, nil
}

func (r *Reader) readUint32() (uint32, error) {
	buffer, err := r.readBytes(4)
	if err != nil {
		return 0, fmt.Errorf("wal readUint32: reading bytes: %w", err)
	}

	return ByteOrder.Uint32(buffer), nil
}

func (r *Reader) readBytes(count int) ([]byte, error) {
	buffer := make([]byte, count)

	n, err := r.ioReader.ReadAt(buffer, r.offset)
	if err != nil {
		return nil, fmt.Errorf("wal readBytes: %w", err)
	}
	if n < count {
		return nil, fmt.Errorf("wal readBytes: %d < %d", n, count)
	}

	r.offset += int64(n)

	return buffer, nil
}
