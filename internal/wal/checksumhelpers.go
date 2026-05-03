package wal

import (
	"crypto/md5"
	"fmt"
	"hash"
)

func createChecksumHash() hash.Hash {
	return md5.New()
}

func updateChecksum(record Record, checksumHash hash.Hash) error {
	typeBytes := make([]byte, 1)
	typeBytes[0] = byte(record.Type)

	if err := updateChecksumFromBuffer(typeBytes, checksumHash); err != nil {
		return fmt.Errorf("updateChecksum: updating checksum by type %w", err)
	}

	if err := updateChecksumFromBuffer(record.Key, checksumHash); err != nil {
		return fmt.Errorf("wal updateChecksum: updating checksum by key %w", err)
	}

	if record.Type == OpPut {
		if err := updateChecksumFromBuffer(record.Value, checksumHash); err != nil {
			return fmt.Errorf("wal updateChecksum: updating checksum by value %w", err)
		}
	}

	return nil
}

func updateChecksumFromBuffer(buffer []byte, checksumHash hash.Hash) error {
	var n int
	var err error

	n, err = checksumHash.Write(buffer)
	if err != nil {
		return fmt.Errorf("wal updateChecksumFromBuffer: %w", err)
	}
	if n < len(buffer) {
		return fmt.Errorf("wal updateChecksumFromBuffer: %d < %d", n, len(buffer))
	}

	return nil
}

func calculateChecksum(checksumHash hash.Hash) []byte {
	return checksumHash.Sum(nil)
}
