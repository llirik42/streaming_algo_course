package sstable

import (
	"crypto/md5"
	"fmt"
	"hash"
)

func createChecksumHash() hash.Hash {
	return md5.New()
}

func updateChecksum(key []byte, value []byte, checksumHash hash.Hash) error {
	var n int
	var err error

	n, err = checksumHash.Write(key)
	if err != nil {
		return fmt.Errorf("sstable writeRecord: write key for hash: %w", err)
	}
	if n < len(key) {
		return fmt.Errorf("sstable writeRecord: write key for hash: %d < %d", n, len(key))
	}

	n, err = checksumHash.Write(value)
	if err != nil {
		return fmt.Errorf("sstable writeRecord: write value for hash: %w", err)
	}
	if n < len(value) {
		return fmt.Errorf("sstable writeRecord: write value for hash: %d < %d", n, len(value))
	}

	return nil
}

func calculateChecksum(checksumHash hash.Hash) []byte {
	return checksumHash.Sum(nil)
}
