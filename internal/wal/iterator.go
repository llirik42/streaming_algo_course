package wal

import "fmt"

type Iterator struct {
	reader                     *Reader
	offset                     int64
	endOffset                  int64
	currentChecksumRecordsRead int
}

func (it *Iterator) Next() (record Record, ok bool, err error) {
	if it.offset == it.endOffset {
		return Record{}, false, nil
	}

	record, readCount1, err := it.reader.readRecord(it.offset)
	if err != nil {
		return Record{}, false, fmt.Errorf("wal Next: read record: %w", err)
	}
	if isGuardRecord(&record) {
		it.Close()
		return Record{}, false, nil
	}

	it.offset += readCount1
	it.currentChecksumRecordsRead++

	if it.currentChecksumRecordsRead == NumberOfRecordInChecksum {
		it.currentChecksumRecordsRead = 0
		_, readCount2, err := it.reader.readChecksum(it.offset)
		if err != nil {
			return Record{}, false, fmt.Errorf("wal Next: read checksum: %w", err)
		}
		it.offset += readCount2
	}

	return record, true, nil
}

func (it *Iterator) Close() {
	it.offset = it.endOffset
}
