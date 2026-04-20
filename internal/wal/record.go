package wal

type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
	OpGuard  OpType = 3
)

type Record struct {
	Type  OpType
	Key   []byte
	Value []byte
}

func createGuardRecord() Record {
	return Record{Type: OpGuard}
}

func isGuardRecord(record *Record) bool {
	return record.Type == OpGuard
}
