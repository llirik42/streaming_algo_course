package wal

type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

type Record struct {
	Type  OpType
	Key   []byte
	Value []byte
}
