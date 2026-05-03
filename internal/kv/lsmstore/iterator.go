package lsmstore

import (
	"kvschool/internal/iterator"
	"kvschool/internal/kv"
)

type Iterator struct {
	lsmIterator iterator.Iterator
}

func (it *Iterator) Next() (kv.Pair, bool, error) {
	key, value, ok, err := it.lsmIterator.Next()
	pair := kv.Pair{Key: key, Value: value}
	return pair, ok, err
}

func (it *Iterator) Close() error {
	return it.lsmIterator.Close()
}
