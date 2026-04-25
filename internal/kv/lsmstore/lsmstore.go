package lsmstore

import (
	"context"
	"errors"
	"fmt"
	"kvschool/internal/iterator"
	"kvschool/internal/kv"
	"kvschool/internal/lsm"
)

// ErrNotImplemented используется в заготовке практики второго дня.
var ErrNotImplemented = errors.New("lsmstore: функция не реализована")

const (
	MemtableFlushThreshold = 25
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

// Store — KV поверх LSM.
// В практической реализации вам нужно использовать пакеты internal/lsm, internal/sstable, internal/wal.
type Store struct {
	engine *lsm.Engine
}

type Options struct {
	Dir string
}

func Open(options Options) (*Store, error) {
	engine, err := lsm.Open(lsm.Options{Dir: options.Dir, MemtableFlushThreshold: MemtableFlushThreshold})
	if err != nil {
		return nil, fmt.Errorf("lsmstore Open: creating LSM engine: %w", err)
	}

	store := &Store{
		engine: engine,
	}

	return store, nil
}

func (s *Store) Put(_ context.Context, key []byte, value []byte) error {
	return s.engine.Put(key, value)
}

func (s *Store) Get(_ context.Context, key []byte) ([]byte, error) {
	return s.engine.Get(key)
}

func (s *Store) Delete(_ context.Context, key []byte) error {
	return s.engine.Delete(key)
}

func (s *Store) Scan(_ context.Context, start []byte, end []byte) (kv.Iterator, error) {
	lsmIterator, err := s.engine.Scan(start, end)
	if err != nil {
		return nil, err
	}

	return &Iterator{lsmIterator: lsmIterator}, nil
}

func (s *Store) Close() error {
	return s.engine.Close()
}

var _ kv.Store = (*Store)(nil)
