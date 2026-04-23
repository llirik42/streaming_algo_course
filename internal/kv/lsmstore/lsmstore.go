package lsmstore

import (
	"context"
	"errors"
	"fmt"
	"kvschool/internal/kv"
	"kvschool/internal/lsm"
)

// ErrNotImplemented используется в заготовке практики второго дня.
var ErrNotImplemented = errors.New("lsmstore: функция не реализована")

// Store — KV поверх LSM.
// В практической реализации вам нужно использовать пакеты internal/lsm, internal/sstable, internal/wal.
type Store struct {
	engine *lsm.Engine
}

type Options struct {
	Dir string
}

func Open(options Options) (*Store, error) {
	engine, err := lsm.Open(lsm.Options{Dir: options.Dir, MemtableFlushThreshold: 100000})
	if err != nil {
		return nil, fmt.Errorf("lsmstore Open: creating LSM engine: %w", err)
	}

	store := &Store{
		engine: engine,
	}

	return store, nil
}

func (s *Store) Put(_ context.Context, _ []byte, _ []byte) error { return ErrNotImplemented }

func (s *Store) Get(_ context.Context, _ []byte) ([]byte, error) { return nil, ErrNotImplemented }

func (s *Store) Delete(_ context.Context, _ []byte) error { return ErrNotImplemented }

func (s *Store) Scan(_ context.Context, _ []byte, _ []byte) (kv.Iterator, error) {
	return nil, ErrNotImplemented
}

func (s *Store) Close() error { return ErrNotImplemented }

var _ kv.Store = (*Store)(nil)
