package lsmstore

import (
	"context"
	"errors"
	"kvschool/internal/skiplist"

	"kvschool/internal/kv"
)

// ErrNotImplemented используется в заготовке практики второго дня.
var ErrNotImplemented = errors.New("lsmstore: функция не реализована")

// Store — KV поверх LSM.
// В практической реализации вам нужно использовать пакеты internal/lsm, internal/sstable, internal/wal.
type Store struct {
	l0 skiplist.SkipList
}

type Options struct {
	Dir string
}

func Open(_ Options) (*Store, error) {
	// TODO: здесь нужно читать WAL (если есть)

	return nil, ErrNotImplemented
}

func (s *Store) Put(_ context.Context, _ []byte, _ []byte) error { return ErrNotImplemented }

func (s *Store) Get(_ context.Context, _ []byte) ([]byte, error) { return nil, ErrNotImplemented }

func (s *Store) Delete(_ context.Context, _ []byte) error { return ErrNotImplemented }

func (s *Store) Scan(_ context.Context, _ []byte, _ []byte) (kv.Iterator, error) {
	return nil, ErrNotImplemented
}

func (s *Store) Close() error { return ErrNotImplemented }

var _ kv.Store = (*Store)(nil)
