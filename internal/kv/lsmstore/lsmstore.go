package lsmstore

import (
	"context"
	"fmt"
	"kvschool/internal/kv"
	"kvschool/internal/lsm"
)

type Store struct {
	engine *lsm.Engine
}

type Options struct {
	Dir string
}

func Open(options Options) (*Store, error) {
	engineOptions := lsm.Options{
		Dir:                    options.Dir,
		MemtableFlushThreshold: 0,
		LevelBase:              2,
		Seed:                   42,
	}

	engine, err := lsm.Open(engineOptions)
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
