package main

import (
	"fmt"
	"kvschool/internal/kv"
	"kvschool/internal/kv/memskiplist"
)

func memSkipListDefault() kv.Store {
	// seed=1 чтобы поведение было воспроизводимым в тестах.
	return memskiplist.New(1)
}

func memSkipListProbability(probability float64) (kv.Store, error) {
	store := memskiplist.New(1)
	err := store.GetSkipList().SetProbability(probability)
	if err != nil {
		return nil, fmt.Errorf("create memskiplist: %w", err)
	}

	return store, nil
}
