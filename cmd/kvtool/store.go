package main

import (
	"kvschool/internal/kv"
	"kvschool/internal/kv/memskiplist"
)

func memSkipListDefault() kv.Store {
	// seed=1 чтобы поведение было воспроизводимым в тестах.
	return memskiplist.New(1)
}
