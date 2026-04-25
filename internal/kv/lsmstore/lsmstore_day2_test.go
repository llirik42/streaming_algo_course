////go:build day2

package lsmstore

import (
	"context"
	"errors"
	. "kvschool/internal/helpers"
	"kvschool/internal/lsm"
	"os"
	"path/filepath"
	"testing"
)

func createTestDirectory(t *testing.T) string {
	dir := filepath.Join(t.TempDir(), "db")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	return dir
}

func TestLSMStore_Empty(t *testing.T) {
	ctx := context.Background()
	dir := createTestDirectory(t)

	// Хранилище до краша
	s1, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, err = s1.Get(ctx, StringToBytes("unknown key"))
	if !errors.Is(lsm.ErrNotFound, err) {
		t.Fatalf("Expected ErrNotFound, got %v", err)
	}
	if err := s1.Delete(ctx, StringToBytes("Some key")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Случается краш

	// Хранилище
	//s2, err := Open(Options{Dir: dir})
	//if err != nil {
	//	t.Fatalf("Open2: %v", err)
	//}
	//
	//_, err = s2.Get(ctx, StringToBytes("key-1"))
	//if !errors.Is(err, lsm.ErrNotFound) {
	//	t.Fatalf("Expected ErrNotFound, got %v", err)
	//}
	//_, err = s2.Get(ctx, StringToBytes("another key"))
	//if !errors.Is(err, lsm.ErrNotFound) {
	//	t.Fatalf("Expected ErrNotFound, got %v", err)
	//}
	//if err := s2.Delete(ctx, StringToBytes("Some key")); err != nil {
	//	t.Fatalf("Delete: %v", err)
	//}
	//iterator, err := s2.Scan(ctx, nil, nil)
	//if err != nil {
	//	t.Fatalf("Scan: %v", err)
	//}
	//
	//// Потому что 13 - несчастливое число
	//for i := 0; i < 13; i++ {
	//	_, ok, err := iterator.Next()
	//	if err != nil {
	//		t.Fatalf("Next: %v", err)
	//	}
	//	if ok {
	//		t.Fatalf("Expected not ok, got ok")
	//	}
	//}
	//if err := iterator.Close(); err != nil {
	//	t.Fatalf("iterator.Close: %v", err)
	//}
	//
	//if err := s2.Close(); err != nil {
	//	t.Fatalf("Close: %v", err)
	//}
}

//
//func TestLSMStore_SingleKeyWithoutDelete(t *testing.T) {
//	ctx := context.Background()
//	dir := createTestDirectory(t)
//	key := StringToBytes("key")
//	anotherKey = StringToBytes("unknown key")
//
//	expectedValue1 := StringToBytes("value")
//	expectedValue2 := StringToBytes("value2")
//	expectedValue3 := StringToBytes("value3")
//
//	s1, err := Open(Options{Dir: dir})
//	if err != nil {
//		t.Fatalf("Open: %v", err)
//	}
//	if err := s1.Put(ctx, key, expectedValue1); err != nil {
//		t.Fatalf("Put: %v", err)
//	}
//	if s1.Get()
//
//
//
//
//
//
//	value1, err := s1.Get(ctx, key)
//
//
//
//
//	if err := s1.Close(); err != nil {
//		t.Fatalf("Close: %v", err)
//	}
//
//	s2, err := Open(Options{Dir: dir})
//	if err != nil {
//		t.Fatalf("Open2: %v", err)
//	}
//
//	_, err = s2.Get(ctx, StringToBytes("key-1"))
//	if !errors.Is(err, lsm.ErrNotFound) {
//		t.Fatalf("Expected ErrNotFound, got %v", err)
//	}
//	_, err = s2.Get(ctx, StringToBytes("another key"))
//	if !errors.Is(err, lsm.ErrNotFound) {
//		t.Fatalf("Expected ErrNotFound, got %v", err)
//	}
//	if err := s2.Delete(ctx, StringToBytes("Some key")); err != nil {
//		t.Fatalf("Delete: %v", err)
//	}
//	iterator, err := s2.Scan(ctx, nil, nil)
//	if err != nil {
//		t.Fatalf("Scan: %v", err)
//	}
//
//	// Потому что 13 - несчастливое число
//	for i := 0; i < 13; i++ {
//		_, ok, err := iterator.Next()
//		if err != nil {
//			t.Fatalf("Next: %v", err)
//		}
//		if ok {
//			t.Fatalf("Expected not ok, got ok")
//		}
//	}
//	if err := iterator.Close(); err != nil {
//		t.Fatalf("iterator.Close: %v", err)
//	}
//
//	if err := s2.Close(); err != nil {
//		t.Fatalf("Close: %v", err)
//	}
//}

//func TestLSMStore_PersistAcrossRestart(t *testing.T) {
//	_ = context.Background()
//	dir := filepath.Join(t.TempDir(), "db")
//	if err := os.MkdirAll(dir, 0o755); err != nil {
//		t.Fatalf("MkdirAll: %v", err)
//	}
//
//	s, err := Open(Options{Dir: dir})
//	if err != nil {
//		t.Fatalf("Open: %v", err)
//	}
//	if err := s.Put(ctx, []byte("a"), []byte("1")); err != nil {
//		t.Fatalf("Put: %v", err)
//	}
//	if err := s.Close(); err != nil {
//		t.Fatalf("Close: %v", err)
//	}
//
//	s2, err := Open(Options{Dir: dir})
//	if err != nil {
//		t.Fatalf("Open2: %v", err)
//	}
//	defer s2.Close()
//
//	got, err := s2.Get(ctx, []byte("a"))
//	if err != nil {
//		t.Fatalf("Get: %v", err)
//	}
//	if string(got) != "1" {
//		t.Fatalf("value mismatch: got=%q want=%q", string(got), "1")
//	}
//}
