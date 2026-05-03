package lsmstore

import (
	"bytes"
	"context"
	"errors"
	"kvschool/internal/kv"
	"kvschool/internal/lsm"
	"os"
	"path/filepath"
	"testing"
)

type testData struct {
	ctx context.Context
	t   *testing.T
}

func createTestDirectory(t *testing.T) string {
	dir := filepath.Join(t.TempDir(), "db")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	return dir
}

func initTest(t *testing.T) (testData, string) {
	return testData{ctx: context.Background(), t: t}, createTestDirectory(t)
}

func putSuccess(data testData, store *Store, key []byte, value []byte) {
	ctx := data.ctx
	t := data.t

	if err := store.Put(ctx, key, value); err != nil {
		t.Fatalf("store Put failed: %v", err)
	}
}

func getSuccess(data testData, store *Store, key, expectedValue []byte) {
	ctx := data.ctx
	t := data.t

	value, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("store Get failed on %s: %v", key, err)
	}

	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("store Get failed on %s: expected %s, got %s", key, expectedValue, value)
	}
}

func getNotFound(data testData, store *Store, key []byte) {
	ctx := data.ctx
	t := data.t

	_, err := store.Get(ctx, key)
	if !errors.Is(err, lsm.ErrNotFound) {
		t.Fatalf("store Get: expected ErrNotFound, got %v", err)
	}
}

func deleteSuccess(data testData, store *Store, key []byte) {
	ctx := data.ctx
	t := data.t

	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("store Delete failed: %v", err)
	}
}

func scanSuccess(data testData, store *Store, start, end []byte) kv.Iterator {
	ctx := data.ctx
	t := data.t

	iterator, err := store.Scan(ctx, start, end)
	if err != nil {
		t.Fatalf("Scan: %v", err)
		return nil
	}

	return iterator
}

func closeIteratorSuccess(data testData, iterator kv.Iterator) {
	t := data.t

	if err := iterator.Close(); err != nil {
		t.Fatalf("iterator close failed: %v", err)
	}
}

func nextSuccess(data testData, iterator kv.Iterator, expectedKey, expectedValue []byte) {
	t := data.t

	pair, ok, err := iterator.Next()
	if err != nil {
		t.Fatalf("iterator Next(): %v", err)
	}
	if !ok {
		t.Fatalf("iterator Next(): expected ok, got not ok")
	}

	if !bytes.Equal(pair.Key, expectedKey) {
		t.Fatalf("iterator Next(): expected key %s, got %s", expectedKey, pair.Key)
	}
	if !bytes.Equal(pair.Value, expectedValue) {
		t.Fatalf("iterator Next(): expected value %s, got %s", expectedValue, pair.Value)
	}
}

func nextEmpty(data testData, iterator kv.Iterator) {
	t := data.t

	_, ok, err := iterator.Next()
	if err != nil {
		t.Fatalf("iterator Next(): %v", err)
	}
	if ok {
		t.Fatalf("iterator Next(): expected not ok, got ok")
	}
}

func openStoreSuccess(data testData, dir string) *Store {
	t := data.t

	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
		return nil
	}
	return store
}
