////go:build day2

package lsmstore

import (
	"bytes"
	"context"
	"errors"
	. "kvschool/internal/helpers"
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
		t.Fatalf("store Get failed: %v", err)
	}

	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("store Get failed: expected %s, got %s", expectedValue, value)
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
		t.Fatalf("iterator Close failed: %v", err)
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
		t.Fatalf("iterator Next(): expected %s, got %s", expectedKey, pair.Key)
	}
	if !bytes.Equal(pair.Value, expectedValue) {
		t.Fatalf("iterator Next(): expected %s, got %s", expectedValue, pair.Value)
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

func TestLSMStore_Empty(t *testing.T) {
	data, dir := initTest(t)

	testActions := func(store *Store) {
		getNotFound(data, store, StringToBytes("some key"))
		deleteSuccess(data, store, StringToBytes("some key"))

		iterator := scanSuccess(data, store, nil, nil)

		// Потому что 13 - несчастливое число
		for i := 0; i < 13; i++ {
			nextEmpty(data, iterator)
		}
		closeIteratorSuccess(data, iterator)
	}

	// Хранилище до краша
	s1 := openStoreSuccess(data, dir)
	testActions(s1)

	//
	// КРАШ
	//

	// Хранилище после краша
	s2 := openStoreSuccess(data, dir)
	testActions(s2)
}

func TestLSMStore_SingleKey(t *testing.T) {
	data, dir := initTest(t)

	key := StringToBytes("key")
	expectedValue1 := StringToBytes("value")
	expectedValue2 := StringToBytes("value2")
	expectedValue3 := StringToBytes("value3")
	anotherKey := StringToBytes("unknown key")

	// Хранилище до краша

	s1 := openStoreSuccess(data, dir)
	putSuccess(data, s1, key, expectedValue1)
	getSuccess(data, s1, key, expectedValue1)
	it1 := scanSuccess(data, s1, nil, nil)
	nextSuccess(data, it1, key, expectedValue1)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it1)
		getNotFound(data, s1, anotherKey)
	}
	closeIteratorSuccess(data, it1)

	putSuccess(data, s1, key, expectedValue2)
	getSuccess(data, s1, key, expectedValue2)
	it2 := scanSuccess(data, s1, nil, nil)
	nextSuccess(data, it2, key, expectedValue2)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it2)
		getNotFound(data, s1, anotherKey)
	}
	closeIteratorSuccess(data, it2)

	//
	// КРАШ
	//

	// Хранилище после краша

	s2 := openStoreSuccess(data, dir)
	getSuccess(data, s2, key, expectedValue2)
	it3 := scanSuccess(data, s2, nil, nil)
	nextSuccess(data, it3, key, expectedValue2)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it3)
		getNotFound(data, s2, anotherKey)
	}
	closeIteratorSuccess(data, it3)

	putSuccess(data, s2, key, expectedValue3)
	getSuccess(data, s2, key, expectedValue3)
	it4 := scanSuccess(data, s2, nil, nil)
	nextSuccess(data, it4, key, expectedValue3)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it4)
		getNotFound(data, s2, anotherKey)
	}
	closeIteratorSuccess(data, it4)
}

func TestLSMStore_SingleKeyDeletion(t *testing.T) {
	data, dir := initTest(t)

	key := StringToBytes("key")
	expectedValue1 := StringToBytes("value")
	expectedValue2 := StringToBytes("value2")
	expectedValue3 := StringToBytes("value3")

	// Хранилище до краша

	s1 := openStoreSuccess(data, dir)
	putSuccess(data, s1, key, expectedValue1)
	getSuccess(data, s1, key, expectedValue1)
	it1 := scanSuccess(data, s1, nil, nil)
	nextSuccess(data, it1, key, expectedValue1)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it1)
	}
	closeIteratorSuccess(data, it1)

	deleteSuccess(data, s1, key)
	getNotFound(data, s1, key)
	it2 := scanSuccess(data, s1, nil, nil)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it2)
	}
	closeIteratorSuccess(data, it2)

	putSuccess(data, s1, key, expectedValue2)
	getSuccess(data, s1, key, expectedValue2)
	it3 := scanSuccess(data, s1, nil, nil)
	nextSuccess(data, it3, key, expectedValue2)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it1)
	}
	closeIteratorSuccess(data, it3)

	deleteSuccess(data, s1, key)
	getNotFound(data, s1, key)
	it4 := scanSuccess(data, s1, nil, nil)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it4)
	}
	closeIteratorSuccess(data, it4)

	//
	// КРАШ
	//

	// Хранилище после краша

	s2 := openStoreSuccess(data, dir)

	getNotFound(data, s2, key)
	it5 := scanSuccess(data, s2, nil, nil)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it5)
	}
	closeIteratorSuccess(data, it5)

	putSuccess(data, s2, key, expectedValue3)
	getSuccess(data, s2, key, expectedValue3)
	it6 := scanSuccess(data, s2, nil, nil)
	nextSuccess(data, it6, key, expectedValue3)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it6)
	}
	closeIteratorSuccess(data, it6)

	deleteSuccess(data, s2, key)
	getNotFound(data, s2, key)
	it7 := scanSuccess(data, s2, nil, nil)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it7)
	}
	closeIteratorSuccess(data, it7)
}

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
