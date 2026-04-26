////go:build day2

package lsmstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	. "kvschool/internal/helpers"
	"kvschool/internal/kv"
	"kvschool/internal/lsm"
	. "kvschool/internal/testutil"
	"math/rand"
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

func TestLSMStore_PersistAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open2: %v", err)
	}
	defer s2.Close()

	got, err := s2.Get(ctx, []byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "1" {
		t.Fatalf("value mismatch: got=%q want=%q", string(got), "1")
	}
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
	s1 = nil

	//
	// КРАШ
	//

	// Хранилище после краша
	s2 := openStoreSuccess(data, dir)
	testActions(s2)
	s2 = nil
}

func TestLSMStore_SingleKey(t *testing.T) {
	data, dir := initTest(t)

	key := StringToBytes("key")
	expectedValue1 := StringToBytes("value")
	expectedValue2 := StringToBytes("value2")
	expectedValue3 := StringToBytes("value3")
	unknownKey := StringToBytes("unknown key")

	// Хранилище до краша

	s1 := openStoreSuccess(data, dir)
	putSuccess(data, s1, key, expectedValue1)
	getSuccess(data, s1, key, expectedValue1)
	it1 := scanSuccess(data, s1, nil, nil)
	nextSuccess(data, it1, key, expectedValue1)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it1)
		getNotFound(data, s1, unknownKey)
	}
	closeIteratorSuccess(data, it1)
	it1 = nil
	putSuccess(data, s1, key, expectedValue2)
	getSuccess(data, s1, key, expectedValue2)
	it2 := scanSuccess(data, s1, nil, nil)
	nextSuccess(data, it2, key, expectedValue2)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it2)
		getNotFound(data, s1, unknownKey)
	}
	closeIteratorSuccess(data, it2)
	it1 = nil
	s1 = nil

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
		getNotFound(data, s2, unknownKey)
	}
	closeIteratorSuccess(data, it3)
	it3 = nil
	putSuccess(data, s2, key, expectedValue3)
	getSuccess(data, s2, key, expectedValue3)
	it4 := scanSuccess(data, s2, nil, nil)
	nextSuccess(data, it4, key, expectedValue3)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it4)
		getNotFound(data, s2, unknownKey)
	}
	closeIteratorSuccess(data, it4)
	it4 = nil
	s2 = nil
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
	it1 = nil

	deleteSuccess(data, s1, key)
	getNotFound(data, s1, key)
	it2 := scanSuccess(data, s1, nil, nil)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it2)
	}
	closeIteratorSuccess(data, it2)
	it2 = nil
	putSuccess(data, s1, key, expectedValue2)
	getSuccess(data, s1, key, expectedValue2)
	it3 := scanSuccess(data, s1, nil, nil)
	nextSuccess(data, it3, key, expectedValue2)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it3)
	}
	closeIteratorSuccess(data, it3)
	it3 = nil
	deleteSuccess(data, s1, key)
	getNotFound(data, s1, key)
	it4 := scanSuccess(data, s1, nil, nil)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it4)
	}
	closeIteratorSuccess(data, it4)
	it4 = nil
	s1 = nil

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
	it5 = nil
	putSuccess(data, s2, key, expectedValue3)
	getSuccess(data, s2, key, expectedValue3)
	it6 := scanSuccess(data, s2, nil, nil)
	nextSuccess(data, it6, key, expectedValue3)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it6)
	}
	closeIteratorSuccess(data, it6)
	it6 = nil
	deleteSuccess(data, s2, key)
	getNotFound(data, s2, key)
	it7 := scanSuccess(data, s2, nil, nil)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it7)
	}
	closeIteratorSuccess(data, it7)
	it7 = nil
	s2 = nil
}

func TestLSMStore_MultipleKeysSmall(t *testing.T) {
	data, dir := initTest(t)

	// Хранилище до краша
	s1 := openStoreSuccess(data, dir)

	start := 100
	end := 999

	for i := end; i >= start; i-- {
		key := StringToBytes(fmt.Sprintf("key%d", i))
		value := StringToBytes(fmt.Sprintf("value%d", i))
		putSuccess(data, s1, key, value)
	}
	for i := start; i <= end; i++ {
		key := StringToBytes(fmt.Sprintf("key%d", i))
		expectedValue := StringToBytes(fmt.Sprintf("value%d", i))
		getSuccess(data, s1, key, expectedValue)
	}
	it1 := scanSuccess(data, s1, nil, nil)
	for i := start; i <= end; i++ {
		expectedKey := StringToBytes(fmt.Sprintf("key%d", i))
		expectedValue := StringToBytes(fmt.Sprintf("value%d", i))
		nextSuccess(data, it1, expectedKey, expectedValue)
	}
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it1)
	}
	closeIteratorSuccess(data, it1)
	it1 = nil
	s1 = nil

	//
	// КРАШ
	//

	// Хранилище после краша
	s2 := openStoreSuccess(data, dir)

	for i := start; i <= end; i++ {
		key := StringToBytes(fmt.Sprintf("key%d", i))
		expectedValue := StringToBytes(fmt.Sprintf("value%d", i))
		getSuccess(data, s2, key, expectedValue)
	}
	it2 := scanSuccess(data, s2, nil, nil)
	for i := start; i <= end; i++ {
		expectedKey := StringToBytes(fmt.Sprintf("key%d", i))
		expectedValue := StringToBytes(fmt.Sprintf("value%d", i))
		nextSuccess(data, it2, expectedKey, expectedValue)
	}
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it2)
	}
	closeIteratorSuccess(data, it2)
	it2 = nil
	for i := start; i <= end; i++ {
		key := StringToBytes(fmt.Sprintf("key%d", i))
		deleteSuccess(data, s2, key)
	}
	for i := start; i <= end; i++ {
		key := StringToBytes(fmt.Sprintf("key%d", i))
		getNotFound(data, s2, key)
	}
	it3 := scanSuccess(data, s2, nil, nil)
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it3)
	}
	closeIteratorSuccess(data, it3)
	it3 = nil
	for i := end; i >= start; i-- {
		key := StringToBytes(fmt.Sprintf("key%d", i))
		value := StringToBytes(fmt.Sprintf("value%d", i))
		putSuccess(data, s2, key, value)
	}
	for i := start; i <= end; i++ {
		key := StringToBytes(fmt.Sprintf("key%d", i))
		expectedValue := StringToBytes(fmt.Sprintf("value%d", i))
		getSuccess(data, s2, key, expectedValue)
	}
	it4 := scanSuccess(data, s2, nil, nil)
	for i := start; i <= end; i++ {
		expectedKey := StringToBytes(fmt.Sprintf("key%d", i))
		expectedValue := StringToBytes(fmt.Sprintf("value%d", i))
		nextSuccess(data, it4, expectedKey, expectedValue)
	}
	// Потому что 13 - несчастливое число
	for i := 0; i < 13; i++ {
		nextEmpty(data, it4)
	}
	closeIteratorSuccess(data, it4)
	it4 = nil
	s2 = nil
}

func TestLSMStore_MultipleKeysSmallRange(t *testing.T) {
	data, dir := initTest(t)
	start := 300
	end := 800
	step := 10

	// Хранилище до краша
	s1 := openStoreSuccess(data, dir)

	for i := end; i >= start; i -= step {
		key := StringToBytes(fmt.Sprintf("key-%d", i))
		value := StringToBytes(fmt.Sprintf("value-%d", i))
		putSuccess(data, s1, key, value)
	}

	cases := []TestCase{
		{nil, nil, 300, 800},
		{StringToBytes(fmt.Sprintf("key-300")), nil, 300, 800},
		{StringToBytes(fmt.Sprintf("key-299")), nil, 300, 800},
		{StringToBytes(fmt.Sprintf("key-301")), nil, 310, 800},
		{nil, StringToBytes(fmt.Sprintf("key-800")), 300, 790},
		{nil, StringToBytes(fmt.Sprintf("key-801")), 300, 800},
		{nil, StringToBytes(fmt.Sprintf("key-799")), 300, 790},
		{StringToBytes(fmt.Sprintf("key-300")), StringToBytes(fmt.Sprintf("key-800")), 300, 790},
		{StringToBytes(fmt.Sprintf("key-400")), StringToBytes(fmt.Sprintf("key-700")), 400, 690},
	}

	for _, c := range cases {
		startKey := c.StartKey
		endKey := c.EndKey
		startNumber := c.StartNumber
		endNumber := c.EndNumber
		iterator := scanSuccess(data, s1, startKey, endKey)
		TestKVIterator(t, iterator, startNumber, endNumber+1, step)
		closeIteratorSuccess(data, iterator)
	}

	s1 = nil

	//
	// КРАШ
	//

	// Хранилище после краша
	s2 := openStoreSuccess(data, dir)

	for _, c := range cases {
		startKey := c.StartKey
		endKey := c.EndKey
		startNumber := c.StartNumber
		endNumber := c.EndNumber
		iterator := scanSuccess(data, s2, startKey, endKey)
		TestKVIterator(t, iterator, startNumber, endNumber+1, step)
		closeIteratorSuccess(data, iterator)
	}

	s2 = nil
}

func TestLSMStore_MultipleKeysLarge(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	keysNumber := 100
	actionsNumber := keysNumber * 10
	maxKeyLength := 32
	maxValueLength := 128

	data, dir := initTest(t)

	// Хранилище до краша
	s1 := openStoreSuccess(data, dir)

	pairs := make([]kv.Pair, keysNumber)

	for i := 0; i < keysNumber; i++ {
		for {
			k := StringToBytes(GenerateRandomString(maxKeyLength, rng))

			exists := false
			for _, p := range pairs {
				if bytes.Compare(p.Key, k) == 0 {
					exists = true
					break
				}
			}

			if !exists {
				v := StringToBytes(GenerateRandomString(maxValueLength, rng))
				pairs[i] = kv.Pair{Key: k, Value: v}
				putSuccess(data, s1, k, v)
				break
			}
		}
	}
	for _, p := range pairs {
		getSuccess(data, s1, p.Key, p.Value)
	}
	sortPairs(pairs)
	it1 := scanSuccess(data, s1, nil, nil)
	for i := 0; i < keysNumber; i++ {
		nextSuccess(data, it1, pairs[i].Key, pairs[i].Value)
	}
	for i := 0; i < 13; i++ {
		nextEmpty(data, it1)
	}
	closeIteratorSuccess(data, it1)
	it1 = nil
	s1 = nil

	//
	// КРАШ
	//

	// Хранилище после краша
	s2 := openStoreSuccess(data, dir)

	for _, p := range pairs {
		getSuccess(data, s2, p.Key, p.Value)
	}
	it2 := scanSuccess(data, s2, nil, nil)
	for i := 0; i < keysNumber; i++ {
		nextSuccess(data, it2, pairs[i].Key, pairs[i].Value)
	}
	for i := 0; i < 13; i++ {
		nextEmpty(data, it2)
	}
	closeIteratorSuccess(data, it2)
	it2 = nil

	iterations := 0
	for i := 0; i < actionsNumber && len(pairs) > 0; i++ {
		iterations++
		n := rng.Int()
		module := n % 6
		if module == 0 {
			// Обновление значение существующего ключа
			keyIndex := rng.Intn(len(pairs))
			newValue := StringToBytes(GenerateRandomString(maxValueLength, rng))
			putSuccess(data, s2, pairs[keyIndex].Key, newValue)
			pairs[keyIndex].Value = newValue
		} else if module == 1 {
			// Получаем значение существующего ключа
			keyIndex := rng.Intn(len(pairs))
			getSuccess(data, s2, pairs[keyIndex].Key, pairs[keyIndex].Value)
		} else if module == 2 {
			// Удаляем существующий ключ
			keyIndex := rng.Intn(len(pairs))
			deleteSuccess(data, s2, pairs[keyIndex].Key)
			pairs = append(pairs[:keyIndex], pairs[keyIndex+1:]...)
		} else if module == 3 {
			// Добавление ключа (возможно и обновление существующего)
			k := StringToBytes(GenerateRandomString(maxKeyLength, rng))
			v := StringToBytes(GenerateRandomString(maxValueLength, rng))
			putSuccess(data, s2, k, v)

			// Если ключ уже существует, обновляем срез pairs
			indexOfPair := -1
			for j, p := range pairs {
				if bytes.Compare(p.Key, k) == 0 {
					indexOfPair = j
					break
				}
			}
			if indexOfPair != -1 {
				pairs[indexOfPair].Value = v
			} else {
				pairs = append(pairs, kv.Pair{Key: k, Value: v})
			}
		} else if module == 4 {
			// Получение значение несуществующего ключа
			unknownKey := StringToBytes(GenerateRandomStringFixed(maxKeyLength+1, rng))
			getNotFound(data, s2, unknownKey)
		}
	}

	t.Logf("Actions: %d/%d", iterations, actionsNumber)

	for _, p := range pairs {
		getSuccess(data, s2, p.Key, p.Value)
	}
	it3 := scanSuccess(data, s2, nil, nil)
	sortPairs(pairs)
	for i := 0; i < len(pairs); i++ {
		nextSuccess(data, it3, pairs[i].Key, pairs[i].Value)
	}
	for i := 0; i < 13; i++ {
		nextEmpty(data, it3)
	}
	closeIteratorSuccess(data, it3)
	it3 = nil
	s2 = nil
}
