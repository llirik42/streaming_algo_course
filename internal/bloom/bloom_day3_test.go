////go:build day3

package bloom

import (
	"fmt"
	. "kvschool/internal/helpers"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

func addKey(t *testing.T, filter *Filter, key []byte) {
	if err := filter.Add(key); err != nil {
		t.Errorf("Error adding key: %v", err)
	}
}

func addKeyB(b *testing.B, filter *Filter, key []byte) {
	if err := filter.Add(key); err != nil {
		b.Errorf("Error adding key: %v", err)
	}
}

func checkDoesntContain(t *testing.T, filter *Filter, key []byte) {
	mayContain, err := filter.MayContain(key)
	if err != nil {
		t.Errorf("Error checking key: %v", err)
	}
	if mayContain {
		t.Fatal("filter must not contain key")
	}
}

func checkMayContain(t *testing.T, filter *Filter, key []byte) {
	mayContain, err := filter.MayContain(key)
	if err != nil {
		t.Errorf("Error checking key: %v", err)
	}
	if !mayContain {
		t.Fatal("filter returned that it doesn't contain key")
	}
}

func TestBloom_NoFalseNegatives(t *testing.T) {
	// Параметры маленькие намеренно: цель теста — свойство "нет false negative",
	// а не качество false positive.
	f := New(1024, 3)

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for _, k := range keys {
		if err := f.Add(k); err != nil {
			t.Fatalf("Add(%q): %v", string(k), err)
		}
	}
	for _, k := range keys {
		ok, err := f.MayContain(k)
		if err != nil {
			t.Fatalf("MayContain(%q): %v", string(k), err)
		}
		if !ok {
			t.Fatalf("false negative for key=%q", string(k))
		}
	}
}

func TestBloom_Empty(t *testing.T) {
	f1 := New(1, 1)
	f2 := New(1024, 3)
	f3 := New(1024*1024, 10)

	keys := [][]byte{
		StringToBytes(""),
		StringToBytes("key-1"),
		StringToBytes("123456789"),
		StringToBytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}

	for _, k := range keys {
		checkDoesntContain(t, f1, k)
		checkDoesntContain(t, f2, k)
		checkDoesntContain(t, f3, k)
	}
}

func TestBloom_SingleKey(t *testing.T) {
	f1 := New(1, 1)
	f2 := New(1024, 3)
	f3 := New(1024*1024, 10)

	key := StringToBytes("key")

	addKey(t, f1, key)
	checkMayContain(t, f1, key)

	addKey(t, f2, key)
	checkMayContain(t, f2, key)

	addKey(t, f3, key)
	checkMayContain(t, f3, key)
}

func TestBloom_MultipleKeysSequential(t *testing.T) {
	filters := []*Filter{
		New(1, 1),
		New(1, 10),
		New(1, 100),
		New(100, 1),
		New(100, 10),
		New(1024, 1),
		New(1024, 3),
		New(1024, 10),
	}

	keysNumber := 1024 * 50
	keys := make([][]byte, keysNumber)
	for i := 0; i < keysNumber; i++ {
		keys[i] = StringToBytes(fmt.Sprintf("key-%d", i+1))
	}

	for _, filter := range filters {
		for _, key := range keys {
			addKey(t, filter, key)
			checkMayContain(t, filter, key)
		}
	}

	// На всякий случай
	for _, filter := range filters {
		for _, key := range keys {
			checkMayContain(t, filter, key)
		}
	}
}

func Benchmark_DDOS(b *testing.B) {
	totalUsersCount := 25000000
	fraudUsersCount := 10000

	knownUsersKeys := make([][]byte, totalUsersCount)
	for i := 0; i < totalUsersCount; i++ {
		knownUsersKeys[i] = StringToBytes(uuid.New().String())
	}

	fraudUsersKeys := make([][]byte, fraudUsersCount)
	for i := 0; i < fraudUsersCount; i++ {
		fraudUsersKeys[i] = StringToBytes(uuid.New().String())
	}

	pList := []float64{
		0.9,
		0.8,
		0.7,
		0.6,
		0.5,
		0.4,
		0.3,
		0.2,
		0.1,
		0.01,
		0.001,
		0.0001,
	}

	for _, p := range pList {
		c := 1 / (math.Ln2 * math.Ln2) * math.Log10(1/p)
		k := max(1, uint8(math.Round(c*math.Ln2)))
		size := uint64(float64(totalUsersCount) * c)

		b.Logf("Creating filter for p=%f, with size=%d, k=%d", p, size, k)
		filter := New(size, k)

		for i := 0; i < totalUsersCount/1000000; i++ {
			b.Logf("Iteration %d. Start %d", i+1, time.Now().UnixNano())
			for j := 0; j < 1000000; j++ {
				addKeyB(b, filter, knownUsersKeys[i*1000000+j])
			}
			b.Logf("Iteration %d. Finish %d", i+1, time.Now().UnixNano())
		}

		fraudDetectedNumber := 0
		for i := 0; i < fraudUsersCount; i++ {
			mayContain, err := filter.MayContain(fraudUsersKeys[i])
			if err != nil {
				b.Fatalf("Error checking key: %v", err)
			}

			if !mayContain {
				fraudDetectedNumber++
			}
		}

		b.Log("Total users count:", totalUsersCount)
		b.Log("Total fraud count:", fraudUsersCount)
		b.Log("Detected fraud count:", fraudDetectedNumber)
	}
}

func Benchmark_Width(b *testing.B) {
	keysNumber := 10000000

	keys := make([][]byte, keysNumber)
	for i := 0; i < keysNumber; i++ {
		keys[i] = StringToBytes(uuid.New().String())
	}

	widthList := []uint64{
		1,
		10,
		100,
		1000,
		10000,
		100000,
		1000000,
		10000000,
		100000000,
		1000000000,
		10000000000,
		100000000000,
	}

	for _, w := range widthList {
		b.Run(fmt.Sprintf("size=%d", w), func(b *testing.B) {
			filter := New(w, 1)

			b.ReportAllocs()
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				for i := 0; i < keysNumber; i++ {
					addKeyB(b, filter, keys[i])
				}
			}
		})
	}
}

func Benchmark_Depth(b *testing.B) {
	keysNumber := 10000000

	keys := make([][]byte, keysNumber)
	for i := 0; i < keysNumber; i++ {
		keys[i] = StringToBytes(uuid.New().String())
	}

	widthList := []uint64{
		1,
		1000,
		1000000,
	}

	var d int
	for d = 10; d < 256; d += 10 {
		depth := uint8(d)

		for _, w := range widthList {
			b.Run(fmt.Sprintf("size=%d, hashes=%d", w, depth), func(b *testing.B) {
				filter := New(w, depth)

				b.ReportAllocs()
				b.ResetTimer()

				for j := 0; j < b.N; j++ {
					for i := 0; i < keysNumber; i++ {
						addKeyB(b, filter, keys[i])
					}
				}
			})
		}
	}
}
