//go:build day3

package stream

import (
	"fmt"
	. "kvschool/internal/helpers"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/google/uuid"
)

func addKey(t *testing.T, cms *CountMinSketch, key []byte) {
	if err := cms.Add(key); err != nil {
		t.Errorf("Adding key error = %v", err)
	}
}

func addKeyB(b *testing.B, cms *CountMinSketch, key []byte) {
	if err := cms.Add(key); err != nil {
		b.Errorf("Adding key error = %v", err)
	}
}

func estimate(t *testing.T, cms *CountMinSketch, key []byte, bottom uint64) {
	est, err := cms.Estimate(key)
	if err != nil {
		t.Errorf("Estimate error = %v", err)
	}

	if est < bottom {
		t.Errorf("Underestimate = %v < %v", est, bottom)
	}
}

func estimateB(b *testing.B, cms *CountMinSketch, key []byte) uint64 {
	est, err := cms.Estimate(key)

	if err != nil {
		b.Errorf("Estimate error = %v", err)
	}

	return est
}

func appendToFile(b *testing.B, file *os.File, content string) {
	n, err := file.Write([]byte(content))

	if err != nil {
		b.Fatal(err)
	}

	if n != len(content) {
		b.Fatalf("Wrote %d, expected %d", n, len(content))
	}
}

func TestCountMinSketch_EstimateMonotone(t *testing.T) {
	cms := NewCountMinSketch(64, 4)

	for i := 0; i < 10; i++ {
		if err := cms.Add([]byte("hot")); err != nil {
			t.Fatalf("Add hot: %v", err)
		}
	}
	est, err := cms.Estimate([]byte("hot"))
	if err != nil {
		t.Fatalf("Estimate hot: %v", err)
	}
	// Для CMS типично: оценка >= истинного значения (overestimate допустим),
	// но undercount — индикатор ошибки.
	if est < 10 {
		t.Fatalf("estimate too small: %d", est)
	}
}

func TestCountMinSketch_Empty(t *testing.T) {
	cmsList := []*CountMinSketch{
		NewCountMinSketch(1, 1),
		NewCountMinSketch(1, 10),
		NewCountMinSketch(10, 1),
		NewCountMinSketch(10, 10),
	}

	keys := [][]byte{
		StringToBytes(""),
		StringToBytes("key-1"),
		StringToBytes("123456789"),
		StringToBytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}

	for _, k := range keys {
		for _, cms := range cmsList {
			estimate(t, cms, k, 0)
		}
	}
}

func TestCountMinSketch_SingleKey(t *testing.T) {
	cmsList := []*CountMinSketch{
		NewCountMinSketch(1, 1),
		NewCountMinSketch(1, 10),
		NewCountMinSketch(10, 1),
		NewCountMinSketch(10, 10),
	}

	key := StringToBytes("key")
	for _, cms := range cmsList {
		estimate(t, cms, key, 0)
	}

	for _, cms := range cmsList {
		addKey(t, cms, key)
		estimate(t, cms, key, 1)
	}

	rng := rand.New(rand.NewSource(0))
	for _, cms := range cmsList {
		additionsNumber := rng.Int() % 1000
		for i := 0; i < additionsNumber; i++ {
			addKey(t, cms, key)
		}
		estimate(t, cms, key, uint64(additionsNumber)+1)
	}
}

func TestCountMinSketch_MultipleKeysSequential(t *testing.T) {
	cmsList := []*CountMinSketch{
		NewCountMinSketch(1, 1),
		NewCountMinSketch(1, 10),
		NewCountMinSketch(10, 1),
		NewCountMinSketch(10, 10),
		NewCountMinSketch(100, 1),
		NewCountMinSketch(1000, 1),
	}

	keysNumber := 10000
	keys := make([][]byte, keysNumber)
	for i := 0; i < keysNumber; i++ {
		keys[i] = StringToBytes(fmt.Sprintf("key-%d", i+1))
	}

	additions := make([]uint64, keysNumber)
	rng := rand.New(rand.NewSource(0))

	for i, key := range keys {
		for _, cms := range cmsList {
			additionsNumber := uint64(rng.Int() % 10)
			additions[i] = additionsNumber
			var i uint64
			for i = 0; i < additionsNumber; i++ {
				addKey(t, cms, key)
			}
			estimate(t, cms, key, additionsNumber)
		}
	}
}

func TestCountMinSketch_MultipleKeysRandom(t *testing.T) {
	cmsList := []*CountMinSketch{
		NewCountMinSketch(1, 1),
		NewCountMinSketch(1, 10),
		NewCountMinSketch(10, 1),
		NewCountMinSketch(10, 10),
		NewCountMinSketch(100, 1),
		NewCountMinSketch(1000, 1),
	}

	keysNumber := 100000
	keys := make([][]byte, keysNumber)
	for i := 0; i < keysNumber; i++ {
		keys[i] = StringToBytes(fmt.Sprintf("key-%d", i+1))
	}

	totalAdditionsNumber := keysNumber * 10
	additions := make([]uint64, keysNumber)
	rng := rand.New(rand.NewSource(0))

	for i := 0; i < totalAdditionsNumber; i++ {
		keyIndex := rng.Int() % keysNumber
		key := keys[keyIndex]
		additions[keyIndex]++
		for _, cms := range cmsList {
			addKey(t, cms, key)
		}
	}

	for i, key := range keys {
		for _, cms := range cmsList {
			estimate(t, cms, key, additions[i])
		}
	}
}

func Benchmark_FrequenciesEstimation(b *testing.B) {
	keysNumber := 10000

	distributionFunctions := map[string]func(keyIndex uint64) uint64{
		"uniform": func(keyIndex uint64) uint64 {
			return uint64(keysNumber / 2)
		},
		"linear": func(keyIndex uint64) uint64 {
			return keyIndex + 1
		},
		"sqrt": func(keyIndex uint64) uint64 {
			return uint64(math.Sqrt(float64(keyIndex))) + 1
		},
		"sin": func(keyIndex uint64) uint64 {
			return uint64((math.Sin(float64(keyIndex)/float64(keysNumber/15)) + 2) * float64(keysNumber) / 2)
		},
		"normal": func(keyIndex uint64) uint64 {
			center := float64(keysNumber / 2)
			sigma := float64(keysNumber / 10)
			normal := 1 / (math.Sqrt(math.Pi*2) * sigma) * math.Exp(-0.5*((float64(keyIndex)-center)*(float64(keyIndex)-center)/(sigma*sigma)))
			return uint64(normal*float64(keysNumber)*float64(keysNumber)/20 + float64(keysNumber)/1000.0)
		},
		"zipf": func(keyIndex uint64) uint64 {
			return uint64(float64(keysNumber)/(float64(keyIndex)+1.0) + float64(keysNumber)/1000.0)
		},
	}

	keys := make([][]byte, keysNumber)
	for i := 0; i < keysNumber; i++ {
		keys[i] = StringToBytes(uuid.New().String())
	}

	epsilon := 0.001
	p := 0.01
	bList := []int{2}

	file, err := os.Create("distribution.log")
	if err != nil {
		b.Fatal(err)
	}

	for title, function := range distributionFunctions {
		b.Logf("Starting %s\n", title)

		for _, base := range bList {
			w := uint32(math.Ceil(float64(base) / epsilon))
			d := max(1, uint32(math.Ceil(math.Log(1/p)/math.Log(float64(base)))))

			cms := NewCountMinSketch(w, d)

			appendToFile(b, file, fmt.Sprintf("function=%s, keys=%d, epsilon=%f, probability=%f, base=%d, width=%d, depth=%d\n", title, keysNumber, epsilon, p, base, w, d))
			for keyIndex, k := range keys {
				expectedFrequency := function(uint64(keyIndex))
				appendToFile(b, file, fmt.Sprintf("keyIndex=%d, real=%d\n", keyIndex, expectedFrequency))

				var i uint64
				for i = 0; i < expectedFrequency; i++ {
					addKeyB(b, cms, k)
				}
			}

			for keyIndex, k := range keys {
				appendToFile(b, file, fmt.Sprintf("keyIndex=%d, estimated=%d\n", keyIndex, estimateB(b, cms, k)))
			}
		}
	}
}
