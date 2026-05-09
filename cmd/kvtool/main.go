package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"kvschool/internal/kv/lsmstore"
	"kvschool/internal/stream"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"kvschool/internal/kv"
	"kvschool/internal/kv/memmap"
	"kvschool/internal/mapreduce"
	"kvschool/internal/testutil"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "wordcount":
		if err := runWordCount(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "ошибка:", err)
			os.Exit(1)
		}
	case "load":
		if err := runLoad(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "ошибка:", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "kvtool <команда> [аргументы]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Команды:")
	fmt.Fprintln(os.Stderr, "  wordcount  -in <файл> [-store memmap|skiplist]   выполнить map/reduce wordcount")
	fmt.Fprintln(os.Stderr, "  load       -count <N> [-zipf <S>] [-store ...]   запустить нагрузочное тестирование")
}

func runWordCount(args []string) error {
	fs := flag.NewFlagSet("wordcount", flag.ContinueOnError)
	inPath := fs.String("in", "", "входной текстовый файл")
	storeKind := fs.String("store", "memmap", "тип хранилища: memmap|skiplist")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *inPath == "" {
		return fmt.Errorf("отсутствует параметр -in")
	}
	b, err := os.ReadFile(*inPath)
	if err != nil {
		return fmt.Errorf("ошибка чтения: %w", err)
	}

	ctx := context.Background()
	st, err := initStore(*storeKind)
	if err != nil {
		return err
	}
	defer st.Close()

	out, err := mapreduce.Run(ctx, bytes.NewReader(b), st, mapreduce.WordCountMapper, mapreduce.SumVarintReducer)
	if err != nil {
		return err
	}
	defer out.Close()

	it, err := out.Scan(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("ошибка scan: %w", err)
	}
	defer it.Close()

	for {
		p, ok, err := it.Next()
		if err != nil {
			return fmt.Errorf("ошибка итерации: %w", err)
		}
		if !ok {
			break
		}
		x, n := binary.Varint(p.Value)
		if n <= 0 {
			return fmt.Errorf("некорректный varint для ключа=%q", string(p.Key))
		}
		fmt.Printf("%s\t%d\n", string(p.Key), x)
	}
	return nil
}

func runLoad(args []string) error {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	count := fs.Int("count", 10000, "количество операций")
	zipf := fs.Float64("zipf", 0, "параметр s для Zipf (0 для равномерного, >1.0 для перекошенного)")
	storeKind := fs.String("store", "memmap", "тип хранилища: memmap|skiplist|lsm")
	report := fs.Bool("report", false, "статистика")

	if err := fs.Parse(args); err != nil {
		return err
	}

	st, err := initStore(*storeKind)
	if err != nil {
		return err
	}
	defer st.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var keyGen testutil.KeyGenerator
	if *zipf > 1.0 {
		// 1000 items dictionary for zipf
		keyGen = testutil.NewZipfGenerator(rng, *zipf, 1.0, 1000, 16)
		fmt.Printf("Нагрузка: Zipf(s=%.1f) на 1000 элементов\n", *zipf)
	} else {
		keyGen = &testutil.UniformGenerator{Rng: rng, Len: 16}
		fmt.Printf("Нагрузка: Равномерная (Uniform)\n")
	}

	start := time.Now()
	ctx := context.Background()

	cms := stream.NewCountMinSketch(uint32(*count/10), 10)
	var additionsCount = map[string]uint64{}

	// Simple Mixed Workload: 50% Put, 50% Get
	for i := 0; i < *count; i++ {
		key := keyGen.Next()
		if i%2 == 0 {
			if err := st.Put(ctx, key, []byte("data")); err != nil {
				return fmt.Errorf("ошибка put: %w", err)
			}

			if err := cms.Add(key); err != nil {
				return fmt.Errorf("ошибка cms.add: %w", err)
			}

			keyString := string(key)
			_, ok := additionsCount[keyString]
			if !ok {
				additionsCount[keyString] = 1
			} else {
				additionsCount[keyString]++
			}
		} else {
			_, _ = st.Get(ctx, key)
		}
	}

	dur := time.Since(start)
	fmt.Printf("Выполнено %d операций за %v (%.1f op/s)\n", *count, dur, float64(*count)/dur.Seconds())

	if *report {
		minEstimationErrorPercent := math.MaxFloat64
		maxEstimationErrorPercent := 0.0
		avgEstimationErrorPercent := 0.0

		for key, realCount := range additionsCount {
			estimatedCount, err := cms.Estimate([]byte(key))
			if err != nil {
				return fmt.Errorf("ошибка cms.estimate: %w", err)
			}
			if estimatedCount < realCount {
				return fmt.Errorf("ошибка cms.estimate: underestimate")
			}

			estimationError := estimatedCount - realCount
			estimationErrorPercent := float64(estimationError) / float64(realCount) * 100

			avgEstimationErrorPercent += estimationErrorPercent

			minEstimationErrorPercent = min(minEstimationErrorPercent, estimationErrorPercent)
			maxEstimationErrorPercent = max(maxEstimationErrorPercent, estimationErrorPercent)

			fmt.Printf("%s: real=%d, estimated=%d, error=%f%%\n", key, realCount, estimatedCount, estimationErrorPercent)
		}

		fmt.Printf("min_error=%f, average_error=%f, max_error=%f\n", minEstimationErrorPercent, avgEstimationErrorPercent/float64(len(additionsCount)), maxEstimationErrorPercent)
	}

	return nil
}

func initStore(kind string) (kv.Store, error) {
	switch kind {
	case "memmap":
		return memmap.New(), nil
	case "skiplist":
		return memSkipListDefault(), nil
	case "lsm":
		dir := "/home/llirik42/db"

		if err := os.RemoveAll(dir); err != nil {
			log.Fatalf("remove dir %q: %v", dir, err)
		}

		store, err := lsmstore.Open(lsmstore.Options{Dir: dir})
		if err != nil {
			log.Fatalf("open: %v", err)
		}
		return store, nil
	// case "lsm": будет добавлен в процессе выполнения заданий
	default:
		return nil, fmt.Errorf("неизвестное хранилище %q", kind)
	}
}

var _ kv.Store = (*memmap.Store)(nil)
