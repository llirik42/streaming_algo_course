package lsm

import (
	"fmt"
	. "kvschool/internal/helpers"
	"kvschool/internal/iterator"
	"kvschool/internal/sstable"
	"os"
	"path"
	"strconv"
	"time"
)

type sstableWrapper struct {
	reader       *sstable.Reader
	file         *os.File
	creationTime time.Time
}

func probablyContains(key []byte, wrapper *sstableWrapper) bool {
	if CompareKeys(wrapper.reader.GetFirstKey(), key) <= 0 && CompareKeys(key, wrapper.reader.GetLastKey()) <= 0 {
		return true
	}

	return false
}

func createSSTable(directory string, recordsSource iterator.Iterator) (*sstableWrapper, error) {
	now := time.Now()
	nowUnixNano := now.UnixNano()

	// Create file
	fileName := strconv.FormatInt(nowUnixNano, 10)
	filePath := path.Join(directory, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("lsm CreateSSTable: creating file %s: %w", filePath, err)
	}

	// Writing records
	writer := sstable.NewWriter(file)
	for {
		key, value, ok, err := recordsSource.Next()
		if err != nil {
			return nil, fmt.Errorf("lsm CreateSSTable: Next() of records source: %w", err)
		}
		if !ok {
			break
		}
		if err := writer.Add(key, value); err != nil {
			return nil, fmt.Errorf("lsm CreateSSTable: writing records to %s: %w", filePath, err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("lsm CreateSSTable: closing writer on %s: %w", filePath, err)
	}

	// Creating wrapper
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("lsm CreateSSTable: stat %s: %w", filePath, err)
	}
	reader, err := sstable.NewReader(file, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("lsm CreateSSTable: creating reader on %s: %w", filePath, err)
	}

	return &sstableWrapper{
		reader:       reader,
		file:         file,
		creationTime: now,
	}, nil
}

func readSSTable(name, directory string) (*sstableWrapper, error) {
	filePath := path.Join(directory, name)

	// Opening file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("lsm ReadSSTable: opening file %s: %w", filePath, err)
	}

	// Reading file
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("lsm ReadSSTable: stat %s: %w", filePath, err)
	}
	reader, err := sstable.NewReader(file, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("lsm ReadSSTable: reading file %s: %w", filePath, err)
	}
	if err := reader.ValidateChecksum(); err != nil {
		return nil, fmt.Errorf("lsm ReadSSTable: validating file %s: %w", filePath, err)
	}

	// Parsing creation time
	creationTimeUnixNano, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("lsm ReadSSTable: invalid file name %s: %w", filePath, err)
	}

	return &sstableWrapper{
		reader:       reader,
		file:         file,
		creationTime: time.Unix(0, creationTimeUnixNano),
	}, nil
}

func doIntersect(w1 *sstableWrapper, w2 *sstableWrapper) bool {
	r1 := w1.reader
	r2 := w2.reader

	cond1 := CompareKeys(r2.GetFirstKey(), r1.GetLastKey()) <= 0
	cond2 := CompareKeys(r1.GetFirstKey(), r2.GetLastKey()) <= 0
	return cond1 && cond2
}

func (w *sstableWrapper) getReader() *sstable.Reader {
	return w.reader
}

func (w *sstableWrapper) getCreationTime() time.Time {
	return w.creationTime
}

func (w *sstableWrapper) close() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("lsm SSTableWrapper.close(): closing file: %w", err)
	}

	return nil
}

func (w *sstableWrapper) remove() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("lsm SSTableWrapper.Remove(): closing file: %w", err)
	}

	if err := os.Remove(w.file.Name()); err != nil {
		return fmt.Errorf("lsm SSTableWrapper.Remove(): removing file: %w", err)
	}

	return nil
}
