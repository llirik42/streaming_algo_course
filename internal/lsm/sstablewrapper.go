package lsm

import (
	"bytes"
	"fmt"
	"kvschool/internal/iterator"
	"kvschool/internal/sstable"
	"os"
	"path"
	"strconv"
	"time"
)

type SSTableWrapper struct {
	reader       *sstable.Reader
	file         *os.File
	creationTime time.Time
}

func CreateSSTable(directory string, recordsSource iterator.Iterator) (*SSTableWrapper, error) {
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

	return &SSTableWrapper{
		reader:       reader,
		file:         file,
		creationTime: now,
	}, nil
}

func ReadSSTable(name, directory string) (*SSTableWrapper, error) {
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

	return &SSTableWrapper{
		reader:       reader,
		file:         file,
		creationTime: time.Unix(0, creationTimeUnixNano),
	}, nil
}

func DoIntersect(w1 *SSTableWrapper, w2 *SSTableWrapper) bool {
	r1 := w1.reader
	r2 := w2.reader

	cond1 := bytes.Compare(r2.GetFirstKey(), r1.GetLastKey()) <= 0
	cond2 := bytes.Compare(r1.GetFirstKey(), r2.GetLastKey()) <= 0
	return cond1 && cond2
}

func (w *SSTableWrapper) GetReader() *sstable.Reader {
	return w.reader
}

func (w *SSTableWrapper) GetCreationTime() time.Time {
	return w.creationTime
}

func (w *SSTableWrapper) Close() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("lsm SSTableWrapper.close(): closing file: %w", err)
	}

	return nil
}

func (w *SSTableWrapper) Remove() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("lsm SSTableWrapper.Remove(): closing file: %w", err)
	}

	if err := os.Remove(w.file.Name()); err != nil {
		return fmt.Errorf("lsm SSTableWrapper.Remove(): removing file: %w", err)
	}

	return nil
}

func ProbablyContains(key []byte, wrapper *SSTableWrapper) bool {
	if bytes.Compare(wrapper.reader.GetFirstKey(), key) <= 0 && bytes.Compare(key, wrapper.reader.GetLastKey()) <= 0 {
		return true
	}

	return false
}
