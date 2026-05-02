package lsm

import (
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

func createSSTable(directory string, recordsSource iterator.Iterator) (*SSTableWrapper, error) {
	now := time.Now()
	nowUnixNano := now.UnixNano()

	// Create file
	fileName := strconv.FormatInt(nowUnixNano, 10)
	filePath := path.Join(directory, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("lsm createSSTable: creating file %s: %w", filePath, err)
	}

	// Writing records
	writer := sstable.NewWriter(file)
	for {
		key, value, ok, err := recordsSource.Next()
		if err != nil {
			return nil, fmt.Errorf("lsm createSSTable: Next() of records source: %w", err)
		}
		if !ok {
			break
		}
		if err := writer.Add(key, value); err != nil {
			return nil, fmt.Errorf("lsm createSSTable: writing records to %s: %w", filePath, err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("lsm createSSTable: closing writer on %s: %w", filePath, err)
	}

	// Creating wrapper
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("lsm createSSTable: stat %s: %w", filePath, err)
	}
	reader, err := sstable.NewReader(file, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("lsm createSSTable: creating reader on %s: %w", filePath, err)
	}

	return &SSTableWrapper{
		reader:       reader,
		file:         file,
		creationTime: now,
	}, nil
}

func readSSTable(name, directory string) (*SSTableWrapper, error) {
	filePath := path.Join(directory, name)

	// Opening file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("lsm readSSTable: opening file %s: %w", filePath, err)
	}

	// Reading file
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("lsm readSSTable: stat %s: %w", filePath, err)
	}
	reader, err := sstable.NewReader(file, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("lsm readSSTable: reading file %s: %w", filePath, err)
	}
	if err := reader.ValidateChecksum(); err != nil {
		return nil, fmt.Errorf("lsm readSSTable: validating file %s: %w", filePath, err)
	}

	// Parsing creation time
	creationTimeUnixNano, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("lsm readSSTable: invalid file name %s: %w", filePath, err)
	}

	return &SSTableWrapper{
		reader:       reader,
		file:         file,
		creationTime: time.Unix(0, creationTimeUnixNano),
	}, nil
}

func (w *SSTableWrapper) getReader() *sstable.Reader {
	return w.reader
}

func (w *SSTableWrapper) getCreationTime() time.Time {
	return w.creationTime
}

func (w *SSTableWrapper) Close() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("lsm SSTableWrapper.Remove(): closing file: %w", err)
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
