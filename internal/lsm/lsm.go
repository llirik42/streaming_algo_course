package lsm

import (
	"errors"
	"fmt"
	"kvschool/internal/skiplist"
	"kvschool/internal/sstable"
	"kvschool/internal/wal"
	"os"
	"path"
	"strconv"
	"time"
)

// ErrNotImplemented используется в заготовке практики второго дня.
var ErrNotImplemented = errors.New("lsm: функция не реализована")

const (
	T = 2
)

// Options задаёт параметры LSM движка.
type Options struct {
	Dir string // Директория для хранения WAL и SSTables

	// Максимальный размер Memtable перед сбросом на диск (Flush).
	// В телекоме это баланс между памятью и частотой I/O.
	MemtableFlushThreshold int
}

type lsmSSTable struct {
	reader       *sstable.Reader
	file         *os.File
	creationTime time.Time
}

// Engine — основной движок CDR Storage.
// Координирует работу Memtable, WAL и SSTables.
// Отвечает за Compaction (сборку мусора).
type Engine struct {
	memTable       *skiplist.SkipList
	memTableSize   int
	sstables       [][]*lsmSSTable
	flushThreshold int
	options        Options

	walFile   *os.File
	walWriter *wal.Writer
}

func directoryExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func Open(options Options) (*Engine, error) {
	if !directoryExists(options.Dir) {
		if err := os.MkdirAll(options.Dir, 0755); err != nil {
			return nil, fmt.Errorf("lsm Open: making directory %s: %w", options.Dir, err)
		}
	}

	walFilePath := path.Join(options.Dir, "wal")
	_, err := os.Stat(walFilePath)
	//

	var tmpWalFile *os.File
	var walWriter *wal.Writer

	engine := &Engine{
		memTable: skiplist.New(42),
	}

	if os.IsNotExist(err) {
		tmpWalFile, err = os.Create(walFilePath)
		if err != nil {
			return nil, fmt.Errorf("lsm Open: creating wal file %s: %w", walFilePath, err)
		}

		walWriter = wal.NewWriter(tmpWalFile)
		engine.walFile = tmpWalFile
		engine.walWriter = walWriter
	} else {
		// Read WAL
		tmpWALFilePath := path.Join(options.Dir, ".wal")
		if err := os.Rename(walFilePath, tmpWALFilePath); err != nil {
			return nil, fmt.Errorf("lsm Open: renaming wal file %s: %w", tmpWALFilePath, err)
		}

		tmpWalFile, err = os.OpenFile(tmpWALFilePath, os.O_RDONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("lsm Open: creating wal file %s: %w", walFilePath, err)
		}

		tmpWalFileStat, err := tmpWalFile.Stat()
		if err != nil {
			return nil, fmt.Errorf("lsm Open: wal file stat() %s: %w", walFilePath, err)
		}
		walReader := wal.NewReader(tmpWalFile, tmpWalFileStat.Size())
		if err := walReader.ValidateChecksums(); err != nil {
			return nil, fmt.Errorf("lsm Open: wal file validate checksums %s: %w", walFilePath, err)
		}
		walIterator := walReader.Iterator()

		walFile, err := os.Create(walFilePath)
		if err != nil {
			return nil, fmt.Errorf("lsm Open: creating wal file %s: %w", walFilePath, err)
		}
		walWriter = wal.NewWriter(walFile)
		engine.walFile = walFile
		engine.walWriter = walWriter

		for {
			record, ok, err := walIterator.Next()
			if err != nil {
				return nil, fmt.Errorf("lsm Open: reading wal: %w", err)
			}
			if !ok {
				break
			}

			if record.Type == wal.OpPut {
				fmt.Printf("Put: %s-%s\n", record.Key, record.Value)
				if err := engine.Put(record.Key, record.Value); err != nil {
					return nil, fmt.Errorf("lsm Open: recovery crash put: %w", err)
				}
			} else {
				fmt.Printf("Delete: %s-%s\n", record.Key, record.Value)
				if err := engine.Delete(record.Key); err != nil {
					return nil, fmt.Errorf("lsm Open: recovery crash delete: %w", err)
				}
			}
		}
		if err := tmpWalFile.Close(); err != nil {
			return nil, fmt.Errorf("lsm Open: closing wal file %s: %w", walFilePath, err)
		}
	}

	return engine, nil
}

func (e *Engine) Put(key []byte, value []byte) error {
	walRecord := wal.Record{
		Type:  wal.OpPut,
		Key:   key,
		Value: value,
	}

	if err := e.walWriter.Append(walRecord); err != nil {
		return fmt.Errorf("lsm Put: add record to WAL: %w", err)
	}

	if err := e.memTable.Put(key, value); err != nil {
		return fmt.Errorf("lsm Put: add record to l0: %w", err)
	}

	e.memTableSize += len(key) + len(value)
	if e.memTableSize > e.flushThreshold {
		// TODO: создаём sstable (дампаем на диск)
		// TODO: где-то здесь должна быть проверка compaction
	}

	return nil
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	value, err := e.memTable.Get(key)

	if err == nil {
		// Нашли ключ в skiplist
		return value, nil
	}

	// TODO: страшная логика
	return nil, ErrNotImplemented
}

func (e *Engine) Delete(key []byte) error {
	walRecord := wal.Record{
		Type: wal.OpDelete,
		Key:  key,
	}

	if err := e.walWriter.Append(walRecord); err != nil {
		return fmt.Errorf("lsm Delete: add record to WAL: %w", err)
	}

	if err := e.memTable.Delete(key); err == nil {
		// Удалили ключ из skiplist
		return nil
	}

	// TODO: страшная логика

	return ErrNotImplemented
}

func (e *Engine) Scan(start []byte, end []byte) error {
	return nil
}

func (e *Engine) Close() error {
	walClosingError := e.walWriter.Close()
	walFileClosingError := e.walFile.Close()
	return errors.Join(walClosingError, walFileClosingError)
}

func (e *Engine) flush() error {
	now := time.Now()
	nowUnix := now.Unix()
	newSSTableName := strconv.FormatInt(nowUnix, 10)
	newSSTablePath := path.Join(e.options.Dir, newSSTableName)

	newSSTableFile, err := os.Create(newSSTablePath)
	if err != nil {
		return fmt.Errorf("lsm flush: creating new sstable file %s: %w", newSSTableName, err)
	}

	memtableIterator, err := e.memTable.Scan(nil, nil)
	if err != nil {
		return fmt.Errorf("lsm flush: creating memtable iterator: %w", err)
	}

	newSSTableWriter := sstable.NewWriter(newSSTableFile)

	for {
		key, value, ok, err := memtableIterator.Next()
		if err != nil {
			return fmt.Errorf("lsm flush: iterate over memtable: %w", err)
		}
		if !ok {
			break
		}
		// TODO: что делать с удалёнными записями?
		if err := newSSTableWriter.Add(key, value); err != nil {
			return fmt.Errorf("lsm flush: write memtable entry to disk: %w", err)
		}
	}
	if err := newSSTableWriter.Close(); err != nil {
		return fmt.Errorf("lsm flush: closing sstable writer: %w", err)
	}

	e.memTable.Clear()
	// TODO: очистить WAL?

	stat, err := newSSTableFile.Stat()
	if err != nil {
		return fmt.Errorf("lsm flush: sstable file stat: %w", err)
	}

	newSSTableReader, err := sstable.NewReader(newSSTableFile, stat.Size())
	if err != nil {
		return fmt.Errorf("lsm flush: opening sstable for reading: %w", err)
	}

	tmp := lsmSSTable{
		reader:       newSSTableReader,
		file:         newSSTableFile,
		creationTime: now,
	}

	e.sstables[0] = append(e.sstables[0], &tmp)

	return nil
}

func (e *Engine) initWALWriter() error {
	return nil
}
