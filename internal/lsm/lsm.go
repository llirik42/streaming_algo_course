package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"kvschool/internal/iterator"
	"kvschool/internal/skiplist"
	"kvschool/internal/sstable"
	"kvschool/internal/wal"
	"math"
	"os"
	"path"
	"strconv"
	"time"
)

// ErrNotImplemented используется в заготовке практики второго дня.
var ErrNotImplemented = errors.New("lsm: функция не реализована")

var ErrNotFound = errors.New("lsm: ключ не найден")

const (
	T = 2
)

type pair struct {
	key    []byte
	value  []byte
	source iterator.Iterator
}

type Iterator struct {
	memTableIterator  iterator.Iterator
	sstablesIterators [][]iterator.Iterator
	pairs             []pair
	toMove            iterator.Iterator
	empty             bool
}

func (it *Iterator) Next() (key []byte, value []byte, ok bool, err error) {
	// TODO: нужно делать всё умнее: не просто добавлять в список пар, а проверять: если уже есть с таким ключом и от кого?

	if it.empty {
		return nil, nil, false, nil
	}

	if it.toMove == nil {
		sortPairs(it.pairs)

		if len(it.pairs) == 0 {
			it.empty = true
			return nil, nil, false, nil
		}

		for {
			firstPair := it.pairs[0]
			it.pairs = it.pairs[1:]

			firstPairRealValue, deleted := extractKeyValue(firstPair.value)
			if !deleted {
				it.toMove = firstPair.source
				return firstPair.key, firstPairRealValue, true, nil
			}

			if len(it.pairs) == 0 {
				break
			}
		}
	}

	it.empty = true
	return nil, nil, false, nil
}

func (it *Iterator) Close() error {
	err1 := it.memTableIterator.Close()
	errorsList := []error{err1}

	for i := 0; i < len(it.sstablesIterators); i++ {
		for j := 0; j < len(it.sstablesIterators[i]); j++ {
			err := it.sstablesIterators[i][j].Close()
			errorsList = append(errorsList, err)
		}
	}

	return errors.Join(errorsList...)
}

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
	// TODO: многие return nil, fmt.errorf() заменить на предупреждение

	engine := &Engine{
		memTable: skiplist.New(42),
		sstables: make([][]*lsmSSTable, 1),
	}

	if !directoryExists(options.Dir) {
		if err := os.MkdirAll(options.Dir, 0755); err != nil {
			return nil, fmt.Errorf("lsm Open: making directory %s: %w", options.Dir, err)
		}
	} else {
		entires, err := os.ReadDir(options.Dir)
		if err != nil {
			return nil, fmt.Errorf("lsm Open: reading directory %s: %w", options.Dir, err)
		}

		for _, entry := range entires {
			if entry.Name() == "wal" {
				continue
				// TODO: перенести чтение WAL сюда
			}

			sstableFilePath := path.Join(options.Dir, entry.Name())
			sstableFile, err := os.Open(sstableFilePath)
			if err != nil {
				return nil, fmt.Errorf("lsm Open: opening sstable file %s: %w", sstableFilePath, err)
			}

			stat, err := sstableFile.Stat()
			if err != nil {
				return nil, fmt.Errorf("lsm Open: stat sstable file %s: %w", sstableFilePath, err)
			}

			sstableReader, err := sstable.NewReader(sstableFile, stat.Size())
			if err != nil {
				return nil, fmt.Errorf("lsm Open: reading sstable file %s: %w", sstableFilePath, err)
			}
			if err := sstableReader.ValidateChecksum(); err != nil {
				return nil, fmt.Errorf("lsm Open: validating sstable file %s: %w", sstableFilePath, err)
			}

			creationTimeUnix, err := strconv.ParseInt(entry.Name(), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("lsm Open: parsing creation time %s: %w", sstableFilePath, err)
			}

			engine.sstables[0] = append(engine.sstables[0], &lsmSSTable{
				reader:       sstableReader,
				file:         sstableFile,
				creationTime: time.Unix(creationTimeUnix, 0),
			})
		}
	}

	walFilePath := path.Join(options.Dir, "wal")
	_, err := os.Stat(walFilePath)
	//

	var tmpWalFile *os.File
	var walWriter *wal.Writer

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
		if err := os.Remove(tmpWALFilePath); err != nil {
			return nil, fmt.Errorf("lsm Open: removing temporary WAL file %s: %w", walFilePath, err)
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

	valueToSave := writeKeyValue(value, false)
	if err := e.memTable.Put(key, valueToSave); err != nil {
		return fmt.Errorf("lsm Put: add record to l0: %w", err)
	}

	e.memTableSize += len(key) + len(valueToSave)
	if e.memTableSize > e.flushThreshold {
		if err := e.flush(); err != nil {
			return fmt.Errorf("lsm Put: flush memtable: %w", err)
		}
	}

	return nil
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	value, err := e.memTable.Get(key)

	if err == nil {
		// Нашли ключ в skiplist
		return value, nil
	}

	for l := 1; l <= len(e.sstables); l++ {
		candidates := e.findCandidates(key, l)

		// На текущем уровне нет кандидатов
		if len(candidates) == 0 {
			if l == len(e.sstables) {
				// Текущий уровень последний (значит в хранилище вообще ключа нет)
				return nil, ErrNotFound
			} else {
				// Опускаемся на уровень ниже
				continue
			}
		}

		// Проходимся по кандидатам и проверяем, действительно ли в них есть искомый ключ
		for _, c := range candidates {
			it, err := c.reader.Iterator(nil, nil)
			if err != nil {
				return nil, fmt.Errorf("lsm Get: get iterator: %w", err)
			}

			for {
				foundKey, foundValue, ok, err := it.Next()
				if err != nil {
					return nil, fmt.Errorf("lsm Get: get iterator Next: %w", err)
				}
				if !ok {
					break
				}

				if !bytes.Equal(key, foundKey) {
					// Не нашли текущий ключ
					continue
				}

				realValue, deleted := extractKeyValue(foundValue)
				if deleted {
					// Нашли информацию об удалении ключа
					return nil, ErrNotFound
				}

				// Нашли ключ без информации о его удалении
				return realValue, nil
			}
		}

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

	valueToSave := writeKeyValue(nil, true)
	if err := e.memTable.Put(key, valueToSave); err != nil {
		return fmt.Errorf("lsm Delete: add record to l0: %w", err)
	}

	e.memTableSize += len(key) + len(valueToSave)
	if e.memTableSize > e.flushThreshold {
		if err := e.flush(); err != nil {
			return fmt.Errorf("lsm Put: flush memtable: %w", err)
		}
	}

	return nil
}

func (e *Engine) Scan(start []byte, end []byte) (iterator.Iterator, error) {
	sortedPairs := make([]pair, 0)

	memTableIterator, err := e.memTable.Scan(start, end)
	if err != nil {
		return nil, fmt.Errorf("lsm Scan: get memtable iterator: %w", err)
	}
	key, value, ok, err := memTableIterator.Next()
	if err != nil {
		return nil, fmt.Errorf("lsm Scan: get memtable iterator: %w", err)
	}
	if !ok {
		memTableIterator = nil
	} else {
		sortedPairs = append(sortedPairs, pair{
			key:    key,
			value:  value,
			source: memTableIterator,
		})
	}

	sstablesIterators := make([][]iterator.Iterator, len(e.sstables))
	for i := 0; i < len(sstablesIterators); i++ {
		sstablesIterators[i] = make([]iterator.Iterator, len(e.sstables[i]))
		for j := 0; j < len(sstablesIterators[i]); j++ {
			sstablesIterators[i][j], err = e.sstables[i][j].reader.Iterator(start, end)
			if err != nil {
				return nil, fmt.Errorf("lsm Scan: get sstable iterator: %w", err)
			}

			key, value, ok, err := sstablesIterators[i][j].Next()
			if err != nil {
				return nil, fmt.Errorf("lsm Scan: get sstable iterator: %w", err)
			}
			if !ok {
				sstablesIterators[i][j] = nil
			} else {
				sortedPairs = append(sortedPairs, pair{
					key:    key,
					value:  value,
					source: sstablesIterators[i][j],
				})
			}
		}
	}

	return &Iterator{
		memTableIterator:  memTableIterator,
		sstablesIterators: sstablesIterators,
		pairs:             sortedPairs,
	}, nil
}

func (e *Engine) Close() error {
	walClosingError := e.walWriter.Close()
	walFileClosingError := e.walFile.Close()

	for i := 0; i < len(e.sstables); i++ {
		for j := 0; j < len(e.sstables[i]); j++ {
			el := e.sstables[i][j]
			if err := el.file.Close(); err != nil {
				return fmt.Errorf("lsm Close: closing sstable file %s: %w", el.file.Name(), err)
			}
		}
	}

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

	//if err := e.compaction(); err != nil {
	//	return fmt.Errorf("lsm flush: compaction: %w", err)
	//}

	return nil
}

func (e *Engine) compaction() error {
	for levelIndex := 0; levelIndex < len(e.sstables); levelIndex++ {
		levelNumber := levelIndex + 1
		maxSSTablesNumber := int(math.Pow(T, float64(levelNumber)))

		if len(e.sstables[levelIndex]) <= maxSSTablesNumber {
			// На текущем уровне перегруз по количеству sstables
			continue
		}

		if levelIndex == 0 {

		} else {

		}

	}

	return nil
}

func (e *Engine) findCandidates(key []byte, levelNumber int) []*lsmSSTable {
	currentLevelTables := e.sstables[levelNumber-1]

	if levelNumber == 1 {
		return currentLevelTables
	}

	for _, t := range currentLevelTables {
		r := t.reader
		if bytes.Compare(r.GetFirstKey(), key) <= 0 && bytes.Compare(key, r.GetLastKey()) <= 0 {
			res := make([]*lsmSSTable, 1)
			res[0] = t
			return res
		}
	}

	return make([]*lsmSSTable, 0)
}

func doesIntercept(r1 *sstable.Reader, r2 *sstable.Reader) bool {
	cond1 := bytes.Compare(r2.GetFirstKey(), r1.GetLastKey()) <= 0
	cond2 := bytes.Compare(r1.GetFirstKey(), r2.GetLastKey()) <= 0
	return cond1 && cond2
}

func extractKeyValue(buffer []byte) (value []byte, deleted bool) {
	return buffer[1:], buffer[0] != 0
}

func writeKeyValue(value []byte, delete bool) []byte {
	if delete {
		res := make([]byte, 1)
		res[0] = 1
		return res
	}

	res := make([]byte, len(value)+1)
	res[0] = 0 // Ключ не удалён
	copy(res[1:], value)
	return res
}
