package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"kvschool/internal/iterator"
	"kvschool/internal/skiplist"
	"kvschool/internal/wal"
	"math"
	"os"
	"path"
	"sort"
	"time"
)

var ErrNotFound = errors.New("lsm: ключ не найден")

const (
	T           = 2
	WALFileName = "wal"
)

func removeByIndexes(slice []*SSTableWrapper, indexes []int) []*SSTableWrapper {
	sort.Sort(sort.Reverse(sort.IntSlice(indexes)))

	for _, i := range indexes {
		if i >= 0 && i < len(slice) {
			slice = append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// Options задаёт параметры LSM движка.
type Options struct {
	Dir string // Директория для хранения WAL и SSTables

	// Максимальный размер Memtable перед сбросом на диск (flush).
	// В телекоме это баланс между памятью и частотой I/O.
	MemtableFlushThreshold int
}

// Engine — основной движок CDR Storage.
// Координирует работу Memtable, WAL и SSTables.
// Отвечает за Compaction (сборку мусора).
type Engine struct {
	memtable               *skiplist.SkipList
	memtableSize           int
	memtableFlushThreshold int

	sstables [][]*SSTableWrapper

	walFile   *os.File
	walWriter *wal.Writer

	directory string
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
		memtable:               skiplist.New(42),
		sstables:               make([][]*SSTableWrapper, 0),
		directory:              options.Dir,
		memtableFlushThreshold: options.MemtableFlushThreshold,
	}

	directory := options.Dir

	if !directoryExists(directory) {
		if err := os.MkdirAll(directory, 0755); err != nil {
			return nil, fmt.Errorf("lsm Open: making directory %s: %w", directory, err)
		}
	}

	if err := engine.initFromDirectory(); err != nil {
		return nil, fmt.Errorf("lsm Open: initializing from directory %s: %w", directory, err)
	}

	return engine, nil
}

func (e *Engine) Put(key, value []byte) error {
	walRecord := wal.Record{
		Type:  wal.OpPut,
		Key:   key,
		Value: value,
	}
	if err := e.walWriter.Append(walRecord); err != nil {
		return fmt.Errorf("lsm Put: adding record to WAL: %w", err)
	}

	augmentedValue := augmentValue(value, false)
	if err := e.addToMemtable(key, augmentedValue); err != nil {
		return fmt.Errorf("lsm Put: writing to memtable: %w", err)
	}

	return nil
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	// Поиск в memtable
	augmentedValue, err := e.memtable.Get(key)
	if err == nil {
		// Нашли ключ в memtable
		value, isTombstone := parseAugmentedValue(augmentedValue)
		if isTombstone {
			return nil, ErrNotFound
		}
		return value, nil
	}

	// Нет SSTables на диске
	if !e.hasSSTables() {
		return nil, ErrNotFound
	}

	numberOfLevels := e.getNumberOfLevels()
	for levelIndex := 0; levelIndex < e.getNumberOfLevels(); levelIndex++ {
		candidates := e.findCandidates(key, levelIndex)
		isLevelLast := levelIndex == numberOfLevels-1

		foundAugmentedValue := make([][]byte, 0, len(candidates))
		foundAugmentedValuesTimes := make([]time.Time, 0, len(candidates))

		// Проходим по кандидатам и проверяем, действительно ли в них есть искомый ключ
		for _, c := range candidates {
			it, err := c.getReader().Iterator(nil, nil)
			if err != nil {
				return nil, fmt.Errorf("lsm Get: iterator creation: %w", err)
			}

			for {
				foundKey, foundValue, ok, err := it.Next()
				if err != nil {
					return nil, fmt.Errorf("lsm Get: iterator Next: %w", err)
				}
				if !ok {
					break
				}

				if !bytes.Equal(key, foundKey) {
					// Не нашли текущий ключ
					continue
				}

				// Нашли ключ
				foundAugmentedValue = append(foundAugmentedValue, foundValue)
				foundAugmentedValuesTimes = append(foundAugmentedValuesTimes, c.getCreationTime())
			}
		}

		// Среди кандидатов текущего уровня ни у кого не оказалось искомого ключа
		if len(foundAugmentedValue) == 0 {
			if isLevelLast {
				// Текущий уровень последний (значит в хранилище вообще ключа нет)
				return nil, ErrNotFound
			}

			// Опускаемся на уровень ниже
			continue
		}

		// Ищем самое "свежее" значение ключа
		latestTimeIndex := 0
		latestTime := foundAugmentedValuesTimes[0]
		for i := 1; i < len(foundAugmentedValue); i++ {
			if foundAugmentedValuesTimes[i].After(latestTime) {
				latestTime = foundAugmentedValuesTimes[i]
				latestTimeIndex = i
			}
		}

		latestAugmentedValue := foundAugmentedValue[latestTimeIndex]
		value, isTombstone := parseAugmentedValue(latestAugmentedValue)
		if isTombstone {
			// Самая свежая запись о ключе на текущем уровне - удаление ключа
			return nil, ErrNotFound
		}

		return value, nil
	}

	return nil, ErrNotFound
}

func (e *Engine) Delete(key []byte) error {
	walRecord := wal.Record{
		Type: wal.OpDelete,
		Key:  key,
	}
	if err := e.walWriter.Append(walRecord); err != nil {
		return fmt.Errorf("lsm Delete: adding record to WAL: %w", err)
	}

	augmentedValue := augmentValue(nil, true)
	if err := e.addToMemtable(key, augmentedValue); err != nil {
		return fmt.Errorf("lsm Delete: writing to memtable: %w", err)
	}

	return nil
}

func (e *Engine) Scan(start []byte, end []byte) (iterator.Iterator, error) {
	memTableIterator, err := e.memtable.Scan(start, end)
	if err != nil {
		return nil, fmt.Errorf("lsm Scan: get memtable iterator: %w", err)
	}

	moveSSTables := make([][]bool, len(e.sstables))
	creationTime := make([][]time.Time, len(e.sstables))

	sstablesIterators := make([][]iterator.Iterator, len(e.sstables))
	for i := 0; i < len(sstablesIterators); i++ {
		sstablesIterators[i] = make([]iterator.Iterator, len(e.sstables[i]))
		moveSSTables[i] = make([]bool, len(e.sstables[i]))
		creationTime[i] = make([]time.Time, len(e.sstables[i]))

		for j := 0; j < len(sstablesIterators[i]); j++ {
			moveSSTables[i][j] = true
			creationTime[i][j] = e.sstables[i][j].getCreationTime()

			sstablesIterators[i][j], err = e.sstables[i][j].getReader().Iterator(start, end)
			if err != nil {
				return nil, fmt.Errorf("lsm Scan: get sstable iterator: %w", err)
			}
		}
	}

	return &Iterator{
		memTableIterator:     memTableIterator,
		moveMemTableIterator: true,
		sstablesIterators:    sstablesIterators,
		moveSSTables:         moveSSTables,
		sstablesCreationTime: creationTime,
	}, nil
}

func (e *Engine) Close() error {
	walClosingError := e.walWriter.Close()
	walFileClosingError := e.walFile.Close()

	for i := 0; i < e.getNumberOfLevels(); i++ {
		for j, sstableWrapper := range e.getSSTables(i) {
			if err := sstableWrapper.Close(); err != nil {
				return fmt.Errorf("lsm Close: closing sstable file %d-%d: %w", i, j, err)
			}
		}
	}

	return errors.Join(walClosingError, walFileClosingError)
}

func (e *Engine) flush() error {
	memtableIterator, err := e.memtable.Scan(nil, nil)
	if err != nil {
		return fmt.Errorf("lsm flush: creating memtable iterator: %w", err)
	}

	sstableWrapper, err := createSSTable(e.directory, memtableIterator)
	if err != nil {
		return fmt.Errorf("lsm flush: creating sstable: %w", err)
	}

	e.memtable.Clear()
	e.memtableSize = 0

	// Очистка WAL
	if err := e.walWriter.Close(); err != nil {
		return fmt.Errorf("lsm flush: closing wal writer: %w", err)
	}
	if err := e.walFile.Close(); err != nil {
		return fmt.Errorf("lsm flush: closing wal file: %w", err)
	}
	walFile, err := os.Create(path.Join(e.directory, "wal"))
	if err != nil {
		return fmt.Errorf("lsm flush: creating wal file: %w", err)
	}
	e.walFile = walFile
	e.walWriter = wal.NewWriter(walFile)

	e.addSSTableToFirstLevel(sstableWrapper)

	if err := e.compaction(); err != nil {
		return fmt.Errorf("lsm flush: compaction: %w", err)
	}

	return nil
}

func (e *Engine) flushIfNeeded() error {
	if e.memtableSize > e.memtableFlushThreshold {
		if err := e.flush(); err != nil {
			return fmt.Errorf("lsm flushIfNeeded: flush() failed: %w", err)
		}
	}

	return nil
}

func (e *Engine) compaction() error {
	for levelIndex := 0; levelIndex < e.getNumberOfLevels(); levelIndex++ {
		levelNumber := levelIndex + 1
		maxSSTablesNumber := int(math.Pow(T, float64(levelNumber)))

		if len(e.sstables[levelIndex]) <= maxSSTablesNumber {
			// На текущем уровне нет перегруза по количеству sstables
			continue
		}

		if levelIndex == len(e.sstables)-1 {
			// Текущий уровень последний
			// Тогда просто перемещаем первый sstable из текущего уровня на следующий
			firstTable := e.sstables[levelIndex][0]
			e.sstables[levelIndex] = e.sstables[levelIndex][1:] // TODO: оптимизировать!
			e.sstables = append(e.sstables, []*SSTableWrapper{firstTable})
			break
		}

		// interceptionTable[i] -> массив индексов SSTableWrapper, с которыми пересекается i-ый sstable текущего уровня
		intersectionTable := make([][]int, len(e.sstables[levelIndex]))

		for curLevelIndex := 0; curLevelIndex < len(e.sstables[levelIndex]); curLevelIndex++ {
			for nextLevelIndex := 0; nextLevelIndex < len(e.sstables[levelIndex+1]); nextLevelIndex++ {
				// Есть пересечение
				if !doIntersect(e.sstables[levelIndex][curLevelIndex], e.sstables[levelIndex+1][nextLevelIndex]) {
					continue
				}

				intersectionTable[curLevelIndex] = append(intersectionTable[curLevelIndex], nextLevelIndex)
			}
		}

		if levelIndex == 0 {
			// Рассматриваем все таблицы текущего (нулевого) уровня, так как они могут пересекаться по ключам

			var allNextLevelIndexesMap = map[int]int{}

			for curLevelIndex := 0; curLevelIndex < len(e.sstables[levelIndex]); curLevelIndex++ {
				for _, i := range intersectionTable[levelIndex] {
					allNextLevelIndexesMap[i] = i
				}
			}

			allNextLevelIndexes := make([]int, 0, len(allNextLevelIndexesMap))
			for _, i := range allNextLevelIndexesMap {
				allNextLevelIndexes = append(allNextLevelIndexes, i)
			}

			moveBottom := make([][]bool, 2)
			creationTime := make([][]time.Time, 2)
			bottomIterators := make([][]iterator.Iterator, 2)

			moveBottom[0] = make([]bool, len(e.sstables[levelIndex]))
			creationTime[0] = make([]time.Time, len(e.sstables[levelIndex]))
			bottomIterators[0] = make([]iterator.Iterator, len(e.sstables[levelIndex]))

			moveBottom[1] = make([]bool, len(allNextLevelIndexes))
			creationTime[1] = make([]time.Time, len(allNextLevelIndexes))
			bottomIterators[1] = make([]iterator.Iterator, len(allNextLevelIndexes))

			for curLevelIndex := 0; curLevelIndex < len(e.sstables[levelIndex]); curLevelIndex++ {
				moveBottom[0][curLevelIndex] = true
				creationTime[0][curLevelIndex] = e.sstables[levelIndex][curLevelIndex].getCreationTime()
				curIterator, err := e.sstables[levelIndex][curLevelIndex].getReader().Iterator(nil, nil)
				if err != nil {
					// TODO: исправить сообщение
					return fmt.Errorf("lsm compaction: get bottom iterator: %w", err)
				}

				bottomIterators[0][curLevelIndex] = curIterator
			}

			for i, nextLevelIndex := range allNextLevelIndexes {
				moveBottom[1][i] = true
				creationTime[1][i] = e.sstables[levelIndex+1][nextLevelIndex].getCreationTime()
				curIterator, err := e.sstables[levelIndex+1][nextLevelIndex].getReader().Iterator(nil, nil)
				if err != nil {
					// TODO: исправить сообщение
					return fmt.Errorf("lsm compaction: get bottom iterator: %w", err)
				}

				bottomIterators[1][i] = curIterator
			}

			it := &Iterator{
				memTableIterator:     nil,
				moveMemTableIterator: false,
				sstablesIterators:    bottomIterators,
				moveSSTables:         moveBottom,
				sstablesCreationTime: creationTime,
				trackTombstones:      true,
			}

			// TODO: копипаста с flush

			sstableWrapper, err := createSSTable(e.directory, it)
			if err != nil {
				return fmt.Errorf("lsm compaction: creating sstable: %w", err)
			}

			// Удаляем все sstable текущего уровня
			for _, r := range e.sstables[levelIndex] {
				if err := r.Remove(); err != nil {
					return fmt.Errorf("lsm compaction: removing sstable file: %w", err)
				}
			}
			e.sstables[levelIndex] = e.sstables[levelIndex][:0]

			// Удаляем sstable следующего уровня
			for _, i := range allNextLevelIndexes {
				r := e.sstables[levelIndex+1][i]
				if err := r.Remove(); err != nil {
					return fmt.Errorf("lsm compaction: removing sstable file: %w", err)
				}
			}
			e.sstables[levelIndex+1] = removeByIndexes(e.sstables[levelIndex+1], allNextLevelIndexes)

			// Добавляем новый sstable на след уровень
			e.sstables[levelIndex+1] = append(e.sstables[levelIndex+1], sstableWrapper)
			continue
		} else {
			// Рассматриваем лишь одну таблицу текущего уровня (которая пересекается с наим числом таблиц следующего)

			minIntersectionIndex := 0
			for i := 1; i < len(intersectionTable); i++ {
				if len(intersectionTable[i]) < len(intersectionTable[minIntersectionIndex]) {
					minIntersectionIndex = i
				}
			}

			// Вообще нет пересечений со след уровнем. Просто перемещаем логически
			if len(intersectionTable[minIntersectionIndex]) == 0 {
				table := e.sstables[levelIndex][minIntersectionIndex]
				e.sstables[levelIndex] = append(e.sstables[levelIndex][:minIntersectionIndex], e.sstables[levelIndex][minIntersectionIndex+1:]...)
				e.sstables[levelIndex+1] = append(e.sstables[levelIndex+1], table)
				continue
			}

			currentLevelSSTable := e.sstables[levelIndex][minIntersectionIndex]
			nextLevelSSTablesIndexes := intersectionTable[minIntersectionIndex]

			// TODO: копипаста с Next
			topIterator, err := currentLevelSSTable.getReader().Iterator(nil, nil)
			if err != nil {
				return fmt.Errorf("lsm compaction: get top iterator: %w", err)
			}

			moveBottom := make([][]bool, 1)
			creationTime := make([][]time.Time, 1)
			bottomIterators := make([][]iterator.Iterator, 1)

			bottomIterators[0] = make([]iterator.Iterator, len(nextLevelSSTablesIndexes))
			moveBottom[0] = make([]bool, len(nextLevelSSTablesIndexes))
			creationTime[0] = make([]time.Time, len(nextLevelSSTablesIndexes))

			for j := 0; j < len(nextLevelSSTablesIndexes); j++ {
				moveBottom[0][j] = true
				creationTime[0][j] = e.sstables[levelIndex+1][nextLevelSSTablesIndexes[j]].getCreationTime()
				bottomIterators[0][j], err = e.sstables[levelIndex+1][nextLevelSSTablesIndexes[j]].getReader().Iterator(nil, nil)
				if err != nil {
					return fmt.Errorf("lsm compaction: get bottom iterator: %w", err)
				}
			}

			it := &Iterator{
				memTableIterator:     topIterator,
				moveMemTableIterator: true,
				sstablesIterators:    bottomIterators,
				moveSSTables:         moveBottom,
				sstablesCreationTime: creationTime,
				trackTombstones:      true,
			}

			// TODO: копипаста с flush

			sstableWrapper, err := createSSTable(e.directory, it)
			if err != nil {
				return fmt.Errorf("lsm compaction: creating sstable: %w", err)
			}

			// Удаляем sstable с текущего уровня
			r := e.sstables[levelIndex][minIntersectionIndex]
			if err := r.Remove(); err != nil {
				return fmt.Errorf("lsm compaction: removing sstable file: %w", err)
			}
			e.sstables[levelIndex] = append(e.sstables[levelIndex][:minIntersectionIndex], e.sstables[levelIndex][minIntersectionIndex+1:]...)

			// Удаляем sstable следующего уровня
			for _, i := range nextLevelSSTablesIndexes {
				r := e.sstables[levelIndex+1][i]
				if err := r.Remove(); err != nil {
					return fmt.Errorf("lsm compaction: removing sstable file: %w", err)
				}
			}
			e.sstables[levelIndex+1] = removeByIndexes(e.sstables[levelIndex+1], nextLevelSSTablesIndexes)

			// Добавляем новый sstable на след уровень
			e.sstables[levelIndex+1] = append(e.sstables[levelIndex+1], sstableWrapper)

			continue
		}

	}

	return nil
}

func (e *Engine) initFromDirectory() error {
	directory := e.directory

	directoryEntries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("lsm initFromDirectory: reading directory %s: %w", directory, err)
	}

	hasWAL := false

	for _, entry := range directoryEntries {
		if entry.Name() == WALFileName {
			hasWAL = true
			continue
		}

		// Initialize an object for sstable
		sstableWrapper, err := readSSTable(entry.Name(), directory)
		if err != nil {
			return fmt.Errorf("lsm initFromDirectory: reading sstable %s: %w", entry.Name(), err)
		}

		e.addSSTableToFirstLevel(sstableWrapper)
	}

	if err := e.compaction(); err != nil {
		return fmt.Errorf("lsm initFromDirectory: compaction sstable files: %w", err)
	}

	if hasWAL {
		if err := e.recoverFromWAL(); err != nil {
			return fmt.Errorf("lsm initFromDirectory: recovering from WAL: %w", err)
		}
		return nil
	}

	if err := e.initWAL(); err != nil {
		return fmt.Errorf("lsm initFromDirectory: initializing WAL: %w", err)
	}

	return nil
}

func (e *Engine) recoverFromWAL() error {
	walFilePath := path.Join(e.directory, WALFileName)
	oldWALFilePath := path.Join(e.directory, fmt.Sprintf("%s.old", WALFileName))

	// Переименовываем старый WAL и открываем для чтения
	if err := os.Rename(walFilePath, oldWALFilePath); err != nil {
		return fmt.Errorf("lsm recoverFromWAL: renaming WAL from %s to %s: %w", walFilePath, oldWALFilePath, err)
	}
	oldWALFile, err := os.OpenFile(oldWALFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("lsm recoverFromWAL: creating WAL file %s: %w", oldWALFilePath, err)
	}

	// Инициализируем новый WAL
	if err := e.initWAL(); err != nil {
		return fmt.Errorf("lsm recoverFromWAL: initializing WAL: %w", err)
	}

	// Создаём итератор для чтения старого WAL
	walTmpFileStat, err := oldWALFile.Stat()
	if err != nil {
		return fmt.Errorf("lsm recoverFromWAL: old WAL file stat() %s: %w", oldWALFilePath, err)
	}
	walReader := wal.NewReader(oldWALFile, walTmpFileStat.Size())
	if err := walReader.ValidateChecksums(); err != nil {
		return fmt.Errorf("lsm recoverFromWAL: old WAL file checksums validation %s: %w", oldWALFilePath, err)
	}
	walIterator := walReader.Iterator()

	// Читаем старый WAL
	for {
		record, ok, err := walIterator.Next()
		if err != nil {
			return fmt.Errorf("lsm recoverFromWAL: reading old WAL %s: %w", oldWALFilePath, err)
		}
		if !ok {
			break
		}

		if record.Type == wal.OpPut {
			if err := e.addToMemtable(record.Key, record.Value); err != nil {
				return fmt.Errorf("lsm recoverFromWAL: recovery crash put: %w", err)
			}
		}
		if record.Type == wal.OpDelete {
			if err := e.Delete(record.Key); err != nil {
				return fmt.Errorf("lsm recoverFromWAL: recovery crash delete: %w", err)
			}
		}
	}

	// Закрываем и удаляем старый WAL после его прочтения
	if err := oldWALFile.Close(); err != nil {
		return fmt.Errorf("lsm recoverFromWAL: closing old WAL file %s: %w", oldWALFilePath, err)
	}
	if err := os.Remove(oldWALFilePath); err != nil {
		return fmt.Errorf("lsm recoverFromWAL: removing old WAL file %s: %w", oldWALFilePath, err)
	}

	return nil
}

func (e *Engine) initWAL() error {
	walFilePath := path.Join(e.directory, WALFileName)

	walFile, err := os.Create(walFilePath)
	if err != nil {
		return fmt.Errorf("lsm initWAL: creating WAL file %s: %w", walFilePath, err)
	}

	e.walFile = walFile
	e.walWriter = wal.NewWriter(walFile)

	return nil
}

func (e *Engine) addToMemtable(key []byte, value []byte) error {
	if err := e.memtable.Put(key, value); err != nil {
		return fmt.Errorf("lsm addToMemtable: adding record to memtable: %w", err)
	}

	e.memtableSize += len(key) + len(value)

	if err := e.flushIfNeeded(); err != nil {
		return fmt.Errorf("lsm addToMemtable: flushing failed: %w", err)
	}

	return nil
}

func (e *Engine) findCandidates(key []byte, levelIndex int) []*SSTableWrapper {
	currentLevelTables := e.sstables[levelIndex]

	if levelIndex == 0 {
		return currentLevelTables
	}

	for _, t := range currentLevelTables {
		r := t.getReader()
		if bytes.Compare(r.GetFirstKey(), key) <= 0 && bytes.Compare(key, r.GetLastKey()) <= 0 {
			res := make([]*SSTableWrapper, 1)
			res[0] = t
			return res
		}
	}

	return make([]*SSTableWrapper, 0)
}

func (e *Engine) hasSSTables() bool {
	return len(e.sstables) > 0
}

func (e *Engine) getNumberOfLevels() int {
	return len(e.sstables)
}

func (e *Engine) getSSTables(levelIndex int) []*SSTableWrapper {
	return e.sstables[levelIndex]
}

func (e *Engine) addSSTableToFirstLevel(sstableWrapper *SSTableWrapper) {
	if len(e.sstables) == 0 {
		e.sstables = [][]*SSTableWrapper{{sstableWrapper}}
	} else {
		e.sstables[0] = append(e.sstables[0], sstableWrapper)
	}
}
