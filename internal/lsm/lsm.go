package lsm

import (
	"errors"
	"fmt"
	. "kvschool/internal/helpers"
	"kvschool/internal/iterator"
	"kvschool/internal/skiplist"
	"kvschool/internal/wal"
	"math"
	"os"
	"path"
	"time"
)

var ErrNotFound = errors.New("lsm: ключ не найден")

type Options struct {
	Dir                    string
	MemtableFlushThreshold int
	LevelBase              int
	Seed                   int
}

type Engine struct {
	memtable               *skiplist.SkipList
	memtableSize           int
	memtableFlushThreshold int

	levelBase float64
	tables    [][]*sstableWrapper

	wal *walWrapper

	directory string
}

func Open(options Options) (*Engine, error) {
	engine := &Engine{
		memtable:               skiplist.New(int64(options.Seed)),
		tables:                 make([][]*sstableWrapper, 0),
		directory:              options.Dir,
		memtableFlushThreshold: options.MemtableFlushThreshold,
		levelBase:              float64(options.LevelBase),
	}

	if err := makeDirectoryIfNotExists(engine.directory); err != nil {
		return nil, fmt.Errorf("lsm Open: making directory %s: %w", engine.directory, err)
	}

	if err := engine.initFromDirectory(); err != nil {
		return nil, fmt.Errorf("lsm Open: initializing from directory %s: %w", engine.directory, err)
	}

	return engine, nil
}

func (e *Engine) Put(key, value []byte) error {
	walRecord := wal.Record{
		Type:  wal.OpPut,
		Key:   key,
		Value: value,
	}
	if err := e.wal.getWriter().Append(walRecord); err != nil {
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
	if !e.hasTables() {
		return nil, ErrNotFound
	}

	numberOfLevels := e.getNumberOfLevels()
	for levelIndex := 0; levelIndex < e.getNumberOfLevels(); levelIndex++ {
		candidates := e.findKeyCandidates(key, levelIndex)
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

				if !KeysEqual(key, foundKey) {
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
	if err := e.wal.getWriter().Append(walRecord); err != nil {
		return fmt.Errorf("lsm Delete: adding record to WAL: %w", err)
	}

	augmentedValue := augmentValue(nil, true)
	if err := e.addToMemtable(key, augmentedValue); err != nil {
		return fmt.Errorf("lsm Delete: writing to memtable: %w", err)
	}

	return nil
}

func (e *Engine) Scan(start []byte, end []byte) (iterator.Iterator, error) {
	topLevelSource, err := createMemtablePairSource(e, start, end)
	if err != nil {
		return nil, fmt.Errorf("lsm Scan: creating memtable pair source: %w", err)
	}

	bottomLevelSources := make([][]*pairSource, e.getNumberOfLevels())
	for levelIndex := 0; levelIndex < e.getNumberOfLevels(); levelIndex++ {
		bottomLevelSources[levelIndex] = make([]*pairSource, e.getNumberOfTables(levelIndex))

		for index := 0; index < e.getNumberOfTables(levelIndex); index++ {
			bottomLevelSources[levelIndex][index], err = createSSTablePairSource(e, levelIndex, index, start, end)

			if err != nil {
				return nil, fmt.Errorf("lsm Scan: creating sstable pair source: %w", err)
			}
		}
	}

	return &Iterator{
		topLevelSource:     topLevelSource,
		bottomLevelSources: bottomLevelSources,
	}, nil
}

func (e *Engine) Close() error {
	// 1 - для ошибки закрытия WAL
	errorsList := make([]error, 1+e.getTotalNumberOfTables())
	errorsList[0] = e.wal.close()

	errorIndex := 1
	for levelIndex := 0; levelIndex < e.getNumberOfLevels(); levelIndex++ {
		for _, sstableWrapper := range e.getTables(levelIndex) {
			errorsList[errorIndex] = sstableWrapper.close()
			errorIndex++
		}
	}

	return errors.Join(errorsList...)
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
	if err := e.wal.close(); err != nil {
		return fmt.Errorf("lsm flush: closing WAL writer: %w", err)
	}
	if err := e.initWAL(); err != nil {
		return fmt.Errorf("lsm flush: resetting WAL: %w", err)
	}

	e.pushTable(sstableWrapper)

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
		maxSSTablesNumber := int(math.Pow(e.levelBase, float64(levelNumber)))

		if len(e.tables[levelIndex]) <= maxSSTablesNumber {
			// На текущем уровне нет перегруза по количеству sstables
			continue
		}

		if levelIndex == len(e.tables)-1 {
			// Текущий уровень последний
			// Тогда просто перемещаем первый sstable из текущего уровня на следующий
			firstTable := e.tables[levelIndex][0]
			e.tables[levelIndex] = e.tables[levelIndex][1:] // TODO: оптимизировать!
			e.tables = append(e.tables, []*sstableWrapper{firstTable})
			break
		}

		// interceptionTable[i] -> массив индексов SSTableWrapper, с которыми пересекается i-ый sstable текущего уровня
		intersectionTable := make([][]int, len(e.tables[levelIndex]))

		for curLevelIndex := 0; curLevelIndex < len(e.tables[levelIndex]); curLevelIndex++ {
			for nextLevelIndex := 0; nextLevelIndex < len(e.tables[levelIndex+1]); nextLevelIndex++ {
				// Есть пересечение
				if !doIntersect(e.tables[levelIndex][curLevelIndex], e.tables[levelIndex+1][nextLevelIndex]) {
					continue
				}

				intersectionTable[curLevelIndex] = append(intersectionTable[curLevelIndex], nextLevelIndex)
			}
		}

		if levelIndex == 0 {
			// Рассматриваем все таблицы текущего (нулевого) уровня, так как они могут пересекаться по ключам

			// Ищем таблицы следующего уровня, которые пересекаются с таблицами текущего
			var allNextLevelIndexesMap = map[int]int{}
			for curLevelIndex := 0; curLevelIndex < len(e.tables[levelIndex]); curLevelIndex++ {
				for _, i := range intersectionTable[curLevelIndex] {
					allNextLevelIndexesMap[i] = i
				}
			}
			allNextLevelIndexes := make([]int, 0, len(allNextLevelIndexesMap))
			for _, i := range allNextLevelIndexesMap {
				allNextLevelIndexes = append(allNextLevelIndexes, i)
			}

			// Создаём итератор
			bottomLevelSources := make([][]*pairSource, 2)
			bottomLevelSources[0] = make([]*pairSource, e.getNumberOfTables(levelIndex))
			bottomLevelSources[1] = make([]*pairSource, len(allNextLevelIndexes))
			for index := 0; index < e.getNumberOfTables(levelIndex); index++ {
				ps, err := createSSTablePairSource(e, levelIndex, index, nil, nil)
				if err != nil {
					return fmt.Errorf("lsm compaction: create sstable pair source %d-%d: %w", levelIndex, index, err)
				}
				bottomLevelSources[0][index] = ps
			}
			nextLevelIndex := levelIndex + 1
			for i, tableIndex := range allNextLevelIndexes {
				ps, err := createSSTablePairSource(e, nextLevelIndex, tableIndex, nil, nil)
				if err != nil {
					return fmt.Errorf("lsm compaction: create sstable pair source %d-%d: %w", nextLevelIndex, tableIndex, err)
				}
				bottomLevelSources[1][i] = ps
			}
			it := &Iterator{
				bottomLevelSources: bottomLevelSources,
				trackTombstones:    true,
			}

			// Создаём новую таблицу
			sstableWrapper, err := createSSTable(e.directory, it)
			if err != nil {
				return fmt.Errorf("lsm compaction: creating sstable: %w", err)
			}

			// Удаляем все таблицы текущего уровня
			for _, r := range e.tables[levelIndex] {
				if err := r.remove(); err != nil {
					return fmt.Errorf("lsm compaction: removing sstable file: %w", err)
				}
			}
			e.tables[levelIndex] = e.tables[levelIndex][:0]

			// Удаляем таблицы следующего уровня (с которыми было пересечение)
			for _, i := range allNextLevelIndexes {
				r := e.tables[levelIndex+1][i]
				if err := r.remove(); err != nil {
					return fmt.Errorf("lsm compaction: removing sstable file: %w", err)
				}
			}
			e.tables[levelIndex+1] = removeByIndexes(e.tables[levelIndex+1], allNextLevelIndexes)

			// Добавляем новую таблицу на следующий уровень
			e.tables[levelIndex+1] = append(e.tables[levelIndex+1], sstableWrapper)
		} else {
			// Рассматриваем лишь одну таблицу текущего уровня (которая пересекается с наим числом таблиц следующего)

			// Ищем таблицу текущего уровня, которая пересекается с наименьшим числом таблиц следующего
			minIntersectionIndex := 0
			for i := 1; i < len(intersectionTable); i++ {
				if len(intersectionTable[i]) < len(intersectionTable[minIntersectionIndex]) {
					minIntersectionIndex = i
				}
			}

			// Вообще нет пересечений со след уровнем. Просто перемещаем логически
			if len(intersectionTable[minIntersectionIndex]) == 0 {
				table := e.tables[levelIndex][minIntersectionIndex]
				e.tables[levelIndex] = append(e.tables[levelIndex][:minIntersectionIndex], e.tables[levelIndex][minIntersectionIndex+1:]...)
				e.tables[levelIndex+1] = append(e.tables[levelIndex+1], table)
				continue
			}

			// Создаём итератор
			nextLevelTablesIndexes := intersectionTable[minIntersectionIndex]
			bottomLevelSources := make([][]*pairSource, 2)
			ps, err := createSSTablePairSource(e, levelIndex, minIntersectionIndex, nil, nil)
			if err != nil {
				return fmt.Errorf("lsm compaction: creating sstable pair source %d-%d: %w", levelIndex, minIntersectionIndex, err)
			}
			bottomLevelSources[0] = []*pairSource{ps}
			bottomLevelSources[1] = make([]*pairSource, len(nextLevelTablesIndexes))
			nextLevelIndex := levelIndex + 1
			for index := 0; index < len(nextLevelTablesIndexes); index++ {
				ps, err := createSSTablePairSource(e, nextLevelIndex, index, nil, nil)
				if err != nil {
					return fmt.Errorf("lsm compaction: creating sstable pair source %d-%d: %w", nextLevelIndex, index, err)
				}
				bottomLevelSources[1][index] = ps
			}
			it := &Iterator{
				bottomLevelSources: bottomLevelSources,
				trackTombstones:    true,
			}

			// Создаём новую таблицу
			newTable, err := createSSTable(e.directory, it)
			if err != nil {
				return fmt.Errorf("lsm compaction: creating sstable: %w", err)
			}

			// Удаляем таблицу с текущего уровня
			t := e.tables[levelIndex][minIntersectionIndex]
			if err := t.remove(); err != nil {
				return fmt.Errorf("lsm compaction: removing sstable file: %w", err)
			}
			e.tables[levelIndex] = append(e.tables[levelIndex][:minIntersectionIndex], e.tables[levelIndex][minIntersectionIndex+1:]...)

			// Удаляем таблицы следующего уровня (с которыми было пересечение)
			for _, i := range nextLevelTablesIndexes {
				t := e.tables[levelIndex+1][i]
				if err := t.remove(); err != nil {
					return fmt.Errorf("lsm compaction: removing sstable file: %w", err)
				}
			}
			e.tables[levelIndex+1] = removeByIndexes(e.tables[levelIndex+1], nextLevelTablesIndexes)

			// Добавляем новую таблицу на следующий уровень
			e.tables[levelIndex+1] = append(e.tables[levelIndex+1], newTable)
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

		e.pushTable(sstableWrapper)
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

	e.wal = &walWrapper{
		file:   walFile,
		writer: wal.NewWriter(walFile),
	}

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

func (e *Engine) findKeyCandidates(key []byte, levelIndex int) []*sstableWrapper {
	currentLevelTables := e.getTables(levelIndex)

	if levelIndex == 0 {
		candidates := make([]*sstableWrapper, 0, len(currentLevelTables))

		for _, table := range currentLevelTables {
			if probablyContains(key, table) {
				candidates = append(candidates, table)
			}
		}

		return candidates
	}

	for _, table := range currentLevelTables {
		if probablyContains(key, table) {
			return []*sstableWrapper{table}
		}
	}

	return make([]*sstableWrapper, 0)
}

func (e *Engine) hasTables() bool {
	return len(e.tables) > 0
}

func (e *Engine) getNumberOfLevels() int {
	return len(e.tables)
}

func (e *Engine) getTables(levelIndex int) []*sstableWrapper {
	return e.tables[levelIndex]
}

func (e *Engine) getTable(levelIndex int, tableIndex int) *sstableWrapper {
	return e.tables[levelIndex][tableIndex]
}

func (e *Engine) getMemtable() *skiplist.SkipList {
	return e.memtable
}

func (e *Engine) getNumberOfTables(levelIndex int) int {
	return len(e.tables[levelIndex])
}

func (e *Engine) getTotalNumberOfTables() int {
	result := 0

	for levelIndex := 0; levelIndex < e.getNumberOfLevels(); levelIndex++ {
		result += e.getNumberOfTables(levelIndex)
	}

	return result
}

func (e *Engine) pushTable(wrapper *sstableWrapper) {
	if len(e.tables) == 0 {
		e.tables = [][]*sstableWrapper{{wrapper}}
	} else {
		e.tables[0] = append(e.tables[0], wrapper)
	}
}
