package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"kvschool/internal/iterator"
	"kvschool/internal/skiplist"
	"kvschool/internal/sstable"
	"kvschool/internal/wal"
	"log"
	"math"
	"sort"

	//"math"
	"os"
	"path"
	"time"
)

var ErrNotFound = errors.New("lsm: ключ не найден")

const (
	T = 2
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

type pairSource struct {
	isMemTable   bool
	levelIndex   int
	index        int
	iterator     iterator.Iterator
	creationTime time.Time
}

func compareSources(ps1 *pairSource, ps2 *pairSource) int {
	if ps1.iterator == ps2.iterator {
		log.Fatalln("Unexpected")
		return 0
	}

	if ps1.isMemTable {
		return 1
	}

	if ps2.isMemTable {
		return -1
	}

	if ps1.levelIndex == ps2.levelIndex {
		if ps1.creationTime.Equal(ps2.creationTime) {
			log.Fatalln("Comparing sstables on the same level with the same creation time")
		}

		if ps2.creationTime.Before(ps1.creationTime) {
			return 1
		}

		return -1
	}

	if ps1.levelIndex < ps2.levelIndex {
		return 1
	}

	return -1
}

type pair struct {
	key    []byte
	value  []byte
	source pairSource
}

type Iterator struct {
	memTableIterator     iterator.Iterator
	moveMemTableIterator bool
	sstablesIterators    [][]iterator.Iterator
	sstablesCreationTime [][]time.Time
	moveSSTables         [][]bool
	pairs                []pair
	toMove               pairSource
	isEmpty              bool
	trackTombstones      bool
}

func (it *Iterator) Next() (key []byte, value []byte, ok bool, err error) {
	if it.isEmpty {
		return nil, nil, false, nil
	}

	for {
		memTableOk := false

		if it.moveMemTableIterator {
			memTableKey, memTableValue, memTableOkTmp, memTableErr := it.memTableIterator.Next()
			memTableSource := pairSource{
				isMemTable: true,
				levelIndex: 0,
				index:      0,
				iterator:   it.memTableIterator,
			}

			memTableOk = memTableOkTmp

			if memTableErr != nil {
				return nil, nil, false, fmt.Errorf("%w", err)
			}

			if memTableOkTmp {
				found := false

				for i := 0; i < len(it.pairs); i++ {
					previousPair := it.pairs[i]
					previousSource := it.pairs[i].source
					if bytes.Equal(previousPair.key, memTableKey) {
						found = true

						// Мы более новые, поэтому меняем value по ключу
						if compareSources(&memTableSource, &previousSource) > 0 {
							it.pairs[i].value = memTableValue
							it.pairs[i].source = pairSource{
								isMemTable: true,
								levelIndex: 0,
								index:      0,
								iterator:   it.memTableIterator,
							}

							it.moveSSTables[previousSource.levelIndex][previousSource.index] = true
						}
					}
				}

				if !found {
					it.pairs = append(it.pairs, pair{
						key:   memTableKey,
						value: memTableValue,
						source: pairSource{
							isMemTable: true,
							levelIndex: 0,
							index:      0,
							iterator:   it.memTableIterator,
						},
					})
				}
			} else {
				// TODO: оптимизировать! (если не ok, то дальше нет смысла вызывать Next для memTableIterator
			}

			it.moveMemTableIterator = false
		}

		hasSSTablesToMove := false // true - есть ещё sstables, у которых можно продвинуться
		for levelIndex := 0; levelIndex < len(it.sstablesIterators); levelIndex++ {
			for index := 0; index < len(it.sstablesIterators[levelIndex]); index++ {
				if !it.moveSSTables[levelIndex][index] {
					continue
				}

				currentIterator := it.sstablesIterators[levelIndex][index]
				currentSource := pairSource{
					isMemTable:   false,
					levelIndex:   levelIndex,
					index:        index,
					iterator:     currentIterator,
					creationTime: it.sstablesCreationTime[levelIndex][index],
				}

				currentKey, currentValue, currentOk, currentErr := currentIterator.Next()
				hasSSTablesToMove = hasSSTablesToMove || currentOk

				if currentErr != nil {
					return nil, nil, false, fmt.Errorf("%w", err)
				}
				if currentOk {
					found := false

					for i := 0; i < len(it.pairs); i++ {
						previousPair := it.pairs[i]
						previousSource := it.pairs[i].source
						if bytes.Equal(previousPair.key, currentKey) {
							found = true

							// Мы более новые, поэтому меняем value по ключу
							if compareSources(&currentSource, &previousSource) > 0 {
								it.pairs[i].value = currentValue
								it.pairs[i].source = pairSource{
									isMemTable:   false,
									levelIndex:   levelIndex,
									index:        index,
									iterator:     currentIterator,
									creationTime: it.sstablesCreationTime[levelIndex][index],
								}

								it.moveSSTables[previousSource.levelIndex][previousSource.index] = true
							} else {
								it.moveSSTables[currentSource.levelIndex][currentSource.index] = true
							}
						}
					}

					if !found {
						it.pairs = append(it.pairs, pair{
							key:   currentKey,
							value: currentValue,
							source: pairSource{
								isMemTable:   false,
								levelIndex:   levelIndex,
								index:        index,
								iterator:     currentIterator,
								creationTime: it.sstablesCreationTime[levelIndex][index],
							},
						})
						it.moveSSTables[levelIndex][index] = false
					}
				} else {
					// TODO: оптимизировать! (если не ok, то дальше нет смысла вызывать Next для (levelIndex, index)
				}

			}
		}

		SortPairs(it.pairs)

		if len(it.pairs) == 0 {
			if !memTableOk && !hasSSTablesToMove {
				it.isEmpty = true
				break
			} else {
				continue
			}
		}

		firstPair := it.pairs[0]
		it.pairs = it.pairs[1:]
		if firstPair.source.isMemTable {
			it.moveMemTableIterator = true
		} else {
			it.moveSSTables[firstPair.source.levelIndex][firstPair.source.index] = true
		}

		realValue, deleted := extractKeyValue(firstPair.value)

		if it.trackTombstones {
			return firstPair.key, firstPair.value, true, nil
		}

		if deleted {
			continue
		}

		return firstPair.key, realValue, true, nil
	}

	return nil, nil, false, nil
}

func (it *Iterator) Close() error {
	it.isEmpty = true

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

// Engine — основной движок CDR Storage.
// Координирует работу Memtable, WAL и SSTables.
// Отвечает за Compaction (сборку мусора).
type Engine struct {
	memTable     *skiplist.SkipList
	memTableSize int
	sstables     [][]*SSTableWrapper

	walFile   *os.File
	walWriter *wal.Writer

	directory      string
	flushThreshold int
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
		memTable:       skiplist.New(42),
		sstables:       make([][]*SSTableWrapper, 0),
		directory:      options.Dir,
		flushThreshold: options.MemtableFlushThreshold,
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

			sstableWrapper, err := readSSTable(entry.Name(), options.Dir)
			if err != nil {
				return nil, fmt.Errorf("lsm Open: reading sstable %s: %w", entry.Name(), err)
			}

			if len(engine.sstables) == 0 {
				engine.sstables = [][]*SSTableWrapper{{sstableWrapper}}
			} else {
				engine.sstables[0] = append(engine.sstables[0], sstableWrapper)
			}
		}

		if len(engine.sstables) > 0 {
			if err := engine.compaction(); err != nil {
				return nil, fmt.Errorf("lsm Open: compaction sstable files: %w", err)
			}
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
				if err := engine.Put(record.Key, record.Value); err != nil {
					return nil, fmt.Errorf("lsm Open: recovery crash put: %w", err)
				}
			} else {
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
	// Поиск в memtable
	value, err := e.memTable.Get(key)
	if err == nil {
		// Нашли ключ в skiplist

		realValue, deleted := extractKeyValue(value)
		if deleted {
			return nil, ErrNotFound
		}

		return realValue, nil
	}

	// Нет SSTables на диске
	if len(e.sstables) == 0 {
		return nil, ErrNotFound
	}

	for levelNumber := 1; levelNumber <= len(e.sstables); levelNumber++ {
		candidates := e.findCandidates(key, levelNumber)

		// На текущем уровне нет кандидатов
		if len(candidates) == 0 {
			if levelNumber == len(e.sstables) {
				// Текущий уровень последний (значит в хранилище вообще ключа нет)
				return nil, ErrNotFound
			}

			// Опускаемся на уровень ниже
			continue
		}

		foundValues := make([][]byte, 0, len(candidates))
		foundValuesTime := make([]time.Time, 0, len(candidates))

		// Проходимся по кандидатам и проверяем, действительно ли в них есть искомый ключ
		for _, c := range candidates {
			it, err := c.getReader().Iterator(nil, nil)
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

				// Нашли ключ
				foundValues = append(foundValues, foundValue)
				foundValuesTime = append(foundValuesTime, c.getCreationTime())
			}
		}

		if len(foundValues) == 0 {
			if levelNumber == len(e.sstables) {
				// Текущий уровень последний (значит в хранилище вообще ключа нет)
				return nil, ErrNotFound
			}

			// Опускаемся на уровень ниже
			continue
		}

		minTimeIndex := 0
		minTime := foundValuesTime[0]
		for i := 1; i < len(foundValues); i++ {
			if foundValuesTime[i].After(minTime) {
				minTime = foundValuesTime[i]
				minTimeIndex = i
			}
		}

		realValue, deleted := extractKeyValue(foundValues[minTimeIndex])
		if deleted {
			return nil, ErrNotFound
		}

		return realValue, nil
	}

	return nil, ErrNotFound
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
	memTableIterator, err := e.memTable.Scan(start, end)
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

	for i := 0; i < len(e.sstables); i++ {
		for j := 0; j < len(e.sstables[i]); j++ {
			el := e.sstables[i][j]
			if err := el.Close(); err != nil {
				return fmt.Errorf("lsm Remove: closing sstable file %d-%d: %w", i, j, err)
			}
		}
	}

	return errors.Join(walClosingError, walFileClosingError)
}

func (e *Engine) flush() error {
	memtableIterator, err := e.memTable.Scan(nil, nil)
	if err != nil {
		return fmt.Errorf("lsm flush: creating memtable iterator: %w", err)
	}

	sstableWrapper, err := createSSTable(e.directory, memtableIterator)
	if err != nil {
		return fmt.Errorf("lsm flush: creating sstable: %w", err)
	}

	e.memTable.Clear()
	e.memTableSize = 0

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

	if len(e.sstables) == 0 {
		zeroLevelSSTables := []*SSTableWrapper{sstableWrapper}
		e.sstables = append(e.sstables, zeroLevelSSTables)
	} else {
		e.sstables[0] = append(e.sstables[0], sstableWrapper)
	}

	if err := e.compaction(); err != nil {
		return fmt.Errorf("lsm flush: compaction: %w", err)
	}

	return nil
}

func (e *Engine) compaction() error {
	// TODO: для каждого уровня нужно проверять len(e.sstables) вручную! => переделать в цикл while
	for levelIndex := 0; ; levelIndex++ {
		if levelIndex >= len(e.sstables) {
			break
		}

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
				if !doesIntersect(e.sstables[levelIndex][curLevelIndex].getReader(), e.sstables[levelIndex+1][nextLevelIndex].getReader()) {
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

func (e *Engine) findCandidates(key []byte, levelNumber int) []*SSTableWrapper {
	currentLevelTables := e.sstables[levelNumber-1]

	if levelNumber == 1 {
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

func (e *Engine) createSSTable() {

}

func doesIntersect(r1 *sstable.Reader, r2 *sstable.Reader) bool {
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
