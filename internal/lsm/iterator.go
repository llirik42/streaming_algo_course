package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"kvschool/internal/iterator"
	"log"
	"time"
)

type pairSourceInfo struct {
	isTopLevel       bool
	indexOfLevel     int
	indexInsideLevel int
	isAllowedToMove  bool
	creationTime     time.Time
}

type pairSource struct {
	it   iterator.Iterator
	info pairSourceInfo
}

func (ps *pairSource) canMove() bool {
	return ps.info.isAllowedToMove
}

func (ps *pairSource) allowMoving() {
	ps.info.isAllowedToMove = true
}

func (ps *pairSource) disallowMoving() {
	ps.info.isAllowedToMove = false
}

func createSSTablePairSource(e *Engine, levelIndex, tableIndex int, start, end []byte) (*pairSource, error) {
	table := e.getTable(levelIndex, tableIndex)

	tableIterator, err := table.GetReader().Iterator(start, end)
	if err != nil {
		return nil, fmt.Errorf("lsm iterator.createSSTablePairSource: get table iterator %d-%d: %w", levelIndex, tableIndex, err)
	}

	return &pairSource{
		it: tableIterator,
		info: pairSourceInfo{
			indexOfLevel:     levelIndex,
			indexInsideLevel: tableIndex,
			isAllowedToMove:  true,
			creationTime:     table.GetCreationTime(),
		},
	}, nil
}

func createMemtablePairSource(e *Engine, start, end []byte) (*pairSource, error) {
	memtable := e.getMemtable()

	memtableIterator, err := memtable.Scan(start, end)
	if err != nil {
		return nil, fmt.Errorf("lsm iterator.createSSTablePairSource: get memtable iterator: %w", err)
	}

	return &pairSource{
		it: memtableIterator,
		info: pairSourceInfo{
			isTopLevel:      true,
			isAllowedToMove: true,
		},
	}, nil
}

func comparePairSources(ps1 *pairSource, ps2 *pairSource) int {
	if ps1 == ps2 {
		log.Fatalln("Unexpected")
		return 0
	}

	info1 := ps1.info
	info2 := ps2.info

	if info1.isTopLevel {
		return 1
	}

	if info2.isTopLevel {
		return -1
	}

	if info1.indexOfLevel == info2.indexOfLevel {
		t1 := info1.creationTime
		t2 := info2.creationTime

		if t1.Equal(t2) {
			log.Fatalln("Comparing iterators on the same level with the same creation time")
		}

		if t2.Before(t1) {
			return 1
		}

		return -1
	}

	if info1.indexOfLevel < info2.indexOfLevel {
		return 1
	}

	return -1
}

type iteratorPair struct {
	key    []byte
	value  []byte
	source *pairSource
}

type Iterator struct {
	topLevelSource     *pairSource
	bottomLevelSources [][]*pairSource
	pairs              []iteratorPair
	isEmpty            bool
	trackTombstones    bool
}

func (it *Iterator) Next() (key []byte, value []byte, ok bool, err error) {
	if it.isEmpty {
		return nil, nil, false, nil
	}

	for {
		topLevelOk, err := it.processSource(it.topLevelSource)
		if err != nil {
			return nil, nil, false, fmt.Errorf("lsm Iterator.Next: processing memtable iterator: %w", err)
		}

		hasBottomIteratorsAllowedToMove := false
		for levelIndex := 0; levelIndex < len(it.bottomLevelSources); levelIndex++ {
			for index, currentSource := range it.bottomLevelSources[levelIndex] {
				currentOk, err := it.processSource(currentSource)
				if err != nil {
					return nil, nil, false, fmt.Errorf("lsm Iterator.Next: processing bottom iterator %d-%d: %w", levelIndex, index, err)
				}

				hasBottomIteratorsAllowedToMove = hasBottomIteratorsAllowedToMove || currentOk
			}
		}

		sortPairs(it.pairs)

		if len(it.pairs) == 0 {
			if !topLevelOk && !hasBottomIteratorsAllowedToMove {
				it.isEmpty = true
				break
			} else {
				continue
			}
		}

		firstPair := it.pairs[0]
		it.pairs = it.pairs[1:]
		firstPair.source.allowMoving()

		if it.trackTombstones {
			return firstPair.key, firstPair.value, true, nil
		}

		augmentedValue := firstPair.value
		value, isTombstone := parseAugmentedValue(augmentedValue)

		if isTombstone {
			continue
		}

		return firstPair.key, value, true, nil
	}

	return nil, nil, false, nil
}

func (it *Iterator) Close() error {
	it.isEmpty = true

	err1 := it.topLevelSource.it.Close()
	errorsList := []error{err1}

	for i := 0; i < len(it.bottomLevelSources); i++ {
		for j := 0; j < len(it.bottomLevelSources[i]); j++ {
			err := it.bottomLevelSources[i][j].it.Close()
			errorsList = append(errorsList, err)
		}
	}

	return errors.Join(errorsList...)
}

func (it *Iterator) processSource(source *pairSource) (bool, error) {
	if source == nil || !source.canMove() {
		return false, nil
	}

	sourceIterator := source.it

	key, value, ok, err := sourceIterator.Next()
	if err != nil {
		return false, fmt.Errorf("lsm Iterator.processSource(): getting new pair: %w", err)
	}
	source.disallowMoving()

	if ok {
		found := false
		for pairIndex, pair := range it.pairs {
			if bytes.Equal(pair.key, key) {
				found = true

				// Мы более новые, поэтому меняем value по ключу
				previousSource := pair.source
				if comparePairSources(source, previousSource) > 0 {
					it.updatePairs(pairIndex, value, source)
				} else {
					source.allowMoving()
				}
			}
		}

		if !found {
			it.pushPair(key, value, source)
		}
	}

	return true, nil
}

func (it *Iterator) updatePairs(pairIndex int, newValue []byte, newSource *pairSource) {
	it.pairs[pairIndex].source.allowMoving()
	it.pairs[pairIndex].value = newValue
	it.pairs[pairIndex].source = newSource
}

func (it *Iterator) pushPair(key, value []byte, source *pairSource) {
	it.pairs = append(it.pairs, iteratorPair{
		key:    key,
		value:  value,
		source: source,
	})
}
