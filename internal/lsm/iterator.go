package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"kvschool/internal/iterator"
	"log"
	"time"
)

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

		augmentedValue := firstPair.value
		value, deleted := parseAugmentedValue(augmentedValue)

		if it.trackTombstones {
			return firstPair.key, firstPair.value, true, nil
		}

		if deleted {
			continue
		}

		return firstPair.key, value, true, nil
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
