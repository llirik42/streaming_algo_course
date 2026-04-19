package sstable

import "fmt"

type Iterator struct {
	reader                   *Reader
	currentIndex             int64
	endIndex                 int64
	currentBlockIndex        int64
	currentBlockLastKeyIndex int64
}

func NewIterator(reader *Reader, startIndex, endIndex, startBlockIndex int64) *Iterator {
	var currentBlockLastKeyIndex int64

	if startBlockIndex < 0 {
		// Чтобы позволить подавать currentBlockLastKeyIndex=-1 для крайнего случая, когда 0 блоков
		currentBlockLastKeyIndex = NoIndex
	} else {
		currentBlockLastKeyIndex = reader.blocksInfo[startBlockIndex].lastKeyIndex
	}

	return &Iterator{
		reader:                   reader,
		currentIndex:             startIndex,
		endIndex:                 endIndex,
		currentBlockLastKeyIndex: currentBlockLastKeyIndex,
		currentBlockIndex:        startBlockIndex,
	}
}

func (it *Iterator) Next() (key, value []byte, ok bool, err error) {
	if it.currentIndex == NoIndex || it.currentIndex == it.endIndex {
		return nil, nil, false, nil
	}

	key, value, nextIndex, err := it.reader.readRecord(it.currentIndex, true)
	if err != nil {
		return nil, nil, false, fmt.Errorf("sstable iterator next: failed to read record: %w", err)
	}

	if it.currentIndex == it.currentBlockLastKeyIndex {
		// текущий блок не последний
		if it.currentBlockIndex+1 < int64(len(it.reader.blocksInfo)) {
			it.currentBlockIndex++
			currentBlock := it.reader.blocksInfo[it.currentBlockIndex]
			it.currentIndex = currentBlock.firstKeyIndex
			it.currentBlockLastKeyIndex = currentBlock.lastKeyIndex
		} else {
			it.currentIndex = NoIndex // дальше блоков нет
		}
	} else {
		it.currentIndex = nextIndex
	}

	return key, value, true, nil
}

func (it *Iterator) Close() error {
	it.currentIndex = NoIndex
	return nil
}
