package sstable

import "fmt"

type Iterator struct {
	reader                   *Reader
	index                    int64
	endIndex                 int64
	currentBlockIndex        int64
	currentBlockLastKeyIndex int64
}

func NewIterator(reader *Reader, startIndex, endIndex, startBlockIndex int64) *Iterator {
	var currentBlockLastKeyIndex int64

	if startBlockIndex < 0 {
		// Чтобы позволить подавать currentBlockLastKeyIndex=-1 для крайнего случая, когда 0 блоков
		currentBlockLastKeyIndex = -1
	} else {
		currentBlockLastKeyIndex = reader.blocks[startBlockIndex].lastKeyIndex
	}

	return &Iterator{
		reader:                   reader,
		index:                    startIndex,
		endIndex:                 endIndex,
		currentBlockLastKeyIndex: currentBlockLastKeyIndex,
		currentBlockIndex:        startBlockIndex,
	}
}

func (it *Iterator) Next() (key, value []byte, ok bool, err error) {
	if it.index == -1 || it.index == it.endIndex {
		return nil, nil, false, nil
	}

	key, value, nextIndex, err := it.reader.readRecord(it.index, true)
	if err != nil {
		return nil, nil, false, fmt.Errorf("sstable iterator next: failed to read record: %w", err)
	}

	if it.index == it.currentBlockLastKeyIndex {
		// текущий блок не последний
		if it.currentBlockIndex+1 < int64(len(it.reader.blocks)) {
			it.currentBlockIndex++
			currentBlock := it.reader.blocks[it.currentBlockIndex]
			it.index = currentBlock.firstKeyIndex
			it.currentBlockLastKeyIndex = currentBlock.lastKeyIndex
		} else {
			it.index = -1 // дальше блоков нет
		}
	} else {
		it.index = nextIndex
	}

	return key, value, true, nil
}

func (it *Iterator) Close() error {
	it.index = -1
	return nil
}
