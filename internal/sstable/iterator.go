package sstable

import "fmt"

type Iterator struct {
	reader                   *Reader
	index                    int64
	endIndex                 int64
	currentBlockLastKeyIndex int64
	currentBlockIndex        int
}

func (it *Iterator) Next() (key, value []byte, ok bool, err error) {
	if it.index == -1 || it.index == it.endIndex {
		return nil, nil, false, nil
	}

	key, value, nextIndex, err := it.reader.readRecord(it.index, true)
	if err != nil {
		return nil, nil, false, fmt.Errorf("sstable iterator: failed to read record: %w", err)
	}

	if it.index == it.currentBlockLastKeyIndex {
		// текущий блок не последний
		if it.currentBlockIndex+1 < len(it.reader.blocksInfo) {
			it.currentBlockIndex++
			currentBlock := it.reader.blocksInfo[it.currentBlockIndex]
			it.index = int64(currentBlock.firstKeyIndex)
			it.currentBlockLastKeyIndex = int64(currentBlock.lastKeyIndex)
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
