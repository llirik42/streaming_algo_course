package sstable

import "fmt"

type Iterator struct {
	reader                   *Reader
	currentIndex             int64
	endIndex                 int64
	currentBlockIndex        int64
	currentBlockLastKeyIndex int64
}

func EmptyIterator() *Iterator {
	return &Iterator{
		reader:                   nil,
		currentIndex:             NoIndex,
		endIndex:                 NoIndex,
		currentBlockIndex:        NoIndex,
		currentBlockLastKeyIndex: NoIndex,
	}
}

func NewIterator(reader *Reader, startIndex, endIndex, startBlockIndex int64) *Iterator {
	return &Iterator{
		reader:                   reader,
		currentIndex:             startIndex,
		endIndex:                 endIndex,
		currentBlockLastKeyIndex: reader.getBlock(startBlockIndex).lastKeyIndex,
		currentBlockIndex:        startBlockIndex,
	}
}

func (it *Iterator) Next() (key, value []byte, ok bool, err error) {
	if it.currentIndex == it.endIndex {
		return nil, nil, false, nil
	}

	var nextIndex int64
	key, value, nextIndex, err = it.reader.readRecord(it.currentIndex, true)
	if err != nil {
		return nil, nil, false, fmt.Errorf("sstable iterator next: failed to read record: %w", err)
	}
	ok = true

	if it.currentIndex != it.currentBlockLastKeyIndex {
		// В текущем блоке ещё есть непрочитанные записи
		it.currentIndex = nextIndex
		return
	}

	if it.currentBlockIndex+1 >= int64(len(it.reader.blocksInfo)) {
		// Текущий блок последний (дальше блоков нет)
		it.currentIndex = NoIndex
		return
	}

	// Инициализируем чтение записей из следующего блока
	it.currentBlockIndex++
	currentBlock := it.reader.getBlock(it.currentBlockIndex)
	it.currentIndex = currentBlock.firstKeyIndex
	it.currentBlockLastKeyIndex = currentBlock.lastKeyIndex
	return
}

func (it *Iterator) Close() error {
	it.currentIndex = NoIndex
	it.endIndex = NoIndex
	return nil
}
