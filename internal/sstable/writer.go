package sstable

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"
)

type WriterBlockInfo struct {
	firstKeyIndex  uint64
	lastKeyIndex   uint64
	nextBlockIndex uint64
}

// Writer пишет отсортированные пары key/value (CDR) в файл.
// Формат файла должен позволять чтение без загрузки всего файла в память.
// Обычно это: [Data Block 1] [Data Block 2] ... [Sparse Index] [Footer].
type Writer struct {
	ioWriter            io.Writer
	blocks              []WriterBlockInfo
	previousIndex       uint64
	currentIndex        uint64
	elementaryBlockSize uint64
	hash                hash.Hash
	order               binary.ByteOrder
}

func NewWriter(ioWriter io.Writer) *Writer {
	return &Writer{
		ioWriter:            ioWriter,
		elementaryBlockSize: 4096,
		blocks:              make([]WriterBlockInfo, 0),
		hash:                md5.New(),
		order:               binary.LittleEndian,
	}
}

// Add добавляет пару. Ключи должны быть строго возрастающими.
func (w *Writer) Add(key []byte, value []byte) error {
	recordSize := w.calculateRecordSize(key, value)

	// в SSTable пока нет блоков
	if uint64(len(w.blocks)) == 0 {
		newBlockSize := w.elementaryBlockSize * uint64(math.Ceil(float64(recordSize)/float64(w.elementaryBlockSize)))
		w.blocks = append(w.blocks, WriterBlockInfo{nextBlockIndex: newBlockSize})

		if err := w.writeRecord(key, value); err != nil {
			return fmt.Errorf("sstable add: failed to write first record: %w", err)
		}

		return nil
	}

	nextBlockIndex := w.blocks[len(w.blocks)-1].nextBlockIndex

	if w.currentIndex+recordSize-1 < nextBlockIndex {
		// Влазим в текущий блок, не создаём новый
		if err := w.writeRecord(key, value); err != nil {
			return fmt.Errorf("sstable add: failed to write record to current block: %w", err)
		}

		return nil
	}

	// В текущий блок не влазим, создаём новый

	// Обновляем последний существующий блок
	w.blocks[len(w.blocks)-1].lastKeyIndex = w.previousIndex

	// Выравнивание до следующего блока
	if err := w.align(nextBlockIndex - w.currentIndex); err != nil {
		return fmt.Errorf("sstable add: failed to align next block: %w", err)
	}

	// создаём новый блок
	newBlockSize := w.elementaryBlockSize * uint64(math.Ceil(float64(recordSize)/float64(w.elementaryBlockSize)))
	w.blocks = append(w.blocks, WriterBlockInfo{firstKeyIndex: nextBlockIndex, nextBlockIndex: nextBlockIndex + newBlockSize})
	w.currentIndex = nextBlockIndex
	if err := w.writeRecord(key, value); err != nil {
		return fmt.Errorf("sstable add: failed to write record to new block: %w", err)
	}

	return nil
}

func (w *Writer) Close() error {
	w.blocks[len(w.blocks)-1].lastKeyIndex = w.previousIndex

	if err := w.writeFooter(); err != nil {
		return fmt.Errorf("sstable close: failed to write footer: %w", err)
	}

	return nil
}

func (w *Writer) calculateRecordSize(key, value []byte) uint64 {
	/*
		Records are stored as:
		- length of key (4 bytes)
		- length of value (4 bytes)
		- key (? bytes)
		- value (? bytes)
	*/

	return uint64(8 + len(key) + len(value))
}

func (w *Writer) writeRecord(key, value []byte) error {
	keyLength := len(key)
	valueLength := len(value)

	ioWriter := w.ioWriter
	if err := binary.Write(ioWriter, w.order, uint32(keyLength)); err != nil {
		return fmt.Errorf("sstable writeRecord: write key length: %w", err)
	}
	if err := binary.Write(ioWriter, w.order, uint32(valueLength)); err != nil {
		return fmt.Errorf("sstable writeRecord: write value length: %w", err)
	}

	var n int
	var err error

	n, err = ioWriter.Write(key)
	if err != nil {
		return fmt.Errorf("sstable add: write key: %w", err)
	}
	if n < keyLength {
		return fmt.Errorf("sstable writeRecord: write key: %d < %d", n, keyLength)
	}

	n, err = ioWriter.Write(value)
	if err != nil {
		return fmt.Errorf("sstable writeRecord: write value: %w", err)
	}
	if n < valueLength {
		return fmt.Errorf("sstable writeRecord: write value: %d < %d", n, valueLength)
	}

	// Для хеша
	n, err = w.hash.Write(key)
	if err != nil {
		return fmt.Errorf("sstable writeRecord: write key for hash: %w", err)
	}
	if n < keyLength {
		return fmt.Errorf("sstable writeRecord: write key for hash: %d < %d", n, valueLength)
	}

	n, err = w.hash.Write(value)
	if err != nil {
		return fmt.Errorf("sstable writeRecord: write value for hash: %w", err)
	}
	if n < valueLength {
		return fmt.Errorf("sstable writeRecord: write value for hash: %d < %d", n, valueLength)
	}

	w.previousIndex = w.currentIndex
	w.currentIndex += w.calculateRecordSize(key, value)

	return nil
}

func (w *Writer) writeFooter() error {
	if err := w.writeBlocksInfo(); err != nil {
		return fmt.Errorf("sstable writeFooter: failed to write blocks info: %w", err)
	}

	if err := w.writeChecksum(); err != nil {
		return fmt.Errorf("sstable writeFooter: failed to write checksum: %w", err)
	}

	return nil
}

func (w *Writer) writeBlocksInfo() error {
	ioWriter := w.ioWriter

	// для каждого блока пишем индекс первого и последнего ключа
	for _, block := range w.blocks {
		if err := binary.Write(ioWriter, w.order, block.firstKeyIndex); err != nil {
			return fmt.Errorf("sstable writeBlocksInfo: write first key index: %w", err)
		}
		if err := binary.Write(ioWriter, w.order, block.lastKeyIndex); err != nil {
			return fmt.Errorf("sstable writeBlocksInfo: write last key index: %w", err)
		}
	}

	// Пишем Количество блоков
	if err := binary.Write(ioWriter, w.order, uint64(len(w.blocks))); err != nil {
		return fmt.Errorf("sstable writeBlocksInfo: write blocks number: %w", err)
	}

	return nil
}

func (w *Writer) writeChecksum() error {
	checkSum := w.hash.Sum(nil)
	checkSumLength := len(checkSum)

	n, err := w.ioWriter.Write(checkSum)
	if err != nil {
		return fmt.Errorf("sstable writeChecksum: checksum: %w", err)
	}
	if n < checkSumLength {
		return fmt.Errorf("sstable writeChecksum: checksum: %d < %d", n, checkSumLength)
	}

	if err := binary.Write(w.ioWriter, w.order, uint32(checkSumLength)); err != nil {
		return fmt.Errorf("sstable writeChecksum: checksum length: %w", err)
	}

	return nil
}

func (w *Writer) align(alignment uint64) error {
	n, err := w.ioWriter.Write(make([]byte, alignment))
	if err != nil {
		return fmt.Errorf("sstable align: %w", err)
	}
	if uint64(n) < alignment {
		return fmt.Errorf("sstable align: %d < %d", n, alignment)
	}

	return nil
}
