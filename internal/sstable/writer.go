package sstable

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"
)

const (
	DiskBlockSize uint64 = 256
)

var ByteOrder binary.ByteOrder = binary.LittleEndian

type writerBlockInfo struct {
	firstKeyIndex  uint64
	lastKeyIndex   uint64
	nextBlockIndex uint64
}

type Writer struct {
	ioWriter      io.Writer
	previousIndex uint64
	currentIndex  uint64
	blocksInfo    []*writerBlockInfo
	checksumHash  hash.Hash
}

func NewWriter(ioWriter io.Writer) *Writer {
	return &Writer{
		ioWriter:     ioWriter,
		blocksInfo:   make([]*writerBlockInfo, 0),
		checksumHash: md5.New(),
	}
}

func (w *Writer) Add(key []byte, value []byte) error {
	recordSize := w.calculateRecordSize(key, value)

	// в SSTable пока нет блоков
	if uint64(len(w.blocksInfo)) == 0 {
		newBlockSize := DiskBlockSize * uint64(math.Ceil(float64(recordSize)/float64(DiskBlockSize)))
		w.blocksInfo = append(w.blocksInfo, &writerBlockInfo{nextBlockIndex: newBlockSize})

		if err := w.writeRecord(key, value); err != nil {
			return fmt.Errorf("sstable add: failed to write first record: %w", err)
		}

		return nil
	}

	nextBlockIndex := w.blocksInfo[len(w.blocksInfo)-1].nextBlockIndex

	if w.currentIndex+recordSize-1 < nextBlockIndex {
		// Влазим в текущий блок, не создаём новый
		if err := w.writeRecord(key, value); err != nil {
			return fmt.Errorf("sstable add: failed to write record to current block: %w", err)
		}

		return nil
	}

	// В текущий блок не влазим, создаём новый

	// Обновляем последний существующий блок
	w.blocksInfo[len(w.blocksInfo)-1].lastKeyIndex = w.previousIndex

	// Выравнивание до следующего блока
	if err := w.align(nextBlockIndex - w.currentIndex); err != nil {
		return fmt.Errorf("sstable add: failed to align next block: %w", err)
	}

	// создаём новый блок
	newBlockSize := DiskBlockSize * uint64(math.Ceil(float64(recordSize)/float64(DiskBlockSize)))
	w.blocksInfo = append(w.blocksInfo, &writerBlockInfo{firstKeyIndex: nextBlockIndex, nextBlockIndex: nextBlockIndex + newBlockSize})
	w.currentIndex = nextBlockIndex
	if err := w.writeRecord(key, value); err != nil {
		return fmt.Errorf("sstable add: failed to write record to new block: %w", err)
	}

	return nil
}

func (w *Writer) Close() error {
	if len(w.blocksInfo) > 0 {
		w.blocksInfo[len(w.blocksInfo)-1].lastKeyIndex = w.previousIndex
	}

	if err := w.writeFooter(); err != nil {
		return fmt.Errorf("sstable close: failed to write footer: %w", err)
	}

	return nil
}

func (w *Writer) calculateRecordSize(key, value []byte) uint64 {
	return uint64(8 + len(key) + len(value))
}

func (w *Writer) writeRecord(key, value []byte) error {
	keyLength := len(key)
	valueLength := len(value)

	ioWriter := w.ioWriter
	if err := binary.Write(ioWriter, ByteOrder, uint32(keyLength)); err != nil {
		return fmt.Errorf("sstable writeRecord: write key length: %w", err)
	}
	if err := binary.Write(ioWriter, ByteOrder, uint32(valueLength)); err != nil {
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
	n, err = w.checksumHash.Write(key)
	if err != nil {
		return fmt.Errorf("sstable writeRecord: write key for hash: %w", err)
	}
	if n < keyLength {
		return fmt.Errorf("sstable writeRecord: write key for hash: %d < %d", n, valueLength)
	}

	n, err = w.checksumHash.Write(value)
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
	for _, block := range w.blocksInfo {
		if err := binary.Write(ioWriter, ByteOrder, block.firstKeyIndex); err != nil {
			return fmt.Errorf("sstable writeBlocksInfo: write first key index: %w", err)
		}
		if err := binary.Write(ioWriter, ByteOrder, block.lastKeyIndex); err != nil {
			return fmt.Errorf("sstable writeBlocksInfo: write last key index: %w", err)
		}
	}

	// Пишем Количество блоков
	if err := binary.Write(ioWriter, ByteOrder, uint64(len(w.blocksInfo))); err != nil {
		return fmt.Errorf("sstable writeBlocksInfo: write blocks number: %w", err)
	}

	return nil
}

func (w *Writer) writeChecksum() error {
	checkSum := w.checksumHash.Sum(nil)

	checkSumLength := len(checkSum)

	n, err := w.ioWriter.Write(checkSum)
	if err != nil {
		return fmt.Errorf("sstable writeChecksum: checksum: %w", err)
	}
	if n < checkSumLength {
		return fmt.Errorf("sstable writeChecksum: checksum: %d < %d", n, checkSumLength)
	}

	if err := binary.Write(w.ioWriter, ByteOrder, uint32(checkSumLength)); err != nil {
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
