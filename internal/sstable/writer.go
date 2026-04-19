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
	DiskBlockSize uint64 = 4096
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
	if !w.hasBlocks() {
		newBlockSize := DiskBlockSize * uint64(math.Ceil(float64(recordSize)/float64(DiskBlockSize)))
		w.blocksInfo = append(w.blocksInfo, &writerBlockInfo{nextBlockIndex: newBlockSize})

		if err := w.writeRecord(key, value); err != nil {
			return fmt.Errorf("sstable add: failed to write first record: %w", err)
		}

		return nil
	}

	lastBlock := w.getLastBlock()
	nextBlockIndex := lastBlock.nextBlockIndex

	if w.currentIndex+recordSize-1 < nextBlockIndex {
		// Влазим в текущий блок, не создаём новый
		if err := w.writeRecord(key, value); err != nil {
			return fmt.Errorf("sstable add: failed to write record to current block: %w", err)
		}

		return nil
	}

	// В текущий блок не влазим, создаём новый

	// Обновляем последний существующий блок

	lastBlock.lastKeyIndex = w.previousIndex

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
	if w.hasBlocks() {
		// Завершаем последний блок
		w.getLastBlock().lastKeyIndex = w.previousIndex
	}

	if err := w.writeFooter(); err != nil {
		return fmt.Errorf("sstable close: failed to write footer: %w", err)
	}

	return nil
}

func (w *Writer) writeRecord(key, value []byte) error {
	keyLength := len(key)
	valueLength := len(value)

	if err := w.writeUint32(uint32(keyLength)); err != nil {
		return fmt.Errorf("sstable writeRecord: write key length: %w", err)
	}
	if err := w.writeUint32(uint32(valueLength)); err != nil {
		return fmt.Errorf("sstable writeRecord: write value length: %w", err)
	}
	if err := w.writeBytes(key); err != nil {
		return fmt.Errorf("sstable add: write key: %w", err)
	}
	if err := w.writeBytes(value); err != nil {
		return fmt.Errorf("sstable add: write value: %w", err)
	}
	if err := w.updateChecksum(key, value); err != nil {
		return fmt.Errorf("sstable add: updating checksum: %w", err)
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
	// Для каждого блока пишем индекс первого и последнего ключа
	for _, block := range w.blocksInfo {
		if err := w.writeUint64(block.firstKeyIndex); err != nil {
			return fmt.Errorf("sstable writeBlocksInfo: write first key index: %w", err)
		}
		if err := w.writeUint64(block.lastKeyIndex); err != nil {
			return fmt.Errorf("sstable writeBlocksInfo: write last key index: %w", err)
		}
	}

	// Пишем Количество блоков
	blocksNumber := w.getBlocksNumber()
	if err := w.writeUint64(uint64(blocksNumber)); err != nil {
		return fmt.Errorf("sstable writeBlocksInfo: write blocks number: %w", err)
	}

	return nil
}

func (w *Writer) writeChecksum() error {
	checksum := w.checksumHash.Sum(nil)
	checksumLength := len(checksum)

	if err := w.writeBytes(checksum); err != nil {
		return fmt.Errorf("sstable writeChecksum: checksum: %w", err)
	}
	if err := w.writeUint32(uint32(checksumLength)); err != nil {
		return fmt.Errorf("sstable writeChecksum: checksum length: %w", err)
	}

	return nil
}

func (w *Writer) writeUint32(value uint32) error {
	if err := binary.Write(w.ioWriter, ByteOrder, value); err != nil {
		return fmt.Errorf("sstable writeUint32: %w", err)
	}

	return nil
}

func (w *Writer) writeUint64(value uint64) error {
	if err := binary.Write(w.ioWriter, ByteOrder, value); err != nil {
		return fmt.Errorf("sstable writeUint64: %w", err)
	}

	return nil
}

func (w *Writer) writeBytes(buffer []byte) error {
	length := len(buffer)
	n, err := w.ioWriter.Write(buffer)

	if err != nil {
		return fmt.Errorf("sstable writeBytes: %w", err)
	}

	if n < length {
		return fmt.Errorf("sstable writeBytes: %d < %d", n, length)
	}

	return nil
}

func (w *Writer) calculateRecordSize(key, value []byte) uint64 {
	// 8 = 4 + 4 (uint32s для длины ключа и значения)
	return uint64(8 + len(key) + len(value))
}

func (w *Writer) align(alignment uint64) error {
	// Выравнивание
	n, err := w.ioWriter.Write(make([]byte, alignment))
	if err != nil {
		return fmt.Errorf("sstable align: %w", err)
	}
	if uint64(n) < alignment {
		return fmt.Errorf("sstable align: %d < %d", n, alignment)
	}
	return nil
}

func (w *Writer) updateChecksum(key []byte, value []byte) error {
	var n int
	var err error

	n, err = w.checksumHash.Write(key)
	if err != nil {
		return fmt.Errorf("sstable writeRecord: write key for hash: %w", err)
	}
	if n < len(key) {
		return fmt.Errorf("sstable writeRecord: write key for hash: %d < %d", n, len(key))
	}

	n, err = w.checksumHash.Write(value)
	if err != nil {
		return fmt.Errorf("sstable writeRecord: write value for hash: %w", err)
	}
	if n < len(value) {
		return fmt.Errorf("sstable writeRecord: write value for hash: %d < %d", n, len(value))
	}

	return nil
}

func (w *Writer) hasBlocks() bool {
	return len(w.blocksInfo) > 0
}

func (w *Writer) getBlocksNumber() int {
	return len(w.blocksInfo)
}

func (w *Writer) getFirstBlock() *writerBlockInfo {
	return w.blocksInfo[0]
}

func (w *Writer) getLastBlock() *writerBlockInfo {
	return w.blocksInfo[len(w.blocksInfo)-1]
}
