package sstable

import (
	"bytes"
	"fmt"
	"io"
	. "kvschool/internal/helpers"
	"kvschool/internal/iterator"
)

const (
	NoIndex int64 = -1
)

type footer struct {
	checksum []byte
}

type readerBlockInfo struct {
	firstKey      []byte
	lastKey       []byte
	firstKeyIndex int64
	lastKeyIndex  int64
}

type Reader struct {
	ioReader   io.ReaderAt
	blocksInfo []*readerBlockInfo
	footer     *footer
	totalSize  int64
}

func NewReader(ioReader io.ReaderAt, totalSize int64) (*Reader, error) {
	reader := &Reader{
		blocksInfo: make([]*readerBlockInfo, 0),
		ioReader:   ioReader,
		totalSize:  totalSize,
	}

	if err := reader.readFooter(); err != nil {
		return nil, fmt.Errorf("sstable NewReader: reading footer: %w", err)
	}

	if err := reader.readAllBlocksInfo(); err != nil {
		return nil, fmt.Errorf("sstable NewReader: reading footer: %w", err)
	}

	return reader, nil
}

func (r *Reader) Iterator(start []byte, end []byte) (iterator.Iterator, error) {
	emptyIterator := EmptyIterator()

	if !r.HasRecords() {
		return emptyIterator, nil
	}

	firstBlock := r.getFirstBlock()
	lastBlock := r.getLastBlock()

	if start == nil && end == nil {
		return NewIterator(r, firstBlock.firstKeyIndex, NoIndex, 0), nil
	}
	if start != nil && end != nil && CompareKeys(start, end) >= 0 {
		return emptyIterator, nil
	}
	if start != nil && CompareKeys(lastBlock.lastKey, start) < 0 {
		return emptyIterator, nil
	}
	if end != nil && CompareKeys(firstBlock.firstKey, end) >= 0 {
		return emptyIterator, nil
	}

	var startIndex int64
	var endIndex int64
	var startBlockIndex int

	if start == nil {
		startIndex = 0
		startBlockIndex = 0
	} else {
		if r.getBlocksNumber() == 1 {
			onlyBlock := firstBlock
			index, err := r.findFirstGreaterOrEqualRecord(onlyBlock, start)
			if err != nil {
				return nil, fmt.Errorf("sstable iterator: failed to find start index: %w", err)
			}
			startIndex = index
		} else {
			blockIndex1, blockIndex2 := r.findGreaterOrEqualBlocks(start)
			block1 := r.getBlock(blockIndex1)
			block2 := r.getBlock(blockIndex2)
			if CompareKeys(block1.lastKey, start) >= 0 {
				index, err := r.findFirstGreaterOrEqualRecord(block1, start)
				if err != nil {
					return nil, fmt.Errorf("sstable iterator: start block1: %w", err)
				}
				startIndex = index
				startBlockIndex = blockIndex1
			} else {
				index, err := r.findFirstGreaterOrEqualRecord(block2, start)
				if err != nil {
					return nil, fmt.Errorf("sstable iterator: start block2: %w", err)
				}
				startIndex = index
				startBlockIndex = blockIndex2
			}
		}
	}

	if end == nil {
		endIndex = NoIndex
	} else {
		if r.getBlocksNumber() == 1 {
			onlyBlock := firstBlock
			if CompareKeys(onlyBlock.lastKey, end) < 0 {
				endIndex = NoIndex
			} else {
				index, err := r.findFirstGreaterOrEqualRecord(onlyBlock, end)
				if err != nil {
					return nil, fmt.Errorf("sstable iterator: failed to find end index: %w", err)
				}
				endIndex = index
			}
		} else {
			blockIndex1, blockIndex2 := r.findLessBlocks(end)
			block1 := r.getBlock(blockIndex1)
			block2 := r.getBlock(blockIndex2)
			if CompareKeys(block2.lastKey, end) < 0 {
				endIndex = NoIndex
			} else if CompareKeys(block2.firstKey, end) < 0 {
				index, err := r.findFirstGreaterOrEqualRecord(block2, end)
				if err != nil {
					return nil, fmt.Errorf("sstable iterator: end block2: %w", err)
				}
				endIndex = index
			} else {
				index, err := r.findFirstGreaterOrEqualRecord(block1, end)
				if err != nil {
					return nil, fmt.Errorf("sstable iterator: end block1: %w", err)
				}
				if index == NoIndex {
					endIndex = r.getBlock(blockIndex1 + 1).firstKeyIndex
				} else {
					endIndex = index
				}
			}
		}
	}

	return NewIterator(r, startIndex, endIndex, startBlockIndex), nil
}

func (r *Reader) ValidateChecksum() error {
	checksumHash := createChecksumHash()

	it, err := r.Iterator(nil, nil)
	if err != nil {
		return fmt.Errorf("sstable ValidateChecksum: creating iterator for checksum: %w", err)
	}

	for {
		key, value, ok, err := it.Next()

		if err != nil {
			return fmt.Errorf("sstable ValidateChecksum: scanning records for checksum: %w", err)
		}
		if !ok {
			break
		}

		if err := updateChecksum(key, value, checksumHash); err != nil {
			return fmt.Errorf("sstable ValidateChecksum: updating checksum: %w", err)
		}
	}

	checksum := calculateChecksum(checksumHash)
	if !bytes.Equal(checksum, r.footer.checksum) {
		return fmt.Errorf("sstable ValidateChecksum: checksum mismatch")
	}

	return nil
}

func (r *Reader) HasRecords() bool {
	return r.getBlocksNumber() > 0
}

func (r *Reader) GetFirstKey() []byte {
	return r.getFirstBlock().firstKey
}

func (r *Reader) GetLastKey() []byte {
	return r.getLastBlock().lastKey
}

func (r *Reader) readFooter() error {
	checksumLength, _, err := r.readUint32(r.getChecksumLengthOffset())
	if err != nil {
		return fmt.Errorf("sstable readFooter: reading checksum length: %w", err)
	}

	checksum, _, err := r.readBytes(r.getChecksumOffset(int64(checksumLength)), int(checksumLength))
	if err != nil {
		return fmt.Errorf("sstable readFooter: reading checksum: %w", err)
	}

	r.footer = &footer{checksum: checksum}

	return nil
}

func (r *Reader) readAllBlocksInfo() error {
	blocksNumberUint, _, err := r.readUint64(r.getBlocksNumberOffset())
	if err != nil {
		return fmt.Errorf("sstable readAllBlocksInfo: reading blocks number: %w", err)
	}

	blocksNumber := int(blocksNumberUint)

	var i int64
	var offset int64

	offset = r.getBlockInfoOffset(0, blocksNumber)
	for i = 0; i < int64(blocksNumberUint); i++ {
		info, off, err := r.readBlockInfo(offset)
		if err != nil {
			return fmt.Errorf("sstable readAllBlocksInfo: reading block info %d: %w", i, err)
		}

		offset = off
		r.blocksInfo = append(r.blocksInfo, info)
	}

	return nil
}

func (r *Reader) readBlockInfo(blockOffset int64) (*readerBlockInfo, int64, error) {
	firstKeyIndex, off2, err := r.readUint64(blockOffset)
	if err != nil {
		return nil, 0, fmt.Errorf("sstable readBlockInfo: failed to read block first key index: %w", err)
	}

	lastKeyIndex, off3, err := r.readUint64(off2)
	if err != nil {
		return nil, 0, fmt.Errorf("sstable readBlockInfo: failed to read block last key index: %w", err)
	}

	firstKey, _, _, err := r.readRecord(int64(firstKeyIndex), false)
	if err != nil {
		return nil, 0, fmt.Errorf("sstable readBlockInfo: failed to read block first key: %w", err)
	}

	lastKey, _, _, err := r.readRecord(int64(lastKeyIndex), false)
	if err != nil {
		return nil, 0, fmt.Errorf("sstable readBlockInfo: failed to read block last key: %w", err)
	}

	return &readerBlockInfo{
		firstKey:      firstKey,
		lastKey:       lastKey,
		firstKeyIndex: int64(firstKeyIndex),
		lastKeyIndex:  int64(lastKeyIndex),
	}, off3, nil
}

func (r *Reader) getChecksumLengthOffset() int64 {
	return r.totalSize - 4
}

func (r *Reader) getChecksumOffset(checksumLength int64) int64 {
	return r.getChecksumLengthOffset() - checksumLength
}

func (r *Reader) getBlocksNumberOffset() int64 {
	return r.getChecksumOffset(int64(len(r.footer.checksum))) - 8
}

func (r *Reader) getBlockInfoOffset(blockIndex int, blocksNumber int) int64 {
	return r.getBlocksNumberOffset() - 16*int64(blocksNumber-blockIndex)
}

func (r *Reader) readRecord(offset int64, readValue bool) ([]byte, []byte, int64, error) {
	keyLength, off1, err := r.readUint32(offset)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("sstable readRecord: failed to read key length: %w", err)
	}

	valueLength, off2, err := r.readUint32(off1)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("sstable readRecord: failed to read value length: %w", err)
	}

	key, off3, err := r.readBytes(off2, int(keyLength))
	if err != nil {
		return nil, nil, 0, fmt.Errorf("sstable readRecord: failed to read key: %w", err)
	}

	if readValue {
		value, off4, err := r.readBytes(off3, int(valueLength))
		if err != nil {
			return nil, nil, 0, fmt.Errorf("sstable readRecord: failed to read value: %w", err)
		}

		return key, value, off4, nil
	}

	return key, nil, off3, nil
}

func (r *Reader) readUint32(offset int64) (uint32, int64, error) {
	buffer, newOffset, err := r.readBytes(offset, 4)

	if err != nil {
		return 0, newOffset, fmt.Errorf("sstable readUint32: reading bytes: %w", err)
	}

	return ByteOrder.Uint32(buffer), newOffset, nil
}

func (r *Reader) readUint64(offset int64) (uint64, int64, error) {
	buffer, newOffset, err := r.readBytes(offset, 8)

	if err != nil {
		return 0, newOffset, fmt.Errorf("sstable readUint64: reading bytes: %w", err)
	}

	return ByteOrder.Uint64(buffer), newOffset, nil
}

func (r *Reader) readBytes(offset int64, count int) ([]byte, int64, error) {
	buffer := make([]byte, count)

	n, err := r.ioReader.ReadAt(buffer, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("sstable readBytes: %w", err)
	}
	if n < count {
		return nil, 0, fmt.Errorf("sstable readBytes: %d < %d", n, count)
	}

	return buffer, offset + int64(count), nil
}

func (r *Reader) findFirstGreaterOrEqualRecord(block *readerBlockInfo, target []byte) (int64, error) {
	var offset = block.firstKeyIndex

	for {
		key, _, nextOffset, err := r.readRecord(offset, true)
		if err != nil {
			return 0, fmt.Errorf("sstable findFirstGreaterOrEqualRecord: failed to scan keys: %w", err)
		}

		if CompareKeys(key, target) >= 0 {
			return offset, nil
		}

		if offset == block.lastKeyIndex {
			break
		}

		offset = nextOffset
	}

	return NoIndex, nil
}

func (r *Reader) findGreaterOrEqualBlocks(key []byte) (int, int) {
	var blockIndex1 = 0
	var blockIndex2 = r.getLastBlockIndex()

	for blockIndex2-blockIndex1 > 1 {
		middleIndex := (blockIndex1 + blockIndex2) / 2
		middleBlock := r.blocksInfo[middleIndex]

		if CompareKeys(middleBlock.lastKey, key) >= 0 {
			blockIndex2 = middleIndex
		} else {
			blockIndex1 = middleIndex
		}
	}

	return blockIndex1, blockIndex2
}

func (r *Reader) findLessBlocks(key []byte) (int, int) {
	var blockIndex1 = 0
	var blockIndex2 = len(r.blocksInfo) - 1

	for blockIndex2-blockIndex1 > 1 {
		middleIndex := (blockIndex1 + blockIndex2) / 2
		middleBlock := r.blocksInfo[middleIndex]

		if CompareKeys(middleBlock.firstKey, key) < 0 {
			blockIndex1 = middleIndex
		} else {
			blockIndex2 = middleIndex
		}
	}

	return blockIndex1, blockIndex2
}

func (r *Reader) getBlocksNumber() int {
	return len(r.blocksInfo)
}

func (r *Reader) getFirstBlock() *readerBlockInfo {
	return r.getBlock(0)
}

func (r *Reader) getLastBlockIndex() int {
	return r.getBlocksNumber() - 1
}

func (r *Reader) getLastBlock() *readerBlockInfo {
	return r.getBlock(r.getLastBlockIndex())
}

func (r *Reader) getBlock(i int) *readerBlockInfo {
	return r.blocksInfo[i]
}
