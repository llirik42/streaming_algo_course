package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
)

type footer struct {
	checksum []byte
}

type blockInfo struct {
	firstKey      []byte
	firstKeyIndex int64
	lastKeyIndex  int64
}

// Reader читает SSTable с диска.
// Использует RandomAccess (io.ReaderAt) для прыжков по индексу.
type Reader struct {
	blocksInfo []*blockInfo
	footer     *footer
	ioReader   io.ReaderAt
	fileSize   int64
	order      binary.ByteOrder
}

func NewReader(ioReader io.ReaderAt, fileSize int64) (*Reader, error) {
	reader := &Reader{
		blocksInfo: make([]*blockInfo, 0),
		ioReader:   ioReader,
		fileSize:   fileSize,
		order:      binary.LittleEndian,
	}

	if err := reader.readFooter(); err != nil {
		return nil, fmt.Errorf("sstable NewReader: reading footer: %w", err)
	}

	if err := reader.readAllBlocksInfo(); err != nil {
		return nil, fmt.Errorf("sstable NewReader: reading footer: %w", err)
	}

	return reader, nil
}

func (r *Reader) readFooter() error {
	checkSumLength, _, err := r.readUint32(r.getChecksumLengthOffset())
	if err != nil {
		return fmt.Errorf("sstable readFooter: reading checksum length: %w", err)
	}

	checksum, _, err := r.readBytes(r.getChecksumOffset(int64(checkSumLength)), int(checkSumLength))
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

	blocksNumber := int64(blocksNumberUint)

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

func (r *Reader) readBlockInfo(blockOffset int64) (*blockInfo, int64, error) {
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

	return &blockInfo{
		firstKey:      firstKey,
		firstKeyIndex: int64(firstKeyIndex),
		lastKeyIndex:  int64(lastKeyIndex),
	}, off3, nil
}

func (r *Reader) getChecksumLengthOffset() int64 {
	return r.fileSize - 4
}

func (r *Reader) getChecksumOffset(checksumLength int64) int64 {
	return r.getChecksumLengthOffset() - checksumLength
}

func (r *Reader) getBlocksNumberOffset() int64 {
	return r.getChecksumOffset(int64(len(r.footer.checksum))) - 8
}

func (r *Reader) getBlockInfoOffset(blockIndex int64, blocksNumber int64) int64 {
	return r.getBlocksNumberOffset() - 16*(blocksNumber-blockIndex)
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

	return r.order.Uint32(buffer), newOffset, nil
}

func (r *Reader) readUint64(offset int64) (uint64, int64, error) {
	buffer, newOffset, err := r.readBytes(offset, 8)

	if err != nil {
		return 0, newOffset, fmt.Errorf("sstable readUint64: reading bytes: %w", err)
	}

	return r.order.Uint64(buffer), newOffset, nil
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

// Iterator возвращает упорядоченную итерацию по диапазону [start, end).
// Использует Sparse Index, чтобы найти нужный блок данных.
func (r *Reader) Iterator(start []byte, end []byte) (*Iterator, error) {
	//if len(r.blocksInfo) == 0 {
	//	return &Iterator{
	//		reader:                   r,
	//		index:                    -1,
	//		endIndex:                 -1,
	//		currentBlockLastKeyIndex: -1,
	//		currentBlockIndex:        -1,
	//	}, nil
	//}
	//
	//if start == nil {
	//	index := r.blocksInfo[0]
	//} else {
	//	// есть только 1 блок
	//	if len(r.blocksInfo) == 1 {
	//		offset := r.blocksInfo[0].firstKeyIndex
	//
	//		for {
	//			key, value, newOffset, err := r.readRecord(offset, false)
	//			if err != nil {
	//				return nil, fmt.Errorf("sstable Iterator: failed to scan blocks: %w", err)
	//			}
	//
	//
	//
	//
	//			if helpers.KeysEqual(start, key) {
	//				index := offset
	//				break
	//			}
	//
	//
	//
	//
	//
	//			if offset == r.blocksInfo[0].lastKeyIndex {
	//				break
	//			}
	//
	//
	//
	//		}
	//
	//
	//		for i :=
	//
	//
	//
	//
	//
	//		r.blocksInfo[0].lastKeyIndex
	//	}
	//
	//
	//	// бин. поиск
	//
	//	i := len(r.blocksInfo) / 2
	//}
	//
	//if end == nil {
	//	endIndex := -1
	//}
	//
	//
	//
	//
	//
	//
	//for _, b := range r.blocksInfo {
	//	if
	//}

	return &Iterator{
		reader:                   r,
		index:                    r.blocksInfo[0].firstKeyIndex,
		endIndex:                 -1,
		currentBlockLastKeyIndex: r.blocksInfo[0].lastKeyIndex,
		currentBlockIndex:        0,
	}, nil
}
