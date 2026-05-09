package bloom

type bitBuffer struct {
	buffer []uint8
}

func newBitBuffer(size uint64) *bitBuffer {
	bytesCount := size>>3 + 1
	buffer := make([]byte, bytesCount)
	return &bitBuffer{buffer}
}

func (bb *bitBuffer) setBit(i uint64, v uint8) {
	byteIndex, bitIndex := calcIndexes_(i)
	setBit_(&bb.buffer[byteIndex], bitIndex, v)
}

func (bb *bitBuffer) getBit(i uint64) (v uint8) {
	byteIndex, bitIndex := calcIndexes_(i)
	return getBit_(&bb.buffer[byteIndex], bitIndex)
}

func calcIndexes_(i uint64) (byteIndex, bitIndex uint64) {
	return i >> 3, i & 7
}

func setBit_(b *uint8, i uint64, v uint8) {
	if v != 0 && v != 1 {
		panic("Invalid value")
	}

	if v == 1 {
		*b |= 1 << i
	} else {
		*b &= ^(1 << i)
	}
}

func getBit_(b *uint8, i uint64) uint8 {
	return (1 << i & *b) >> i
}
