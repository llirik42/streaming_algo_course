package lsm

func augmentValue(value []byte, isTombstone bool) []byte {
	if isTombstone {
		res := make([]byte, 1)
		res[0] = 1
		return res
	}

	res := make([]byte, len(value)+1)
	res[0] = 0
	copy(res[1:], value)
	return res
}

func parseAugmentedValue(buffer []byte) (value []byte, isTombstone bool) {
	return buffer[1:], buffer[0] != 0
}
