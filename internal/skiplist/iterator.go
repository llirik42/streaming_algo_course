package skiplist

import "kvschool/internal/helpers"

type Iterator struct {
	current *Node
	end     *Node
}

func (it *Iterator) Next() (key, value []byte, ok bool, err error) {
	if !it.Has() {
		return nil, nil, false, nil
	}

	node := it.current
	zeroLevel := 0
	it.current = it.current.next[zeroLevel]

	return helpers.CloneBytes(node.key), helpers.CloneBytes(node.value), true, nil
}

func (it *Iterator) Has() bool {
	if it.current == nil {
		return false
	}

	if it.current == it.end {
		return false
	}

	return true
}

func (it *Iterator) Close() error {
	it.current = nil
	it.end = nil
	return nil
}
