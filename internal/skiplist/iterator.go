package skiplist

import "kvschool/internal/helpers"

type Iterator struct {
	current *Node
	end     *Node
}

func (it *Iterator) Next() (key, value []byte, ok bool, err error) {
	if it.current == nil {
		return nil, nil, false, nil
	}

	if it.current == it.end {
		return nil, nil, false, nil
	}

	node := it.current
	zeroLevel := 0
	it.current = it.current.next[zeroLevel]

	return helpers.CloneBytes(node.key), helpers.CloneBytes(node.value), true, nil
}

func (it *Iterator) Close() error {
	it.current = nil
	it.end = nil
	return nil
}
