package skiplist

// Iterator — упорядоченная итерация по диапазону ключей (Range Scan).
// В HLR используется для выгрузки абонентов по префиксу IMSI.
type Iterator interface {
	Next() (key, value []byte, ok bool, err error)
	Close() error
}

type SkipListIterator struct {
	current *Node
	end     *Node
}

func (s *SkipListIterator) Next() (key, value []byte, ok bool, err error) {
	if s.current == nil {
		return nil, nil, false, nil
	}

	if s.current == s.end {
		return nil, nil, false, nil
	}

	node := s.current
	zeroLevel := 0
	s.current = s.current.next[zeroLevel]

	return cloneBytes(node.key), cloneBytes(node.value), true, nil
}

func (s *SkipListIterator) Close() error {
	s.current = nil
	s.end = nil
	return nil
}
