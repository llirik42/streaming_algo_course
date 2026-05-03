package lsm

import (
	"bytes"
	"sort"
)

func sortPairs(pairs []iteratorPair) {
	by := func(p1, p2 *iteratorPair) bool {
		return bytes.Compare(p1.key, p2.key) <= 0
	}

	ps := &pairSorter{
		pairs: pairs,
		by:    by,
	}
	sort.Sort(ps)
}

type pairSorter struct {
	pairs []iteratorPair
	by    func(p1, p2 *iteratorPair) bool
}

func (s *pairSorter) Len() int {
	return len(s.pairs)
}

func (s *pairSorter) Swap(i, j int) {
	s.pairs[i], s.pairs[j] = s.pairs[j], s.pairs[i]
}

func (s *pairSorter) Less(i, j int) bool {
	return s.by(&s.pairs[i], &s.pairs[j])
}

func removeByIndexes(slice []*sstableWrapper, indexes []int) []*sstableWrapper {
	sort.Sort(sort.Reverse(sort.IntSlice(indexes)))

	for _, i := range indexes {
		if i >= 0 && i < len(slice) {
			slice = append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}
