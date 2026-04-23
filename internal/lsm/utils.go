package lsm

import (
	"bytes"
	"sort"
)

func sortPairs(pairs []pair) {
	by := func(p1, p2 *pair) bool {
		return bytes.Equal(p1.key, p2.key)
	}

	ps := &pairSorter{
		pairs: pairs,
		by:    by,
	}
	sort.Sort(ps)
}

type pairSorter struct {
	pairs []pair
	by    func(p1, p2 *pair) bool
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
