package lsmstore

import (
	"bytes"
	"kvschool/internal/kv"
	"sort"
)

func sortPairs(pairs []kv.Pair) {
	by := func(p1, p2 *kv.Pair) bool {
		return bytes.Compare(p1.Key, p2.Key) <= 0
	}

	ps := &pairSorter{
		pairs: pairs,
		by:    by,
	}
	sort.Sort(ps)
}

type pairSorter struct {
	pairs []kv.Pair
	by    func(p1, p2 *kv.Pair) bool
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
