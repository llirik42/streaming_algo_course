package skiplist

type Node struct {
	key   []byte
	value []byte
	next  []*Node
}

func removeAfter(predecessor, node *Node, level int) {
	predecessor.next[level] = node.next[level]
}

func insertAfter(predecessor, node *Node, level int) {
	node.next = append(node.next, predecessor.next[level])
	predecessor.next[level] = node
}
