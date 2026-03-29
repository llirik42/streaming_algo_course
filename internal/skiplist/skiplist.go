package skiplist

import (
	"errors"
	"fmt"
	"kvschool/internal/coinflipper"
)

// ErrNotFound означает отсутствие ключа (IMSI).
var ErrNotFound = errors.New("skiplist: ключ не найден")

// SkipList — In-Memory движок для HLR.
// Обеспечивает O(log N) на чтение/запись и упорядоченный доступ.
//
// В практической реализации вам нужно хранить:
// - ключи/значения как []byte
// - уровни (forward pointers)
// - генератор уровней с фиксируемым seed (для детерминизма тестов)
type SkipList struct {
	head        *Node
	coinFlipper *coinflipper.CoinFlipper
}

// New создаёт SkipList. seed требуется для детерминируемых тестов (воспроизводимость поведения при ошибках).
func New(seed int64) *SkipList {
	defaultProbability := 0.5
	head := Node{key: []byte{}, next: []*Node{nil}}

	return &SkipList{
		head:        &head,
		coinFlipper: coinflipper.New(seed, defaultProbability),
	}
}

func (s *SkipList) SetProbability(probability float64) error {
	if probability <= 0 || probability >= 1 {
		return errors.New("skiplist: вероятность должна быть больше нуля и меньше единицы")
	}

	s.coinFlipper = coinflipper.New(s.coinFlipper.GetSeed(), probability)

	return nil
}

func (s *SkipList) IsEmpty() bool {
	return s.head.next[0] == nil
}

func (s *SkipList) GetLevelsNumber() int {
	return len(s.head.next)
}

func (s *SkipList) Put(key, value []byte) error {
	predecessors := s.findPredecessors(key)
	zeroLevel := 0
	zeroLevelPredecessor := predecessors[zeroLevel]
	zeroLevelPredecessorSuccessor := zeroLevelPredecessor.next[zeroLevel]

	if zeroLevelPredecessorSuccessor != nil && keysEqual(zeroLevelPredecessorSuccessor.key, key) {
		zeroLevelPredecessorSuccessor.value = value
		return nil
	}

	newNode := &Node{
		key:   cloneBytes(key),
		value: cloneBytes(value),
		next:  make([]*Node, 0, 1), // Allocate for the 0th level
	}
	insertAfter(zeroLevelPredecessor, newNode, zeroLevel)
	levelsNumber := s.GetLevelsNumber()

	for level := 1; ; level++ {
		if !s.coinFlipper.Flip() {
			break
		}

		if level >= levelsNumber {
			s.createLevel(newNode)
		} else {
			insertAfter(predecessors[level], newNode, level)
		}
	}

	return nil
}

func (s *SkipList) Get(key []byte) ([]byte, error) {
	predecessors := s.findPredecessors(key)
	zeroLevel := 0
	zeroLevelPredecessor := predecessors[0]
	zeroLevelPredecessorSuccessor := zeroLevelPredecessor.next[zeroLevel]

	if zeroLevelPredecessorSuccessor != nil && keysEqual(zeroLevelPredecessorSuccessor.key, key) {
		return cloneBytes(zeroLevelPredecessorSuccessor.value), nil
	}

	return nil, ErrNotFound
}

func (s *SkipList) Delete(key []byte) error {
	predecessors := s.findPredecessors(key)
	zeroLevel := 0
	zeroLevelPredecessor := predecessors[zeroLevel]
	zeroLevelPredecessorSuccessor := zeroLevelPredecessor.next[zeroLevel]

	if zeroLevelPredecessorSuccessor == nil || !keysEqual(zeroLevelPredecessorSuccessor.key, key) {
		return ErrNotFound
	}

	node := zeroLevelPredecessorSuccessor
	for level := s.GetLevelsNumber() - 1; level >= 0; level-- {
		levelPredecessor := predecessors[level]
		levelPredecessorSuccessor := levelPredecessor.next[level]

		if levelPredecessorSuccessor == nil || !keysEqual(levelPredecessorSuccessor.key, key) {
			// The key is not present on the current level
			continue
		}

		// level > 0 - so we always keep 0th level
		if levelPredecessor == s.head && node.next[level] == nil && level > 0 {
			s.removeHighestLevel()
		} else {
			removeAfter(levelPredecessor, node, level)
		}
	}
	node.next = nil // Help GC to quickly free node

	return nil
}

// Scan возвращает итератор по диапазону [start, end).
// Если start == nil, считается -∞ (начало списка).
// Если end == nil, считается +∞ (конец списка).
func (s *SkipList) Scan(start, end []byte) (Iterator, error) {
	if start != nil && end != nil && keysCompare(start, end) >= 0 {
		return &SkipListIterator{current: nil, end: nil}, nil
	}

	zeroLevel := 0

	var startNode *Node
	if start == nil {
		startNode = s.head.next[zeroLevel]
	} else {
		startPredecessors := s.findPredecessors(start)
		zeroLevelStartPredecessor := startPredecessors[zeroLevel]
		zeroLevelStartPredecessorSuccessor := zeroLevelStartPredecessor.next[zeroLevel]
		startNode = zeroLevelStartPredecessorSuccessor
	}

	var endNode *Node
	if end == nil {
		endNode = nil
	} else {
		endPredecessors := s.findPredecessors(end)
		zeroLevelEndPredecessor := endPredecessors[zeroLevel]
		zeroLevelEndPredecessorSuccessor := zeroLevelEndPredecessor.next[zeroLevel]
		endNode = zeroLevelEndPredecessorSuccessor
	}

	return &SkipListIterator{current: startNode, end: endNode}, nil
}

func (s *SkipList) GetRepresentation() string {
	if s.IsEmpty() {
		return "Empty SkipList"
	}

	levelsNumber := s.GetLevelsNumber()
	representation := fmt.Sprintf("%d levels\n", levelsNumber)

	for level := 0; level < levelsNumber; level++ {
		currentNode := s.head.next[level]
		representation += string(currentNode.key)
		currentNode = currentNode.next[level]

		for currentNode != nil {
			representation += "->" + string(currentNode.key)
			currentNode = currentNode.next[level]
		}

		representation += "\n\n"
	}

	return representation
}

func (s *SkipList) removeHighestLevel() {
	head := s.head
	head.next = head.next[:len(head.next)-1]
}

func (s *SkipList) createLevel(firstNode *Node) {
	head := s.head
	firstNode.next = append(firstNode.next, nil)
	head.next = append(head.next, firstNode)
}

func (s *SkipList) findPredecessors(key []byte) []*Node {
	levelsNumber := s.GetLevelsNumber()
	result := make([]*Node, levelsNumber)

	currentNode := s.head
	for level := levelsNumber - 1; level >= 0; level-- {
		for {
			nextNode := currentNode.next[level]

			if nextNode == nil {
				result[level] = currentNode
				break
			}

			if keysCompare(nextNode.key, key) >= 0 {
				result[level] = currentNode
				break
			}

			currentNode = nextNode
		}
	}

	return result
}
