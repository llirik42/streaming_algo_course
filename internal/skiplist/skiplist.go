package skiplist

import (
	"bytes"
	"errors"
	"kvschool/internal/coinflipper"
)

// ErrNotFound означает отсутствие ключа (IMSI).
var ErrNotFound = errors.New("skiplist: ключ не найден")

// ErrNotImplemented используется в заготовке практики первого дня.
var ErrNotImplemented = errors.New("skiplist: функция не реализована")

// Iterator — упорядоченная итерация по диапазону ключей (Range Scan).
// В HLR используется для выгрузки абонентов по префиксу IMSI.
type Iterator interface {
	Next() (key, value []byte, ok bool, err error)
	Close() error
}

type Node struct {
	key       []byte
	value     []byte
	nextNodes []*Node
}

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
	probability := 0.1
	head := Node{key: []byte{}, nextNodes: []*Node{nil}}

	return &SkipList{
		head:        &head,
		coinFlipper: coinflipper.New(seed, probability),
	}
}

func (s *SkipList) IsEmpty() bool {
	return s.head.nextNodes[0] == nil
}

func (s *SkipList) findNearestNodes(key []byte) []*Node {
	levelsNumber := len(s.head.nextNodes)
	result := make([]*Node, levelsNumber)

	current := s.head
	for level := levelsNumber - 1; level >= 0; level-- {
		for {
			if current.nextNodes[level] == nil {
				result[level] = current
				break
			}

			if bytes.Compare(current.key, key) == 0 {
				panic("UnImplemented")
			}

			if (current == s.head || bytes.Compare(current.key, key) < 0) && bytes.Compare(current.nextNodes[level].key, key) > 0 {
				result[level] = current
				break
			}

			current = current.nextNodes[level]
		}
	}

	return result
}

func (s *SkipList) Put(key, value []byte) {
	nearestNodes := s.findNearestNodes(key)
	newNode := &Node{
		key:       key,
		value:     value,
		nextNodes: make([]*Node, 1),
	}

	// Гарантированная обработка нулевого уровня
	newNode.nextNodes[0] = nearestNodes[0].nextNodes[0]
	nearestNodes[0].nextNodes[0] = newNode

	for i := 1; ; i++ {
		if !s.coinFlipper.Flip() {
			break
		}

		if i >= len(nearestNodes) {
			newNode.nextNodes = append(newNode.nextNodes, nil)
			s.head.nextNodes = append(s.head.nextNodes, newNode)
		} else {
			newNode.nextNodes = append(newNode.nextNodes, nearestNodes[i].nextNodes[i])
			nearestNodes[i].nextNodes[i] = newNode
		}
	}
}

func (s *SkipList) Get(key []byte) ([]byte, error) {
	_ = s
	_ = key
	return nil, ErrNotImplemented
}

func (s *SkipList) Delete(key []byte) error {
	_ = s
	_ = key
	return ErrNotImplemented
}

// Scan возвращает итератор по диапазону [start, end).
// Если start == nil, считается -∞ (начало списка).
// Если end == nil, считается +∞ (конец списка).
func (s *SkipList) Scan(start, end []byte) (Iterator, error) {
	_ = s
	_ = start
	_ = end
	return nil, ErrNotImplemented
}

func (s *SkipList) GetRepresentation() string {
	if s.IsEmpty() {
		return "Empty SkipList"
	}

	representation := ""
	for i := range s.head.nextNodes {
		currentNode := s.head.nextNodes[i]
		representation += string(currentNode.key)
		currentNode = currentNode.nextNodes[i]

		for {
			if currentNode == nil {
				break
			}

			representation += "->" + string(currentNode.key)
			currentNode = currentNode.nextNodes[i]
		}

		representation += "\n"
	}

	return representation
}
