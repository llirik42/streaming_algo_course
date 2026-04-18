package iterator

type Iterator interface {
	Next() (key, value []byte, ok bool, err error)
	Close() error
}
