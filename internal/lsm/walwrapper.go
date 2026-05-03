package lsm

import (
	"errors"
	"kvschool/internal/wal"
	"os"
)

const (
	WALFileName = "wal"
)

type WALWrapper struct {
	file   *os.File
	writer *wal.Writer
}

func (w *WALWrapper) GetWriter() *wal.Writer {
	return w.writer
}

func (w *WALWrapper) Close() error {
	writerClosingError := w.writer.Close()
	fileClosingError := w.file.Close()
	return errors.Join(writerClosingError, fileClosingError)
}
