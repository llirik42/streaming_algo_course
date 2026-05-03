package lsm

import (
	"errors"
	"kvschool/internal/wal"
	"os"
)

const (
	WALFileName = "wal"
)

type walWrapper struct {
	file   *os.File
	writer *wal.Writer
}

func (w *walWrapper) getWriter() *wal.Writer {
	return w.writer
}

func (w *walWrapper) close() error {
	writerClosingError := w.writer.Close()
	fileClosingError := w.file.Close()
	return errors.Join(writerClosingError, fileClosingError)
}
