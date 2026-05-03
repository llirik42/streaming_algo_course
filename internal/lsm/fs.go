package lsm

import (
	"fmt"
	"os"
)

func makeDirectoryIfNotExists(path string) error {
	if !directoryExists(path) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("lsm makeDirectoryIfNotExists: making directory %s: %w", path, err)
		}
	}

	return nil
}

func directoryExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}
