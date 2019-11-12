package common

import "os"

// pathExists returns true if the given path exists.
func PathExists(p string) bool {
	if _, err := os.Lstat(p); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

// Memset sets each uint64 in data to value.
func Memset(data []uint64, value uint64) {
	if len(data) != 0 {
		data[0] = value
		for i := 1; i < len(data); i *= 2 {
			copy(data[i:], data[:i])
		}
	}
}
