package ioutils

// SyncDirectory is unavailable through Go's directory handles on Windows.
// File data is still flushed, but namespace durability is not guaranteed.
func SyncDirectory(path string) error {
	return nil
}
