package partstore

import (
	"encoding/hex"

	"github.com/oklog/ulid/v2"
)

// NewTemporaryPartFilename creates a unique staging filename for a part upload.
func NewTemporaryPartFilename(id PartId) string {
	return "." + hex.EncodeToString(id.Bytes()) + ".tmp." + ulid.Make().String()
}

// IsTemporaryPartFilename recognizes temporary uploads created by the local
// filesystem and SFTP stores. It deliberately excludes final parts and legacy
// transaction backups. Cleanup is safe only under exclusive store ownership.
func IsTemporaryPartFilename(name string) bool {
	if len(name) != 64 || name[0] != '.' || name[33:38] != ".tmp." {
		return false
	}
	if _, err := hex.DecodeString(name[1:33]); err != nil {
		return false
	}
	_, err := ulid.ParseStrict(name[38:])
	return err == nil
}
