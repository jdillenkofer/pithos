package partstore

import "encoding/hex"

const partFilenameLength = 32 // 16 part id bytes, hex encoded
const shardDirNameLength = 2

// PartFilename returns the canonical filename for a part: the hex-encoded
// part id.
func PartFilename(partId PartId) string {
	return hex.EncodeToString(partId.Bytes())
}

// ShardDirName returns the name of the shard directory a part file is stored
// in. Part ids are ULIDs whose leading bytes encode a timestamp, so the
// leading hex characters barely vary between parts; the trailing characters
// come from the random portion and give a uniform 256-way fan-out.
func ShardDirName(partFilename string) string {
	return partFilename[len(partFilename)-shardDirNameLength:]
}

// IsShardDirName reports whether name is a valid shard directory name.
func IsShardDirName(name string) bool {
	if len(name) != shardDirNameLength {
		return false
	}
	_, err := hex.DecodeString(name)
	return err == nil
}

// TryGetPartIdFromFilename parses a part id from its filename (without
// directory components).
func TryGetPartIdFromFilename(filename string) (*PartId, bool) {
	if len(filename) != partFilenameLength {
		return nil, false
	}
	partIdBytes, err := hex.DecodeString(filename)
	if err != nil {
		return nil, false
	}
	partId, err := NewPartIdFromBytes(partIdBytes)
	if err != nil {
		return nil, false
	}
	return partId, true
}
