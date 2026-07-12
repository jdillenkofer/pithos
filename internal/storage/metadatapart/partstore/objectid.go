package partstore

import (
	"crypto/sha256"
	"encoding/hex"
)

// ObjectId is an opaque grouping key for placement hints. It identifies one
// logical object write so that stores can co-locate its parts; it carries no
// meaning beyond equality and is never used as identity (deduplication can
// attach one part to many objects).
type ObjectId [16]byte

// DeriveObjectId derives a stable grouping key from an object's coordinates.
// The discriminator separates concurrent writes to the same key when one is
// available (multipart uploadId, versionId); pass "" when there is none.
func DeriveObjectId(bucket string, key string, discriminator string) ObjectId {
	h := sha256.New()
	h.Write([]byte(bucket))
	h.Write([]byte{0})
	h.Write([]byte(key))
	h.Write([]byte{0})
	h.Write([]byte(discriminator))
	var id ObjectId
	copy(id[:], h.Sum(nil))
	return id
}

func (o ObjectId) String() string {
	return hex.EncodeToString(o[:])
}
