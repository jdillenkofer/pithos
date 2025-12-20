package metadatastore

import (
	"errors"
	"fmt"

	"github.com/oklog/ulid/v2"
)

// UploadId is a value object representing a valid S3 multipart upload ID.
type UploadId struct {
	value string
}

var (
	// ErrInvalidUploadId is returned when an upload ID is invalid
	ErrInvalidUploadId = errors.New("invalid upload ID")
)

func NewRandomUploadId() UploadId {
	return UploadId{value: ulid.Make().String()}
}

// NewUploadId creates a new UploadId.
// Returns an error if the ID is invalid (e.g. empty).
func NewUploadId(id string) (UploadId, error) {
	if id == "" {
		return UploadId{}, fmt.Errorf("%w: empty upload ID", ErrInvalidUploadId)
	}
	return UploadId{value: id}, nil
}

// MustNewUploadId creates a new UploadId and panics if the ID is invalid.
func MustNewUploadId(id string) UploadId {
	uid, err := NewUploadId(id)
	if err != nil {
		panic(err)
	}
	return uid
}

// String returns the upload ID as a string.
func (uid UploadId) String() string {
	return uid.value
}

// Equals checks if two UploadId instances are equal.
func (uid UploadId) Equals(other UploadId) bool {
	return uid.value == other.value
}
