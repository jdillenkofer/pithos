package metadatastore

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

// ObjectKey is a value object representing a valid S3 object key.
type ObjectKey struct {
	value string
}

var (
	// ErrInvalidObjectKey is returned when an object key violates S3 naming rules
	ErrInvalidObjectKey = errors.New("invalid object key")
)

// NewObjectKey creates a new ObjectKey after validating it against S3 naming rules.
// Returns an error if the key is invalid.
//
// AWS S3 Object Key Naming Rules:
// - The name for a key is a sequence of Unicode characters whose UTF-8 encoding is at most 1024 bytes long.
func NewObjectKey(key string) (ObjectKey, error) {
	if err := validateObjectKey(key); err != nil {
		return ObjectKey{}, fmt.Errorf("%w: %v", ErrInvalidObjectKey, err)
	}
	return ObjectKey{value: key}, nil
}

// MustNewObjectKey creates a new ObjectKey and panics if the key is invalid.
// Use this only when you are certain the key is valid (e.g., in tests or with hardcoded values).
func MustNewObjectKey(key string) ObjectKey {
	ok, err := NewObjectKey(key)
	if err != nil {
		panic(err)
	}
	return ok
}

// String returns the object key as a string.
func (ok ObjectKey) String() string {
	return ok.value
}

// Equals checks if two ObjectKey instances are equal.
func (ok ObjectKey) Equals(other ObjectKey) bool {
	return ok.value == other.value
}

// IsEmpty returns true if the ObjectKey is the zero value.
func (ok ObjectKey) IsEmpty() bool {
	return ok.value == ""
}

// validateObjectKey validates an object key against S3 naming rules.
func validateObjectKey(key string) error {
	// Length check: UTF-8 encoding must be at most 1024 bytes long
	if len(key) > 1024 {
		return errors.New("object key must be at most 1024 bytes long")
	}

	// Key cannot be empty
	if len(key) == 0 {
		return errors.New("object key cannot be empty")
	}

	if !utf8.ValidString(key) {
		return errors.New("object key must be valid UTF-8")
	}

	return nil
}
