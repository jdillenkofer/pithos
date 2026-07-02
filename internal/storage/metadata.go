package storage

import "errors"

// MaxUserMetadataSize is the maximum total size in bytes of the user-defined
// (x-amz-meta-*) metadata of an object, measured as the sum of the UTF-8 byte
// lengths of each key (without the "x-amz-meta-" prefix) and value, matching
// the S3 limit of 2 KB.
const MaxUserMetadataSize = 2 * 1024

// ErrMetadataTooLarge is returned when the user-defined metadata supplied on a
// PUT exceeds MaxUserMetadataSize.
var ErrMetadataTooLarge error = errors.New("MetadataTooLarge")

// ValidateUserMetadata checks that the given user-defined metadata satisfies
// the S3 size restriction. A nil or empty map is valid.
func ValidateUserMetadata(userMetadata map[string]string) error {
	size := 0
	for key, value := range userMetadata {
		size += len(key) + len(value)
	}
	if size > MaxUserMetadataSize {
		return ErrMetadataTooLarge
	}
	return nil
}
