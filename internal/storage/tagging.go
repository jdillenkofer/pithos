package storage

import (
	"errors"
	"net/url"
)

// Object tagging limits as defined by Amazon S3.
const (
	// MaxObjectTags is the maximum number of tags that may be associated with a
	// single object.
	MaxObjectTags = 10
	// MaxTagKeyLength is the maximum length of a tag key in characters.
	MaxTagKeyLength = 128
	// MaxTagValueLength is the maximum length of a tag value in characters.
	MaxTagValueLength = 256
)

// ErrInvalidTag is returned when a supplied tag set violates the S3 tagging
// restrictions (too many tags, duplicate keys, or over-long key/value).
var ErrInvalidTag error = errors.New("InvalidTag")

// ValidateTags checks that the given tag set satisfies the S3 object tagging
// restrictions. It returns ErrInvalidTag if any restriction is violated.
// A nil or empty map is valid.
func ValidateTags(tags map[string]string) error {
	if len(tags) > MaxObjectTags {
		return ErrInvalidTag
	}
	for key, value := range tags {
		if len(key) == 0 || len([]rune(key)) > MaxTagKeyLength {
			return ErrInvalidTag
		}
		if len([]rune(value)) > MaxTagValueLength {
			return ErrInvalidTag
		}
	}
	return nil
}

// ParseTaggingHeader parses the value of the x-amz-tagging header, which is a
// URL-query-encoded set of key/value pairs (e.g. "k1=v1&k2=v2"). An empty
// header yields an empty (non-nil) map. Duplicate keys are rejected with
// ErrInvalidTag. The returned tags are validated with ValidateTags.
func ParseTaggingHeader(header string) (map[string]string, error) {
	tags := map[string]string{}
	if header == "" {
		return tags, nil
	}
	values, err := url.ParseQuery(header)
	if err != nil {
		return nil, ErrInvalidTag
	}
	for key, vs := range values {
		// Duplicate keys (multiple values for the same key) are not allowed.
		if len(vs) != 1 {
			return nil, ErrInvalidTag
		}
		tags[key] = vs[0]
	}
	if err := ValidateTags(tags); err != nil {
		return nil, err
	}
	return tags, nil
}
