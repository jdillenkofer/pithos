package metadatastore

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

// BucketName is a value object representing a valid S3 bucket name.
// It enforces all AWS S3 bucket naming rules.
type BucketName struct {
	value string
}

var (
	// ErrInvalidBucketName is returned when a bucket name violates S3 naming rules
	ErrInvalidBucketName = errors.New("invalid bucket name")
)

// Bucket names are validated for every row read from the database, so the
// character checks below are hand-written instead of using regular
// expressions.

func isLowerAlnum(c byte) bool {
	return ('a' <= c && c <= 'z') || ('0' <= c && c <= '9')
}

// hasValidBucketNameFormat reports whether name starts and ends with a
// lowercase letter or number and only contains lowercase letters, numbers,
// hyphens and dots (^[a-z0-9][a-z0-9.-]*[a-z0-9]$).
func hasValidBucketNameFormat(name string) bool {
	if len(name) < 2 || !isLowerAlnum(name[0]) || !isLowerAlnum(name[len(name)-1]) {
		return false
	}
	for i := 1; i < len(name)-1; i++ {
		if c := name[i]; !isLowerAlnum(c) && c != '-' && c != '.' {
			return false
		}
	}
	return true
}

// hasValidLabelFormat reports whether a multi-character label starts and ends
// with a lowercase letter or number and only contains lowercase letters,
// numbers and hyphens (^[a-z0-9][a-z0-9-]*[a-z0-9]$).
func hasValidLabelFormat(label string) bool {
	if len(label) < 2 || !isLowerAlnum(label[0]) || !isLowerAlnum(label[len(label)-1]) {
		return false
	}
	for i := 1; i < len(label)-1; i++ {
		if c := label[i]; !isLowerAlnum(c) && c != '-' {
			return false
		}
	}
	return true
}

// couldBeIPAddress reports whether name only consists of digits and dots. Any
// other character accepted by hasValidBucketNameFormat rules out an IP
// address, which lets the common case skip net.ParseIP and its allocation.
func couldBeIPAddress(name string) bool {
	for i := 0; i < len(name); i++ {
		if c := name[i]; c != '.' && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// NewBucketName creates a new BucketName after validating it against S3 naming rules.
// Returns an error if the name is invalid.
//
// AWS S3 Bucket Naming Rules:
// - Bucket names must be between 3 and 63 characters long
// - Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-)
// - Bucket names must begin and end with a letter or number
// - Bucket names must not be formatted as an IP address (e.g., 192.168.5.4)
// - Bucket names must not start with the prefix xn--
// - Bucket names must not start with the prefix sthree- or sthree-configurator
// - Bucket names must not end with the suffix -s3alias or --ol-s3
// - Each label (segment between dots) must start and end with lowercase letter or number
// - Labels must not contain consecutive hyphens
func NewBucketName(name string) (BucketName, error) {
	if err := validateBucketName(name); err != nil {
		return BucketName{}, fmt.Errorf("%w: %v", ErrInvalidBucketName, err)
	}
	return BucketName{value: name}, nil
}

// MustNewBucketName creates a new BucketName and panics if the name is invalid.
// Use this only when you are certain the name is valid (e.g., in tests or with hardcoded values).
func MustNewBucketName(name string) BucketName {
	bn, err := NewBucketName(name)
	if err != nil {
		panic(err)
	}
	return bn
}

// String returns the bucket name as a string.
func (bn BucketName) String() string {
	return bn.value
}

// Equals checks if two BucketName instances are equal.
func (bn BucketName) Equals(other BucketName) bool {
	return bn.value == other.value
}

// IsEmpty returns true if the BucketName is the zero value.
func (bn BucketName) IsEmpty() bool {
	return bn.value == ""
}

// validateBucketName validates a bucket name against all S3 naming rules.
func validateBucketName(name string) error {
	// Length check: must be between 3 and 63 characters
	if len(name) < 3 || len(name) > 63 {
		return errors.New("bucket name must be between 3 and 63 characters long")
	}

	// Basic format check
	if !hasValidBucketNameFormat(name) {
		return errors.New("bucket name must contain only lowercase letters, numbers, dots, and hyphens, and must start and end with a letter or number")
	}

	// Check for IP address format
	if couldBeIPAddress(name) && net.ParseIP(name) != nil {
		return errors.New("bucket name must not be formatted as an IP address")
	}

	// Check for prohibited prefixes
	if strings.HasPrefix(name, "xn--") {
		return errors.New("bucket name must not start with 'xn--'")
	}
	if strings.HasPrefix(name, "sthree-") || strings.HasPrefix(name, "sthree-configurator") {
		return errors.New("bucket name must not start with 'sthree-' or 'sthree-configurator'")
	}

	// Check for prohibited suffixes
	if strings.HasSuffix(name, "-s3alias") || strings.HasSuffix(name, "--ol-s3") {
		return errors.New("bucket name must not end with '-s3alias' or '--ol-s3'")
	}

	// Check consecutive hyphens
	if strings.Contains(name, "--") {
		return errors.New("bucket name must not contain consecutive hyphens")
	}

	// Check dots adjacent to hyphens
	if strings.Contains(name, ".-") || strings.Contains(name, "-.") {
		return errors.New("bucket name must not have dots adjacent to hyphens")
	}

	// Validate each label (segment between dots)
	if strings.Contains(name, ".") {
		for label := range strings.SplitSeq(name, ".") {
			if len(label) == 0 {
				return errors.New("bucket name must not contain consecutive dots")
			}
			if len(label) == 1 {
				if !isLowerAlnum(label[0]) {
					return errors.New("each label must contain only lowercase letters, numbers, and hyphens")
				}
			} else {
				if !hasValidLabelFormat(label) {
					return errors.New("each label must start and end with a lowercase letter or number")
				}
			}
		}
	}

	return nil
}
