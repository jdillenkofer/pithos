package metadatastore

import (
	"errors"
	"testing"
)

func TestNewBucketName(t *testing.T) {
	tests := []struct {
		name        string
		bucketName  string
		expectError bool
		errorMsg    string
	}{
		// Valid bucket names
		{
			name:        "valid simple name",
			bucketName:  "mybucket",
			expectError: false,
		},
		{
			name:        "valid name with numbers",
			bucketName:  "my-bucket-123",
			expectError: false,
		},
		{
			name:        "valid name with dots",
			bucketName:  "my.bucket.name",
			expectError: false,
		},
		{
			name:        "valid name with hyphens and dots",
			bucketName:  "my-bucket.name-123",
			expectError: false,
		},
		{
			name:        "minimum length (3 chars)",
			bucketName:  "abc",
			expectError: false,
		},
		{
			name:        "maximum length (63 chars)",
			bucketName:  "a123456789012345678901234567890123456789012345678901234567890bc",
			expectError: false,
		},

		// Invalid bucket names - length
		{
			name:        "too short (2 chars)",
			bucketName:  "ab",
			expectError: true,
			errorMsg:    "must be between 3 and 63 characters long",
		},
		{
			name:        "too long (64 chars)",
			bucketName:  "a1234567890123456789012345678901234567890123456789012345678901234",
			expectError: true,
			errorMsg:    "must be between 3 and 63 characters long",
		},

		// Invalid bucket names - character restrictions
		{
			name:        "uppercase letters",
			bucketName:  "MyBucket",
			expectError: true,
			errorMsg:    "must contain only lowercase letters",
		},
		{
			name:        "underscore",
			bucketName:  "my_bucket",
			expectError: true,
			errorMsg:    "must contain only lowercase letters",
		},
		{
			name:        "special characters",
			bucketName:  "my-bucket!",
			expectError: true,
			errorMsg:    "must contain only lowercase letters",
		},
		{
			name:        "spaces",
			bucketName:  "my bucket",
			expectError: true,
			errorMsg:    "must contain only lowercase letters",
		},

		// Invalid bucket names - start/end restrictions
		{
			name:        "starts with hyphen",
			bucketName:  "-mybucket",
			expectError: true,
			errorMsg:    "must start and end with a letter or number",
		},
		{
			name:        "ends with hyphen",
			bucketName:  "mybucket-",
			expectError: true,
			errorMsg:    "must start and end with a letter or number",
		},
		{
			name:        "starts with dot",
			bucketName:  ".mybucket",
			expectError: true,
			errorMsg:    "must start and end with a letter or number",
		},
		{
			name:        "ends with dot",
			bucketName:  "mybucket.",
			expectError: true,
			errorMsg:    "must start and end with a letter or number",
		},

		// Invalid bucket names - IP address format
		{
			name:        "formatted as IPv4 address",
			bucketName:  "192.168.1.1",
			expectError: true,
			errorMsg:    "must not be formatted as an IP address",
		},

		// Invalid bucket names - prohibited prefixes
		{
			name:        "starts with xn--",
			bucketName:  "xn--mybucket",
			expectError: true,
			errorMsg:    "must not start with 'xn--'",
		},
		{
			name:        "starts with sthree-",
			bucketName:  "sthree-mybucket",
			expectError: true,
			errorMsg:    "must not start with 'sthree-'",
		},
		{
			name:        "starts with sthree-configurator",
			bucketName:  "sthree-configurator-bucket",
			expectError: true,
			errorMsg:    "must not start with 'sthree-'",
		},

		// Invalid bucket names - prohibited suffixes
		{
			name:        "ends with -s3alias",
			bucketName:  "mybucket-s3alias",
			expectError: true,
			errorMsg:    "must not end with '-s3alias'",
		},
		{
			name:        "ends with --ol-s3",
			bucketName:  "mybucket--ol-s3",
			expectError: true,
			errorMsg:    "must not end with",
		},

		// Invalid bucket names - consecutive characters
		{
			name:        "consecutive hyphens",
			bucketName:  "my--bucket",
			expectError: true,
			errorMsg:    "must not contain consecutive hyphens",
		},
		{
			name:        "dot adjacent to hyphen (dot-hyphen)",
			bucketName:  "my.-bucket",
			expectError: true,
			errorMsg:    "must not have dots adjacent to hyphens",
		},
		{
			name:        "hyphen adjacent to dot (hyphen-dot)",
			bucketName:  "my-.bucket",
			expectError: true,
			errorMsg:    "must not have dots adjacent to hyphens",
		},
		{
			name:        "consecutive dots",
			bucketName:  "my..bucket",
			expectError: true,
			errorMsg:    "must not contain consecutive dots",
		},

		// Invalid bucket names - label restrictions
		{
			name:        "label starts with hyphen",
			bucketName:  "my.-test.bucket",
			expectError: true,
			errorMsg:    "must not have dots adjacent to hyphens",
		},
		{
			name:        "label ends with hyphen",
			bucketName:  "my.test-.bucket",
			expectError: true,
			errorMsg:    "must not have dots adjacent to hyphens",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bn, err := NewBucketName(tt.bucketName)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none for bucket name: %s", tt.bucketName)
					return
				}
				if !errors.Is(err, ErrInvalidBucketName) {
					t.Errorf("expected ErrInvalidBucketName but got: %v", err)
				}
				if tt.errorMsg != "" && !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error message to contain %q but got: %v", tt.errorMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for valid bucket name %q: %v", tt.bucketName, err)
					return
				}
				if bn.String() != tt.bucketName {
					t.Errorf("expected bucket name %q but got %q", tt.bucketName, bn.String())
				}
			}
		})
	}
}

func TestMustNewBucketName(t *testing.T) {
	t.Run("valid name does not panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("MustNewBucketName panicked on valid name: %v", r)
			}
		}()
		bn := MustNewBucketName("validbucket")
		if bn.String() != "validbucket" {
			t.Errorf("expected 'validbucket' but got %q", bn.String())
		}
	})

	t.Run("invalid name panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustNewBucketName should have panicked on invalid name")
			}
		}()
		MustNewBucketName("ab") // too short
	})
}

func TestBucketName_String(t *testing.T) {
	bn := MustNewBucketName("testbucket")
	if bn.String() != "testbucket" {
		t.Errorf("expected 'testbucket' but got %q", bn.String())
	}
}

func TestBucketName_Equals(t *testing.T) {
	bn1 := MustNewBucketName("mybucket")
	bn2 := MustNewBucketName("mybucket")
	bn3 := MustNewBucketName("otherbucket")

	if !bn1.Equals(bn2) {
		t.Error("expected bn1 and bn2 to be equal")
	}

	if bn1.Equals(bn3) {
		t.Error("expected bn1 and bn3 to not be equal")
	}
}

func TestBucketName_IsEmpty(t *testing.T) {
	var bn BucketName
	if !bn.IsEmpty() {
		t.Error("expected zero value BucketName to be empty")
	}

	bn = MustNewBucketName("mybucket")
	if bn.IsEmpty() {
		t.Error("expected non-zero BucketName to not be empty")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
