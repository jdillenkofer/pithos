package s3client

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestCopySourceValueEscapesSourceKey(t *testing.T) {
	srcBucket := storage.MustNewBucketName("source-bucket")
	srcKey := storage.MustNewObjectKey("folder/a b#c%25.txt")

	require.Equal(t, "source-bucket/folder%2Fa%20b%23c%2525.txt", copySourceValue(srcBucket, srcKey))
}
