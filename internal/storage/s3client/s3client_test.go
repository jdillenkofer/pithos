package s3client

import (
	"errors"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestCopySourceValueEscapesSourceKey(t *testing.T) {
	testutils.SkipIfIntegration(t)

	srcBucket := storage.MustNewBucketName("source-bucket")
	srcKey := storage.MustNewObjectKey("folder/a b#c%25.txt")

	require.Equal(t, "source-bucket/folder%2Fa%20b%23c%2525.txt", copySourceValue(srcBucket, srcKey))
}

func TestTranslateS3CopyErrorMapsS3ErrorCodes(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tests := []struct {
		name string
		code string
		want error
	}{
		{name: "no such bucket", code: "NoSuchBucket", want: storage.ErrNoSuchBucket},
		{name: "no such key", code: "NoSuchKey", want: storage.ErrNoSuchKey},
		{name: "precondition failed", code: "PreconditionFailed", want: storage.ErrPreconditionFailed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := translateS3CopyError(&smithy.GenericAPIError{Code: tc.code, Message: tc.code})

			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestTranslateS3CopyErrorPreservesUnknownErrors(t *testing.T) {
	testutils.SkipIfIntegration(t)

	unknownErr := errors.New("remote failure")

	err := translateS3CopyError(unknownErr)

	require.ErrorIs(t, err, unknownErr)
}
