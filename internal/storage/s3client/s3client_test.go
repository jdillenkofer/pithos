package s3client

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestIsExistingBucketError(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "access denied API error",
			err:  &smithy.GenericAPIError{Code: "AccessDenied", Message: "access denied"},
			want: true,
		},
		{
			name: "forbidden HTTP response",
			err: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusForbidden}},
				Err:      errors.New("forbidden"),
			},
			want: true,
		},
		{
			name: "unrelated API error",
			err:  &smithy.GenericAPIError{Code: "InternalError", Message: "remote failure"},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isExistingBucketError(tc.err))
		})
	}
}

func TestPublicBucketTagsHidesOwnerAccountID(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tags := publicBucketTags([]types.Tag{
		{Key: aws.String(ownerAccountIDBucketTag), Value: aws.String("account-a")},
		{Key: aws.String("environment"), Value: aws.String("production")},
	})

	require.Equal(t, map[string]string{"environment": "production"}, tags)
}

func TestBucketTagSetWithOwnerPreservesAuthoritativeOwner(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tagSet := bucketTagSetWithOwner(map[string]string{
		ownerAccountIDBucketTag: "account-b",
		"environment":           "production",
	}, "account-a")

	require.Contains(t, tagSet, types.Tag{Key: aws.String("environment"), Value: aws.String("production")})
	require.Contains(t, tagSet, types.Tag{Key: aws.String(ownerAccountIDBucketTag), Value: aws.String("account-a")})
	require.NotContains(t, tagSet, types.Tag{Key: aws.String(ownerAccountIDBucketTag), Value: aws.String("account-b")})
}

func TestS3ClientUserBucketTagLimitReservesSystemTags(t *testing.T) {
	testutils.SkipIfIntegration(t)

	require.Equal(t, 45, maxUserBucketTags)
	tags := make(map[string]string, maxUserBucketTags)
	for i := range maxUserBucketTags {
		tags[fmt.Sprintf("tag-%d", i)] = "value"
	}
	require.NoError(t, validateUserBucketTags(tags))

	tags["one-too-many"] = "value"
	require.ErrorIs(t, validateUserBucketTags(tags), storage.ErrInvalidTag)
}

func TestCopySourceValueEscapesSourceKey(t *testing.T) {
	testutils.SkipIfIntegration(t)

	srcBucket := storage.MustNewBucketName("source-bucket")
	srcKey := storage.MustNewObjectKey("folder/a b#c%25.txt")

	require.Equal(t, "source-bucket/folder%2Fa%20b%23c%2525.txt", copySourceValue(srcBucket, srcKey, nil))
	versionID := "v 1/2"
	require.Equal(t, "source-bucket/folder%2Fa%20b%23c%2525.txt?versionId=v+1%2F2", copySourceValue(srcBucket, srcKey, &versionID))
}

func TestByteRangeToAWSRangeUsesExactSuffixLength(t *testing.T) {
	testutils.SkipIfIntegration(t)

	suffixLength := int64(5)

	require.Equal(t, "bytes=-5", byteRangeToAWSRange(storage.ByteRange{End: &suffixLength}))
}

func TestChecksumAlgorithmFromInputOnlyForDeclaredChecksums(t *testing.T) {
	testutils.SkipIfIntegration(t)

	algorithm := "crc32c"
	checksum := "AAAAAA=="

	require.Equal(t, "", string(checksumAlgorithmFromInput(&storage.ChecksumInput{
		ChecksumAlgorithm: &algorithm,
	})))
	require.Equal(t, "CRC32C", string(checksumAlgorithmFromInput(&storage.ChecksumInput{
		ChecksumAlgorithm: &algorithm,
		ChecksumCRC32C:    &checksum,
	})))
}

func TestContentMD5FromETagConvertsQuotedHexDigest(t *testing.T) {
	testutils.SkipIfIntegration(t)

	etag := "\"5d41402abc4b2a76b9719d911017c592\""
	invalidETag := "\"not-md5\""

	md5Header := contentMD5FromETag(&etag)

	require.NotNil(t, md5Header)
	require.Equal(t, "XUFAKrxLKna5cZ2REBfFkg==", *md5Header)
	require.Nil(t, contentMD5FromETag(&invalidETag))
	require.Nil(t, contentMD5FromETag(nil))
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

func TestConvertLifecycleRulePreservesTransitions(t *testing.T) {
	testutils.SkipIfIntegration(t)

	date := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	internalRule := storage.LifecycleRule{
		ID:     aws.String("transition-cold"),
		Status: storage.LifecycleRuleStatusEnabled,
		Filter: &storage.LifecycleFilter{
			Prefix: aws.String("cold/"),
		},
		Transitions: []storage.LifecycleTransition{
			{Days: aws.Int32(30), StorageClass: "STANDARD_IA"},
			{Date: &date, StorageClass: "GLACIER"},
		},
	}

	sdkRule := convertLifecycleRuleToSdk(internalRule)
	require.Len(t, sdkRule.Transitions, 2)
	require.Equal(t, int32(30), aws.ToInt32(sdkRule.Transitions[0].Days))
	require.Equal(t, types.TransitionStorageClassStandardIa, sdkRule.Transitions[0].StorageClass)
	require.Equal(t, date, aws.ToTime(sdkRule.Transitions[1].Date))
	require.Equal(t, types.TransitionStorageClassGlacier, sdkRule.Transitions[1].StorageClass)

	converted := convertLifecycleRuleFromSdk(sdkRule)
	require.Equal(t, internalRule.Transitions, converted.Transitions)
}

func TestConvertLifecycleRulePreservesNoncurrentVersionTransitions(t *testing.T) {
	testutils.SkipIfIntegration(t)

	internalRule := storage.LifecycleRule{
		ID:     aws.String("noncurrent-transition-cold"),
		Status: storage.LifecycleRuleStatusEnabled,
		Filter: &storage.LifecycleFilter{
			Prefix: aws.String("cold/"),
		},
		NoncurrentVersionTransitions: []storage.LifecycleNoncurrentVersionTransition{
			{
				NoncurrentDays:          aws.Int32(30),
				NewerNoncurrentVersions: aws.Int32(2),
				StorageClass:            "GLACIER",
			},
		},
	}

	sdkRule := convertLifecycleRuleToSdk(internalRule)
	require.Len(t, sdkRule.NoncurrentVersionTransitions, 1)
	require.Equal(t, int32(30), aws.ToInt32(sdkRule.NoncurrentVersionTransitions[0].NoncurrentDays))
	require.Equal(t, int32(2), aws.ToInt32(sdkRule.NoncurrentVersionTransitions[0].NewerNoncurrentVersions))
	require.Equal(t, types.TransitionStorageClassGlacier, sdkRule.NoncurrentVersionTransitions[0].StorageClass)

	converted := convertLifecycleRuleFromSdk(sdkRule)
	require.Equal(t, internalRule.NoncurrentVersionTransitions, converted.NoncurrentVersionTransitions)
}
