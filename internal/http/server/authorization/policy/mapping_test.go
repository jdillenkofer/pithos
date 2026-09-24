package policy

import (
	"context"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestBucketDefaultRetentionNeedsOnlyBucketPermission(t *testing.T) {
	testutils.SkipIfIntegration(t)
	s := compileTestPolicy(t, `{"Effect":"Allow","Action":"s3:PutBucketObjectLockConfiguration","Resource":"arn:aws:s3:::bucket"}`)
	r := request(authorization.OperationPutObjectLockConfiguration, "bucket", "")
	r.Key = nil
	r.ObjectLockMode = stringPointer("GOVERNANCE")
	days := int32(30)
	r.ObjectLockDays = &days
	d, err := s.AuthorizeRequest(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, authorization.Allow, d.Effect)
}

func TestLockMetadataRequiresPermissionsOnlyOnObjectWrites(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		operation string
		actions   []string
	}{
		{authorization.OperationPutObject, []string{"s3:PutObject", "s3:PutObjectRetention", "s3:PutObjectLegalHold"}},
		{authorization.OperationAppendObject, []string{"s3:PutObject", "s3:PutObjectRetention", "s3:PutObjectLegalHold"}},
		{authorization.OperationCreateMultipartUpload, []string{"s3:PutObject", "s3:PutObjectRetention", "s3:PutObjectLegalHold"}},
		{authorization.OperationCopyObject, []string{"s3:GetObject", "s3:PutObject", "s3:PutObjectRetention", "s3:PutObjectLegalHold"}},
		{authorization.OperationUploadPartCopy, []string{"s3:GetObject", "s3:PutObject"}},
		{authorization.OperationUploadPart, []string{"s3:PutObject"}},
		{authorization.OperationCompleteMultipartUpload, []string{"s3:PutObject"}},
		{authorization.OperationGetObject, []string{"s3:GetObject"}},
		{authorization.OperationPutObjectRetention, []string{"s3:PutObjectRetention"}},
		{authorization.OperationPutObjectLegalHold, []string{"s3:PutObjectLegalHold"}},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			r := request(tc.operation, "bucket", "key")
			r.SourceBucket, r.SourceKey = stringPointer("source"), stringPointer("original")
			r.ObjectLockMode = stringPointer("GOVERNANCE")
			r.ObjectLockRetainUntilDate = stringPointer("2030-01-01T00:00:00Z")
			r.ObjectLockLegalHold = stringPointer("ON")
			checks, err := checksFor(r)
			require.NoError(t, err)
			actions := make([]string, len(checks))
			for i, c := range checks {
				actions[i] = c.action
			}
			require.Equal(t, tc.actions, actions)
		})
	}
}
