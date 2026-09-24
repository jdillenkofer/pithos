package policy

import (
	"fmt"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
)

type check struct {
	action, resource string
	source           bool
}

var operationActions = map[string]string{
	authorization.OperationListBuckets: "s3:ListAllMyBuckets", authorization.OperationHeadBucket: "s3:ListBucket",
	authorization.OperationListObjects: "s3:ListBucket", authorization.OperationListObjectVersions: "s3:ListBucketVersions",
	authorization.OperationListMultipartUploads: "s3:ListBucketMultipartUploads", authorization.OperationCreateBucket: "s3:CreateBucket",
	authorization.OperationDeleteBucket: "s3:DeleteBucket", authorization.OperationGetObject: "s3:GetObject",
	authorization.OperationHeadObject: "s3:GetObject", authorization.OperationGetObjectVersion: "s3:GetObjectVersion",
	authorization.OperationHeadObjectVersion: "s3:GetObjectVersion", authorization.OperationPutObject: "s3:PutObject",
	authorization.OperationAppendObject: "s3:PutObject", authorization.OperationCreateMultipartUpload: "s3:PutObject",
	authorization.OperationUploadPart: "s3:PutObject", authorization.OperationCompleteMultipartUpload: "s3:PutObject",
	authorization.OperationAbortMultipartUpload: "s3:AbortMultipartUpload", authorization.OperationListParts: "s3:ListMultipartUploadParts",
	authorization.OperationDeleteObject: "s3:DeleteObject", authorization.OperationDeleteObjectVersion: "s3:DeleteObjectVersion",
	authorization.OperationGetBucketCORS: "s3:GetBucketCORS", authorization.OperationPutBucketCORS: "s3:PutBucketCORS", authorization.OperationDeleteBucketCORS: "s3:PutBucketCORS",
	authorization.OperationGetBucketWebsite: "s3:GetBucketWebsite", authorization.OperationPutBucketWebsite: "s3:PutBucketWebsite", authorization.OperationDeleteBucketWebsite: "s3:DeleteBucketWebsite",
	authorization.OperationGetBucketVersioning: "s3:GetBucketVersioning", authorization.OperationPutBucketVersioning: "s3:PutBucketVersioning",
	authorization.OperationGetObjectTagging: "s3:GetObjectTagging", authorization.OperationPutObjectTagging: "s3:PutObjectTagging", authorization.OperationDeleteObjectTagging: "s3:DeleteObjectTagging",
	authorization.OperationGetObjectVersionTagging: "s3:GetObjectVersionTagging", authorization.OperationPutObjectVersionTagging: "s3:PutObjectVersionTagging", authorization.OperationDeleteObjectVersionTagging: "s3:DeleteObjectVersionTagging",
	authorization.OperationGetBucketTagging: "s3:GetBucketTagging", authorization.OperationPutBucketTagging: "s3:PutBucketTagging", authorization.OperationDeleteBucketTagging: "s3:PutBucketTagging",
	authorization.OperationGetBucketLifecycle: "s3:GetLifecycleConfiguration", authorization.OperationPutBucketLifecycle: "s3:PutLifecycleConfiguration", authorization.OperationDeleteBucketLifecycle: "s3:PutLifecycleConfiguration",
	authorization.OperationGetBucketNotification: "s3:GetBucketNotification", authorization.OperationPutBucketNotification: "s3:PutBucketNotification",
	authorization.OperationGetObjectLockConfiguration: "s3:GetBucketObjectLockConfiguration", authorization.OperationPutObjectLockConfiguration: "s3:PutBucketObjectLockConfiguration",
	authorization.OperationGetObjectRetention: "s3:GetObjectRetention", authorization.OperationPutObjectRetention: "s3:PutObjectRetention",
	authorization.OperationGetObjectLegalHold: "s3:GetObjectLegalHold", authorization.OperationPutObjectLegalHold: "s3:PutObjectLegalHold",
	authorization.OperationBypassGovernanceRetention: "s3:BypassGovernanceRetention",
}

func checksFor(r *authorization.Request) ([]check, error) {
	resource := "*"
	if r.Bucket != nil {
		resource = "arn:aws:s3:::" + *r.Bucket
		if r.Key != nil {
			resource += "/" + *r.Key
		}
	}
	if r.Operation == authorization.OperationCopyObject || r.Operation == authorization.OperationUploadPartCopy {
		if r.SourceBucket == nil || r.SourceKey == nil {
			return nil, fmt.Errorf("copy request has no source")
		}
		read := "s3:GetObject"
		if r.VersionID != nil {
			read = "s3:GetObjectVersion"
		}
		checks := []check{{read, "arn:aws:s3:::" + *r.SourceBucket + "/" + *r.SourceKey, true}, {"s3:PutObject", resource, false}}
		if len(r.RequestObjectTags) > 0 {
			checks = append(checks, check{"s3:PutObjectTagging", resource, false})
		}
		return appendObjectLockChecks(checks, r, resource), nil
	}
	action, ok := operationActions[r.Operation]
	if !ok {
		return nil, fmt.Errorf("unsupported operation %q", r.Operation)
	}
	checks := []check{{action, resource, false}}
	if len(r.RequestObjectTags) > 0 && (r.Operation == authorization.OperationPutObject || r.Operation == authorization.OperationCreateMultipartUpload) {
		checks = append(checks, check{"s3:PutObjectTagging", resource, false})
	}
	return appendObjectLockChecks(checks, r, resource), nil
}

func appendObjectLockChecks(checks []check, r *authorization.Request, resource string) []check {
	// Only these operations store lock metadata alongside an object write.
	// Bucket defaults and dedicated retention/legal-hold requests are already
	// covered by their own primary action.
	switch r.Operation {
	case authorization.OperationPutObject, authorization.OperationAppendObject,
		authorization.OperationCreateMultipartUpload, authorization.OperationCopyObject:
	default:
		return checks
	}
	if r.ObjectLockRetainUntilDate != nil || r.ObjectLockMode != nil {
		checks = append(checks, check{"s3:PutObjectRetention", resource, false})
	}
	if r.ObjectLockLegalHold != nil {
		checks = append(checks, check{"s3:PutObjectLegalHold", resource, false})
	}
	return checks
}

func SupportedActions() []string {
	seen := map[string]bool{}
	out := []string{}
	for _, a := range operationActions {
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	out = append(out, "s3:GetObjectVersion", "s3:PutObject")
	return out
}
