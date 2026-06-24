package authorization

import "context"

type Authorization struct {
	AccessKeyId *string
}

type HTTPRequest struct {
	Method        string
	Path          string
	Query         string
	QueryParams   map[string][]string
	Headers       map[string][]string
	Host          string
	Proto         string
	ContentLength *int
	RemoteAddr    string
	RemoteIP      *string
	ClientIP      *string
	Scheme        string
}

const (
	OperationListBuckets             = "ListBuckets"
	OperationHeadBucket              = "HeadBucket"
	OperationListMultipartUploads    = "ListMultipartUploads"
	OperationListObjects             = "ListObjects"
	OperationCreateBucket            = "CreateBucket"
	OperationDeleteBucket            = "DeleteBucket"
	OperationHeadObject              = "HeadObject"
	OperationListParts               = "ListParts"
	OperationGetObject               = "GetObject"
	OperationCreateMultipartUpload   = "CreateMultipartUpload"
	OperationCompleteMultipartUpload = "CompleteMultipartUpload"
	OperationUploadPart              = "UploadPart"
	OperationUploadPartCopy          = "UploadPartCopy"
	OperationPutObject               = "PutObject"
	OperationCopyObject              = "CopyObject"
	OperationAppendObject            = "AppendObject"
	OperationAbortMultipartUpload    = "AbortMultipartUpload"
	OperationDeleteObject            = "DeleteObject"
	OperationDeleteObjects           = "DeleteObjects"
	OperationGetBucketCORS           = "GetBucketCORS"
	OperationPutBucketCORS           = "PutBucketCORS"
	OperationDeleteBucketCORS        = "DeleteBucketCORS"
	OperationGetBucketWebsite        = "GetBucketWebsite"
	OperationPutBucketWebsite        = "PutBucketWebsite"
	OperationDeleteBucketWebsite     = "DeleteBucketWebsite"
	OperationGetObjectTagging        = "GetObjectTagging"
	OperationPutObjectTagging        = "PutObjectTagging"
	OperationDeleteObjectTagging     = "DeleteObjectTagging"
)

type Request struct {
	Operation     string
	Authorization Authorization
	Bucket        *string
	Key           *string
	// SourceBucket and SourceKey identify the copy source for server-side copy
	// operations (CopyObject and UploadPartCopy). They are nil for all other
	// operations. Bucket/Key always refer to the destination.
	SourceBucket *string
	SourceKey    *string
	HttpRequest  HTTPRequest
	// ResolveExistingObjectTags lazily returns the tags currently stored on the
	// object this request targets (the s3:ExistingObjectTag condition). It is nil
	// when the request has no single target object (e.g. ListBuckets). The server
	// implements it over the storage layer; the authorizer must only invoke it
	// when a policy actually reads object tags. A returned error must fail closed.
	ResolveExistingObjectTags func(ctx context.Context) (map[string]string, error)
	// RequestObjectTags holds the tags supplied in the request itself (the
	// s3:RequestObjectTag condition) via the x-amz-tagging header or the
	// PutObjectTagging body. Nil when the request carries no tags.
	RequestObjectTags map[string]string
}

type RequestAuthorizer interface {
	AuthorizeRequest(ctx context.Context, request *Request) (bool, error)
}

type RequestResourceAuthorizer interface {
	AuthorizeListBucket(ctx context.Context, request *Request, bucketName string) (bool, error)
	AuthorizeListObject(ctx context.Context, request *Request, key string) (bool, error)
	AuthorizeDeleteObjectEntry(ctx context.Context, request *Request, key string) (bool, error)
	AuthorizeListMultipartUpload(ctx context.Context, request *Request, key string, uploadID string) (bool, error)
	AuthorizeListPart(ctx context.Context, request *Request, partNumber int32) (bool, error)
}
