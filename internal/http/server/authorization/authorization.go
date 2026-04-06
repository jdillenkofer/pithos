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
	OperationPutObject               = "PutObject"
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
)

type Request struct {
	Operation     string
	Authorization Authorization
	Bucket        *string
	Key           *string
	HttpRequest   HTTPRequest
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
