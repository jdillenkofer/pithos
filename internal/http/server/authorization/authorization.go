package authorization

import "context"

type Authorization struct {
	AccessKeyId *string
}

type HTTPRequest struct {
	Method      string
	Path        string
	Query       string
	QueryParams map[string][]string
	Headers     map[string][]string
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
