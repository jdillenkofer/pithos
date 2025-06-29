package authorization

type Authorization struct {
	AccessKeyId string
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
	OperationAbortMultipartUpload    = "AbortMultipartUpload"
	OperationDeleteObject            = "DeleteObject"
)

type Request struct {
	Operation     string
	Authorization Authorization
	Bucket        *string
	Key           *string
}

type RequestAuthorizer interface {
	AuthorizeRequest(request *Request) (bool, error)
}
