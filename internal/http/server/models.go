package server

import (
	"encoding/xml"
	"fmt"
)

type BucketResult struct {
	XMLName      xml.Name `xml:"Bucket"`
	CreationDate string   `xml:"CreationDate"`
	Name         string   `xml:"Name"`
}

type OwnerResult struct {
	XMLName     xml.Name `xml:"Owner"`
	DisplayName string   `xml:"DisplayName"`
	Id          string   `xml:"ID"`
}

type ListAllMyBucketsResult struct {
	XMLName xml.Name        `xml:"ListAllMyBucketsResult"`
	Buckets []*BucketResult `xml:">Buckets"`
	Owner   *OwnerResult    `xml:"Owner"`
}

type Prefix struct {
	XMLName xml.Name `xml:"Prefix"`
}

type ContentResult struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type CommonPrefixResult struct {
	Prefix string `xml:"Prefix"`
}

type ListBucketResult struct {
	XMLName        xml.Name              `xml:"ListBucketResult"`
	IsTruncated    bool                  `xml:"IsTruncated"`
	Contents       []*ContentResult      `xml:"Contents"`
	Name           string                `xml:"Name"`
	Prefix         *string               `xml:"Prefix"`
	Delimiter      *string               `xml:"Delimiter"`
	MaxKeys        int32                 `xml:"MaxKeys"`
	CommonPrefixes []*CommonPrefixResult `xml:"CommonPrefixes"`
	KeyCount       int32                 `xml:"KeyCount"`
	StartAfter     *string               `xml:"StartAfter"`
	Marker         *string               `xml:"Marker"`
	NextMarker     *string               `xml:"NextMarker"`
}

type ListBucketV2Result struct {
	XMLName               xml.Name              `xml:"ListBucketResult"`
	IsTruncated           bool                  `xml:"IsTruncated"`
	Contents              []*ContentResult      `xml:"Contents"`
	Name                  string                `xml:"Name"`
	Prefix                *string               `xml:"Prefix"`
	Delimiter             *string               `xml:"Delimiter"`
	MaxKeys               int32                 `xml:"MaxKeys"`
	CommonPrefixes        []*CommonPrefixResult `xml:"CommonPrefixes"`
	KeyCount              int32                 `xml:"KeyCount"`
	ContinuationToken     *string               `xml:"ContinuationToken"`
	NextContinuationToken *string               `xml:"NextContinuationToken"`
	StartAfter            *string               `xml:"StartAfter"`
}

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type Part struct {
	ChecksumCRC32     *string `xml:"ChecksumCRC32"`
	ChecksumCRC32C    *string `xml:"ChecksumCRC32C"`
	ChecksumCRC64NVME *string `xml:"ChecksumCRC64NVME"`
	ChecksumSHA1      *string `xml:"ChecksumSHA1"`
	ChecksumSHA256    *string `xml:"ChecksumSHA256"`
	ETag              string  `xml:"ETag"`
	PartNumber        int32   `xml:"PartNumber"`
}

type CompleteMultipartUploadRequest struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []*Part  `xml:"Part"`
}

type CompleteMultipartUploadResult struct {
	XMLName           xml.Name `xml:"CompleteMultipartUploadResult"`
	Location          string   `xml:"Location"`
	Bucket            string   `xml:"Bucket"`
	Key               string   `xml:"Key"`
	ETag              string   `xml:"ETag"`
	ChecksumCRC32     *string  `xml:"ChecksumCRC32"`
	ChecksumCRC32C    *string  `xml:"ChecksumCRC32C"`
	ChecksumCRC64NVME *string  `xml:"ChecksumCRC64NVME"`
	ChecksumSHA1      *string  `xml:"ChecksumSHA1"`
	ChecksumSHA256    *string  `xml:"ChecksumSHA256"`
	ChecksumType      *string  `xml:"ChecksumType"`
}

type UploadResult struct {
	Key          string `xml:"Key"`
	UploadId     string `xml:"UploadId"`
	Initiated    string `xml:"Initiated"`
	StorageClass string `xml:"StorageClass"`
}

type ListMultipartUploadsResult struct {
	XMLName            xml.Name              `xml:"ListMultipartUploadsResult"`
	Bucket             string                `xml:"Bucket"`
	KeyMarker          *string               `xml:"KeyMarker"`
	UploadIdMarker     *string               `xml:"UploadIdMarker"`
	NextKeyMarker      *string               `xml:"NextKeyMarker"`
	NextUploadIdMarker *string               `xml:"NextUploadIdMarker"`
	MaxUploads         int32                 `xml:"MaxUploads"`
	IsTruncated        bool                  `xml:"IsTruncated"`
	Delimiter          *string               `xml:"Delimiter"`
	Prefix             *string               `xml:"Prefix"`
	Uploads            []*UploadResult       `xml:"Upload"`
	CommonPrefixes     []*CommonPrefixResult `xml:"CommonPrefixes"`
}

type PartResult struct {
	ETag              string  `xml:"ETag"`
	ChecksumCRC32     *string `xml:"ChecksumCRC32"`
	ChecksumCRC32C    *string `xml:"ChecksumCRC32C"`
	ChecksumCRC64NVME *string `xml:"ChecksumCRC64NVME"`
	ChecksumSHA1      *string `xml:"ChecksumSHA1"`
	ChecksumSHA256    *string `xml:"ChecksumSHA256"`
	LastModified      string  `xml:"LastModified"`
	PartNumber        int32   `xml:"PartNumber"`
	Size              int64   `xml:"Size"`
}

type ListPartsResult struct {
	XMLName              xml.Name      `xml:"ListPartsResult"`
	Bucket               string        `xml:"Bucket"`
	Key                  string        `xml:"Key"`
	UploadId             string        `xml:"UploadId"`
	PartNumberMarker     *string       `xml:"PartNumberMarker"`
	NextPartNumberMarker *string       `xml:"NextPartNumberMarker"`
	MaxParts             int32         `xml:"MaxParts"`
	IsTruncated          bool          `xml:"IsTruncated"`
	Parts                []*PartResult `xml:"Part"`
	// @TODO: Initiator and Owner missing
	StorageClass      string  `xml:"StorageClass"`
	ChecksumAlgorithm *string `xml:"ChecksumAlgorithm"`
	ChecksumType      *string `xml:"ChecksumType"`
}

type WebsiteConfigurationIndexDocument struct {
	Suffix string `xml:"Suffix"`
}

type WebsiteConfigurationErrorDocument struct {
	Key string `xml:"Key"`
}

type WebsiteConfigurationRedirectAllRequestsTo struct {
	HostName string `xml:"HostName"`
	Protocol string `xml:"Protocol,omitempty"`
}

type WebsiteConfigurationRoutingRuleCondition struct {
	KeyPrefixEquals             *string `xml:"KeyPrefixEquals"`
	HttpErrorCodeReturnedEquals *string `xml:"HttpErrorCodeReturnedEquals"`
}

type WebsiteConfigurationRedirect struct {
	HostName             *string `xml:"HostName,omitempty"`
	Protocol             *string `xml:"Protocol,omitempty"`
	ReplaceKeyPrefixWith *string `xml:"ReplaceKeyPrefixWith,omitempty"`
	ReplaceKeyWith       *string `xml:"ReplaceKeyWith,omitempty"`
	HttpRedirectCode     *string `xml:"HttpRedirectCode,omitempty"`
}

type WebsiteConfigurationRoutingRule struct {
	Condition *WebsiteConfigurationRoutingRuleCondition `xml:"Condition"`
	Redirect  *WebsiteConfigurationRedirect             `xml:"Redirect"`
}

type WebsiteConfigurationRequest struct {
	XMLName               xml.Name                                   `xml:"WebsiteConfiguration"`
	IndexDocument         *WebsiteConfigurationIndexDocument         `xml:"IndexDocument"`
	ErrorDocument         *WebsiteConfigurationErrorDocument         `xml:"ErrorDocument"`
	RedirectAllRequestsTo *WebsiteConfigurationRedirectAllRequestsTo `xml:"RedirectAllRequestsTo"`
	RoutingRules          []WebsiteConfigurationRoutingRule          `xml:"RoutingRules>RoutingRule"`
}

type WebsiteConfigurationResponse struct {
	XMLName               xml.Name                                   `xml:"WebsiteConfiguration"`
	Xmlns                 string                                     `xml:"xmlns,attr"`
	IndexDocument         *WebsiteConfigurationIndexDocument         `xml:"IndexDocument,omitempty"`
	ErrorDocument         *WebsiteConfigurationErrorDocument         `xml:"ErrorDocument,omitempty"`
	RedirectAllRequestsTo *WebsiteConfigurationRedirectAllRequestsTo `xml:"RedirectAllRequestsTo,omitempty"`
	RoutingRules          []WebsiteConfigurationRoutingRule          `xml:"RoutingRules>RoutingRule,omitempty"`
}

type BucketVersioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Status  *string  `xml:"Status,omitempty"`
}

type VersionEntry struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type DeleteMarkerVersionEntry struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
}

type ListObjectVersionsResult struct {
	XMLName             xml.Name                    `xml:"ListVersionsResult"`
	Name                string                      `xml:"Name"`
	Prefix              *string                     `xml:"Prefix"`
	Delimiter           *string                     `xml:"Delimiter"`
	KeyMarker           *string                     `xml:"KeyMarker"`
	VersionIDMarker     *string                     `xml:"VersionIdMarker"`
	NextKeyMarker       *string                     `xml:"NextKeyMarker,omitempty"`
	NextVersionIDMarker *string                     `xml:"NextVersionIdMarker,omitempty"`
	MaxKeys             int32                       `xml:"MaxKeys"`
	IsTruncated         bool                        `xml:"IsTruncated"`
	Versions            []*VersionEntry             `xml:"Version"`
	DeleteMarkers       []*DeleteMarkerVersionEntry `xml:"DeleteMarker"`
	CommonPrefixes      []*CommonPrefixResult       `xml:"CommonPrefixes"`
}

type CORSConfigurationRule struct {
	ID             *string  `xml:"ID,omitempty"`
	AllowedOrigins []string `xml:"AllowedOrigin"`
	AllowedMethods []string `xml:"AllowedMethod"`
	AllowedHeaders []string `xml:"AllowedHeader,omitempty"`
	ExposeHeaders  []string `xml:"ExposeHeader,omitempty"`
	MaxAgeSeconds  *int     `xml:"MaxAgeSeconds,omitempty"`
}

type CORSConfiguration struct {
	XMLName xml.Name                `xml:"CORSConfiguration"`
	Xmlns   string                  `xml:"xmlns,attr,omitempty"`
	Rules   []CORSConfigurationRule `xml:"CORSRule"`
}

type LifecycleConfigurationTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type LifecycleConfigurationAndOperator struct {
	Prefix                *string                     `xml:"Prefix"`
	Tags                  []LifecycleConfigurationTag `xml:"Tag"`
	ObjectSizeGreaterThan *int64                      `xml:"ObjectSizeGreaterThan"`
	ObjectSizeLessThan    *int64                      `xml:"ObjectSizeLessThan"`
}

type LifecycleConfigurationFilter struct {
	Prefix                *string                            `xml:"Prefix"`
	Tag                   *LifecycleConfigurationTag         `xml:"Tag"`
	ObjectSizeGreaterThan *int64                             `xml:"ObjectSizeGreaterThan"`
	ObjectSizeLessThan    *int64                             `xml:"ObjectSizeLessThan"`
	And                   *LifecycleConfigurationAndOperator `xml:"And"`
}

type LifecycleConfigurationExpiration struct {
	// Date is kept as a string so that the ISO 8601 variants S3 accepts can be
	// parsed explicitly (see parseLifecycleDate).
	Days                      *int32  `xml:"Days"`
	Date                      *string `xml:"Date"`
	ExpiredObjectDeleteMarker *bool   `xml:"ExpiredObjectDeleteMarker"`
}

type LifecycleConfigurationAbortIncompleteMultipartUpload struct {
	DaysAfterInitiation *int32 `xml:"DaysAfterInitiation"`
}

type LifecycleConfigurationTransition struct {
	Days *int32 `xml:"Days"`
	// Date is kept as a string so that the ISO 8601 variants S3 accepts can be
	// parsed explicitly (see parseLifecycleDate).
	Date         *string `xml:"Date"`
	StorageClass string  `xml:"StorageClass"`
}

type LifecycleConfigurationNoncurrentVersionExpiration struct {
	NoncurrentDays          *int32 `xml:"NoncurrentDays"`
	NewerNoncurrentVersions *int32 `xml:"NewerNoncurrentVersions"`
}

type LifecycleConfigurationNoncurrentVersionTransition struct {
	NoncurrentDays          *int32 `xml:"NoncurrentDays"`
	NewerNoncurrentVersions *int32 `xml:"NewerNoncurrentVersions"`
	StorageClass            string `xml:"StorageClass"`
}

type LifecycleConfigurationRule struct {
	ID                             *string                                               `xml:"ID"`
	Status                         string                                                `xml:"Status"`
	Prefix                         *string                                               `xml:"Prefix"`
	Filter                         *LifecycleConfigurationFilter                         `xml:"Filter"`
	Expiration                     *LifecycleConfigurationExpiration                     `xml:"Expiration"`
	AbortIncompleteMultipartUpload *LifecycleConfigurationAbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload"`
	Transitions                    []LifecycleConfigurationTransition                    `xml:"Transition"`
	NoncurrentVersionTransitions   []LifecycleConfigurationNoncurrentVersionTransition   `xml:"NoncurrentVersionTransition"`
	NoncurrentVersionExpiration    *LifecycleConfigurationNoncurrentVersionExpiration    `xml:"NoncurrentVersionExpiration"`
}

type LifecycleConfiguration struct {
	XMLName xml.Name                     `xml:"LifecycleConfiguration"`
	Xmlns   string                       `xml:"xmlns,attr,omitempty"`
	Rules   []LifecycleConfigurationRule `xml:"Rule"`
}

type Tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type Tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	TagSet  []Tag    `xml:"TagSet>Tag"`
}

type DeleteObjectEntry struct {
	Key       string  `xml:"Key"`
	VersionId *string `xml:"VersionId"`
	ETag      *string `xml:"ETag"`
}

type DeleteObjectsRequest struct {
	XMLName xml.Name             `xml:"Delete"`
	Quiet   bool                 `xml:"Quiet"`
	Objects []*DeleteObjectEntry `xml:"Object"`
}

type DeletedEntry struct {
	Key                   string  `xml:"Key"`
	VersionId             *string `xml:"VersionId,omitempty"`
	DeleteMarker          *bool   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionId *string `xml:"DeleteMarkerVersionId,omitempty"`
}

type DeleteErrorEntry struct {
	Key       string  `xml:"Key"`
	VersionId *string `xml:"VersionId,omitempty"`
	Code      string  `xml:"Code"`
	Message   string  `xml:"Message"`
}

type DeleteObjectsResult struct {
	XMLName xml.Name            `xml:"DeleteResult"`
	Deleted []*DeletedEntry     `xml:"Deleted"`
	Errors  []*DeleteErrorEntry `xml:"Error"`
}

type CopyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

type CopyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

var ErrInvalidRequest = fmt.Errorf("InvalidRequest")
var ErrInvalidArgument = fmt.Errorf("InvalidArgument")

type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestId string   `xml:"RequestId"`
}
