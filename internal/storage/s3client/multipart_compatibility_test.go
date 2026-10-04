package s3client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestMultipartListingsAcceptOmittedOptionalUpstreamFields(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Has("uploads") {
			// No delimiter was requested, and an untruncated page needs no next markers.
			io.WriteString(w, `<ListMultipartUploadsResult><Bucket>bucket</Bucket><KeyMarker></KeyMarker><UploadIdMarker></UploadIdMarker><Prefix></Prefix><MaxUploads>1000</MaxUploads><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`)
		} else {
			io.WriteString(w, `<ListPartsResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>upload</UploadId><PartNumberMarker>0</PartNumberMarker><MaxParts>1000</MaxParts><IsTruncated>false</IsTruncated></ListPartsResult>`)
		}
	}))
	t.Cleanup(endpoint.Close)
	backend, err := NewStorage(s3.New(s3.Options{BaseEndpoint: aws.String(endpoint.URL), Region: "us-east-1", UsePathStyle: true, Credentials: aws.AnonymousCredentials{}}))
	require.NoError(t, err)
	bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
	uploads, err := backend.ListMultipartUploads(t.Context(), bucket, storage.ListMultipartUploadsOptions{MaxUploads: 1000})
	require.NoError(t, err)
	require.False(t, uploads.IsTruncated)
	require.Empty(t, uploads.Delimiter)
	require.Empty(t, uploads.NextKeyMarker)
	require.Empty(t, uploads.NextUploadIdMarker)
	parts, err := backend.ListParts(t.Context(), bucket, key, storage.MustNewUploadId("upload"), storage.ListPartsOptions{MaxParts: 1000})
	require.NoError(t, err)
	require.False(t, parts.IsTruncated)
	require.Nil(t, parts.NextPartNumberMarker)
	require.Nil(t, parts.ChecksumAlgorithm)
	require.Nil(t, parts.ChecksumType)
	require.Nil(t, parts.Owner)
	require.Nil(t, parts.Initiator)
	require.Nil(t, parts.AbortDate)
}

func TestMultipartChecksumMetadataPassesThroughUpstream(t *testing.T) {
	var gotAlgorithm, gotType string
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.Method == http.MethodPost:
			gotAlgorithm = r.Header.Get("x-amz-checksum-algorithm")
			gotType = r.Header.Get("x-amz-checksum-type")
			io.WriteString(w, `<InitiateMultipartUploadResult><UploadId>upload</UploadId></InitiateMultipartUploadResult>`)
		case r.URL.Query().Has("uploads"):
			io.WriteString(w, `<ListMultipartUploadsResult><Bucket>bucket</Bucket><KeyMarker></KeyMarker><UploadIdMarker></UploadIdMarker><Prefix></Prefix><Delimiter></Delimiter><NextKeyMarker></NextKeyMarker><NextUploadIdMarker></NextUploadIdMarker><MaxUploads>1</MaxUploads><IsTruncated>false</IsTruncated><Upload><Key>key</Key><UploadId>upload</UploadId><Initiated>2026-10-01T00:00:00Z</Initiated><ChecksumAlgorithm>SHA256</ChecksumAlgorithm><ChecksumType>COMPOSITE</ChecksumType></Upload></ListMultipartUploadsResult>`)
		default:
			w.Header().Set("x-amz-abort-date", "Thu, 08 Oct 2026 00:00:00 GMT")
			w.Header().Set("x-amz-abort-rule-id", "cleanup-uploads")
			io.WriteString(w, `<ListPartsResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>upload</UploadId><PartNumberMarker>0</PartNumberMarker><MaxParts>1</MaxParts><IsTruncated>false</IsTruncated><ChecksumAlgorithm>SHA256</ChecksumAlgorithm><ChecksumType>COMPOSITE</ChecksumType></ListPartsResult>`)
		}
	}))
	t.Cleanup(endpoint.Close)
	backend, err := NewStorage(s3.New(s3.Options{BaseEndpoint: aws.String(endpoint.URL), Region: "us-east-1", UsePathStyle: true, Credentials: aws.AnonymousCredentials{}}))
	require.NoError(t, err)
	bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
	algorithm := "SHA256"
	created, err := backend.CreateMultipartUpload(t.Context(), bucket, key, nil, nil, &storage.CreateMultipartUploadOptions{ChecksumAlgorithm: &algorithm})
	require.NoError(t, err)
	require.Equal(t, "SHA256", gotAlgorithm)
	require.Equal(t, "COMPOSITE", gotType)
	parts, err := backend.ListParts(t.Context(), bucket, key, created.UploadId, storage.ListPartsOptions{MaxParts: 1})
	require.NoError(t, err)
	require.Equal(t, aws.String("SHA256"), parts.ChecksumAlgorithm)
	require.Equal(t, aws.String("COMPOSITE"), parts.ChecksumType)
	expectedAbortDate := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	require.Equal(t, &expectedAbortDate, parts.AbortDate)
	require.Equal(t, aws.String("cleanup-uploads"), parts.AbortRuleID)
	uploads, err := backend.ListMultipartUploads(t.Context(), bucket, storage.ListMultipartUploadsOptions{MaxUploads: 1})
	require.NoError(t, err)
	require.Len(t, uploads.Uploads, 1)
	require.Equal(t, aws.String("SHA256"), uploads.Uploads[0].ChecksumAlgorithm)
	require.Equal(t, aws.String("COMPOSITE"), uploads.Uploads[0].ChecksumType)
}
