package s3client

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestRetentionPrecisionAcrossSDKSerialization(t *testing.T) {
	until := time.Date(2030, 1, 1, 0, 0, 0, 123456789, time.UTC)
	var header, body string
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header = r.Header.Get("x-amz-object-lock-retain-until-date")
		data, _ := io.ReadAll(r.Body)
		body = string(data)
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("x-amz-version-id", "version")
		w.WriteHeader(http.StatusOK)
	}))
	defer endpoint.Close()
	client := s3.New(s3.Options{BaseEndpoint: aws.String(endpoint.URL), Region: "us-east-1", UsePathStyle: true, Credentials: aws.AnonymousCredentials{}})
	backend, err := NewStorage(client)
	require.NoError(t, err)
	bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
	retention := &storage.ObjectRetention{Mode: storage.RetentionModeGovernance, RetainUntilDate: until}
	_, err = backend.PutObject(t.Context(), bucket, key, nil, strings.NewReader("body"), nil, &storage.PutObjectOptions{ObjectLock: storage.ObjectLock{Retention: retention}})
	require.NoError(t, err)
	require.Equal(t, until.Format(time.RFC3339Nano), header)
	require.NoError(t, backend.PutObjectRetention(t.Context(), bucket, key, retention, nil))
	var decoded struct {
		Until string `xml:"RetainUntilDate"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &decoded))
	require.Equal(t, until.Format(time.RFC3339Nano), decoded.Until)
}
