package s3client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestAppendForwardsOffsetAndChecksums(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "implicit offset", true: "explicit offset"}[explicit], func(t *testing.T) {
			var gotOffset, gotChecksum, gotBody string
			heads := 0
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					heads++
					w.Header().Set("Content-Length", "3")
					w.WriteHeader(200)
					return
				}
				gotOffset = r.Header.Get("x-amz-write-offset-bytes")
				gotChecksum = r.Header.Get("x-amz-checksum-sha256")
				body, _ := io.ReadAll(r.Body)
				gotBody = string(body)
				w.Header().Set("ETag", `"combined"`)
				w.Header().Set("x-amz-object-size", "6")
				w.WriteHeader(200)
			}))
			defer endpoint.Close()
			backend, err := NewStorage(s3.New(s3.Options{BaseEndpoint: aws.String(endpoint.URL), Region: "us-east-1", UsePathStyle: true, Credentials: aws.AnonymousCredentials{}}))
			require.NoError(t, err)
			var opts *storage.AppendObjectOptions
			if explicit {
				opts = &storage.AppendObjectOptions{WriteOffset: aws.Int64(3)}
			}
			checksum := "test-checksum"
			result, err := backend.AppendObject(t.Context(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), strings.NewReader("def"), &storage.ChecksumInput{ChecksumSHA256: &checksum}, opts)
			require.NoError(t, err)
			require.Equal(t, "3", gotOffset)
			require.Equal(t, checksum, gotChecksum)
			require.Equal(t, "def", gotBody)
			require.EqualValues(t, 6, result.Size)
			require.Equal(t, `"combined"`, result.ETag)
			if explicit {
				require.Zero(t, heads)
			} else {
				require.Equal(t, 1, heads)
			}
		})
	}
}
