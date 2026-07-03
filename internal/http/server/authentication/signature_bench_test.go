package authentication

import (
	"net/http"
	"testing"
)

func BenchmarkGenerateStringToSign(b *testing.B) {
	r, err := http.NewRequest("GET", "https://examplebucket.s3.amazonaws.com/photos/2024/photo%20album/img_0001.jpg?versionId=3HL4kqCxf3vjVBH40Nrjfkd&partNumber=7", nil)
	if err != nil {
		b.Fatal(err)
	}
	r.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
	r.Header.Set("Range", "bytes=0-9")
	r.Header.Set("x-amz-date", "20130524T000000Z")
	r.Header.Set("x-amz-storage-class", "REDUCED_REDUNDANCY")
	r.Header.Set("x-amz-meta-author", "someone@example.com")
	headersToInclude := []string{"host", "range", "x-amz-content-sha256", "x-amz-date", "x-amz-storage-class", "x-amz-meta-author"}

	b.ReportAllocs()
	for b.Loop() {
		_, err := generateStringToSign(r, "20130524T000000Z", "20130524/us-east-1/s3/aws4_request", headersToInclude, false)
		if err != nil {
			b.Fatal(err)
		}
	}
}
