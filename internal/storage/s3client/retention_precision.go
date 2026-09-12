package s3client

import (
	"context"
	"encoding/xml"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// The SDK's date-time serializer truncates to milliseconds. Retention must
// preserve the primary's absolute deadline, including its fractional seconds.
func preserveRetentionPrecision(stack *middleware.Stack) error {
	return stack.Serialize.Add(middleware.SerializeMiddlewareFunc("PithosRetentionPrecision", func(ctx context.Context, in middleware.SerializeInput, next middleware.SerializeHandler) (middleware.SerializeOutput, middleware.Metadata, error) {
		var until *time.Time
		var retention *s3.PutObjectRetentionInput
		switch p := in.Parameters.(type) {
		case *s3.PutObjectInput:
			until = p.ObjectLockRetainUntilDate
		case *s3.CopyObjectInput:
			until = p.ObjectLockRetainUntilDate
		case *s3.CreateMultipartUploadInput:
			until = p.ObjectLockRetainUntilDate
		case *s3.PutObjectRetentionInput:
			retention = p
			if p.Retention != nil {
				until = p.Retention.RetainUntilDate
			}
		}
		if until != nil {
			request := in.Request.(*smithyhttp.Request)
			if retention == nil {
				request.Header.Set("X-Amz-Object-Lock-Retain-Until-Date", until.UTC().Format(time.RFC3339Nano))
			} else {
				body := struct {
					XMLName xml.Name `xml:"Retention"`
					Xmlns   string   `xml:"xmlns,attr"`
					Mode    string   `xml:"Mode"`
					Until   string   `xml:"RetainUntilDate"`
				}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Mode: string(retention.Retention.Mode), Until: until.UTC().Format(time.RFC3339Nano)}
				data, err := xml.Marshal(body)
				if err != nil {
					return middleware.SerializeOutput{}, middleware.Metadata{}, err
				}
				replacement, err := request.SetStream(strings.NewReader(string(data)))
				if err != nil {
					return middleware.SerializeOutput{}, middleware.Metadata{}, err
				}
				replacement.ContentLength = int64(len(data))
				in.Request = replacement
			}
		}
		return next.HandleSerialize(ctx, in)
	}), middleware.After)
}
