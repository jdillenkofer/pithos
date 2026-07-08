package notification

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type capturedRequest struct {
	Method    string
	Path      string
	Target    string
	Auth      string
	Host      string
	Body      string
	RawQuery  string
	FormValue map[string]string
}

// fakeAWS is an httptest server that records incoming AWS SDK requests and
// returns minimal valid responses so the SDK calls succeed without retries.
type fakeAWS struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests []capturedRequest
	queueURL string
}

func newFakeAWS(t *testing.T) *fakeAWS {
	t.Helper()
	f := &fakeAWS{}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	f.queueURL = f.server.URL + "/000000000000/queue"
	return f
}

func (f *fakeAWS) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	target := r.Header.Get("X-Amz-Target")
	captured := capturedRequest{
		Method:    r.Method,
		Path:      r.URL.Path,
		Target:    target,
		Auth:      r.Header.Get("Authorization"),
		Host:      r.Host,
		Body:      string(body),
		RawQuery:  r.URL.RawQuery,
		FormValue: map[string]string{},
	}
	if values, err := parseForm(string(body)); err == nil {
		captured.FormValue = values
	}
	f.mu.Lock()
	f.requests = append(f.requests, captured)
	f.mu.Unlock()

	switch {
	case strings.HasSuffix(target, "GetQueueUrl"):
		writeJSON(w, "application/x-amz-json-1.0", map[string]any{"QueueUrl": f.queueURL})
	case strings.HasSuffix(target, "SendMessage"):
		var sendReq struct {
			MessageBody string `json:"MessageBody"`
		}
		_ = json.Unmarshal(body, &sendReq)
		sum := md5.Sum([]byte(sendReq.MessageBody))
		writeJSON(w, "application/x-amz-json-1.0", map[string]any{"MessageId": "msg-1", "MD5OfMessageBody": hex.EncodeToString(sum[:])})
	case strings.HasSuffix(target, "PutEvents"):
		writeJSON(w, "application/x-amz-json-1.1", map[string]any{"FailedEntryCount": 0, "Entries": []any{map[string]any{"EventId": "e-1"}}})
	case captured.FormValue["Action"] == "Publish":
		w.Header().Set("Content-Type", "text/xml")
		io.WriteString(w, `<PublishResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/"><PublishResult><MessageId>id-1</MessageId></PublishResult><ResponseMetadata><RequestId>req-1</RequestId></ResponseMetadata></PublishResponse>`)
	default:
		// Lambda Invoke and anything else: 200 with empty body is sufficient.
		w.WriteHeader(http.StatusOK)
	}
}

func parseForm(body string) (map[string]string, error) {
	values := map[string]string{}
	for _, pair := range strings.Split(body, "&") {
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		key := kv[0]
		val := ""
		if len(kv) == 2 {
			val = kv[1]
		}
		values[key] = val
	}
	return values, nil
}

func writeJSON(w http.ResponseWriter, contentType string, payload map[string]any) {
	w.Header().Set("Content-Type", contentType)
	_ = json.NewEncoder(w).Encode(payload)
}

func (f *fakeAWS) captured() []capturedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]capturedRequest(nil), f.requests...)
}

func testAWSDestination(f *fakeAWS) *Destination {
	return &Destination{
		Type: DestinationTypeAWS,
		AWS: AWSConfig{
			Region:          "eu-central-1",
			Endpoint:        f.server.URL,
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
		},
	}
}

func TestPublishSNS(t *testing.T) {
	f := newFakeAWS(t)
	entry := &OutboxEntry{DestinationARN: "arn:aws:sns:eu-central-1:000000000000:topic", Payload: []byte(`{"Records":[]}`)}
	require.NoError(t, publishAWS(context.Background(), entry, testAWSDestination(f)))

	reqs := f.captured()
	require.Len(t, reqs, 1)
	require.Equal(t, "Publish", reqs[0].FormValue["Action"])
	require.Contains(t, reqs[0].Auth, "eu-central-1")
	require.Contains(t, reqs[0].Auth, "test-access-key")
}

func TestPublishSQSWithExplicitQueueURL(t *testing.T) {
	f := newFakeAWS(t)
	destination := testAWSDestination(f)
	destination.AWS.QueueURL = f.queueURL
	entry := &OutboxEntry{DestinationARN: "arn:aws:sqs:eu-central-1:000000000000:queue", Payload: []byte(`{"Records":[]}`)}
	require.NoError(t, publishAWS(context.Background(), entry, destination))

	reqs := f.captured()
	require.Len(t, reqs, 1, "explicit queueUrl must skip GetQueueUrl")
	require.True(t, strings.HasSuffix(reqs[0].Target, "SendMessage"))
	require.Contains(t, reqs[0].Body, f.queueURL)
}

func TestPublishSQSARNFallback(t *testing.T) {
	f := newFakeAWS(t)
	entry := &OutboxEntry{DestinationARN: "arn:aws:sqs:eu-central-1:000000000000:queue", Payload: []byte(`{"Records":[]}`)}
	require.NoError(t, publishAWS(context.Background(), entry, testAWSDestination(f)))

	reqs := f.captured()
	require.Len(t, reqs, 2, "ARN fallback resolves the queue URL first")
	require.True(t, strings.HasSuffix(reqs[0].Target, "GetQueueUrl"))
	require.True(t, strings.HasSuffix(reqs[1].Target, "SendMessage"))
}

func TestPublishLambda(t *testing.T) {
	f := newFakeAWS(t)
	entry := &OutboxEntry{DestinationARN: "arn:aws:lambda:eu-central-1:000000000000:function:fn", Payload: []byte(`{"Records":[]}`)}
	require.NoError(t, publishAWS(context.Background(), entry, testAWSDestination(f)))

	reqs := f.captured()
	require.Len(t, reqs, 1)
	require.Contains(t, reqs[0].Path, "/functions/")
	require.Contains(t, reqs[0].Path, "/invocations")
}

func TestPublishEventBridge(t *testing.T) {
	f := newFakeAWS(t)
	payload, err := BuildEventBridgePayload(ObjectEvent{
		EventName: EventObjectCreatedPut,
		Bucket:    storage.MustNewBucketName("bucket"),
		Key:       storage.MustNewObjectKey("images/a.jpg"),
	})
	require.NoError(t, err)
	entry := &OutboxEntry{DestinationARN: eventBridgeARNPrefix + "bucket", PayloadFormat: PayloadFormatEventBridge, Payload: payload}
	require.NoError(t, publishEventBridge(context.Background(), entry, testAWSDestination(f)))

	reqs := f.captured()
	require.Len(t, reqs, 1)
	require.True(t, strings.HasSuffix(reqs[0].Target, "PutEvents"))
	var putEvents struct {
		Entries []struct {
			Source     string `json:"Source"`
			DetailType string `json:"DetailType"`
			Detail     string `json:"Detail"`
		} `json:"Entries"`
	}
	require.NoError(t, json.Unmarshal([]byte(reqs[0].Body), &putEvents))
	require.Len(t, putEvents.Entries, 1)
	require.Equal(t, "aws.s3", putEvents.Entries[0].Source)
	require.Equal(t, EventObjectCreatedPut, putEvents.Entries[0].DetailType)
	require.Contains(t, putEvents.Entries[0].Detail, "images/a.jpg")
}

func TestPublishAWSFallbackRoutesEventBridge(t *testing.T) {
	f := newFakeAWS(t)
	payload, err := BuildEventBridgePayload(ObjectEvent{EventName: EventObjectCreatedPut, Bucket: storage.MustNewBucketName("bucket"), Key: storage.MustNewObjectKey("k")})
	require.NoError(t, err)
	entry := &OutboxEntry{DestinationARN: eventBridgeARNPrefix + "bucket", PayloadFormat: PayloadFormatEventBridge, Payload: payload}
	require.NoError(t, publishAWSFallback(context.Background(), entry, testAWSDestination(f)))

	reqs := f.captured()
	require.Len(t, reqs, 1)
	require.True(t, strings.HasSuffix(reqs[0].Target, "PutEvents"))
}
