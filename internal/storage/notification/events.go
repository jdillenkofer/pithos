package notification

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/oklog/ulid/v2"
)

const (
	EventObjectCreatedPut                     = "s3:ObjectCreated:Put"
	EventObjectCreatedCopy                    = "s3:ObjectCreated:Copy"
	EventObjectCreatedCompleteMultipartUpload = "s3:ObjectCreated:CompleteMultipartUpload"
	EventObjectRemovedDelete                  = "s3:ObjectRemoved:Delete"
	EventObjectRemovedDeleteMarkerCreated     = "s3:ObjectRemoved:DeleteMarkerCreated"
	EventObjectTaggingPut                     = "s3:ObjectTagging:Put"
	EventObjectTaggingDelete                  = "s3:ObjectTagging:Delete"
	EventLifecycleExpirationDelete            = "s3:LifecycleExpiration:Delete"
	EventLifecycleExpirationDeleteMarker      = "s3:LifecycleExpiration:DeleteMarkerCreated"
	EventLifecycleTransition                  = "s3:LifecycleTransition"
	EventTestEvent                            = "s3:TestEvent"
)

type ObjectEvent struct {
	EventName string
	Bucket    storage.BucketName
	Key       storage.ObjectKey
	VersionID *string
	ETag      *string
	Size      *int64
	EventTime time.Time
}

type PayloadFormat string

const (
	PayloadFormatS3Records   PayloadFormat = "s3-records"
	PayloadFormatEventBridge PayloadFormat = "eventbridge"
)

func RuleMatches(rule storage.NotificationConfigurationRule, event ObjectEvent) bool {
	matchesEvent := false
	for _, configuredEvent := range rule.Events {
		if configuredEvent == event.EventName {
			matchesEvent = true
			break
		}
		if strings.HasSuffix(configuredEvent, ":*") && strings.HasPrefix(event.EventName, strings.TrimSuffix(configuredEvent, "*")) {
			matchesEvent = true
			break
		}
	}
	if !matchesEvent {
		return false
	}
	key := event.Key.String()
	for _, filterRule := range rule.FilterRules {
		switch filterRule.Name {
		case "prefix":
			if !strings.HasPrefix(key, filterRule.Value) {
				return false
			}
		case "suffix":
			if !strings.HasSuffix(key, filterRule.Value) {
				return false
			}
		}
	}
	return true
}

func BuildS3RecordsPayload(event ObjectEvent) ([]byte, error) {
	record := map[string]any{
		"eventVersion": "2.1",
		"eventSource":  "aws:s3",
		"awsRegion":    "eu-central-1",
		"eventTime":    event.EventTime.UTC().Format(time.RFC3339Nano),
		"eventName":    strings.TrimPrefix(event.EventName, "s3:"),
		"s3": map[string]any{
			"s3SchemaVersion": "1.0",
			"bucket": map[string]any{
				"name": event.Bucket.String(),
				"arn":  "arn:aws:s3:::" + event.Bucket.String(),
			},
			"object": objectPayload(event),
		},
	}
	return json.Marshal(map[string]any{"Records": []any{record}})
}

func BuildEventBridgePayload(event ObjectEvent) ([]byte, error) {
	return json.Marshal(map[string]any{
		"version":     "0",
		"id":          "",
		"detail-type": event.EventName,
		"source":      "aws.s3",
		"time":        event.EventTime.UTC().Format(time.RFC3339Nano),
		"resources":   []string{"arn:aws:s3:::" + event.Bucket.String()},
		"detail": map[string]any{
			"bucket": map[string]any{"name": event.Bucket.String()},
			"object": objectPayload(event),
		},
	})
}

// BuildTestEventPayload builds an s3:TestEvent payload used to validate that a
// notification destination is reachable when a bucket notification
// configuration is applied. AWS S3 emits this message once, synchronously, and
// never persists it to the durable event stream.
func BuildTestEventPayload(payloadFormat PayloadFormat, bucket storage.BucketName) ([]byte, error) {
	now := time.Now().UTC()
	switch payloadFormat {
	case "", PayloadFormatS3Records:
		return json.Marshal(map[string]any{
			"Service":   "Amazon S3",
			"Event":     EventTestEvent,
			"Time":      now.Format(time.RFC3339Nano),
			"Bucket":    bucket.String(),
			"RequestId": ulid.Make().String(),
			"HostId":    ulid.Make().String(),
		})
	case PayloadFormatEventBridge:
		return json.Marshal(map[string]any{
			"version":     "0",
			"id":          "",
			"detail-type": EventTestEvent,
			"source":      "aws.s3",
			"time":        now.Format(time.RFC3339Nano),
			"resources":   []string{"arn:aws:s3:::" + bucket.String()},
			"detail": map[string]any{
				"bucket": map[string]any{"name": bucket.String()},
			},
		})
	default:
		return nil, fmt.Errorf("unsupported notification payload format %q", payloadFormat)
	}
}

func objectPayload(event ObjectEvent) map[string]any {
	object := map[string]any{"key": event.Key.String()}
	if event.VersionID != nil {
		object["version-id"] = *event.VersionID
	}
	if event.ETag != nil {
		object["etag"] = strings.Trim(*event.ETag, `"`)
	}
	if event.Size != nil {
		object["size"] = *event.Size
	}
	return object
}
