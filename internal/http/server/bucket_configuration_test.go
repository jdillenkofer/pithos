package server

import (
	"reflect"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
)

func TestReusableWebsiteValidationMatchesS3(t *testing.T) {
	httpProtocol, ftp, badCode, prefix, key := "http", "ftp", "200", "prefix", "key"
	tests := []*storage.WebsiteConfiguration{
		{},
		{IndexDocumentSuffix: "index.html"},
		{RedirectAllRequestsTo: &storage.WebsiteRedirectAllRequestsTo{HostName: "example.com", Protocol: &httpProtocol}},
		{RedirectAllRequestsTo: &storage.WebsiteRedirectAllRequestsTo{}},
		{RedirectAllRequestsTo: &storage.WebsiteRedirectAllRequestsTo{HostName: "example.com", Protocol: &ftp}},
		{IndexDocumentSuffix: "index.html", RedirectAllRequestsTo: &storage.WebsiteRedirectAllRequestsTo{HostName: "example.com"}},
		{IndexDocumentSuffix: "index.html", RoutingRules: []storage.WebsiteRoutingRule{{Redirect: storage.WebsiteRedirect{ReplaceKeyWith: &key}}}},
		{IndexDocumentSuffix: "index.html", RoutingRules: []storage.WebsiteRoutingRule{{Redirect: storage.WebsiteRedirect{ReplaceKeyWith: &key, HttpRedirectCode: &badCode}}}},
		{IndexDocumentSuffix: "index.html", RoutingRules: []storage.WebsiteRoutingRule{{Redirect: storage.WebsiteRedirect{ReplaceKeyWith: &key, ReplaceKeyPrefixWith: &prefix}}}},
		{IndexDocumentSuffix: "index.html", RoutingRules: []storage.WebsiteRoutingRule{{Redirect: storage.WebsiteRedirect{Protocol: &ftp}}}},
		{IndexDocumentSuffix: "index.html", RoutingRules: []storage.WebsiteRoutingRule{{}}},
		{IndexDocumentSuffix: "index.html", RoutingRules: []storage.WebsiteRoutingRule{{Condition: &storage.WebsiteRoutingRuleCondition{}, Redirect: storage.WebsiteRedirect{ReplaceKeyWith: &key}}}},
	}
	for _, config := range tests {
		response := websiteConfigurationResponseFromStorage(config)
		expected, expectedErr := websiteConfigurationRequestToStorage(WebsiteConfigurationRequest{IndexDocument: response.IndexDocument, ErrorDocument: response.ErrorDocument, RedirectAllRequestsTo: response.RedirectAllRequestsTo, RoutingRules: response.RoutingRules})
		actual, err := ValidateWebsiteConfiguration(config)
		if (err == nil) != (expectedErr == nil) || !reflect.DeepEqual(actual, expected) {
			t.Fatalf("reusable validation differs from S3: %+v %v / %+v %v", actual, err, expected, expectedErr)
		}
	}
}

func TestReusableNotificationValidationMatchesS3(t *testing.T) {
	queueARN := "arn:aws:sqs:us-east-1:123456789012:queue"
	id := "duplicate"
	tests := []*storage.BucketNotificationConfiguration{
		{},
		{EventBridgeEnabled: true},
		{QueueConfigurations: []storage.NotificationConfigurationRule{{DestinationARN: queueARN, Events: []string{"s3:ObjectCreated:*"}}}},
		{QueueConfigurations: []storage.NotificationConfigurationRule{{DestinationARN: "bad", Events: []string{"s3:ObjectCreated:*"}}}},
		{QueueConfigurations: []storage.NotificationConfigurationRule{{DestinationARN: queueARN, Events: []string{"bad"}}}},
		{QueueConfigurations: []storage.NotificationConfigurationRule{{DestinationARN: queueARN}}},
		{QueueConfigurations: []storage.NotificationConfigurationRule{{DestinationARN: queueARN, Events: []string{"s3:ObjectCreated:*"}, FilterRules: []storage.NotificationFilterRule{{Name: "bad", Value: "value"}}}}},
		{QueueConfigurations: []storage.NotificationConfigurationRule{{ID: &id, DestinationARN: queueARN, Events: []string{"s3:ObjectCreated:*"}}, {ID: &id, DestinationARN: queueARN, Events: []string{"s3:ObjectRemoved:*"}}}},
	}
	for _, config := range tests {
		expected, expectedErr := convertNotificationConfigurationFromXML(convertNotificationConfigurationToXML(config))
		if expectedErr == nil {
			expectedErr = validateNotificationDestinations(expected)
		}
		actual, err := ValidateNotificationConfiguration(config)
		if (err == nil) != (expectedErr == nil) {
			t.Fatalf("reusable validation differs from S3: %v / %v", err, expectedErr)
		}
		if err == nil && !reflect.DeepEqual(actual, expected) {
			t.Fatalf("different normalized configuration: %+v / %+v", actual, expected)
		}
	}
}
