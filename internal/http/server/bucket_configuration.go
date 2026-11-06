package server

import (
	"sort"

	"github.com/jdillenkofer/pithos/internal/storage"
)

// SupportedNotificationEvents returns the event names accepted by notification
// validation. Callers receive a sorted copy, not the shared validation registry.
func SupportedNotificationEvents() []string {
	events := make([]string, 0, len(supportedNotificationEvents))
	for event := range supportedNotificationEvents {
		events = append(events, event)
	}
	sort.Strings(events)
	return events
}

// ValidateWebsiteConfiguration reuses the S3 website conversion and validation.
// The returned configuration includes S3 default redirect codes.
func ValidateWebsiteConfiguration(config *storage.WebsiteConfiguration) (*storage.WebsiteConfiguration, error) {
	response := websiteConfigurationResponseFromStorage(config)
	return websiteConfigurationRequestToStorage(WebsiteConfigurationRequest{
		IndexDocument: response.IndexDocument, ErrorDocument: response.ErrorDocument,
		RedirectAllRequestsTo: response.RedirectAllRequestsTo, RoutingRules: response.RoutingRules,
	})
}

// ValidateNotificationConfiguration applies the same structural and destination
// validation as a normal S3 PUT (without skipping destination validation).
func ValidateNotificationConfiguration(config *storage.BucketNotificationConfiguration) (*storage.BucketNotificationConfiguration, error) {
	validated, err := convertNotificationConfigurationFromXML(convertNotificationConfigurationToXML(config))
	if err != nil {
		return nil, err
	}
	if err := validateNotificationDestinations(validated); err != nil {
		return nil, err
	}
	return validated, nil
}
