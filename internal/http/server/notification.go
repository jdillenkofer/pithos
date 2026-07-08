package server

import (
	"encoding/xml"
	"fmt"
	"html"
	"net/http"
	"strings"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/oklog/ulid/v2"
)

const maxPutBucketNotificationBodySize int64 = 1024 * 1024
const skipDestinationValidationHeader = "x-amz-skip-destination-validation"

type NotificationConfiguration struct {
	XMLName                     xml.Name                          `xml:"NotificationConfiguration"`
	Xmlns                       string                            `xml:"xmlns,attr,omitempty"`
	TopicConfigurations         []TopicNotificationConfiguration  `xml:"TopicConfiguration"`
	QueueConfigurations         []QueueNotificationConfiguration  `xml:"QueueConfiguration"`
	CloudFunctionConfigurations []LambdaNotificationConfiguration `xml:"CloudFunctionConfiguration"`
	EventBridgeConfiguration    *EventBridgeConfiguration         `xml:"EventBridgeConfiguration"`
}

type TopicNotificationConfiguration struct {
	ID     *string                          `xml:"Id"`
	Topic  string                           `xml:"Topic"`
	Events []string                         `xml:"Event"`
	Filter *NotificationConfigurationFilter `xml:"Filter"`
}

type QueueNotificationConfiguration struct {
	ID     *string                          `xml:"Id"`
	Queue  string                           `xml:"Queue"`
	Events []string                         `xml:"Event"`
	Filter *NotificationConfigurationFilter `xml:"Filter"`
}

type LambdaNotificationConfiguration struct {
	ID            *string                          `xml:"Id"`
	CloudFunction string                           `xml:"CloudFunction"`
	Events        []string                         `xml:"Event"`
	Filter        *NotificationConfigurationFilter `xml:"Filter"`
}

type EventBridgeConfiguration struct{}

type NotificationConfigurationFilter struct {
	S3Key NotificationConfigurationS3KeyFilter `xml:"S3Key"`
}

type NotificationConfigurationS3KeyFilter struct {
	FilterRules []NotificationConfigurationFilterRule `xml:"FilterRule"`
}

type NotificationConfigurationFilterRule struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

type notificationValidationError struct {
	Code    string
	Message string
}

var supportedNotificationEvents = map[string]struct{}{
	"s3:ReducedRedundancyLostObject":                   {},
	"s3:ObjectCreated:*":                               {},
	"s3:ObjectCreated:Put":                             {},
	"s3:ObjectCreated:Post":                            {},
	"s3:ObjectCreated:Copy":                            {},
	"s3:ObjectCreated:CompleteMultipartUpload":         {},
	"s3:ObjectRemoved:*":                               {},
	"s3:ObjectRemoved:Delete":                          {},
	"s3:ObjectRemoved:DeleteMarkerCreated":             {},
	"s3:ObjectRestore:*":                               {},
	"s3:ObjectRestore:Post":                            {},
	"s3:ObjectRestore:Completed":                       {},
	"s3:ObjectRestore:Delete":                          {},
	"s3:Replication:*":                                 {},
	"s3:Replication:OperationFailedReplication":        {},
	"s3:Replication:OperationMissedThreshold":          {},
	"s3:Replication:OperationReplicatedAfterThreshold": {},
	"s3:Replication:OperationNotTracked":               {},
	"s3:LifecycleExpiration:*":                         {},
	"s3:LifecycleExpiration:Delete":                    {},
	"s3:LifecycleExpiration:DeleteMarkerCreated":       {},
	"s3:LifecycleTransition":                           {},
	"s3:IntelligentTiering":                            {},
	"s3:ObjectTagging:*":                               {},
	"s3:ObjectTagging:Put":                             {},
	"s3:ObjectTagging:Delete":                          {},
	"s3:ObjectAcl:Put":                                 {},
	"s3:ObjectAnnotation:*":                            {},
	"s3:ObjectAnnotation:Put":                          {},
	"s3:ObjectAnnotation:Delete":                       {},
}

func (e *notificationValidationError) Error() string {
	return e.Message
}

func writeNotificationValidationError(w http.ResponseWriter, r *http.Request, validationErr *notificationValidationError) {
	errResponse := ErrorResponse{
		Code:      validationErr.Code,
		Message:   validationErr.Message,
		Resource:  html.EscapeString(r.RequestURI),
		RequestId: ulid.Make().String(),
	}
	xmlErrorResponse, err := xmlMarshalWithDocType(errResponse)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(http.StatusBadRequest)
	w.Write(xmlErrorResponse)
}

func convertNotificationConfigurationFromXML(request *NotificationConfiguration) (*storage.BucketNotificationConfiguration, *notificationValidationError) {
	config := &storage.BucketNotificationConfiguration{
		TopicConfigurations:         make([]storage.NotificationConfigurationRule, 0, len(request.TopicConfigurations)),
		QueueConfigurations:         make([]storage.NotificationConfigurationRule, 0, len(request.QueueConfigurations)),
		CloudFunctionConfigurations: make([]storage.NotificationConfigurationRule, 0, len(request.CloudFunctionConfigurations)),
		EventBridgeEnabled:          request.EventBridgeConfiguration != nil,
	}
	seenIDs := map[string]struct{}{}

	for _, rule := range request.TopicConfigurations {
		converted, err := convertNotificationRule(rule.ID, storage.NotificationDestinationTopic, rule.Topic, rule.Events, rule.Filter, seenIDs)
		if err != nil {
			return nil, err
		}
		config.TopicConfigurations = append(config.TopicConfigurations, *converted)
	}
	for _, rule := range request.QueueConfigurations {
		converted, err := convertNotificationRule(rule.ID, storage.NotificationDestinationQueue, rule.Queue, rule.Events, rule.Filter, seenIDs)
		if err != nil {
			return nil, err
		}
		config.QueueConfigurations = append(config.QueueConfigurations, *converted)
	}
	for _, rule := range request.CloudFunctionConfigurations {
		converted, err := convertNotificationRule(rule.ID, storage.NotificationDestinationCloudFunction, rule.CloudFunction, rule.Events, rule.Filter, seenIDs)
		if err != nil {
			return nil, err
		}
		config.CloudFunctionConfigurations = append(config.CloudFunctionConfigurations, *converted)
	}

	return config, nil
}

func convertNotificationRule(id *string, destinationType storage.NotificationDestinationType, arn string, events []string, filter *NotificationConfigurationFilter, seenIDs map[string]struct{}) (*storage.NotificationConfigurationRule, *notificationValidationError) {
	if id != nil && *id != "" {
		if _, ok := seenIDs[*id]; ok {
			return nil, invalidNotificationArgument("Notification configuration IDs must be unique")
		}
		seenIDs[*id] = struct{}{}
	}
	if arn == "" {
		return nil, invalidNotificationArgument("Notification destination ARN must be specified")
	}
	if len(events) == 0 {
		return nil, invalidNotificationArgument("At least one Event must be specified")
	}
	for _, event := range events {
		if _, ok := supportedNotificationEvents[event]; !ok {
			return nil, invalidNotificationArgument(fmt.Sprintf("Unsupported notification event %q", event))
		}
	}
	filterRules, err := convertNotificationFilter(filter)
	if err != nil {
		return nil, err
	}
	return &storage.NotificationConfigurationRule{
		ID:              id,
		DestinationType: destinationType,
		DestinationARN:  arn,
		Events:          events,
		FilterRules:     filterRules,
	}, nil
}

func convertNotificationFilter(filter *NotificationConfigurationFilter) ([]storage.NotificationFilterRule, *notificationValidationError) {
	if filter == nil {
		return nil, nil
	}
	rules := make([]storage.NotificationFilterRule, 0, len(filter.S3Key.FilterRules))
	seen := map[string]struct{}{}
	for _, rule := range filter.S3Key.FilterRules {
		if rule.Name != "prefix" && rule.Name != "suffix" {
			return nil, invalidNotificationArgument("FilterRule Name must be either prefix or suffix")
		}
		if _, ok := seen[rule.Name]; ok {
			return nil, invalidNotificationArgument("FilterRule Name must be unique")
		}
		seen[rule.Name] = struct{}{}
		rules = append(rules, storage.NotificationFilterRule{Name: rule.Name, Value: rule.Value})
	}
	return rules, nil
}

func invalidNotificationArgument(message string) *notificationValidationError {
	return &notificationValidationError{Code: "InvalidArgument", Message: message}
}

func validateNotificationDestinationARN(rule storage.NotificationConfigurationRule) *notificationValidationError {
	parts := strings.SplitN(rule.DestinationARN, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[1] == "" || parts[2] == "" || parts[5] == "" {
		return invalidNotificationArgument("Invalid notification destination ARN")
	}
	expectedService := map[storage.NotificationDestinationType]string{
		storage.NotificationDestinationTopic:         "sns",
		storage.NotificationDestinationQueue:         "sqs",
		storage.NotificationDestinationCloudFunction: "lambda",
	}[rule.DestinationType]
	if parts[2] != expectedService {
		return invalidNotificationArgument("Notification destination ARN service does not match configuration type")
	}
	return nil
}

func validateNotificationDestinations(config *storage.BucketNotificationConfiguration) *notificationValidationError {
	for _, rule := range config.TopicConfigurations {
		if err := validateNotificationDestinationARN(rule); err != nil {
			return err
		}
	}
	for _, rule := range config.QueueConfigurations {
		if err := validateNotificationDestinationARN(rule); err != nil {
			return err
		}
	}
	for _, rule := range config.CloudFunctionConfigurations {
		if err := validateNotificationDestinationARN(rule); err != nil {
			return err
		}
	}
	return nil
}

func convertNotificationConfigurationToXML(config *storage.BucketNotificationConfiguration) *NotificationConfiguration {
	response := &NotificationConfiguration{
		Xmlns:                       "http://s3.amazonaws.com/doc/2006-03-01/",
		TopicConfigurations:         make([]TopicNotificationConfiguration, 0, len(config.TopicConfigurations)),
		QueueConfigurations:         make([]QueueNotificationConfiguration, 0, len(config.QueueConfigurations)),
		CloudFunctionConfigurations: make([]LambdaNotificationConfiguration, 0, len(config.CloudFunctionConfigurations)),
	}
	if config.EventBridgeEnabled {
		response.EventBridgeConfiguration = &EventBridgeConfiguration{}
	}
	for _, rule := range config.TopicConfigurations {
		response.TopicConfigurations = append(response.TopicConfigurations, TopicNotificationConfiguration{
			ID:     rule.ID,
			Topic:  rule.DestinationARN,
			Events: rule.Events,
			Filter: convertNotificationFilterToXML(rule.FilterRules),
		})
	}
	for _, rule := range config.QueueConfigurations {
		response.QueueConfigurations = append(response.QueueConfigurations, QueueNotificationConfiguration{
			ID:     rule.ID,
			Queue:  rule.DestinationARN,
			Events: rule.Events,
			Filter: convertNotificationFilterToXML(rule.FilterRules),
		})
	}
	for _, rule := range config.CloudFunctionConfigurations {
		response.CloudFunctionConfigurations = append(response.CloudFunctionConfigurations, LambdaNotificationConfiguration{
			ID:            rule.ID,
			CloudFunction: rule.DestinationARN,
			Events:        rule.Events,
			Filter:        convertNotificationFilterToXML(rule.FilterRules),
		})
	}
	return response
}

func convertNotificationFilterToXML(rules []storage.NotificationFilterRule) *NotificationConfigurationFilter {
	if len(rules) == 0 {
		return nil
	}
	filterRules := make([]NotificationConfigurationFilterRule, 0, len(rules))
	for _, rule := range rules {
		filterRules = append(filterRules, NotificationConfigurationFilterRule{Name: rule.Name, Value: rule.Value})
	}
	return &NotificationConfigurationFilter{S3Key: NotificationConfigurationS3KeyFilter{FilterRules: filterRules}}
}

func (s *Server) getBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.getBucketNotificationHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	if s.authorizeRequest(ctx, authorization.OperationGetBucketNotification, ptrutils.ToPtr(bucketName.String()), nil, w, r) {
		return
	}

	config, err := s.storage.GetBucketNotificationConfiguration(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}

	response := convertNotificationConfigurationToXML(config)
	w.Header().Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(http.StatusOK)
	out, _ := xmlMarshalWithDocType(response)
	w.Write(out)
}

func (s *Server) putBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.putBucketNotificationHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	if s.authorizeRequest(ctx, authorization.OperationPutBucketNotification, ptrutils.ToPtr(bucketName.String()), nil, w, r) {
		return
	}

	data, err := readLimitedBody(r, w, maxPutBucketNotificationBodySize)
	if err != nil {
		handleError(err, w, r)
		return
	}

	var request NotificationConfiguration
	if err := xml.Unmarshal(data, &request); err != nil {
		writeMalformedXML(w, r)
		return
	}

	config, validationErr := convertNotificationConfigurationFromXML(&request)
	if validationErr != nil {
		writeNotificationValidationError(w, r, validationErr)
		return
	}
	skipDestinationValidation := strings.EqualFold(r.Header.Get(skipDestinationValidationHeader), "true")
	if !skipDestinationValidation {
		if validationErr := validateNotificationDestinations(config); validationErr != nil {
			writeNotificationValidationError(w, r, validationErr)
			return
		}
	} else {
		ctx = storage.WithSkipNotificationDestinationValidation(ctx)
	}

	if err := s.storage.PutBucketNotificationConfiguration(ctx, bucketName, config); err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(http.StatusOK)
}
