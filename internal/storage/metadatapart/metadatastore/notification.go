package metadatastore

type NotificationDestinationType string

const (
	NotificationDestinationTopic         NotificationDestinationType = "Topic"
	NotificationDestinationQueue         NotificationDestinationType = "Queue"
	NotificationDestinationCloudFunction NotificationDestinationType = "CloudFunction"
)

type NotificationFilterRule struct {
	Name  string
	Value string
}

type NotificationConfigurationRule struct {
	ID              *string
	DestinationType NotificationDestinationType
	DestinationARN  string
	Events          []string
	FilterRules     []NotificationFilterRule
}

type BucketNotificationConfiguration struct {
	TopicConfigurations         []NotificationConfigurationRule
	QueueConfigurations         []NotificationConfigurationRule
	CloudFunctionConfigurations []NotificationConfigurationRule
	EventBridgeEnabled          bool
}
