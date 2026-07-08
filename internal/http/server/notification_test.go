package server

import (
	"encoding/xml"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestNotificationConfigurationRoundTripsAWSXML(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := `<NotificationConfiguration>
		<QueueConfiguration>
			<Id>images</Id>
			<Queue>arn:aws:sqs:eu-central-1:000000000000:image-jobs</Queue>
			<Event>s3:ObjectCreated:*</Event>
			<Event>s3:ObjectTagging:Put</Event>
			<Filter><S3Key><FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule><FilterRule><Name>suffix</Name><Value>.jpg</Value></FilterRule></S3Key></Filter>
		</QueueConfiguration>
		<EventBridgeConfiguration/>
	</NotificationConfiguration>`

	var request NotificationConfiguration
	require.NoError(t, xml.Unmarshal([]byte(body), &request))

	config, validationErr := convertNotificationConfigurationFromXML(&request)
	require.Nil(t, validationErr)
	require.Nil(t, validateNotificationDestinations(config))
	require.True(t, config.EventBridgeEnabled)
	require.Len(t, config.QueueConfigurations, 1)
	require.Equal(t, "images", *config.QueueConfigurations[0].ID)
	require.Equal(t, []string{"s3:ObjectCreated:*", "s3:ObjectTagging:Put"}, config.QueueConfigurations[0].Events)
	require.Len(t, config.QueueConfigurations[0].FilterRules, 2)

	response := convertNotificationConfigurationToXML(config)
	require.NotNil(t, response.EventBridgeConfiguration)
	require.Len(t, response.QueueConfigurations, 1)
	require.Equal(t, "arn:aws:sqs:eu-central-1:000000000000:image-jobs", response.QueueConfigurations[0].Queue)
	require.Len(t, response.QueueConfigurations[0].Filter.S3Key.FilterRules, 2)
}

func TestNotificationConfigurationRejectsInvalidValues(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := `<NotificationConfiguration>
		<TopicConfiguration>
			<Id>dup</Id>
			<Topic>arn:aws:sns:eu-central-1:000000000000:topic</Topic>
			<Event>s3:TestEvent</Event>
		</TopicConfiguration>
	</NotificationConfiguration>`

	var request NotificationConfiguration
	require.NoError(t, xml.Unmarshal([]byte(body), &request))
	_, validationErr := convertNotificationConfigurationFromXML(&request)
	require.NotNil(t, validationErr)
	require.Equal(t, "InvalidArgument", validationErr.Code)
}

func TestNotificationDestinationValidationMatchesConfigurationType(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := `<NotificationConfiguration>
		<QueueConfiguration>
			<Queue>arn:aws:sns:eu-central-1:000000000000:not-a-queue</Queue>
			<Event>s3:ObjectRemoved:*</Event>
		</QueueConfiguration>
	</NotificationConfiguration>`

	var request NotificationConfiguration
	require.NoError(t, xml.Unmarshal([]byte(body), &request))
	config, validationErr := convertNotificationConfigurationFromXML(&request)
	require.Nil(t, validationErr)
	validationErr = validateNotificationDestinations(config)
	require.NotNil(t, validationErr)
	require.Equal(t, "InvalidArgument", validationErr.Code)
}
