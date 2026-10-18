package admin

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	s3 "github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

func notificationTestConfig() *storage.BucketNotificationConfiguration {
	return &storage.BucketNotificationConfiguration{
		TopicConfigurations:         []storage.NotificationConfigurationRule{{ID: ptrutils.ToPtr("topic"), DestinationType: storage.NotificationDestinationTopic, DestinationARN: "arn:aws:sns:eu-central-1:123456789012:topic", Events: []string{"s3:ObjectCreated:*"}}},
		QueueConfigurations:         []storage.NotificationConfigurationRule{{DestinationType: storage.NotificationDestinationQueue, DestinationARN: "arn:aws:sqs:eu-central-1:123456789012:queue", Events: []string{"s3:ObjectRemoved:Delete"}, FilterRules: []storage.NotificationFilterRule{{Name: "prefix", Value: " keep/ "}, {Name: "suffix", Value: " .json "}}}},
		CloudFunctionConfigurations: []storage.NotificationConfigurationRule{{ID: ptrutils.ToPtr("lambda"), DestinationType: storage.NotificationDestinationCloudFunction, DestinationARN: "arn:aws:lambda:eu-central-1:123456789012:function:processor", Events: []string{"s3:Replication:OperationFailedReplication"}}},
		EventBridgeEnabled:          true,
	}
}

func notificationTestForm(config *storage.BucketNotificationConfiguration) url.Values {
	if config == nil {
		config = &storage.BucketNotificationConfiguration{}
	}
	return url.Values{
		"notification_hash":   {policyHash([]byte(configurationText(config)))},
		"notification_action": {"save"}, "destination_type": {"Queue"},
		"destination_arn": {"arn:aws:sqs:eu-central-1:123456789012:new-queue"},
		"events":          {"s3:ObjectCreated:Put", "s3:ObjectTagging:*"},
		"id":              {"image-events"}, "prefix": {" images/ "}, "suffix": {" .jpg "},
	}
}

func TestNotificationCreateAllDestinationTypes(t *testing.T) {
	for _, tc := range []struct{ kind, arn string }{
		{"Topic", "arn:aws:sns:eu-central-1:123456789012:new-topic"},
		{"Queue", "arn:aws:sqs:eu-central-1:123456789012:new-queue"},
		{"CloudFunction", "arn:aws:lambda:eu-central-1:123456789012:function:new-function"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			config := notificationTestConfig()
			s := &bucketTestStorage{notifications: config}
			form := notificationTestForm(config)
			form.Set("destination_type", tc.kind)
			form.Set("destination_arn", " "+tc.arn+" ")
			w := structuredBucketRequest(bucketTestApp(t, s), "notifications", form)
			require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
			require.Equal(t, "/buckets/detail?bucket=valid-bucket&tab=notifications#notifications", w.Header().Get("Location"))
			actual := s.last.(*storage.BucketNotificationConfiguration)
			require.True(t, actual.EventBridgeEnabled)
			for _, kind := range []string{"Topic", "Queue", "CloudFunction"} {
				rules := *notificationRules(actual, kind)
				original := *notificationRules(config, kind)
				require.Equal(t, original, rules[:len(original)], "preserve unrelated rules")
				if kind == tc.kind {
					require.Len(t, rules, len(original)+1)
					rule := rules[len(original)]
					require.Equal(t, storage.NotificationDestinationType(kind), rule.DestinationType)
					require.Equal(t, tc.arn, rule.DestinationARN)
					require.Equal(t, "image-events", *rule.ID)
					require.Equal(t, form["events"], rule.Events)
					require.Equal(t, []storage.NotificationFilterRule{{Name: "prefix", Value: " images/ "}, {Name: "suffix", Value: " .jpg "}}, rule.FilterRules)
				} else {
					require.Equal(t, original, rules)
				}
			}
			require.Equal(t, notificationTestConfig(), config, "do not mutate the shared snapshot")
		})
	}
}

func TestNotificationEditAndChangeDestinationType(t *testing.T) {
	config := notificationTestConfig()
	s := &bucketTestStorage{notifications: config}
	a := bucketTestApp(t, s)
	form := notificationTestForm(config)
	form.Set("rule_type", "Queue")
	form.Set("rule_index", "0")
	form.Set("id", "")
	w := structuredBucketRequest(a, "notifications", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	actual := s.last.(*storage.BucketNotificationConfiguration)
	require.Len(t, actual.QueueConfigurations, 1)
	require.Nil(t, actual.QueueConfigurations[0].ID, "unnamed rules remain editable")
	require.Equal(t, config.TopicConfigurations, actual.TopicConfigurations)
	require.Equal(t, config.CloudFunctionConfigurations, actual.CloudFunctionConfigurations)

	form.Set("destination_type", "Topic")
	form.Set("destination_arn", "arn:aws:sns:eu-central-1:123456789012:other-topic")
	w = structuredBucketRequest(a, "notifications", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	actual = s.last.(*storage.BucketNotificationConfiguration)
	require.Empty(t, actual.QueueConfigurations)
	require.Len(t, actual.TopicConfigurations, 2)
	require.Equal(t, config.TopicConfigurations[0], actual.TopicConfigurations[0])
	require.Equal(t, storage.NotificationDestinationTopic, actual.TopicConfigurations[1].DestinationType)
	require.Equal(t, config.CloudFunctionConfigurations, actual.CloudFunctionConfigurations)
	require.True(t, actual.EventBridgeEnabled)
	require.Equal(t, notificationTestConfig(), config)
}

func TestNotificationEditPreservesRuleOrderAndClearsFilters(t *testing.T) {
	config := notificationTestConfig()
	for _, name := range []string{"second", "third"} {
		config.QueueConfigurations = append(config.QueueConfigurations, storage.NotificationConfigurationRule{
			ID: ptrutils.ToPtr(name), DestinationType: storage.NotificationDestinationQueue,
			DestinationARN: "arn:aws:sqs:eu-central-1:123456789012:" + name,
			Events:         []string{"s3:ObjectCreated:Put"},
		})
	}
	s := &bucketTestStorage{notifications: config}
	a := bucketTestApp(t, s)
	form := notificationTestForm(config)
	form.Set("rule_type", "Queue")
	form.Set("rule_index", "0")
	form.Set("prefix", "")
	form.Set("suffix", "")
	w := structuredBucketRequest(a, "notifications", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	actual := s.last.(*storage.BucketNotificationConfiguration)
	require.Len(t, actual.QueueConfigurations, 3)
	require.Empty(t, actual.QueueConfigurations[0].FilterRules)
	require.Equal(t, config.QueueConfigurations[1:], actual.QueueConfigurations[1:])

	form.Set("rule_index", "1")
	form.Set("notification_action", "delete")
	w = structuredBucketRequest(a, "notifications", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	actual = s.last.(*storage.BucketNotificationConfiguration)
	require.Equal(t, []storage.NotificationConfigurationRule{config.QueueConfigurations[0], config.QueueConfigurations[2]}, actual.QueueConfigurations)
}

func TestNotificationRuleRoundTrip(t *testing.T) {
	config := notificationTestConfig()
	for _, kind := range []string{"Topic", "Queue", "CloudFunction"} {
		t.Run(kind, func(t *testing.T) {
			rule := (*notificationRules(config, kind))[0]
			view := notificationRuleForm(rule, kind, "0")
			form := notificationTestForm(config)
			form.Set("rule_type", kind)
			form.Set("rule_index", view.Index)
			form.Set("destination_type", view.Type)
			form.Set("destination_arn", view.Destination)
			form.Set("id", view.ID)
			form.Set("prefix", view.Prefix)
			form.Set("suffix", view.Suffix)
			form["events"] = rule.Events
			s := &bucketTestStorage{notifications: config}
			w := structuredBucketRequest(bucketTestApp(t, s), "notifications", form)
			require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
			require.Equal(t, config, s.last)
		})
	}
}

func TestNotificationDeleteAndEventBridge(t *testing.T) {
	for _, action := range []string{"delete", "delete_all", "eventbridge"} {
		t.Run(action, func(t *testing.T) {
			config := notificationTestConfig()
			s := &bucketTestStorage{notifications: config}
			form := notificationTestForm(config)
			form.Set("notification_action", action)
			form.Set("rule_type", "Queue")
			form.Set("rule_index", "0")
			form.Set("eventbridge", "disabled")
			w := structuredBucketRequest(bucketTestApp(t, s), "notifications", form)
			require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
			actual := s.last.(*storage.BucketNotificationConfiguration)
			if action == "eventbridge" {
				require.False(t, actual.EventBridgeEnabled)
				require.Equal(t, config.QueueConfigurations, actual.QueueConfigurations)
			} else {
				require.True(t, actual.EventBridgeEnabled, "deleting rules preserves EventBridge")
				require.Empty(t, actual.QueueConfigurations)
			}
			if action == "delete_all" {
				require.Empty(t, actual.TopicConfigurations)
				require.Empty(t, actual.CloudFunctionConfigurations)
			} else {
				require.Equal(t, config.TopicConfigurations, actual.TopicConfigurations)
				require.Equal(t, config.CloudFunctionConfigurations, actual.CloudFunctionConfigurations)
			}
			require.Equal(t, notificationTestConfig(), config)
		})
	}
	// An empty bucket can enable EventBridge independently of destination rules.
	s := &bucketTestStorage{}
	form := notificationTestForm(nil)
	form.Set("notification_action", "eventbridge")
	form.Set("eventbridge", "enabled")
	w := structuredBucketRequest(bucketTestApp(t, s), "notifications", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.True(t, s.last.(*storage.BucketNotificationConfiguration).EventBridgeEnabled)
}

func TestNotificationInvalidSubmissionsNeverWrite(t *testing.T) {
	for _, tc := range []struct{ field, value string }{
		{"notification_hash", "stale"}, {"notification_hash", ""}, {"notification_action", "unknown"},
		{"destination_type", "unknown"}, {"destination_type", ""}, {"destination_arn", ""},
		{"destination_arn", "bad"}, {"destination_arn", "arn:aws:sns:eu-central-1:123456789012:topic"},
		{"events", ""}, {"events", "s3:BadEvent"}, {"id", strings.Repeat("a", 256)}, {"id", "topic"},
		{"rule_index", "0"}, {"rule_index", "-1"}, {"rule_index", "bad"}, {"rule_type", "Queue"},
	} {
		t.Run(tc.field+"="+tc.value, func(t *testing.T) {
			s := &bucketTestStorage{notifications: notificationTestConfig()}
			form := notificationTestForm(s.notifications)
			form.Set(tc.field, tc.value)
			w := structuredBucketRequest(bucketTestApp(t, s), "notifications", form)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			require.Zero(t, s.writes)
		})
	}
	for _, action := range []string{"delete", "eventbridge", "save"} {
		s := &bucketTestStorage{notifications: notificationTestConfig()}
		form := notificationTestForm(s.notifications)
		form.Set("notification_action", action)
		form.Del("events")
		w := structuredBucketRequest(bucketTestApp(t, s), "notifications", form)
		require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		require.Zero(t, s.writes)
	}
	// Overlapping subscriptions for the same destination use S3 validation.
	s := &bucketTestStorage{notifications: notificationTestConfig()}
	form := notificationTestForm(s.notifications)
	form.Set("destination_type", "Topic")
	form.Set("destination_arn", s.notifications.TopicConfigurations[0].DestinationARN)
	w := structuredBucketRequest(bucketTestApp(t, s), "notifications", form)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "overlapping")
	require.Zero(t, s.writes)
}

func TestNotificationDeletionConfirmationAndStaleForms(t *testing.T) {
	for _, action := range []string{"delete", "delete_all"} {
		s := &bucketTestStorage{notifications: notificationTestConfig()}
		a := bucketTestApp(t, s)
		form := notificationTestForm(s.notifications)
		form.Set("bucket", "valid-bucket")
		form.Set("tab", "notifications")
		form.Set("editor", "form")
		form.Set("notification_action", action)
		form.Set("rule_type", "Queue")
		form.Set("rule_index", "0")
		w := bucketRequest(a, "POST", "/buckets/configure", form)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), "Delete")
		require.Contains(t, w.Body.String(), `name="notification_hash"`)
		require.Zero(t, s.writes)
		s.notifications.EventBridgeEnabled = false
		form.Set("confirm", "yes")
		w = bucketRequest(a, "POST", "/buckets/configure", form)
		require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), "changed")
		require.Zero(t, s.writes)
	}
}

func TestNotificationEditorPages(t *testing.T) {
	s := &bucketTestStorage{buckets: []storage.Bucket{{Name: storage.MustNewBucketName("valid-bucket")}}, notifications: notificationTestConfig()}
	a := bucketTestApp(t, s)
	for _, suffix := range []string{"", "&editor=create", "&rule_type=Topic&rule_index=0", "&rule_type=Queue&rule_index=0", "&rule_type=CloudFunction&rule_index=0"} {
		w := bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=notifications"+suffix, nil)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		body := w.Body.String()
		require.NotContains(t, body, `name="configuration"`)
		require.NotContains(t, body, "<textarea")
		require.Equal(t, strings.Count(body, "<form"), strings.Count(body, `name="csrf_token"`))
		if suffix == "" {
			require.Contains(t, body, "Edit EventBridge setting")
			require.Contains(t, body, " keep/ ")
			require.Contains(t, body, " .json ")
		} else {
			require.Contains(t, body, `class="container-body form-stack"`)
			require.NotContains(t, body, "Delete bucket", "rule editor is a separate page")
			for _, field := range []string{"id", "prefix", "suffix", "destination_type", "destination_arn", "events", "notification_hash"} {
				require.Contains(t, body, `name="`+field+`"`)
			}
			for _, event := range s3.SupportedNotificationEvents() {
				require.Contains(t, body, `value="`+event+`"`)
			}
		}
		if strings.Contains(suffix, "CloudFunction") {
			require.Contains(t, body, `value="s3:Replication:OperationFailedReplication" checked`)
			require.Contains(t, body, `<details open>`)
		}
		if strings.Contains(suffix, "rule_index=0") {
			require.Contains(t, body, "Edit event notification")
		}
	}
	for _, suffix := range []string{"&rule_type=Topic&rule_index=1", "&rule_type=Unknown&rule_index=0", "&rule_type=Queue", "&rule_index=0", "&rule_type=Topic&rule_index=-1", "&rule_type=Topic&rule_index=bad"} {
		w := bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=notifications"+suffix, nil)
		require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
	}
}

func TestNotificationEditorEscapesValues(t *testing.T) {
	unsafe := `"><script>alert(1)</script>`
	config := notificationTestConfig()
	rule := &config.QueueConfigurations[0]
	rule.ID, rule.DestinationARN = &unsafe, unsafe
	rule.FilterRules = []storage.NotificationFilterRule{{Name: "prefix", Value: unsafe}, {Name: "suffix", Value: unsafe}}
	s := &bucketTestStorage{buckets: []storage.Bucket{{Name: storage.MustNewBucketName("valid-bucket")}}, notifications: config}
	a := bucketTestApp(t, s)
	for _, suffix := range []string{"", "&rule_type=Queue&rule_index=0"} {
		w := bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=notifications"+suffix, nil)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotContains(t, w.Body.String(), "<script>")
		require.Contains(t, w.Body.String(), "&lt;script&gt;")
	}
}

func TestNotificationLoadFailureDoesNotReplaceConfiguration(t *testing.T) {
	failure := errors.New("notification load failed")
	s := &bucketTestStorage{notificationFailure: failure}
	_, _, err := notificationFromForm(context.Background(), s, storage.MustNewBucketName("valid-bucket"), notificationTestForm(nil))
	require.ErrorIs(t, err, failure)
	require.Zero(t, s.writes)
}

func TestNotificationEventGroupsMatchValidator(t *testing.T) {
	selected := map[string]bool{"s3:ObjectCreated:Put": true}
	var events []string
	for i, group := range notificationGroups(selected) {
		require.NotEmpty(t, group.Name)
		require.NotEmpty(t, group.Events, strconv.Itoa(i))
		require.Equal(t, i >= 3, group.Advanced)
		require.Equal(t, selected, group.Selected)
		events = append(events, group.Events...)
	}
	require.ElementsMatch(t, s3.SupportedNotificationEvents(), events)
}
