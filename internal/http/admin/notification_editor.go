package admin

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"

	s3 "github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/storage"
)

type notificationFormView struct {
	OriginalType, Index, ID, Type, Destination, Prefix, Suffix string
	Events                                                     map[string]bool
}

type notificationEventGroup struct {
	Name     string
	Events   []string
	Advanced bool
	Selected map[string]bool
}

func notificationGroups(selected map[string]bool) []notificationEventGroup {
	groups := []notificationEventGroup{
		{Name: "Object creation"}, {Name: "Object removal"}, {Name: "Object tagging"},
	}
	indices := map[string]int{"ObjectCreated": 0, "ObjectRemoved": 1, "ObjectTagging": 2}
	labels := map[string]string{
		"ObjectRestore": "Object restore", "Replication": "Replication",
		"LifecycleExpiration": "Lifecycle expiration", "LifecycleTransition": "Lifecycle transitions",
		"IntelligentTiering": "Intelligent tiering", "ReducedRedundancyLostObject": "Reduced-redundancy loss",
		"ObjectAcl": "Object ACL changes", "ObjectAnnotation": "Object annotations",
	}
	for _, event := range s3.SupportedNotificationEvents() {
		category := strings.Split(strings.TrimPrefix(event, "s3:"), ":")[0]
		index, ok := indices[category]
		if !ok {
			index = len(groups)
			indices[category] = index
			label := labels[category]
			if label == "" {
				label = category
			}
			groups = append(groups, notificationEventGroup{Name: label, Advanced: true})
		}
		groups[index].Events = append(groups[index].Events, event)
	}
	for i := range groups {
		groups[i].Selected = selected
	}
	return groups
}

func notificationRules(config *storage.BucketNotificationConfiguration, kind string) *[]storage.NotificationConfigurationRule {
	switch storage.NotificationDestinationType(kind) {
	case storage.NotificationDestinationTopic:
		return &config.TopicConfigurations
	case storage.NotificationDestinationQueue:
		return &config.QueueConfigurations
	case storage.NotificationDestinationCloudFunction:
		return &config.CloudFunctionConfigurations
	default:
		return nil
	}
}

func notificationRuleForm(rule storage.NotificationConfigurationRule, kind, index string) notificationFormView {
	view := notificationFormView{OriginalType: kind, Index: index, Type: kind, Destination: rule.DestinationARN, Events: map[string]bool{}}
	if view.Type == "" {
		view.Type = string(storage.NotificationDestinationTopic)
	}
	if rule.ID != nil {
		view.ID = *rule.ID
	}
	for _, event := range rule.Events {
		view.Events[event] = true
	}
	for _, filter := range rule.FilterRules {
		switch filter.Name {
		case "prefix":
			view.Prefix = filter.Value
		case "suffix":
			view.Suffix = filter.Value
		}
	}
	return view
}

func notificationRuleFromFields(form url.Values) (storage.NotificationConfigurationRule, error) {
	rule := storage.NotificationConfigurationRule{
		DestinationType: storage.NotificationDestinationType(form.Get("destination_type")),
		DestinationARN:  strings.TrimSpace(form.Get("destination_arn")),
		Events:          append([]string(nil), form["events"]...),
	}
	if id := form.Get("id"); id != "" {
		rule.ID = &id
	}
	if notificationRules(&storage.BucketNotificationConfiguration{}, string(rule.DestinationType)) == nil {
		return rule, errors.New("choose a notification destination type")
	}
	// Filters are literal object-key characters, including leading/trailing spaces.
	for _, name := range []string{"prefix", "suffix"} {
		if value := form.Get(name); value != "" {
			rule.FilterRules = append(rule.FilterRules, storage.NotificationFilterRule{Name: name, Value: value})
		}
	}
	return rule, nil
}

// notificationFromForm edits one rule or the EventBridge setting without
// replacing unrelated rules. Snapshot hashes also protect unnamed rules.
func notificationFromForm(ctx context.Context, st storage.Storage, name storage.BucketName, form url.Values) (string, string, error) {
	current, err := st.GetBucketNotificationConfiguration(ctx, name)
	if err != nil {
		return "", "", err
	}
	if current == nil {
		current = &storage.BucketNotificationConfiguration{}
	}
	if form.Get("notification_hash") != policyHash([]byte(configurationText(current))) {
		return "", "", errors.New("event notifications changed; reload the page before saving")
	}
	config := *current
	config.TopicConfigurations = append([]storage.NotificationConfigurationRule(nil), current.TopicConfigurations...)
	config.QueueConfigurations = append([]storage.NotificationConfigurationRule(nil), current.QueueConfigurations...)
	config.CloudFunctionConfigurations = append([]storage.NotificationConfigurationRule(nil), current.CloudFunctionConfigurations...)
	switch form.Get("notification_action") {
	case "eventbridge":
		switch form.Get("eventbridge") {
		case "enabled":
			config.EventBridgeEnabled = true
		case "disabled":
			config.EventBridgeEnabled = false
		default:
			return "", "", errors.New("choose whether EventBridge notifications are enabled")
		}
	case "delete_all":
		config.TopicConfigurations = nil
		config.QueueConfigurations = nil
		config.CloudFunctionConfigurations = nil
	case "save", "delete":
		var original *[]storage.NotificationConfigurationRule
		index := -1
		if text := form.Get("rule_index"); text != "" {
			original = notificationRules(&config, form.Get("rule_type"))
			index, err = strconv.Atoi(text)
			if err != nil || original == nil || index < 0 || index >= len(*original) {
				return "", "", errors.New("notification rule not found; reload the page")
			}
		} else if form.Get("rule_type") != "" {
			return "", "", errors.New("choose a notification rule to edit")
		}
		if form.Get("notification_action") == "delete" {
			if original == nil {
				return "", "", errors.New("choose a notification rule to delete")
			}
			*original = append((*original)[:index], (*original)[index+1:]...)
		} else {
			rule, err := notificationRuleFromFields(form)
			if err != nil {
				return "", "", err
			}
			target := notificationRules(&config, string(rule.DestinationType))
			if original == target {
				(*target)[index] = rule
			} else {
				if original != nil {
					*original = append((*original)[:index], (*original)[index+1:]...)
				}
				*target = append(*target, rule)
			}
		}
	default:
		return "", "", errors.New("unknown notification action")
	}
	// The normal bucket mutation path performs structural and destination validation.
	return mustMarshal(config)
}
