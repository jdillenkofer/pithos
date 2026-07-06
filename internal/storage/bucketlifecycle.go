package storage

import (
	"fmt"
	"strings"
	"time"
)

// MaxLifecycleRules is the maximum number of rules a bucket lifecycle
// configuration may contain, as defined by Amazon S3.
const MaxLifecycleRules = 1000

// maxLifecycleRuleIDLength is the maximum length of a lifecycle rule ID.
const maxLifecycleRuleIDLength = 255

// LifecycleValidationError describes why a lifecycle configuration was
// rejected. Code and Message follow the S3 error model so the HTTP layer can
// return them verbatim.
type LifecycleValidationError struct {
	// Code is the S3 error code ("MalformedXML", "InvalidArgument" or
	// "InvalidRequest").
	Code    string
	Message string
}

func (e *LifecycleValidationError) Error() string {
	return e.Message
}

func malformedLifecycleXML(message string) *LifecycleValidationError {
	return &LifecycleValidationError{Code: "MalformedXML", Message: message}
}

func invalidLifecycleArgument(message string) *LifecycleValidationError {
	return &LifecycleValidationError{Code: "InvalidArgument", Message: message}
}

func invalidLifecycleRequest(message string) *LifecycleValidationError {
	return &LifecycleValidationError{Code: "InvalidRequest", Message: message}
}

// ValidateBucketLifecycleConfiguration checks a lifecycle configuration
// against the S3 rules for PutBucketLifecycleConfiguration. It returns a
// *LifecycleValidationError describing the first violation found, or nil.
func ValidateBucketLifecycleConfiguration(config *BucketLifecycleConfiguration) *LifecycleValidationError {
	if len(config.Rules) == 0 {
		return malformedLifecycleXML("The XML you provided was not well-formed or did not validate against our published schema")
	}
	if len(config.Rules) > MaxLifecycleRules {
		return invalidLifecycleRequest(fmt.Sprintf("Lifecycle configuration allows a maximum of %d rules", MaxLifecycleRules))
	}
	seenIDs := map[string]struct{}{}
	for i := range config.Rules {
		rule := &config.Rules[i]
		if rule.ID != nil {
			if len(*rule.ID) > maxLifecycleRuleIDLength {
				return invalidLifecycleArgument(fmt.Sprintf("ID length should not exceed allowed limit of %d", maxLifecycleRuleIDLength))
			}
			if _, ok := seenIDs[*rule.ID]; ok {
				return invalidLifecycleArgument("Rule ID must be unique. Found same ID for more than one rule")
			}
			seenIDs[*rule.ID] = struct{}{}
		}
		if err := validateLifecycleRule(rule); err != nil {
			return err
		}
	}
	return nil
}

func validateLifecycleRule(rule *LifecycleRule) *LifecycleValidationError {
	if rule.Status != LifecycleRuleStatusEnabled && rule.Status != LifecycleRuleStatusDisabled {
		return malformedLifecycleXML("The XML you provided was not well-formed or did not validate against our published schema")
	}
	// A rule must select its objects either via the legacy top-level Prefix or
	// via Filter, but not both and not neither.
	if rule.Prefix != nil && rule.Filter != nil {
		return malformedLifecycleXML("Rule cannot specify both Filter and Prefix")
	}
	if rule.Prefix == nil && rule.Filter == nil {
		return malformedLifecycleXML("Rule must specify either Filter or Prefix")
	}
	if rule.Filter != nil {
		if err := validateLifecycleFilter(rule.Filter); err != nil {
			return err
		}
	}
	if rule.Expiration == nil && rule.AbortIncompleteMultipartUpload == nil && len(rule.Transitions) == 0 {
		return invalidLifecycleRequest("At least one action needs to be specified in a rule")
	}
	if rule.Expiration != nil {
		if err := validateLifecycleExpiration(rule); err != nil {
			return err
		}
	}
	if rule.AbortIncompleteMultipartUpload != nil {
		abort := rule.AbortIncompleteMultipartUpload
		if abort.DaysAfterInitiation == nil || *abort.DaysAfterInitiation <= 0 {
			return invalidLifecycleArgument("'DaysAfterInitiation' for AbortIncompleteMultipartUpload action must be a positive integer")
		}
		if lifecycleRuleHasTagFilter(rule) || lifecycleRuleHasSizeFilter(rule) {
			return invalidLifecycleRequest("AbortIncompleteMultipartUpload cannot be specified with Tags or Object Size")
		}
	}
	if len(rule.Transitions) > 0 {
		if err := validateLifecycleTransitions(rule); err != nil {
			return err
		}
	}
	return nil
}

func validateLifecycleTransitions(rule *LifecycleRule) *LifecycleValidationError {
	seenTargets := map[string]struct{}{}
	for i := range rule.Transitions {
		transition := &rule.Transitions[i]
		if transition.Days != nil && transition.Date != nil {
			return malformedLifecycleXML("Days and Date are mutually exclusive in a Transition action")
		}
		if transition.Days == nil && transition.Date == nil {
			return malformedLifecycleXML("Transition action must specify one of Days or Date")
		}
		// Unlike Expiration, S3 allows Days=0 for transitions.
		if transition.Days != nil && *transition.Days < 0 {
			return invalidLifecycleArgument("'Days' for Transition action must be a non-negative integer")
		}
		if transition.Date != nil {
			date := transition.Date.UTC()
			if !date.Equal(time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)) {
				return invalidLifecycleArgument("'Date' must be at midnight GMT")
			}
		}
		if transition.StorageClass == "" {
			return malformedLifecycleXML("Transition action must specify a StorageClass")
		}
		if !IsValidStorageClass(transition.StorageClass) {
			return invalidLifecycleArgument("'StorageClass' must be a valid storage class")
		}
		if transition.StorageClass == StorageClassStandard {
			return invalidLifecycleArgument("'StorageClass' for Transition action must not be STANDARD")
		}
		if _, ok := seenTargets[transition.StorageClass]; ok {
			return invalidLifecycleArgument("'StorageClass' must be different for 'Transition' actions in same 'Rule'")
		}
		seenTargets[transition.StorageClass] = struct{}{}
		// The transition must be due before the expiration deletes the object,
		// otherwise it can never take effect.
		if transition.Days != nil && rule.Expiration != nil && rule.Expiration.Days != nil && *transition.Days >= *rule.Expiration.Days {
			return invalidLifecycleArgument("'Days' in the Expiration action for filter must be greater than 'Days' in the Transition action")
		}
	}
	return nil
}

func validateLifecycleFilter(filter *LifecycleFilter) *LifecycleValidationError {
	// At most one predicate may be set directly on the filter; combinations
	// must use And.
	predicates := 0
	if filter.Prefix != nil {
		predicates++
	}
	if filter.Tag != nil {
		predicates++
	}
	if filter.ObjectSizeGreaterThan != nil {
		predicates++
	}
	if filter.ObjectSizeLessThan != nil {
		predicates++
	}
	if filter.And != nil {
		predicates++
	}
	if predicates > 1 {
		return malformedLifecycleXML("Filter must have exactly one of Prefix, Tag, ObjectSizeGreaterThan, ObjectSizeLessThan or And")
	}
	if filter.ObjectSizeGreaterThan != nil && *filter.ObjectSizeGreaterThan < 0 {
		return invalidLifecycleArgument("'ObjectSizeGreaterThan' must be a non-negative integer")
	}
	if filter.ObjectSizeLessThan != nil && *filter.ObjectSizeLessThan <= 0 {
		return invalidLifecycleArgument("'ObjectSizeLessThan' must be a positive integer")
	}
	var tags []LifecycleTag
	if filter.Tag != nil {
		tags = append(tags, *filter.Tag)
	}
	if filter.And != nil {
		and := filter.And
		if and.ObjectSizeGreaterThan != nil && *and.ObjectSizeGreaterThan < 0 {
			return invalidLifecycleArgument("'ObjectSizeGreaterThan' must be a non-negative integer")
		}
		if and.ObjectSizeLessThan != nil && *and.ObjectSizeLessThan <= 0 {
			return invalidLifecycleArgument("'ObjectSizeLessThan' must be a positive integer")
		}
		if and.ObjectSizeGreaterThan != nil && and.ObjectSizeLessThan != nil && *and.ObjectSizeGreaterThan >= *and.ObjectSizeLessThan {
			return invalidLifecycleArgument("'ObjectSizeGreaterThan' should be less than 'ObjectSizeLessThan'")
		}
		tags = append(tags, and.Tags...)
	}
	seenTagKeys := map[string]struct{}{}
	for _, tag := range tags {
		if len(tag.Key) == 0 || len([]rune(tag.Key)) > MaxTagKeyLength {
			return invalidLifecycleArgument("The TagKey you have provided is invalid")
		}
		if len([]rune(tag.Value)) > MaxTagValueLength {
			return invalidLifecycleArgument("The TagValue you have provided is invalid")
		}
		if _, ok := seenTagKeys[tag.Key]; ok {
			return invalidLifecycleArgument("Duplicate Tag Keys are not allowed")
		}
		seenTagKeys[tag.Key] = struct{}{}
	}
	return nil
}

func validateLifecycleExpiration(rule *LifecycleRule) *LifecycleValidationError {
	expiration := rule.Expiration
	set := 0
	if expiration.Days != nil {
		set++
	}
	if expiration.Date != nil {
		set++
	}
	if expiration.ExpiredObjectDeleteMarker != nil {
		set++
	}
	if set == 0 {
		return malformedLifecycleXML("Expiration action must specify one of Days, Date or ExpiredObjectDeleteMarker")
	}
	if set > 1 {
		return malformedLifecycleXML("Days, Date and ExpiredObjectDeleteMarker are mutually exclusive in an Expiration action")
	}
	if expiration.Days != nil && *expiration.Days <= 0 {
		return invalidLifecycleArgument("'Days' for Expiration action must be a positive integer")
	}
	if expiration.Date != nil {
		date := expiration.Date.UTC()
		if !date.Equal(time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)) {
			return invalidLifecycleArgument("'Date' must be at midnight GMT")
		}
	}
	if expiration.ExpiredObjectDeleteMarker != nil && lifecycleRuleHasTagFilter(rule) {
		return invalidLifecycleRequest("An ExpiredObjectDeleteMarker cannot be specified with Tags")
	}
	return nil
}

func lifecycleRuleHasTagFilter(rule *LifecycleRule) bool {
	if rule.Filter == nil {
		return false
	}
	if rule.Filter.Tag != nil {
		return true
	}
	return rule.Filter.And != nil && len(rule.Filter.And.Tags) > 0
}

func lifecycleRuleHasSizeFilter(rule *LifecycleRule) bool {
	if rule.Filter == nil {
		return false
	}
	if rule.Filter.ObjectSizeGreaterThan != nil || rule.Filter.ObjectSizeLessThan != nil {
		return true
	}
	return rule.Filter.And != nil && (rule.Filter.And.ObjectSizeGreaterThan != nil || rule.Filter.And.ObjectSizeLessThan != nil)
}

// lifecycleRulePrefix returns the key prefix a rule applies to ("" if the rule
// applies to the whole bucket).
func lifecycleRulePrefix(rule *LifecycleRule) string {
	if rule.Prefix != nil {
		return *rule.Prefix
	}
	if rule.Filter == nil {
		return ""
	}
	if rule.Filter.Prefix != nil {
		return *rule.Filter.Prefix
	}
	if rule.Filter.And != nil && rule.Filter.And.Prefix != nil {
		return *rule.Filter.And.Prefix
	}
	return ""
}

// LifecycleRuleMatchesObject reports whether the rule's filter selects the
// given object. tags must contain the object's full tag set when the rule uses
// tag predicates.
func LifecycleRuleMatchesObject(rule *LifecycleRule, key string, size int64, tags map[string]string) bool {
	if !strings.HasPrefix(key, lifecycleRulePrefix(rule)) {
		return false
	}
	if rule.Filter == nil {
		return true
	}
	filter := rule.Filter
	sizeGreaterThan := filter.ObjectSizeGreaterThan
	sizeLessThan := filter.ObjectSizeLessThan
	var filterTags []LifecycleTag
	if filter.Tag != nil {
		filterTags = append(filterTags, *filter.Tag)
	}
	if filter.And != nil {
		if filter.And.ObjectSizeGreaterThan != nil {
			sizeGreaterThan = filter.And.ObjectSizeGreaterThan
		}
		if filter.And.ObjectSizeLessThan != nil {
			sizeLessThan = filter.And.ObjectSizeLessThan
		}
		filterTags = append(filterTags, filter.And.Tags...)
	}
	if sizeGreaterThan != nil && size <= *sizeGreaterThan {
		return false
	}
	if sizeLessThan != nil && size >= *sizeLessThan {
		return false
	}
	for _, tag := range filterTags {
		value, ok := tags[tag.Key]
		if !ok || value != tag.Value {
			return false
		}
	}
	return true
}

// LifecycleRuleNeedsObjectTags reports whether evaluating the rule requires
// the object's tag set.
func LifecycleRuleNeedsObjectTags(rule *LifecycleRule) bool {
	return lifecycleRuleHasTagFilter(rule)
}

// lifecycleNextMidnightUTC returns the first midnight UTC strictly after t.
// S3 rounds day-based lifecycle timestamps "to the next day midnight UTC":
// an object created 2014-01-15T10:30Z with Days=3 expires 2014-01-19T00:00Z.
func lifecycleNextMidnightUTC(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
}

// LifecycleExpirationDueTime returns the time at which an object created at
// the given time expires under the rule's Expiration action, or nil when the
// action never deletes objects (e.g. ExpiredObjectDeleteMarker without
// versioning).
func LifecycleExpirationDueTime(rule *LifecycleRule, objectCreated time.Time) *time.Time {
	if rule.Expiration == nil {
		return nil
	}
	if rule.Expiration.Date != nil {
		due := rule.Expiration.Date.UTC()
		return &due
	}
	if rule.Expiration.Days != nil {
		due := lifecycleNextMidnightUTC(objectCreated.AddDate(0, 0, int(*rule.Expiration.Days)))
		return &due
	}
	return nil
}

// LifecycleTransitionDueTime returns the time at which an object created at
// the given time becomes due for the given Transition action.
func LifecycleTransitionDueTime(transition *LifecycleTransition, objectCreated time.Time) *time.Time {
	if transition.Date != nil {
		due := transition.Date.UTC()
		return &due
	}
	if transition.Days != nil {
		due := lifecycleNextMidnightUTC(objectCreated.AddDate(0, 0, int(*transition.Days)))
		return &due
	}
	return nil
}

// LifecycleAbortDueTime returns the time at which a multipart upload initiated
// at the given time becomes eligible for aborting, or nil when the rule has no
// AbortIncompleteMultipartUpload action.
func LifecycleAbortDueTime(rule *LifecycleRule, uploadInitiated time.Time) *time.Time {
	if rule.AbortIncompleteMultipartUpload == nil || rule.AbortIncompleteMultipartUpload.DaysAfterInitiation == nil {
		return nil
	}
	due := lifecycleNextMidnightUTC(uploadInitiated.AddDate(0, 0, int(*rule.AbortIncompleteMultipartUpload.DaysAfterInitiation)))
	return &due
}
