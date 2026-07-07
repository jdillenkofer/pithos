package storage

import (
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validExpirationRule() LifecycleRule {
	return LifecycleRule{
		ID:         ptrutils.ToPtr("rule-1"),
		Status:     LifecycleRuleStatusEnabled,
		Filter:     &LifecycleFilter{Prefix: ptrutils.ToPtr("logs/")},
		Expiration: &LifecycleExpiration{Days: ptrutils.ToPtr(int32(30))},
	}
}

func TestValidateLifecycleAcceptsValidConfiguration(t *testing.T) {
	testutils.SkipIfIntegration(t)

	config := &BucketLifecycleConfiguration{Rules: []LifecycleRule{validExpirationRule()}}
	require.Nil(t, ValidateBucketLifecycleConfiguration(config))
}

func TestValidateLifecycleAcceptsLegacyPrefixRule(t *testing.T) {
	testutils.SkipIfIntegration(t)

	config := &BucketLifecycleConfiguration{Rules: []LifecycleRule{{
		Status:     LifecycleRuleStatusEnabled,
		Prefix:     ptrutils.ToPtr("logs/"),
		Expiration: &LifecycleExpiration{Days: ptrutils.ToPtr(int32(1))},
	}}}
	require.Nil(t, ValidateBucketLifecycleConfiguration(config))
}

func TestValidateLifecycleAcceptsEmptyFilter(t *testing.T) {
	testutils.SkipIfIntegration(t)

	config := &BucketLifecycleConfiguration{Rules: []LifecycleRule{{
		Status:     LifecycleRuleStatusEnabled,
		Filter:     &LifecycleFilter{},
		Expiration: &LifecycleExpiration{Days: ptrutils.ToPtr(int32(1))},
	}}}
	require.Nil(t, ValidateBucketLifecycleConfiguration(config))
}

func TestValidateLifecycleRejectsEmptyConfiguration(t *testing.T) {
	testutils.SkipIfIntegration(t)

	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{})
	require.NotNil(t, err)
	assert.Equal(t, "MalformedXML", err.Code)
}

func TestValidateLifecycleRejectsTooManyRules(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rules := make([]LifecycleRule, 0, MaxLifecycleRules+1)
	for i := 0; i <= MaxLifecycleRules; i++ {
		rule := validExpirationRule()
		rule.ID = nil
		rules = append(rules, rule)
	}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: rules})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidRequest", err.Code)
}

func TestValidateLifecycleRejectsDuplicateRuleIDs(t *testing.T) {
	testutils.SkipIfIntegration(t)

	config := &BucketLifecycleConfiguration{Rules: []LifecycleRule{validExpirationRule(), validExpirationRule()}}
	err := ValidateBucketLifecycleConfiguration(config)
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestValidateLifecycleRejectsOverlongRuleID(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validExpirationRule()
	longID := make([]byte, maxLifecycleRuleIDLength+1)
	for i := range longID {
		longID[i] = 'a'
	}
	rule.ID = ptrutils.ToPtr(string(longID))
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestValidateLifecycleRejectsInvalidStatus(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validExpirationRule()
	rule.Status = "enabled"
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "MalformedXML", err.Code)
}

func TestValidateLifecycleRejectsRuleWithoutAction(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validExpirationRule()
	rule.Expiration = nil
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidRequest", err.Code)
	assert.Contains(t, err.Message, "At least one action")
}

func TestValidateLifecycleRejectsRuleWithoutFilterAndPrefix(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validExpirationRule()
	rule.Filter = nil
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "MalformedXML", err.Code)
}

func TestValidateLifecycleRejectsRuleWithFilterAndPrefix(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validExpirationRule()
	rule.Prefix = ptrutils.ToPtr("logs/")
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "MalformedXML", err.Code)
}

func TestValidateLifecycleRejectsFilterWithMultiplePredicates(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validExpirationRule()
	rule.Filter = &LifecycleFilter{
		Prefix: ptrutils.ToPtr("logs/"),
		Tag:    &LifecycleTag{Key: "env", Value: "dev"},
	}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "MalformedXML", err.Code)
}

func TestValidateLifecycleRejectsNonPositiveExpirationDays(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validExpirationRule()
	rule.Expiration = &LifecycleExpiration{Days: ptrutils.ToPtr(int32(0))}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestValidateLifecycleRejectsExpirationWithDaysAndDate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validExpirationRule()
	rule.Expiration = &LifecycleExpiration{
		Days: ptrutils.ToPtr(int32(1)),
		Date: ptrutils.ToPtr(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "MalformedXML", err.Code)
}

func TestValidateLifecycleRejectsNonMidnightDate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validExpirationRule()
	rule.Expiration = &LifecycleExpiration{
		Date: ptrutils.ToPtr(time.Date(2030, 1, 1, 12, 30, 0, 0, time.UTC)),
	}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
	assert.Contains(t, err.Message, "midnight")
}

func TestValidateLifecycleRejectsExpiredObjectDeleteMarkerWithTags(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := LifecycleRule{
		Status:     LifecycleRuleStatusEnabled,
		Filter:     &LifecycleFilter{Tag: &LifecycleTag{Key: "env", Value: "dev"}},
		Expiration: &LifecycleExpiration{ExpiredObjectDeleteMarker: ptrutils.ToPtr(true)},
	}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidRequest", err.Code)
}

func TestValidateLifecycleRejectsAbortWithTagFilter(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := LifecycleRule{
		Status:                         LifecycleRuleStatusEnabled,
		Filter:                         &LifecycleFilter{Tag: &LifecycleTag{Key: "env", Value: "dev"}},
		AbortIncompleteMultipartUpload: &LifecycleAbortIncompleteMultipartUpload{DaysAfterInitiation: ptrutils.ToPtr(int32(7))},
	}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidRequest", err.Code)
}

func TestValidateLifecycleRejectsAbortWithSizeFilter(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := LifecycleRule{
		Status:                         LifecycleRuleStatusEnabled,
		Filter:                         &LifecycleFilter{ObjectSizeGreaterThan: ptrutils.ToPtr(int64(100))},
		AbortIncompleteMultipartUpload: &LifecycleAbortIncompleteMultipartUpload{DaysAfterInitiation: ptrutils.ToPtr(int32(7))},
	}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidRequest", err.Code)
}

func TestValidateLifecycleRejectsNonPositiveDaysAfterInitiation(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := LifecycleRule{
		Status:                         LifecycleRuleStatusEnabled,
		Filter:                         &LifecycleFilter{},
		AbortIncompleteMultipartUpload: &LifecycleAbortIncompleteMultipartUpload{DaysAfterInitiation: ptrutils.ToPtr(int32(0))},
	}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestValidateLifecycleRejectsInvalidSizeRangeInAnd(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validExpirationRule()
	rule.Filter = &LifecycleFilter{And: &LifecycleFilterAnd{
		ObjectSizeGreaterThan: ptrutils.ToPtr(int64(100)),
		ObjectSizeLessThan:    ptrutils.ToPtr(int64(100)),
	}}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestValidateLifecycleRejectsDuplicateTagKeysInAnd(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validExpirationRule()
	rule.Filter = &LifecycleFilter{And: &LifecycleFilterAnd{
		Tags: []LifecycleTag{{Key: "env", Value: "dev"}, {Key: "env", Value: "prod"}},
	}}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestLifecycleRuleMatchesObjectByPrefix(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validExpirationRule()
	assert.True(t, LifecycleRuleMatchesObject(&rule, "logs/app.log", 10, nil))
	assert.False(t, LifecycleRuleMatchesObject(&rule, "data/app.log", 10, nil))
}

func TestLifecycleRuleMatchesObjectBySize(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := LifecycleRule{
		Status: LifecycleRuleStatusEnabled,
		Filter: &LifecycleFilter{And: &LifecycleFilterAnd{
			ObjectSizeGreaterThan: ptrutils.ToPtr(int64(100)),
			ObjectSizeLessThan:    ptrutils.ToPtr(int64(200)),
		}},
	}
	assert.False(t, LifecycleRuleMatchesObject(&rule, "k", 100, nil), "ObjectSizeGreaterThan is exclusive")
	assert.True(t, LifecycleRuleMatchesObject(&rule, "k", 150, nil))
	assert.False(t, LifecycleRuleMatchesObject(&rule, "k", 200, nil), "ObjectSizeLessThan is exclusive")
}

func TestLifecycleRuleMatchesObjectByTags(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := LifecycleRule{
		Status: LifecycleRuleStatusEnabled,
		Filter: &LifecycleFilter{And: &LifecycleFilterAnd{
			Prefix: ptrutils.ToPtr("logs/"),
			Tags:   []LifecycleTag{{Key: "env", Value: "dev"}, {Key: "team", Value: "storage"}},
		}},
	}
	matchingTags := map[string]string{"env": "dev", "team": "storage", "extra": "ok"}
	assert.True(t, LifecycleRuleMatchesObject(&rule, "logs/a", 1, matchingTags))
	assert.False(t, LifecycleRuleMatchesObject(&rule, "logs/a", 1, map[string]string{"env": "dev"}))
	assert.False(t, LifecycleRuleMatchesObject(&rule, "logs/a", 1, map[string]string{"env": "prod", "team": "storage"}))
	assert.False(t, LifecycleRuleMatchesObject(&rule, "other/a", 1, matchingTags))
}

func TestLifecycleExpirationDueTimeRoundsToNextMidnightUTC(t *testing.T) {
	testutils.SkipIfIntegration(t)

	// Example from the S3 documentation: an object created 2014-01-15T10:30Z
	// with Days=3 expires at 2014-01-19T00:00Z.
	rule := LifecycleRule{
		Expiration: &LifecycleExpiration{Days: ptrutils.ToPtr(int32(3))},
	}
	created := time.Date(2014, 1, 15, 10, 30, 0, 0, time.UTC)
	due := LifecycleExpirationDueTime(&rule, created)
	require.NotNil(t, due)
	assert.Equal(t, time.Date(2014, 1, 19, 0, 0, 0, 0, time.UTC), *due)
}

func TestLifecycleExpirationDueTimeUsesDate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	date := time.Date(2030, 6, 1, 0, 0, 0, 0, time.UTC)
	rule := LifecycleRule{
		Expiration: &LifecycleExpiration{Date: &date},
	}
	due := LifecycleExpirationDueTime(&rule, time.Now())
	require.NotNil(t, due)
	assert.Equal(t, date, *due)
}

func TestLifecycleExpirationDueTimeNilForDeleteMarkerOnly(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := LifecycleRule{
		Expiration: &LifecycleExpiration{ExpiredObjectDeleteMarker: ptrutils.ToPtr(true)},
	}
	assert.Nil(t, LifecycleExpirationDueTime(&rule, time.Now()))
}

func TestLifecycleAbortDueTime(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := LifecycleRule{
		AbortIncompleteMultipartUpload: &LifecycleAbortIncompleteMultipartUpload{DaysAfterInitiation: ptrutils.ToPtr(int32(7))},
	}
	initiated := time.Date(2014, 1, 15, 10, 30, 0, 0, time.UTC)
	due := LifecycleAbortDueTime(&rule, initiated)
	require.NotNil(t, due)
	assert.Equal(t, time.Date(2014, 1, 23, 0, 0, 0, 0, time.UTC), *due)
}

func validTransitionRule() LifecycleRule {
	return LifecycleRule{
		ID:     ptrutils.ToPtr("transition-rule"),
		Status: LifecycleRuleStatusEnabled,
		Filter: &LifecycleFilter{Prefix: ptrutils.ToPtr("logs/")},
		Transitions: []LifecycleTransition{
			{Days: ptrutils.ToPtr(int32(30)), StorageClass: "GLACIER"},
		},
	}
}

func TestValidateLifecycleAcceptsTransitionOnlyRule(t *testing.T) {
	testutils.SkipIfIntegration(t)

	config := &BucketLifecycleConfiguration{Rules: []LifecycleRule{validTransitionRule()}}
	require.Nil(t, ValidateBucketLifecycleConfiguration(config))
}

func TestValidateLifecycleAcceptsTransitionWithZeroDays(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validTransitionRule()
	rule.Transitions[0].Days = ptrutils.ToPtr(int32(0))
	config := &BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}}
	require.Nil(t, ValidateBucketLifecycleConfiguration(config))
}

func TestValidateLifecycleAcceptsTransitionWithMidnightDate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validTransitionRule()
	rule.Transitions[0].Days = nil
	rule.Transitions[0].Date = ptrutils.ToPtr(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	config := &BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}}
	require.Nil(t, ValidateBucketLifecycleConfiguration(config))
}

func TestValidateLifecycleRejectsTransitionWithDaysAndDate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validTransitionRule()
	rule.Transitions[0].Date = ptrutils.ToPtr(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "MalformedXML", err.Code)
}

func TestValidateLifecycleRejectsTransitionWithoutDaysOrDate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validTransitionRule()
	rule.Transitions[0].Days = nil
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "MalformedXML", err.Code)
}

func TestValidateLifecycleRejectsTransitionWithNegativeDays(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validTransitionRule()
	rule.Transitions[0].Days = ptrutils.ToPtr(int32(-1))
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestValidateLifecycleRejectsTransitionWithNonMidnightDate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validTransitionRule()
	rule.Transitions[0].Days = nil
	rule.Transitions[0].Date = ptrutils.ToPtr(time.Date(2030, 1, 1, 10, 30, 0, 0, time.UTC))
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestValidateLifecycleRejectsTransitionWithInvalidStorageClass(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validTransitionRule()
	rule.Transitions[0].StorageClass = "FROZEN_SOLID"
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestValidateLifecycleRejectsTransitionToStandard(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validTransitionRule()
	rule.Transitions[0].StorageClass = StorageClassStandard
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestValidateLifecycleRejectsDuplicateTransitionTargets(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validTransitionRule()
	rule.Transitions = append(rule.Transitions, LifecycleTransition{Days: ptrutils.ToPtr(int32(60)), StorageClass: "GLACIER"})
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestValidateLifecycleRejectsTransitionDaysNotBeforeExpirationDays(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := validTransitionRule()
	rule.Expiration = &LifecycleExpiration{Days: ptrutils.ToPtr(int32(30))}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestLifecycleTransitionDueTimeRoundsToNextMidnightUTC(t *testing.T) {
	testutils.SkipIfIntegration(t)

	transition := LifecycleTransition{Days: ptrutils.ToPtr(int32(3)), StorageClass: "GLACIER"}
	created := time.Date(2014, 1, 15, 10, 30, 0, 0, time.UTC)
	due := LifecycleTransitionDueTime(&transition, created)
	require.NotNil(t, due)
	assert.Equal(t, time.Date(2014, 1, 19, 0, 0, 0, 0, time.UTC), *due)
}

func TestLifecycleTransitionDueTimeUsesDateVerbatim(t *testing.T) {
	testutils.SkipIfIntegration(t)

	date := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	transition := LifecycleTransition{Date: &date, StorageClass: "GLACIER"}
	due := LifecycleTransitionDueTime(&transition, time.Now())
	require.NotNil(t, due)
	assert.Equal(t, date, *due)
}

func TestValidateLifecycleAcceptsNoncurrentVersionExpiration(t *testing.T) {
	testutils.SkipIfIntegration(t)

	config := &BucketLifecycleConfiguration{Rules: []LifecycleRule{{
		Status: LifecycleRuleStatusEnabled,
		Filter: &LifecycleFilter{Prefix: ptrutils.ToPtr("logs/")},
		NoncurrentVersionExpiration: &LifecycleNoncurrentVersionExpiration{
			NoncurrentDays:          ptrutils.ToPtr(int32(30)),
			NewerNoncurrentVersions: ptrutils.ToPtr(int32(2)),
		},
	}}}
	require.Nil(t, ValidateBucketLifecycleConfiguration(config))
}

func TestValidateLifecycleRejectsInvalidNoncurrentVersionExpiration(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := LifecycleRule{
		Status: LifecycleRuleStatusEnabled,
		Filter: &LifecycleFilter{},
		NoncurrentVersionExpiration: &LifecycleNoncurrentVersionExpiration{
			NoncurrentDays: ptrutils.ToPtr(int32(0)),
		},
	}
	err := ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)

	rule.NoncurrentVersionExpiration = &LifecycleNoncurrentVersionExpiration{
		NewerNoncurrentVersions: ptrutils.ToPtr(int32(-1)),
	}
	err = ValidateBucketLifecycleConfiguration(&BucketLifecycleConfiguration{Rules: []LifecycleRule{rule}})
	require.NotNil(t, err)
	assert.Equal(t, "InvalidArgument", err.Code)
}

func TestLifecycleNoncurrentExpirationDueTimeRoundsToNextMidnightUTC(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rule := LifecycleRule{
		NoncurrentVersionExpiration: &LifecycleNoncurrentVersionExpiration{NoncurrentDays: ptrutils.ToPtr(int32(3))},
	}
	created := time.Date(2014, 1, 15, 10, 30, 0, 0, time.UTC)
	due := LifecycleNoncurrentExpirationDueTime(&rule, created)
	require.NotNil(t, due)
	assert.Equal(t, time.Date(2014, 1, 19, 0, 0, 0, 0, time.UTC), *due)
}
