package metadatastore

import (
	"errors"
	"testing"
	"time"
)

func TestObjectLockDeleteProtection(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	for _, mode := range []RetentionMode{"", RetentionModeGovernance, RetentionModeCompliance} {
		for _, delta := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
			for _, hold := range []LegalHoldStatus{LegalHoldOff, LegalHoldOn} {
				for _, bypass := range []bool{false, true} {
					lock := ObjectLock{LegalHold: &hold}
					if mode != "" {
						lock.Retention = &ObjectRetention{Mode: mode, RetainUntilDate: now.Add(delta)}
					}
					used, err := lock.CheckDelete(now, bypass)
					active := mode != "" && delta > 0
					denied := hold == LegalHoldOn || active && (mode == RetentionModeCompliance || !bypass)
					wantUsed := !denied && active && bypass
					if denied != errors.Is(err, ErrObjectLockAccessDenied) || used != wantUsed {
						t.Errorf("mode=%q delta=%v hold=%s bypass=%v: used=%v err=%v; want denied=%v used=%v", mode, delta, hold, bypass, used, err, denied, wantUsed)
					}
				}
			}
		}
	}
}

func TestObjectLockRetentionChanges(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	until := now.Add(time.Hour)
	for _, mode := range []RetentionMode{RetentionModeGovernance, RetentionModeCompliance} {
		for _, nextMode := range []RetentionMode{"", RetentionModeGovernance, RetentionModeCompliance} {
			for _, delta := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
				for _, bypass := range []bool{false, true} {
					for _, expired := range []bool{false, true} {
						hold := LegalHoldOn
						lock := ObjectLock{Retention: &ObjectRetention{Mode: mode, RetainUntilDate: until}, LegalHold: &hold}
						var next *ObjectRetention
						if nextMode != "" {
							next = &ObjectRetention{Mode: nextMode, RetainUntilDate: until.Add(delta)}
						}
						at := now
						if expired {
							at = until
						}
						used, err := lock.CheckRetentionChange(next, at, bypass)
						needsBypass := !expired && (nextMode != mode || delta < 0)
						denied := needsBypass && (mode == RetentionModeCompliance || !bypass)
						if denied != errors.Is(err, ErrObjectLockAccessDenied) || used != (needsBypass && !denied) {
							t.Errorf("mode=%s next=%s delta=%v bypass=%v expired=%v: used=%v err=%v", mode, nextMode, delta, bypass, expired, used, err)
						}
					}
				}
			}
		}
	}
}

func TestDefaultRetentionValidation(t *testing.T) {
	positive, zero, negative := int32(1), int32(0), int32(-1)
	for _, tc := range []struct {
		name  string
		value *DefaultRetention
		valid bool
	}{
		{"absent", nil, true},
		{"days", &DefaultRetention{Mode: RetentionModeGovernance, Days: &positive}, true},
		{"years", &DefaultRetention{Mode: RetentionModeCompliance, Years: &positive}, true},
		{"both", &DefaultRetention{Mode: RetentionModeGovernance, Days: &positive, Years: &positive}, false},
		{"neither", &DefaultRetention{Mode: RetentionModeGovernance}, false},
		{"zero days", &DefaultRetention{Mode: RetentionModeGovernance, Days: &zero}, false},
		{"negative years", &DefaultRetention{Mode: RetentionModeGovernance, Years: &negative}, false},
		{"missing mode", &DefaultRetention{Days: &positive}, false},
		{"lowercase mode", &DefaultRetention{Mode: "governance", Days: &positive}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.value.Validate(); (err == nil) != tc.valid {
				t.Fatalf("Validate() = %v; valid=%v", err, tc.valid)
			}
		})
	}
	for _, config := range []*ObjectLockConfiguration{nil, {}, {ObjectLockEnabled: "Disabled"}, {ObjectLockEnabled: "enabled"}} {
		if err := config.Validate(); !errors.Is(err, ErrInvalidObjectLockConfiguration) {
			t.Errorf("configuration %#v: %v", config, err)
		}
	}
	if err := (&ObjectLockConfiguration{ObjectLockEnabled: "Enabled"}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestEffectiveObjectLock(t *testing.T) {
	// Calculate in UTC even when the caller supplies a non-UTC timestamp.
	now := time.Date(2026, 3, 28, 12, 0, 0, 123, time.FixedZone("offset", 3600))
	days := int32(2)
	config := &ObjectLockConfiguration{ObjectLockEnabled: "Enabled", DefaultRetention: &DefaultRetention{Mode: RetentionModeGovernance, Days: &days}}
	hold := LegalHoldOn
	got, err := EffectiveObjectLock(config, ObjectLock{LegalHold: &hold}, now)
	if err != nil {
		t.Fatal(err)
	}
	if got.Retention.Mode != RetentionModeGovernance || !got.Retention.RetainUntilDate.Equal(now.Add(48*time.Hour).Truncate(time.Microsecond)) || got.Retention.RetainUntilDate.Location() != time.UTC {
		t.Fatalf("incorrect default retention: %#v", got.Retention)
	}
	if got.LegalHold == &hold || *got.LegalHold != LegalHoldOn {
		t.Fatal("legal hold must be preserved without aliasing the request")
	}
	// Explicit retention is preserved, including expired protection being
	// reconciled from a primary. It must not acquire a fresh default duration.
	explicit := &ObjectRetention{Mode: RetentionModeCompliance, RetainUntilDate: now.Add(-time.Hour)}
	got, err = EffectiveObjectLock(config, ObjectLock{Retention: explicit}, now)
	if err != nil || got.Retention == explicit || !got.Retention.RetainUntilDate.Equal(explicit.RetainUntilDate.Truncate(time.Microsecond)) || got.Retention.Mode != explicit.Mode {
		t.Fatalf("explicit retention: %#v, %v", got, err)
	}
	if _, err := EffectiveObjectLock(nil, ObjectLock{LegalHold: &hold}, now); !errors.Is(err, ErrObjectLockConfigurationNotFound) {
		t.Fatalf("lock on disabled bucket: %v", err)
	}
	if got, err := EffectiveObjectLock(nil, ObjectLock{}, now); err != nil || got.Retention != nil || got.LegalHold != nil {
		t.Fatalf("ordinary bucket: %#v, %v", got, err)
	}
	years := int32(1)
	config.DefaultRetention = &DefaultRetention{Mode: RetentionModeCompliance, Years: &years}
	got, err = EffectiveObjectLock(config, ObjectLock{}, now)
	if err != nil || !got.Retention.RetainUntilDate.Equal(now.UTC().AddDate(1, 0, 0).Truncate(time.Microsecond)) {
		t.Fatalf("year default: %#v, %v", got, err)
	}
	years = 2147483647
	if _, err := EffectiveObjectLock(config, ObjectLock{}, now); !errors.Is(err, ErrInvalidObjectLockConfiguration) {
		t.Fatalf("unrepresentable expiry: %v", err)
	}
}

func TestObjectLockMalformedMetadataFailsClosed(t *testing.T) {
	now := time.Now()
	invalidHold := LegalHoldStatus("invalid")
	for _, lock := range []ObjectLock{
		{Retention: &ObjectRetention{}},
		{Retention: &ObjectRetention{Mode: "invalid", RetainUntilDate: now}},
		{Retention: &ObjectRetention{Mode: RetentionModeCompliance}},
		{LegalHold: &invalidHold},
	} {
		for _, bypass := range []bool{false, true} {
			if used, err := lock.CheckDelete(now, bypass); err == nil || used {
				t.Errorf("malformed protection permitted deletion: %#v", lock)
			}
			if used, err := lock.CheckRetentionChange(nil, now, bypass); err == nil || used {
				t.Errorf("malformed protection permitted removal: %#v", lock)
			}
		}
	}
}
