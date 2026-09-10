package serialization

import (
	"bytes"
	"encoding/json"
	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
	"time"
)

func TestObjectLockV4RoundTripAndHashCoverage(t *testing.T) {
	days, years := int32(2), int32(3)
	lock := &auditlog.ObjectLockDetails{Requested: auditlog.ObjectLockValues{Enabled: "Enabled", Mode: "GOVERNANCE", RetainUntilDate: "2027-01-01T00:00:00Z", LegalHold: "ON", Days: &days, Years: &years}, Effective: auditlog.ObjectLockValues{Enabled: "Enabled", Mode: "COMPLIANCE", RetainUntilDate: "2028-01-01T00:00:00Z", LegalHold: "OFF", Days: &days, Years: &years}, BypassRequested: true, BypassAuthorized: true, BypassUsed: true}
	entry := &auditlog.Entry{Version: 4, Timestamp: time.Now().UTC(), Type: auditlog.EntryTypeLog, Details: &auditlog.LogDetails{Operation: auditlog.OpPutObjectRetention, Phase: auditlog.PhaseComplete, Resource: auditlog.ResourceDetails{Bucket: "bucket", Key: "key", VersionID: "version", SourceBucket: "source", SourceKey: "source-key"}, ObjectLock: lock}, PreviousHash: make([]byte, 64), SignatureEd25519: make([]byte, 64)}
	entry.Hash = entry.CalculateHash()
	for _, serializer := range []Serializer{&BinarySerializer{}, &JsonSerializer{}, &TextSerializer{}} {
		var buffer bytes.Buffer
		require.NoError(t, serializer.Encode(&buffer, entry))
		decoded, err := serializer.NewDecoder(&buffer).Decode()
		require.NoError(t, err)
		require.Equal(t, entry.Details, decoded.Details)
		require.Equal(t, entry.Hash, decoded.CalculateHash())
	}
	original, _ := json.Marshal(lock)
	var mutate func(reflect.Value, func())
	mutate = func(value reflect.Value, check func()) {
		if value.Kind() == reflect.Pointer {
			mutate(value.Elem(), check)
			return
		}
		if value.Kind() == reflect.Struct {
			for i := 0; i < value.NumField(); i++ {
				mutate(value.Field(i), check)
			}
			return
		}
		switch value.Kind() {
		case reflect.String:
			old := value.String()
			value.SetString(old + "tamper")
			check()
			value.SetString(old)
		case reflect.Bool:
			old := value.Bool()
			value.SetBool(!old)
			check()
			value.SetBool(old)
		case reflect.Int32:
			old := value.Int()
			value.SetInt(old + 1)
			check()
			value.SetInt(old)
		default:
			t.Fatalf("uncovered audit field: %s", value.Kind())
		}
	}
	mutate(reflect.ValueOf(lock), func() { require.NotEqual(t, entry.Hash, entry.CalculateHash(), "every new field must change the hash") })
	after, _ := json.Marshal(lock)
	require.Equal(t, original, after)
	details := entry.Details.(*auditlog.LogDetails)
	for _, field := range []*string{&details.Resource.VersionID, &details.Resource.SourceBucket, &details.Resource.SourceKey} {
		old := *field
		*field += "tamper"
		require.NotEqual(t, entry.Hash, entry.CalculateHash())
		*field = old
	}
	// New fields never alter the historical v1-v3 hash calculation.
	for _, version := range []uint16{1, 2, 3} {
		entry.Version = version
		before := entry.CalculateHash()
		details.ObjectLock = nil
		details.Resource.VersionID = "different"
		require.Equal(t, before, entry.CalculateHash())
		details.ObjectLock = lock
		details.Resource.VersionID = "version"
	}
}
