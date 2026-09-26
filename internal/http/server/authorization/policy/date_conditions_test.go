package policy

import (
	"context"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestDateConditionsPreserveRangeAndPrecision(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		name, actual, expected string
		less, equal            bool
	}{
		{"far future", "2500-01-01T00:00:00Z", "2030-01-01T00:00:00Z", false, false},
		{"far past", "0001-01-01T00:00:00Z", "2030-01-01T00:00:00Z", true, false},
		{"nanosecond before", "2030-01-01T00:00:00.000000001Z", "2030-01-01T00:00:00.000000002Z", true, false},
		{"nanosecond after", "2030-01-01T00:00:00.000000002Z", "2030-01-01T00:00:00.000000001Z", false, false},
		{"same instant different offset", "2500-01-01T01:00:00+01:00", "2500-01-01T00:00:00Z", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for operator, want := range map[string]bool{
				"DateEquals": tc.equal, "DateNotEquals": !tc.equal,
				"DateLessThan": tc.less, "DateLessThanEquals": tc.less || tc.equal,
				"DateGreaterThan": !tc.less && !tc.equal, "DateGreaterThanEquals": !tc.less,
			} {
				t.Run(operator, func(t *testing.T) {
					got, err := compareCondition(condition{operator: operator, values: []string{tc.expected}}, []string{tc.actual}, true)
					require.NoError(t, err)
					require.Equal(t, want, got)
				})
			}
		})
	}
}

func TestRemainingRetentionDaysPreservesRangeAndRounding(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		name, now, until string
		want             int64
	}{
		{"full date range", "0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999999999Z", 3652059},
		{"full negative range", "9999-12-31T23:59:59.999999999Z", "0001-01-01T00:00:00Z", -3652058},
		{"exact day", "2030-01-01T00:00:00.000000001Z", "2030-01-02T00:00:00.000000001Z", 1},
		{"nanosecond beyond day", "2030-01-01T00:00:00.000000001Z", "2030-01-02T00:00:00.000000002Z", 2},
		{"nanosecond before day", "2030-01-01T00:00:00.000000002Z", "2030-01-02T00:00:00.000000001Z", 1},
		{"partial day", "2030-01-01T00:00:00Z", "2030-01-01T12:00:00Z", 1},
		{"expired partial day", "2030-01-01T12:00:00Z", "2030-01-01T00:00:00Z", 0},
		{"expired exact day", "2030-01-02T00:00:00Z", "2030-01-01T00:00:00Z", -1},
		{"expired just under day", "2030-01-02T00:00:00Z", "2030-01-01T00:00:00.000000001Z", 0},
		{"equal with offset", "2030-01-01T01:00:00+01:00", "2030-01-01T00:00:00Z", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, err := time.Parse(time.RFC3339Nano, tc.now)
			require.NoError(t, err)
			until, err := time.Parse(time.RFC3339Nano, tc.until)
			require.NoError(t, err)
			require.Equal(t, tc.want, remainingRetentionDays(until, now))
		})
	}
}

func TestNumericRetentionLimitBeyondDurationRange(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		name, statements string
		want             authorization.Effect
	}{
		{"conditional allow", `{"Effect":"Allow","Action":"s3:PutObjectRetention","Resource":"*","Condition":{"NumericLessThanEquals":{"s3:object-lock-remaining-retention-days":"110000"}}}`, authorization.ImplicitDeny},
		{"explicit deny", `[{"Effect":"Allow","Action":"s3:PutObjectRetention","Resource":"*"},{"Effect":"Deny","Action":"s3:PutObjectRetention","Resource":"*","Condition":{"NumericGreaterThan":{"s3:object-lock-remaining-retention-days":"110000"}}}]`, authorization.ExplicitDeny},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := compileTestPolicy(t, tc.statements)
			r := request(authorization.OperationPutObjectRetention, "bucket", "key")
			r.ObjectLockRetainUntilDate = stringPointer("9999-12-31T23:59:59Z")
			d, err := s.AuthorizeRequest(context.Background(), r)
			require.NoError(t, err)
			require.Equal(t, tc.want, d.Effect)
		})
	}
}

func TestDateDenyEnforcesRetentionLimitBeyondUnixNanoRange(t *testing.T) {
	testutils.SkipIfIntegration(t)
	s := compileTestPolicy(t, `[{"Effect":"Allow","Action":"s3:PutObjectRetention","Resource":"*"},{"Effect":"Deny","Action":"s3:PutObjectRetention","Resource":"*","Condition":{"DateGreaterThan":{"s3:object-lock-retain-until-date":"2030-01-01T00:00:00Z"}}}]`)
	r := request(authorization.OperationPutObjectRetention, "bucket", "key")
	r.ObjectLockRetainUntilDate = stringPointer("2500-01-01T00:00:00Z")
	d, err := s.AuthorizeRequest(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, authorization.ExplicitDeny, d.Effect)
}
