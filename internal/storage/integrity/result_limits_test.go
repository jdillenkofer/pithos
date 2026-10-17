package integrity

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestRetainResultBoundsFailureDetails(t *testing.T) {
	v := NewValidator(nil, false, false).WithResultLimits(ResultLimits{MaxResults: 1, MaxFailuresPerResult: 3, MaxErrorLength: 20})
	report := &ValidationReport{}
	long := strings.Repeat("é", 40)
	result := ValidationResult{
		ErrorType:      long,
		PartFailures:   []PartFailure{{Error: long}, {Error: "b"}},
		ObjectFailures: []string{long, "y", "z"},
	}
	v.retainResult(report, result)
	v.retainResult(report, ValidationResult{ErrorType: "second"})
	require.Len(t, report.Results, 1)
	require.Equal(t, 1, report.OmittedResults)
	retained := report.Results[0]
	require.Len(t, retained.PartFailures, 2)
	require.Len(t, retained.ObjectFailures, 1)
	require.Equal(t, 2, retained.OmittedFailures)
	for _, message := range []string{retained.ErrorType, retained.PartFailures[0].Error, retained.ObjectFailures[0]} {
		require.LessOrEqual(t, len(message), 20)
		require.True(t, utf8.ValidString(message))
	}
	require.Equal(t, strings.Repeat("é", 40), long, "the caller's data must not be modified")

	successOnly := NewValidator(nil, false, false).WithResultLimits(ResultLimits{FailuresOnly: true})
	report = &ValidationReport{}
	successOnly.retainResult(report, ValidationResult{Success: true})
	require.Empty(t, report.Results)
	require.Zero(t, report.OmittedResults)
}

func TestTruncateErrorKeepsValidUTF8(t *testing.T) {
	require.Equal(t, "short", truncateError("short", 10))
	require.Equal(t, "é", truncateError("éé", 3))
	truncated := truncateError(strings.Repeat("é", 20), len(truncationMarker)+3)
	require.Equal(t, "é"+truncationMarker, truncated)
}
