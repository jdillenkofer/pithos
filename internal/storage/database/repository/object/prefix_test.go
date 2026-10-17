package object

import (
	"strings"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestPrefixUpperBound(t *testing.T) {
	testutils.SkipIfIntegration(t)

	for _, tc := range []struct {
		prefix string
		upper  string
	}{
		{prefix: "", upper: ""},
		{prefix: "logs/", upper: "logs0"},
		{prefix: "logs%", upper: "logs&"},
		{prefix: "logs_", upper: "logs`"},
		{prefix: "ö", upper: "÷"},
		{prefix: "\u007f", upper: "\u0080"},
		{prefix: "\u07ff", upper: "\u0800"},
		{prefix: "\ud7ff", upper: "\ue000"},
		{prefix: "\uffff", upper: "\U00010000"},
		{prefix: "a\U0010ffff", upper: "b"},
		{prefix: "a\U0010ffff\U0010ffff", upper: "b"},
		{prefix: "\U0010ffff", upper: ""},
		{prefix: "\U0010ffff\U0010ffff", upper: ""},
		{prefix: "a\x00", upper: "a\x01"},
		{prefix: "\x80", upper: "\x80"},
	} {
		t.Run(tc.prefix, func(t *testing.T) {
			assert.Equal(t, tc.upper, PrefixUpperBound(tc.prefix))
		})
	}
}

func TestPrefixRangeMatchesLiteralPrefix(t *testing.T) {
	testutils.SkipIfIntegration(t)

	values := []string{
		"", "A", "a", "a/", "a%", "a_", "a\\", "a[", "a*", "a?", "a\x00",
		"a\u007f", "a\u0080", "aö", "a界", "a\ud7ff", "a\ue000", "a\uffff",
		"a\U00010000", "a\U0010ffff", "a\U0010ffff\U0010ffff", "b", "ö", "Ö", "界",
		"\U0010ffff", "\U0010ffff\U0010ffff",
	}
	for _, prefix := range values {
		upper := PrefixUpperBound(prefix)
		for _, key := range values {
			inRange := key >= prefix && (upper == "" || key < upper)
			assert.Equal(t, strings.HasPrefix(key, prefix), inRange, "prefix %q, key %q, upper %q", prefix, key, upper)
		}
	}
}
