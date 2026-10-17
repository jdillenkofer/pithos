package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetermineCommonPrefix(t *testing.T) {
	for _, tc := range []struct {
		name           string
		prefix         string
		key            string
		delimiter      string
		expectedPrefix string
	}{
		{name: "root", key: "logs/app/file", delimiter: "/", expectedPrefix: "logs/"},
		{name: "nested prefix", prefix: "logs/", key: "logs/app/file", delimiter: "/", expectedPrefix: "logs/app/"},
		{name: "partial segment", prefix: "log", key: "logs/app/file", delimiter: "/", expectedPrefix: "logs/"},
		{name: "direct key", prefix: "logs/", key: "logs/file", delimiter: "/"},
		{name: "exact key", prefix: "logs/", key: "logs/", delimiter: "/"},
		{name: "unrelated prefix", prefix: "other/", key: "logs/app/file", delimiter: "/"},
		{name: "empty delimiter", key: "logs/app/file"},
		{name: "delimiter across boundary", prefix: "a:", key: "a::b", delimiter: "::"},
		{name: "delimiter after boundary", prefix: "a:", key: "a::b::c", delimiter: "::", expectedPrefix: "a::b::"},
		{name: "overlapping delimiter", prefix: "a:", key: "a:::b", delimiter: "::", expectedPrefix: "a:::"},
		{name: "multibyte delimiter across boundary", prefix: "a界", key: "a界界b", delimiter: "界界"},
		{name: "multibyte prefix and delimiter", prefix: "ö/", key: "ö/子界界file", delimiter: "界界", expectedPrefix: "ö/子界界"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := determineCommonPrefix(tc.prefix, tc.key, tc.delimiter)
			if tc.expectedPrefix == "" {
				assert.Nil(t, result)
				return
			}
			require.NotNil(t, result)
			assert.Equal(t, tc.expectedPrefix, *result)
		})
	}
}
