package server

import (
	"context"
	"encoding/xml"
	"net/http/httptest"
	"net/url"
	"slices"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type objectPaginationStorage struct {
	accountStorage
	calls   int
	markers []string
}

func (s *objectPaginationStorage) ListObjects(_ context.Context, _ storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	s.calls++
	marker := ""
	if opts.StartAfter != nil {
		marker = *opts.StartAfter
	}
	s.markers = append(s.markers, marker)
	// Mixed pages must paginate by combined order, not objects followed by prefixes.
	keys := []string{"a/", "b", "c/", "d"}
	result := &storage.ListBucketResult{}
	count := int32(0)
	for _, key := range keys {
		if key <= marker {
			continue
		}
		if count == opts.MaxKeys {
			result.IsTruncated = true
			break
		}
		if key == "a/" || key == "c/" {
			result.CommonPrefixes = append(result.CommonPrefixes, key)
		} else {
			result.Objects = append(result.Objects, storage.Object{Key: storage.MustNewObjectKey(key)})
		}
		count++
	}
	return result, nil
}

func TestObjectListingHTTPPagination(t *testing.T) {
	authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(r) return true end`)
	require.NoError(t, err)
	for _, v2 := range []bool{false, true} {
		for _, size := range []string{"0", "1", "2", "4"} {
			t.Run(size+map[bool]string{false: "-v1", true: "-v2"}[v2], func(t *testing.T) {
				store := &objectPaginationStorage{}
				handler := SetupServer(nil, "us-east-1", "s3.test", "website.test", authorizer, store)
				marker := ""
				got := []string{}
				for page := 0; ; page++ {
					require.Less(t, page, 5)
					query := url.Values{"delimiter": {"/"}, "max-keys": {size}}
					if v2 {
						query.Set("list-type", "2")
						query.Set("encoding-type", "url")
						if marker != "" {
							query.Set("continuation-token", marker)
						}
					} else if marker != "" {
						query.Set("marker", marker)
					}
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, httptest.NewRequest("GET", "http://s3.test/bucket?"+query.Encode(), nil))
					require.Equal(t, 200, w.Code, w.Body.String())
					var result struct {
						IsTruncated           bool
						KeyCount              int
						NextMarker            string
						NextContinuationToken string
						Contents              []struct{ Key string }
						CommonPrefixes        []struct{ Prefix string }
					}
					require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &result))
					entries := []string{}
					for _, obj := range result.Contents {
						entries = append(entries, obj.Key)
					}
					for _, group := range result.CommonPrefixes {
						key := group.Prefix
						if v2 {
							key, err = url.QueryUnescape(key)
							require.NoError(t, err)
						}
						entries = append(entries, key)
					}
					if v2 {
						require.Equal(t, len(entries), result.KeyCount)
					}
					// With these fixtures each two-entry page is already in combined order
					// after sorting, regardless of XML element order.
					slices.Sort(entries)
					got = append(got, entries...)
					if !result.IsTruncated {
						require.Empty(t, result.NextMarker)
						require.Empty(t, result.NextContinuationToken)
						break
					}
					require.NotEmpty(t, entries)
					marker = result.NextMarker
					if v2 {
						marker = result.NextContinuationToken
					}
					require.Equal(t, entries[len(entries)-1], marker)
				}
				if size == "0" {
					require.Empty(t, got)
					require.Zero(t, store.calls)
				} else {
					require.Equal(t, []string{"a/", "b", "c/", "d"}, got)
				}
			})
		}
	}
}
