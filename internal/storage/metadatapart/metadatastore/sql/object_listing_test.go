package sql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/tag"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

// Embedding the interface deliberately leaves count and unbounded fetch methods
// unimplemented: calling either makes this regression test fail.
type seekingObjectRepository struct {
	object.Repository
	keys     []string
	minimums []string
	fetched  int
	limits   []int32
}

func (r *seekingObjectRepository) FindObjectsFromKey(_ context.Context, _ *sql.Tx, _ storage.BucketName, prefix, startAfter, minimumKey string, limit int32) ([]object.Entity, error) {
	r.minimums = append(r.minimums, minimumKey)
	r.limits = append(r.limits, limit)
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= prefix && r.keys[i] > startAfter && r.keys[i] >= minimumKey })
	upper := object.PrefixUpperBound(prefix)
	entities := []object.Entity{}
	for ; i < len(r.keys) && len(entities) < int(limit); i++ {
		if upper != "" && r.keys[i] >= upper {
			break
		}
		id := ulid.Make()
		entities = append(entities, object.Entity{Id: &id, Key: storage.MustNewObjectKey(r.keys[i])})
	}
	r.fetched += len(entities)
	return entities, nil
}

type listingTagRepository struct{ tag.Repository }

func (r *listingTagRepository) FindTagsByObjectIdsOrderByObjectIdAndKeyAsc(context.Context, *sql.Tx, []ulid.ULID) ([]tag.Entity, error) {
	return nil, nil
}

func TestDelimiterListingSeeksPastHugeSubtree(t *testing.T) {
	for _, descendants := range []int{1, 100000} {
		t.Run(fmt.Sprint(descendants), func(t *testing.T) {
			repo := &seekingObjectRepository{}
			for i := range descendants {
				repo.keys = append(repo.keys, fmt.Sprintf("a/%06d", i))
			}
			// The successor itself must not be skipped by an exclusive seek.
			repo.keys = append(repo.keys, "a0", "z")
			store := &sqlMetadataStore{objectRepository: repo, tagRepository: &listingTagRepository{}}
			delimiter := "/"
			result, err := store.listObjects(t.Context(), nil, storage.MustNewBucketName("bucket"), metadatastore.ListObjectsOptions{Delimiter: &delimiter, MaxKeys: 1})
			require.NoError(t, err)
			require.Equal(t, []string{"a/"}, result.CommonPrefixes)
			require.Empty(t, result.Objects)
			require.True(t, result.IsTruncated)
			if descendants == 1 {
				require.Equal(t, []string{""}, repo.minimums)
				require.Equal(t, 2, repo.fetched)
			} else {
				require.Equal(t, []string{"", "a0"}, repo.minimums)
				require.Equal(t, 3, repo.fetched)
			}

			repo.minimums = nil
			marker := "a/050000"
			result, err = store.listObjects(t.Context(), nil, storage.MustNewBucketName("bucket"), metadatastore.ListObjectsOptions{Delimiter: &delimiter, StartAfter: &marker, MaxKeys: 1})
			require.NoError(t, err)
			require.Empty(t, result.CommonPrefixes)
			require.Len(t, result.Objects, 1)
			require.Equal(t, "a0", result.Objects[0].Key.String())
			require.True(t, result.IsTruncated)
			require.Equal(t, []string{"a0"}, repo.minimums)
		})
	}
}

func TestDelimiterListingBatchesDirectObjects(t *testing.T) {
	for _, count := range []int{0, 63, 64, 65, 999, 1000, 1001, 100000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			repo := &seekingObjectRepository{}
			for i := range count {
				repo.keys = append(repo.keys, fmt.Sprintf("key%06d", i))
			}
			store := &sqlMetadataStore{objectRepository: repo, tagRepository: &listingTagRepository{}}
			delimiter := "/"
			result, err := store.listObjects(t.Context(), nil, storage.MustNewBucketName("bucket"), metadatastore.ListObjectsOptions{Delimiter: &delimiter, MaxKeys: 1000})
			require.NoError(t, err)
			require.Len(t, result.Objects, min(count, 1000))
			require.Empty(t, result.CommonPrefixes)
			require.Equal(t, count > 1000, result.IsTruncated)
			// Batches double from 2 to 64: 5 ramp-up queries, then 64 rows each.
			require.LessOrEqual(t, len(repo.minimums), 20)
			require.Equal(t, min(count, 1001), repo.fetched)
			for _, limit := range repo.limits {
				require.LessOrEqual(t, limit, int32(64))
			}
		})
	}
}

func TestDelimiterListingBatchBoundaries(t *testing.T) {
	for _, directCount := range []int{0, 63, 64, 65} {
		for _, descendantCount := range []int{1, 64, 65, 500} {
			for _, pageSize := range []int32{1, 2, 63, 64, 65, 128} {
				t.Run(fmt.Sprintf("direct=%d/descendants=%d/page=%d", directCount, descendantCount, pageSize), func(t *testing.T) {
					repo := &seekingObjectRepository{}
					expected := []string{}
					for _, prefix := range []string{"a", "c"} {
						for i := range directCount {
							key := fmt.Sprintf("%s%06d", prefix, i)
							repo.keys = append(repo.keys, key)
							expected = append(expected, key)
						}
					}
					for _, group := range []string{"b/", "d/"} {
						expected = append(expected, group)
						for i := range descendantCount {
							repo.keys = append(repo.keys, fmt.Sprintf("%s%06d", group, i))
						}
					}
					repo.keys = append(repo.keys, "e")
					expected = append(expected, "e")
					sort.Strings(repo.keys)
					sort.Strings(expected)
					store := &sqlMetadataStore{objectRepository: repo, tagRepository: &listingTagRepository{}}
					delimiter, marker := "/", ""
					got := []string{}
					for page := 0; ; page++ {
						require.Less(t, page, len(expected)+1)
						result, err := store.listObjects(t.Context(), nil, storage.MustNewBucketName("bucket"), metadatastore.ListObjectsOptions{Delimiter: &delimiter, StartAfter: &marker, MaxKeys: pageSize})
						require.NoError(t, err)
						entries := append([]string{}, result.CommonPrefixes...)
						for _, entity := range result.Objects {
							entries = append(entries, entity.Key.String())
						}
						sort.Strings(entries)
						require.LessOrEqual(t, len(entries), int(pageSize))
						got = append(got, entries...)
						require.Equal(t, len(got) < len(expected), result.IsTruncated)
						if !result.IsTruncated {
							break
						}
						require.NotEmpty(t, entries)
						marker = entries[len(entries)-1]
					}
					require.Equal(t, expected, got)
				})
			}
		}
	}
}
