package metadatapart

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestDelimiterListingPagination(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, backend := range []struct {
		name string
		new  func(*testing.T) (*metadataPartStorage, func())
	}{{"sqlite", newTestStorage}, {"postgres", newPostgresTestStorage}} {
		t.Run(backend.name, func(t *testing.T) {
			if backend.name == "postgres" {
				testutils.SkipOnWindowsInGitHubActions(t)
				testutils.SkipOnMacOSInGitHubActions(t)
			}
			st, cleanup := backend.new(t)
			defer cleanup()
			ctx := t.Context()
			bucket := storage.MustNewBucketName("bucket")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			repo, err := repositoryfactory.NewObjectRepository(st.db)
			require.NoError(t, err)
			keys := []string{"a", "b/", "b/1", "b/2", "b0", "c", "d/1", "d/2", "e", "logs/a", "logs/b/c", "logs/b/d", "logs/z", "logs%/a", "logs%/b/c", "logs_/a", "logs_/b/c", "logs\\/a", "logs\\/b/c", "A/x", "a/x", "Ö/a", "ö/a", "界/a", "界/子", "x::a", "x::b::c", "x::b::d", "x::z", "ababa"}
			for i := range 2000 {
				keys = append(keys, fmt.Sprintf("b/huge/%06d", i))
			}
			slices.Sort(keys)
			require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{}, func(ctx context.Context, tx database.Tx) error {
				if backend.name == "postgres" {
					_, err := tx.SqlTx().ExecContext(ctx, `CREATE COLLATION delimiter_test_english (provider = icu, locale = 'en-US')`)
					require.NoError(t, err)
					_, err = tx.SqlTx().ExecContext(ctx, `ALTER TABLE objects ALTER COLUMN key TYPE TEXT COLLATE delimiter_test_english`)
					require.NoError(t, err)
				}
				for _, key := range keys {
					entity := object.Entity{BucketName: bucket, Key: storage.MustNewObjectKey(key), UploadStatus: object.UploadStatusCompleted, IsLatest: true}
					require.NoError(t, repo.SaveObject(ctx, tx.SqlTx(), &entity))
				}
				return nil
			}))
			for _, tc := range []struct{ prefix, delimiter string }{{"", "/"}, {"logs/", "/"}, {"logs%/", "/"}, {"logs_/", "/"}, {"logs\\/", "/"}, {"界/", "/"}, {"x::", "::"}, {"ab", "aba"}, {"missing/", "/"}} {
				for _, initialMarker := range []string{"", "b/", "b/1", "b/huge/000010", "logs/b/cc", "Ö/", "x::b::c"} {
					// Reference model groups before filtering: a marker inside a group
					// suppresses the whole group, not just its first descendant.
					expected := []string{}
					for _, key := range keys {
						suffix, ok := strings.CutPrefix(key, tc.prefix)
						if !ok {
							continue
						}
						logical := key
						if i := strings.Index(suffix, tc.delimiter); i >= 0 {
							logical = tc.prefix + suffix[:i+len(tc.delimiter)]
						}
						if logical > initialMarker && (len(expected) == 0 || expected[len(expected)-1] != logical) {
							expected = append(expected, logical)
						}
					}
					for _, size := range []int32{0, 1, 2, 3, 7, 1000} {
						t.Run(fmt.Sprintf("%s/%s/%d", tc.prefix, initialMarker, size), func(t *testing.T) {
							marker := initialMarker
							got := []string{}
							for page := 0; ; page++ {
								require.Less(t, page, len(expected)+2)
								var result *metadatastore.ListBucketResult
								require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{}, func(ctx context.Context, tx database.Tx) error {
									var err error
									result, err = st.metadataStore.ListObjects(ctx, tx.SqlTx(), bucket, metadatastore.ListObjectsOptions{Prefix: &tc.prefix, Delimiter: &tc.delimiter, StartAfter: &marker, MaxKeys: size, SkipPartFetch: true})
									return err
								}))
								entries := append([]string{}, result.CommonPrefixes...)
								for _, obj := range result.Objects {
									entries = append(entries, obj.Key.String())
								}
								slices.Sort(entries)
								require.LessOrEqual(t, len(entries), int(size))
								got = append(got, entries...)
								require.Equal(t, size > 0 && len(got) < len(expected), result.IsTruncated)
								if !result.IsTruncated {
									break
								}
								require.NotEmpty(t, entries)
								marker = entries[len(entries)-1]
							}
							if size == 0 {
								require.Empty(t, got)
							} else {
								require.Equal(t, expected, got)
							}
						})
					}
				}
			}
		})
	}
}

func TestDelimiterVersionListingPagination(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, backend := range []struct {
		name string
		new  func(*testing.T) (*metadataPartStorage, func())
	}{{"sqlite", newTestStorage}, {"postgres", newPostgresTestStorage}} {
		t.Run(backend.name, func(t *testing.T) {
			if backend.name == "postgres" {
				testutils.SkipOnWindowsInGitHubActions(t)
				testutils.SkipOnMacOSInGitHubActions(t)
			}
			st, cleanup := backend.new(t)
			defer cleanup()
			ctx := t.Context()
			bucket := storage.MustNewBucketName("bucket")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			repo, err := repositoryfactory.NewObjectRepository(st.db)
			require.NoError(t, err)
			keys := []string{"a", "b/", "b/1", "b/2", "b0", "c", "d/1", "d/2", "e", "logs/a", "logs/b/c", "logs/b/d", "logs/z", "x::a", "x::b::c", "x::z", "ababa"}
			for i := range 300 {
				keys = append(keys, fmt.Sprintf("b/huge/%06d", i))
			}
			slices.Sort(keys)
			// Every other key also gets an older "null" version, which sorts last
			// within its key.
			versionsByKey := map[string][]string{}
			require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{}, func(ctx context.Context, tx database.Tx) error {
				for i, key := range keys {
					versionIDs := []string{fmt.Sprintf("v%06d", i)}
					if i%2 == 0 {
						versionIDs = append(versionIDs, "null")
					}
					versionsByKey[key] = versionIDs
					for j, versionID := range versionIDs {
						entity := object.Entity{BucketName: bucket, Key: storage.MustNewObjectKey(key), UploadStatus: object.UploadStatusCompleted, VersionID: &versionID, IsLatest: j == 0}
						require.NoError(t, repo.SaveObject(ctx, tx.SqlTx(), &entity))
					}
				}
				return nil
			}))
			for _, tc := range []struct{ prefix, delimiter string }{{"", "/"}, {"logs/", "/"}, {"x::", "::"}, {"ab", "aba"}, {"missing/", "/"}} {
				for _, initialMarker := range []string{"", "b/", "b/1", "b/huge/000010", "logs/b/cc", "x::b::c"} {
					expected := []string{}
					lastGroup := ""
					for _, key := range keys {
						suffix, ok := strings.CutPrefix(key, tc.prefix)
						if !ok {
							continue
						}
						if i := strings.Index(suffix, tc.delimiter); i >= 0 {
							group := tc.prefix + suffix[:i+len(tc.delimiter)]
							if group > initialMarker && group != lastGroup {
								expected = append(expected, group)
								lastGroup = group
							}
							continue
						}
						if key > initialMarker {
							for _, versionID := range versionsByKey[key] {
								expected = append(expected, key+"#"+versionID)
							}
						}
					}
					slices.Sort(expected)
					for _, size := range []int32{1, 2, 3, 7, 1000} {
						t.Run(fmt.Sprintf("%s/%s/%d", tc.prefix, initialMarker, size), func(t *testing.T) {
							keyMarker := initialMarker
							var versionIDMarker *string
							got := []string{}
							for page := 0; ; page++ {
								require.Less(t, page, len(expected)+2)
								var result *metadatastore.ListObjectVersionsResult
								require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{}, func(ctx context.Context, tx database.Tx) error {
									var err error
									result, err = st.metadataStore.ListObjectVersions(ctx, tx.SqlTx(), bucket, metadatastore.ListObjectVersionsOptions{Prefix: &tc.prefix, Delimiter: &tc.delimiter, KeyMarker: &keyMarker, VersionIDMarker: versionIDMarker, MaxKeys: size})
									return err
								}))
								entries := append([]string{}, result.CommonPrefixes...)
								for _, version := range result.Versions {
									entries = append(entries, version.Key.String()+"#"+version.VersionID)
								}
								require.LessOrEqual(t, len(entries), int(size))
								got = append(got, entries...)
								require.Equal(t, len(got) < len(expected), result.IsTruncated)
								if !result.IsTruncated {
									break
								}
								require.NotNil(t, result.NextKeyMarker)
								keyMarker = *result.NextKeyMarker
								versionIDMarker = result.NextVersionIDMarker
							}
							slices.Sort(got)
							require.Equal(t, expected, got)
						})
					}
				}
			}
		})
	}
}

func TestDelimiterMultipartUploadListingPagination(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, backend := range []struct {
		name string
		new  func(*testing.T) (*metadataPartStorage, func())
	}{{"sqlite", newTestStorage}, {"postgres", newPostgresTestStorage}} {
		t.Run(backend.name, func(t *testing.T) {
			if backend.name == "postgres" {
				testutils.SkipOnWindowsInGitHubActions(t)
				testutils.SkipOnMacOSInGitHubActions(t)
			}
			st, cleanup := backend.new(t)
			defer cleanup()
			ctx := t.Context()
			bucket := storage.MustNewBucketName("bucket")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			repo, err := repositoryfactory.NewObjectRepository(st.db)
			require.NoError(t, err)
			keys := []string{"a", "b/", "b/1", "b/2", "b0", "c", "d/1", "d/2", "e", "logs/a", "logs/b/c", "logs/b/d", "logs/z", "x::a", "x::b::c", "x::z", "ababa"}
			for i := range 300 {
				keys = append(keys, fmt.Sprintf("b/huge/%06d", i))
			}
			slices.Sort(keys)
			// Every other key has two concurrent uploads.
			uploadIdsByKey := map[string][]string{}
			require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{}, func(ctx context.Context, tx database.Tx) error {
				for i, key := range keys {
					uploadIds := []string{fmt.Sprintf("u%06d-a", i)}
					if i%2 == 0 {
						uploadIds = append(uploadIds, fmt.Sprintf("u%06d-b", i))
					}
					uploadIdsByKey[key] = uploadIds
					for _, uploadId := range uploadIds {
						id := storage.MustNewUploadId(uploadId)
						entity := object.Entity{BucketName: bucket, Key: storage.MustNewObjectKey(key), UploadStatus: object.UploadStatusPending, UploadId: &id}
						require.NoError(t, repo.SaveObject(ctx, tx.SqlTx(), &entity))
					}
				}
				return nil
			}))
			for _, tc := range []struct{ prefix, delimiter string }{{"", "/"}, {"logs/", "/"}, {"x::", "::"}, {"ab", "aba"}, {"missing/", "/"}} {
				for _, initialMarker := range []string{"", "b/", "b/1", "b/huge/000010", "logs/b/cc", "x::b::c"} {
					expected := []string{}
					lastGroup := ""
					for _, key := range keys {
						suffix, ok := strings.CutPrefix(key, tc.prefix)
						if !ok {
							continue
						}
						if i := strings.Index(suffix, tc.delimiter); i >= 0 {
							group := tc.prefix + suffix[:i+len(tc.delimiter)]
							if group > initialMarker && group != lastGroup {
								expected = append(expected, group)
								lastGroup = group
							}
							continue
						}
						if key > initialMarker {
							for _, uploadId := range uploadIdsByKey[key] {
								expected = append(expected, key+"#"+uploadId)
							}
						}
					}
					slices.Sort(expected)
					for _, size := range []int32{1, 2, 3, 7, 1000} {
						t.Run(fmt.Sprintf("%s/%s/%d", tc.prefix, initialMarker, size), func(t *testing.T) {
							keyMarker := initialMarker
							uploadIdMarker := ""
							got := []string{}
							for page := 0; ; page++ {
								require.Less(t, page, len(expected)+2)
								var result *metadatastore.ListMultipartUploadsResult
								require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{}, func(ctx context.Context, tx database.Tx) error {
									var err error
									result, err = st.metadataStore.ListMultipartUploads(ctx, tx.SqlTx(), bucket, metadatastore.ListMultipartUploadsOptions{Prefix: &tc.prefix, Delimiter: &tc.delimiter, KeyMarker: &keyMarker, UploadIdMarker: &uploadIdMarker, MaxUploads: size})
									return err
								}))
								require.Equal(t, keyMarker, result.KeyMarker)
								require.Equal(t, uploadIdMarker, result.UploadIdMarker)
								entries := append([]string{}, result.CommonPrefixes...)
								for _, upload := range result.Uploads {
									entries = append(entries, upload.Key.String()+"#"+upload.UploadId.String())
								}
								require.LessOrEqual(t, len(entries), int(size))
								got = append(got, entries...)
								require.Equal(t, len(got) < len(expected), result.IsTruncated)
								if !result.IsTruncated {
									break
								}
								keyMarker = result.NextKeyMarker
								uploadIdMarker = result.NextUploadIdMarker
							}
							slices.Sort(got)
							require.Equal(t, expected, got)
						})
					}
				}
			}
		})
	}
}
