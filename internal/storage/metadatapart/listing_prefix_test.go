package metadatapart

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

func TestSQLListingPrefixesAreLiteralAndCaseSensitive(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, backend := range []struct {
		name string
		new  func(*testing.T) (*metadataPartStorage, func())
	}{
		{name: "sqlite", new: newTestStorage},
		{name: "postgres", new: newPostgresTestStorage},
	} {
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

			err = database.WithTx(ctx, st.db, &sql.TxOptions{}, func(ctx context.Context, tx database.Tx) error {
				sqlTx := tx.SqlTx()
				if backend.name == "postgres" {
					// Prefix ranges and pagination must not depend on the database locale.
					_, err := sqlTx.ExecContext(ctx, `CREATE COLLATION prefix_test_english (provider = icu, locale = 'en-US')`)
					require.NoError(t, err)
					_, err = sqlTx.ExecContext(ctx, `ALTER TABLE objects ALTER COLUMN key TYPE TEXT COLLATE prefix_test_english`)
					require.NoError(t, err)
					var localeSortsLowercaseFirst bool
					err = sqlTx.QueryRowContext(ctx, `SELECT 'a' COLLATE prefix_test_english < 'B'`).Scan(&localeSortsLowercaseFirst)
					require.NoError(t, err)
					require.True(t, localeSortsLowercaseFirst)
				}

				keys := []string{
					"logs/", "logs/a", "logs/b", "logs/deleted", "Logs/a", "logsX/a",
					"logs%/a", "logs%/b", "logs_/a", "logs_/b", "logs\\/a", "logs\\/b",
					"logs[/a", "logs?/a", "logs*/a", "ö/a", "ö/b", "Ö/a", "界/a", "界/子",
					"\u007f/a", "\u0080/a", "\ud7ff/a", "\ue000/a", "\uffff/a", "\U00010000/a",
					"a\U0010ffff/a", "a\U0010ffff/b", "b/a", "\U0010ffff/a", "\U0010ffff/b",
				}
				prefixes := []string{
					"", "logs/", "Logs/", "logs%", "logs_", "logs\\", "logs[", "logs?", "logs*",
					"ö", "Ö", "界/", "\u007f", "\u0080", "\ud7ff", "\ue000", "\uffff", "\U00010000",
					"a\U0010ffff", "\U0010ffff", "missing/",
				}
				if backend.name == "sqlite" {
					// SQLite can also store NULs, which pattern matching would truncate.
					keys = append(keys, "logs\x00/a", "logs\x00/b")
					prefixes = append(prefixes, "logs\x00", "\x80")
				}

				var objects, versions, uploads []object.Entity
				for _, key := range keys {
					for _, latest := range []bool{false, true} {
						versionID := "null"
						if latest {
							versionID = ulid.Make().String()
						}
						entity := object.Entity{
							BucketName: bucket, Key: storage.MustNewObjectKey(key), VersionID: &versionID,
							UploadStatus: object.UploadStatusCompleted, IsLatest: latest,
							IsDeleteMarker: latest && key == "logs/deleted",
						}
						require.NoError(t, repo.SaveObject(ctx, sqlTx, &entity))
						versions = append(versions, entity)
						if latest && !entity.IsDeleteMarker {
							objects = append(objects, entity)
						}

						uploadID := storage.MustNewUploadId(ulid.Make().String())
						entity = object.Entity{
							BucketName: bucket, Key: storage.MustNewObjectKey(key), UploadId: &uploadID,
							UploadStatus: object.UploadStatusPending,
						}
						require.NoError(t, repo.SaveObject(ctx, sqlTx, &entity))
						uploads = append(uploads, entity)
					}
				}
				slices.SortFunc(objects, func(a, b object.Entity) int { return cmp.Compare(a.Key.String(), b.Key.String()) })
				slices.SortFunc(uploads, func(a, b object.Entity) int {
					if order := cmp.Compare(a.Key.String(), b.Key.String()); order != 0 {
						return order
					}
					return cmp.Compare(a.UploadId.String(), b.UploadId.String())
				})
				slices.SortFunc(versions, func(a, b object.Entity) int {
					if order := cmp.Compare(a.Key.String(), b.Key.String()); order != 0 {
						return order
					}
					// The null version sorts after every ULID version.
					versionA, versionB := *a.VersionID, *b.VersionID
					if versionA == "null" {
						versionA = ""
					}
					if versionB == "null" {
						versionB = ""
					}
					return cmp.Compare(versionB, versionA)
				})

				for _, prefix := range prefixes {
					t.Run(fmt.Sprintf("prefix=%q", prefix), func(t *testing.T) {
						for _, listing := range []prefixListingQueries{
							{
								name: "objects", expected: entitiesWithLiteralPrefix(objects, prefix),
								page: func(key, _ string, limit int32) ([]object.Entity, error) {
									return repo.FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscWithLimit(ctx, sqlTx, bucket, prefix, key, limit)
								},
								fromKey: func(key, _ string, limit int32) ([]object.Entity, error) {
									return repo.FindObjectsFromKey(ctx, sqlTx, bucket, prefix, key, "", limit)
								},
								secondaryMarker: func(object.Entity) string { return "" },
							},
							{
								name: "uploads", expected: entitiesWithLiteralPrefix(uploads, prefix),
								page: func(key, uploadID string, limit int32) ([]object.Entity, error) {
									return repo.FindUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAscWithLimit(ctx, sqlTx, bucket, prefix, key, uploadID, limit)
								},
								fromKey: func(key, uploadID string, limit int32) ([]object.Entity, error) {
									return repo.FindUploadsFromKey(ctx, sqlTx, bucket, prefix, key, uploadID, "", limit)
								},
								secondaryMarker: func(entity object.Entity) string { return entity.UploadId.String() },
							},
							{
								name: "versions", expected: entitiesWithLiteralPrefix(versions, prefix),
								page: func(key, versionID string, limit int32) ([]object.Entity, error) {
									return repo.FindObjectVersionsByBucketNameAndPrefixAndKeyMarkerAndVersionIDMarkerOrderByKeyAscAndVersionIDDescWithLimit(ctx, sqlTx, bucket, prefix, key, versionID, limit)
								},
								fromKey: func(key, versionID string, limit int32) ([]object.Entity, error) {
									return repo.FindObjectVersionsFromKey(ctx, sqlTx, bucket, prefix, key, versionID, "", limit)
								},
								secondaryMarker: func(entity object.Entity) string { return *entity.VersionID },
							},
						} {
							t.Run(listing.name, listing.check)
						}
					})
				}
				return nil
			})
			require.NoError(t, err)
		})
	}
}

type prefixListingQueries struct {
	name            string
	expected        []object.Entity
	page            func(keyMarker, secondaryMarker string, limit int32) ([]object.Entity, error)
	fromKey         func(keyMarker, secondaryMarker string, limit int32) ([]object.Entity, error)
	secondaryMarker func(object.Entity) string
}

func (listing prefixListingQueries) check(t *testing.T) {
	t.Helper()
	// Check every continuation position, including before the first entry and
	// after the last, for both the paged and the seeking repository queries.
	for cursor := 0; cursor <= len(listing.expected); cursor++ {
		keyMarker, secondaryMarker := "", ""
		if cursor > 0 {
			previous := listing.expected[cursor-1]
			keyMarker = previous.Key.String()
			secondaryMarker = listing.secondaryMarker(previous)
		}
		expected := listing.expected[cursor:]
		for _, query := range []func(string, string, int32) ([]object.Entity, error){listing.page, listing.fromKey} {
			for _, limit := range []int32{1, 3, math.MaxInt32} {
				actual, err := query(keyMarker, secondaryMarker, limit)
				require.NoError(t, err)
				require.Equal(t, listingEntityIDs(expected[:min(len(expected), int(limit))]), listingEntityIDs(actual))
			}
		}
	}
}

func entitiesWithLiteralPrefix(entities []object.Entity, prefix string) []object.Entity {
	matching := []object.Entity{}
	for _, entity := range entities {
		if strings.HasPrefix(entity.Key.String(), prefix) {
			matching = append(matching, entity)
		}
	}
	return matching
}

func listingEntityIDs(entities []object.Entity) []string {
	ids := make([]string, len(entities))
	for i, entity := range entities {
		ids[i] = entity.Id.String()
	}
	return ids
}
