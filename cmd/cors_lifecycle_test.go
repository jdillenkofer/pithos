package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/lifecyclereconciler"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestBucketCORS(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should put get and delete bucket cors configuration"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)

			maxAge := int32(600)
			_, err = s3Client.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{
				Bucket: bucketName,
				CORSConfiguration: &types.CORSConfiguration{
					CORSRules: []types.CORSRule{{
						AllowedOrigins: []string{"https://app.example.com"},
						AllowedMethods: []string{"GET", "PUT"},
						AllowedHeaders: []string{"content-type", "x-amz-*"},
						ExposeHeaders:  []string{"etag"},
						MaxAgeSeconds:  &maxAge,
					}},
				},
			})
			require.NoError(t, err)

			getResult, err := s3Client.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: bucketName})
			require.NoError(t, err)
			require.Len(t, getResult.CORSRules, 1)
			assert.Equal(t, []string{"https://app.example.com"}, getResult.CORSRules[0].AllowedOrigins)
			assert.Equal(t, []string{"GET", "PUT"}, getResult.CORSRules[0].AllowedMethods)

			_, err = s3Client.DeleteBucketCors(context.Background(), &s3.DeleteBucketCorsInput{Bucket: bucketName})
			require.NoError(t, err)

			_, err = s3Client.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: bucketName})
			assert.Error(t, err)
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) {
				assert.Equal(t, "NoSuchCORSConfiguration", apiErr.ErrorCode())
			}
		})

		t.Run("it should apply cors headers to preflight request"+testSuffix, func(t *testing.T) {
			s3Client, listenerAddr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)

			_, err = s3Client.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{
				Bucket: bucketName,
				CORSConfiguration: &types.CORSConfiguration{
					CORSRules: []types.CORSRule{{
						AllowedOrigins: []string{"https://app.example.com"},
						AllowedMethods: []string{"PUT"},
						AllowedHeaders: []string{"content-type"},
					}},
				},
			})
			require.NoError(t, err)

			addr, err := net.ResolveTCPAddr("tcp", listenerAddr)
			require.NoError(t, err)

			client := buildHttpClient()
			url := fmt.Sprintf("http://%s:%d/%s/test-object", testAPIEndpoint, addr.Port, *bucketName)
			req, err := http.NewRequest(http.MethodOptions, url, nil)
			require.NoError(t, err)
			req.Header.Set("Origin", "https://app.example.com")
			req.Header.Set("Access-Control-Request-Method", "PUT")
			req.Header.Set("Access-Control-Request-Headers", "content-type")

			resp, err := client.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, "https://app.example.com", resp.Header.Get("Access-Control-Allow-Origin"))
			assert.Equal(t, "PUT", resp.Header.Get("Access-Control-Allow-Methods"))
			assert.Equal(t, "content-type", resp.Header.Get("Access-Control-Allow-Headers"))
		})

		t.Run("it should apply cors headers to virtual-host-style preflight request"+testSuffix, func(t *testing.T) {
			s3Client, listenerAddr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)

			_, err = s3Client.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{
				Bucket: bucketName,
				CORSConfiguration: &types.CORSConfiguration{
					CORSRules: []types.CORSRule{{
						AllowedOrigins: []string{"https://app.example.com"},
						AllowedMethods: []string{"PUT"},
						AllowedHeaders: []string{"content-type"},
					}},
				},
			})
			require.NoError(t, err)

			// Virtual-hosted-style: the bucket is in the Host header, not the path.
			// The CORS resolver must see the bucket after virtual-host rewriting.
			client := buildWebsiteHttpClient(listenerAddr)
			url := fmt.Sprintf("http://%s.%s/test-object", *bucketName, testAPIEndpoint)
			req, err := http.NewRequest(http.MethodOptions, url, nil)
			require.NoError(t, err)
			req.Header.Set("Origin", "https://app.example.com")
			req.Header.Set("Access-Control-Request-Method", "PUT")
			req.Header.Set("Access-Control-Request-Headers", "content-type")

			resp, err := client.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, "https://app.example.com", resp.Header.Get("Access-Control-Allow-Origin"))
			assert.Equal(t, "PUT", resp.Header.Get("Access-Control-Allow-Methods"))
			assert.Equal(t, "content-type", resp.Header.Get("Access-Control-Allow-Headers"))
		})
	})
}

func TestBucketLifecycle(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should put get and delete bucket lifecycle configuration"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)

			_, err = s3Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: bucketName})
			require.Error(t, err)
			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "NoSuchLifecycleConfiguration", apiErr.ErrorCode())

			_, err = s3Client.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
				Bucket: bucketName,
				LifecycleConfiguration: &types.BucketLifecycleConfiguration{
					Rules: []types.LifecycleRule{
						{
							ID:     aws.String("expire-logs"),
							Status: types.ExpirationStatusEnabled,
							Filter: &types.LifecycleRuleFilter{
								Prefix: aws.String("logs/"),
							},
							Expiration: &types.LifecycleExpiration{
								Days: aws.Int32(30),
							},
						},
						{
							ID:     aws.String("expire-tagged"),
							Status: types.ExpirationStatusDisabled,
							Filter: &types.LifecycleRuleFilter{
								And: &types.LifecycleRuleAndOperator{
									Prefix:                aws.String("tmp/"),
									Tags:                  []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
									ObjectSizeGreaterThan: aws.Int64(1024),
								},
							},
							Expiration: &types.LifecycleExpiration{
								Date: aws.Time(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)),
							},
						},
						{
							ID:     aws.String("abort-uploads"),
							Status: types.ExpirationStatusEnabled,
							Filter: &types.LifecycleRuleFilter{
								Prefix: aws.String(""),
							},
							AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{
								DaysAfterInitiation: aws.Int32(7),
							},
						},
						{
							ID:     aws.String("transition-cold"),
							Status: types.ExpirationStatusEnabled,
							Filter: &types.LifecycleRuleFilter{
								Prefix: aws.String("cold/"),
							},
							Transitions: []types.Transition{{
								Days:         aws.Int32(30),
								StorageClass: types.TransitionStorageClassGlacier,
							}},
						},
						{
							ID:     aws.String("transition-noncurrent-cold"),
							Status: types.ExpirationStatusEnabled,
							Filter: &types.LifecycleRuleFilter{
								Prefix: aws.String("cold/"),
							},
							NoncurrentVersionTransitions: []types.NoncurrentVersionTransition{{
								NoncurrentDays:          aws.Int32(30),
								NewerNoncurrentVersions: aws.Int32(2),
								StorageClass:            types.TransitionStorageClassDeepArchive,
							}},
						},
					},
				},
			})
			require.NoError(t, err)

			getResult, err := s3Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: bucketName})
			require.NoError(t, err)
			require.Len(t, getResult.Rules, 5)

			assert.Equal(t, "expire-logs", aws.ToString(getResult.Rules[0].ID))
			assert.Equal(t, types.ExpirationStatusEnabled, getResult.Rules[0].Status)
			require.NotNil(t, getResult.Rules[0].Filter)
			assert.Equal(t, "logs/", aws.ToString(getResult.Rules[0].Filter.Prefix))
			require.NotNil(t, getResult.Rules[0].Expiration)
			assert.Equal(t, int32(30), aws.ToInt32(getResult.Rules[0].Expiration.Days))

			assert.Equal(t, "expire-tagged", aws.ToString(getResult.Rules[1].ID))
			assert.Equal(t, types.ExpirationStatusDisabled, getResult.Rules[1].Status)
			require.NotNil(t, getResult.Rules[1].Filter)
			require.NotNil(t, getResult.Rules[1].Filter.And)
			assert.Equal(t, "tmp/", aws.ToString(getResult.Rules[1].Filter.And.Prefix))
			require.Len(t, getResult.Rules[1].Filter.And.Tags, 1)
			assert.Equal(t, "env", aws.ToString(getResult.Rules[1].Filter.And.Tags[0].Key))
			assert.Equal(t, "dev", aws.ToString(getResult.Rules[1].Filter.And.Tags[0].Value))
			assert.Equal(t, int64(1024), aws.ToInt64(getResult.Rules[1].Filter.And.ObjectSizeGreaterThan))
			require.NotNil(t, getResult.Rules[1].Expiration)
			require.NotNil(t, getResult.Rules[1].Expiration.Date)
			assert.Equal(t, time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), getResult.Rules[1].Expiration.Date.UTC())

			assert.Equal(t, "abort-uploads", aws.ToString(getResult.Rules[2].ID))
			require.NotNil(t, getResult.Rules[2].AbortIncompleteMultipartUpload)
			assert.Equal(t, int32(7), aws.ToInt32(getResult.Rules[2].AbortIncompleteMultipartUpload.DaysAfterInitiation))

			assert.Equal(t, "transition-cold", aws.ToString(getResult.Rules[3].ID))
			require.Len(t, getResult.Rules[3].Transitions, 1)
			assert.Equal(t, int32(30), aws.ToInt32(getResult.Rules[3].Transitions[0].Days))
			assert.Equal(t, types.TransitionStorageClassGlacier, getResult.Rules[3].Transitions[0].StorageClass)

			assert.Equal(t, "transition-noncurrent-cold", aws.ToString(getResult.Rules[4].ID))
			require.Len(t, getResult.Rules[4].NoncurrentVersionTransitions, 1)
			assert.Equal(t, int32(30), aws.ToInt32(getResult.Rules[4].NoncurrentVersionTransitions[0].NoncurrentDays))
			assert.Equal(t, int32(2), aws.ToInt32(getResult.Rules[4].NoncurrentVersionTransitions[0].NewerNoncurrentVersions))
			assert.Equal(t, types.TransitionStorageClassDeepArchive, getResult.Rules[4].NoncurrentVersionTransitions[0].StorageClass)

			_, err = s3Client.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{Bucket: bucketName})
			require.NoError(t, err)

			_, err = s3Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: bucketName})
			require.Error(t, err)
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "NoSuchLifecycleConfiguration", apiErr.ErrorCode())

			// Deleting an absent lifecycle configuration succeeds, like on AWS.
			_, err = s3Client.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{Bucket: bucketName})
			require.NoError(t, err)
		})

		t.Run("it should reject invalid lifecycle configurations"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)

			putLifecycle := func(rule types.LifecycleRule) error {
				_, err := s3Client.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
					Bucket: bucketName,
					LifecycleConfiguration: &types.BucketLifecycleConfiguration{
						Rules: []types.LifecycleRule{rule},
					},
				})
				return err
			}

			expectErrorCode := func(t *testing.T, err error, code string) {
				t.Helper()
				require.Error(t, err)
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, code, apiErr.ErrorCode())
			}

			// Expiration days must be a positive integer.
			expectErrorCode(t, putLifecycle(types.LifecycleRule{
				Status:     types.ExpirationStatusEnabled,
				Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(0)},
			}), "InvalidArgument")

			// A rule needs at least one action.
			expectErrorCode(t, putLifecycle(types.LifecycleRule{
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
			}), "InvalidRequest")

			// Days and Date are mutually exclusive.
			expectErrorCode(t, putLifecycle(types.LifecycleRule{
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Expiration: &types.LifecycleExpiration{
					Days: aws.Int32(1),
					Date: aws.Time(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)),
				},
			}), "MalformedXML")

			// The expiration date must be at midnight UTC.
			expectErrorCode(t, putLifecycle(types.LifecycleRule{
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Expiration: &types.LifecycleExpiration{
					Date: aws.Time(time.Date(2030, 1, 1, 12, 30, 0, 0, time.UTC)),
				},
			}), "InvalidArgument")

			// Transition to STANDARD is rejected (it is not a valid target).
			expectErrorCode(t, putLifecycle(types.LifecycleRule{
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Transitions: []types.Transition{{
					Days:         aws.Int32(30),
					StorageClass: types.TransitionStorageClass("STANDARD"),
				}},
			}), "InvalidArgument")

			// A transition must be due before the expiration deletes the object.
			expectErrorCode(t, putLifecycle(types.LifecycleRule{
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Transitions: []types.Transition{{
					Days:         aws.Int32(30),
					StorageClass: types.TransitionStorageClassGlacier,
				}},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(30)},
			}), "InvalidArgument")

			// NoncurrentVersionTransition days must be positive.
			expectErrorCode(t, putLifecycle(types.LifecycleRule{
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
				NoncurrentVersionTransitions: []types.NoncurrentVersionTransition{{
					NoncurrentDays: aws.Int32(0),
					StorageClass:   types.TransitionStorageClassGlacier,
				}},
			}), "InvalidArgument")

			// AbortIncompleteMultipartUpload cannot be combined with tag filters.
			expectErrorCode(t, putLifecycle(types.LifecycleRule{
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{Tag: &types.Tag{Key: aws.String("env"), Value: aws.String("dev")}},
				AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{
					DaysAfterInitiation: aws.Int32(7),
				},
			}), "InvalidRequest")
		})

		t.Run("it should enforce lifecycle rules via the background reconciler"+testSuffix, func(t *testing.T) {
			ctx := context.Background()
			cleanups := make([]func(), 0, 4)
			addCleanup := func(fn func()) {
				cleanups = append(cleanups, fn)
			}
			t.Cleanup(func() {
				for i := len(cleanups) - 1; i >= 0; i-- {
					cleanups[i]()
				}
			})

			storagePath := mustTempDir(addCleanup, "pithos-test-data-")
			db, dbCleanup, err := setupDatabase(ctx, dbType, storagePath)
			require.NoError(t, err)
			addDatabaseCleanup(addCleanup, db, dbCleanup, "Couldn't close database")

			encryptionPassword := ""
			if encryptionType != storageFactory.EncryptionTypeNone {
				encryptionPassword = partStoreEncryptionPassword
			}
			innerStore := storageFactory.CreateStorage(storagePath, db, useFilesystemPartStore, usePartStoreCompression, encryptionType, encryptionPassword, wrapPartStoreWithOutbox, prometheus.NewRegistry())

			// Pretend the sweep runs ten days in the future so day-based rules
			// are due without waiting.
			store := lifecyclereconciler.NewStorageMiddleware(innerStore,
				lifecyclereconciler.WithReconcileInterval(100*time.Millisecond),
				lifecyclereconciler.WithNow(func() time.Time { return time.Now().UTC().AddDate(0, 0, 10) }))

			require.NoError(t, store.Start(ctx))
			addCleanup(func() {
				err := store.Stop(ctx)
				mustNoErr(err, "Couldn't stop storage")
			})

			lifecycleBucket := storage.MustNewBucketName("lifecycle-test")
			require.NoError(t, store.CreateBucket(ctx, lifecycleBucket))

			_, err = store.PutObject(ctx, lifecycleBucket, storage.MustNewObjectKey("logs/old.log"), nil, ioutils.NewByteReadSeekCloser(body), nil, nil)
			require.NoError(t, err)
			_, err = store.PutObject(ctx, lifecycleBucket, storage.MustNewObjectKey("data/keep.bin"), nil, ioutils.NewByteReadSeekCloser(body), nil, nil)
			require.NoError(t, err)

			initiateResult, err := store.CreateMultipartUpload(ctx, lifecycleBucket, storage.MustNewObjectKey("uploads/stale"), nil, nil, nil)
			require.NoError(t, err)
			_, err = store.UploadPart(ctx, lifecycleBucket, storage.MustNewObjectKey("uploads/stale"), initiateResult.UploadId, 1, ioutils.NewByteReadSeekCloser(body), nil)
			require.NoError(t, err)

			require.NoError(t, store.PutBucketLifecycleConfiguration(ctx, lifecycleBucket, &storage.BucketLifecycleConfiguration{
				Rules: []storage.LifecycleRule{
					{
						ID:         aws.String("expire-logs"),
						Status:     storage.LifecycleRuleStatusEnabled,
						Filter:     &storage.LifecycleFilter{Prefix: aws.String("logs/")},
						Expiration: &storage.LifecycleExpiration{Days: aws.Int32(3)},
					},
					{
						ID:                             aws.String("abort-uploads"),
						Status:                         storage.LifecycleRuleStatusEnabled,
						Filter:                         &storage.LifecycleFilter{Prefix: aws.String("uploads/")},
						AbortIncompleteMultipartUpload: &storage.LifecycleAbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(7)},
					},
				},
			}))

			require.Eventually(t, func() bool {
				listResult, err := store.ListObjects(ctx, lifecycleBucket, storage.ListObjectsOptions{MaxKeys: 1000})
				if err != nil || len(listResult.Objects) != 1 {
					return false
				}
				uploads, err := store.ListMultipartUploads(ctx, lifecycleBucket, storage.ListMultipartUploadsOptions{MaxUploads: 1000})
				return err == nil && len(uploads.Uploads) == 0
			}, 15*time.Second, 100*time.Millisecond, "expired object should be deleted and stale upload aborted")

			listResult, err := store.ListObjects(ctx, lifecycleBucket, storage.ListObjectsOptions{MaxKeys: 1000})
			require.NoError(t, err)
			require.Len(t, listResult.Objects, 1)
			assert.Equal(t, "data/keep.bin", listResult.Objects[0].Key.String())
		})
	})
}
