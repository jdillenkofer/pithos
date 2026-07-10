package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net"
	"net/http"
	"testing"
)

func TestBucketWebsite(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should put and get website configuration"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}

			// Put website configuration with index document only
			_, err = s3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{
						Suffix: aws.String("index.html"),
					},
				},
			})
			if err != nil {
				assert.Fail(t, "PutBucketWebsite failed", "err %v", err)
			}

			// Get and verify
			getResult, err := s3Client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "GetBucketWebsite failed", "err %v", err)
			}
			assert.NotNil(t, getResult.IndexDocument)
			assert.Equal(t, "index.html", *getResult.IndexDocument.Suffix)
			assert.Nil(t, getResult.ErrorDocument)
		})

		t.Run("it should put website configuration with error document"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}

			_, err = s3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{
						Suffix: aws.String("index.html"),
					},
					ErrorDocument: &types.ErrorDocument{
						Key: aws.String("error.html"),
					},
				},
			})
			if err != nil {
				assert.Fail(t, "PutBucketWebsite failed", "err %v", err)
			}

			getResult, err := s3Client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "GetBucketWebsite failed", "err %v", err)
			}
			assert.NotNil(t, getResult.IndexDocument)
			assert.Equal(t, "index.html", *getResult.IndexDocument.Suffix)
			assert.NotNil(t, getResult.ErrorDocument)
			assert.Equal(t, "error.html", *getResult.ErrorDocument.Key)
		})

		t.Run("it should delete website configuration"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}

			// Put website config
			_, err = s3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{
						Suffix: aws.String("index.html"),
					},
				},
			})
			if err != nil {
				assert.Fail(t, "PutBucketWebsite failed", "err %v", err)
			}

			// Delete website config
			_, err = s3Client.DeleteBucketWebsite(context.Background(), &s3.DeleteBucketWebsiteInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "DeleteBucketWebsite failed", "err %v", err)
			}

			// Get should fail with NoSuchWebsiteConfiguration
			_, err = s3Client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{
				Bucket: bucketName,
			})
			assert.Error(t, err, "GetBucketWebsite should fail after deletion")
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) {
				assert.Equal(t, "NoSuchWebsiteConfiguration", apiErr.ErrorCode())
			} else {
				assert.Fail(t, "Expected API error", "err %v", err)
			}
		})

		t.Run("it should return error getting website config for unconfigured bucket"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}

			// Get website config for bucket without one
			_, err = s3Client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{
				Bucket: bucketName,
			})
			assert.Error(t, err, "GetBucketWebsite should fail for unconfigured bucket")
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) {
				assert.Equal(t, "NoSuchWebsiteConfiguration", apiErr.ErrorCode())
			} else {
				assert.Fail(t, "Expected API error", "err %v", err)
			}
		})

		t.Run("it should overwrite existing website configuration"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}

			// Put initial config
			_, err = s3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{
						Suffix: aws.String("index.html"),
					},
				},
			})
			if err != nil {
				assert.Fail(t, "PutBucketWebsite failed", "err %v", err)
			}

			// Overwrite with new config
			_, err = s3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{
						Suffix: aws.String("default.html"),
					},
					ErrorDocument: &types.ErrorDocument{
						Key: aws.String("404.html"),
					},
				},
			})
			if err != nil {
				assert.Fail(t, "PutBucketWebsite (overwrite) failed", "err %v", err)
			}

			// Verify updated config
			getResult, err := s3Client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "GetBucketWebsite failed", "err %v", err)
			}
			assert.Equal(t, "default.html", *getResult.IndexDocument.Suffix)
			assert.NotNil(t, getResult.ErrorDocument)
			assert.Equal(t, "404.html", *getResult.ErrorDocument.Key)
		})

		t.Run("it should put and get redirect all website configuration"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)

			_, err = s3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					RedirectAllRequestsTo: &types.RedirectAllRequestsTo{
						HostName: aws.String("www.example.com"),
						Protocol: types.ProtocolHttps,
					},
				},
			})
			require.NoError(t, err)

			getResult, err := s3Client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: bucketName})
			require.NoError(t, err)
			require.NotNil(t, getResult.RedirectAllRequestsTo)
			assert.Equal(t, "www.example.com", *getResult.RedirectAllRequestsTo.HostName)
			assert.Equal(t, types.ProtocolHttps, getResult.RedirectAllRequestsTo.Protocol)
			assert.Nil(t, getResult.IndexDocument)
			assert.Nil(t, getResult.ErrorDocument)
		})

		t.Run("it should put and get ordered website routing rules"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)

			_, err = s3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
					RoutingRules: []types.RoutingRule{
						{
							Condition: &types.Condition{KeyPrefixEquals: aws.String("docs/")},
							Redirect: &types.Redirect{
								ReplaceKeyPrefixWith: aws.String("documents/"),
								HttpRedirectCode:     aws.String("302"),
							},
						},
						{
							Condition: &types.Condition{HttpErrorCodeReturnedEquals: aws.String("404")},
							Redirect: &types.Redirect{
								HostName:         aws.String("errors.example.com"),
								ReplaceKeyWith:   aws.String("not-found.html"),
								HttpRedirectCode: aws.String("307"),
							},
						},
					},
				},
			})
			require.NoError(t, err)

			getResult, err := s3Client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: bucketName})
			require.NoError(t, err)
			require.Len(t, getResult.RoutingRules, 2)
			assert.Equal(t, "docs/", *getResult.RoutingRules[0].Condition.KeyPrefixEquals)
			assert.Equal(t, "documents/", *getResult.RoutingRules[0].Redirect.ReplaceKeyPrefixWith)
			assert.Equal(t, "302", *getResult.RoutingRules[0].Redirect.HttpRedirectCode)
			assert.Equal(t, "404", *getResult.RoutingRules[1].Condition.HttpErrorCodeReturnedEquals)
			assert.Equal(t, "errors.example.com", *getResult.RoutingRules[1].Redirect.HostName)
			assert.Equal(t, "not-found.html", *getResult.RoutingRules[1].Redirect.ReplaceKeyWith)
			assert.Equal(t, "307", *getResult.RoutingRules[1].Redirect.HttpRedirectCode)
		})

		t.Run("it should overwrite between index and redirect website configurations"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)

			_, err = s3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
				},
			})
			require.NoError(t, err)

			_, err = s3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					RedirectAllRequestsTo: &types.RedirectAllRequestsTo{HostName: aws.String("www.example.com")},
				},
			})
			require.NoError(t, err)
			redirectResult, err := s3Client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: bucketName})
			require.NoError(t, err)
			require.NotNil(t, redirectResult.RedirectAllRequestsTo)
			assert.Nil(t, redirectResult.IndexDocument)

			_, err = s3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{Suffix: aws.String("default.html")},
				},
			})
			require.NoError(t, err)
			indexResult, err := s3Client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: bucketName})
			require.NoError(t, err)
			require.NotNil(t, indexResult.IndexDocument)
			assert.Equal(t, "default.html", *indexResult.IndexDocument.Suffix)
			assert.Nil(t, indexResult.RedirectAllRequestsTo)
		})

		t.Run("it should delete redirect website configuration"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)
			_, err = s3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					RedirectAllRequestsTo: &types.RedirectAllRequestsTo{HostName: aws.String("www.example.com")},
				},
			})
			require.NoError(t, err)

			_, err = s3Client.DeleteBucketWebsite(context.Background(), &s3.DeleteBucketWebsiteInput{Bucket: bucketName})
			require.NoError(t, err)
			_, err = s3Client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: bucketName})
			require.Error(t, err)
			var apiErr smithy.APIError
			require.True(t, errors.As(err, &apiErr))
			assert.Equal(t, "NoSuchWebsiteConfiguration", apiErr.ErrorCode())
		})

		t.Run("it should reject invalid website redirect rules"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)

			_, err = s3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
					RoutingRules: []types.RoutingRule{{
						Condition: &types.Condition{KeyPrefixEquals: aws.String("docs/")},
						Redirect: &types.Redirect{
							ReplaceKeyPrefixWith: aws.String("documents/"),
							ReplaceKeyWith:       aws.String("single.html"),
						},
					}},
				},
			})
			require.Error(t, err)
			var apiErr smithy.APIError
			require.True(t, errors.As(err, &apiErr))
			assert.Equal(t, "InvalidArgument", apiErr.ErrorCode())
		})
	})
}

func TestWebsiteHosting(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		// Helper: set up a bucket with website config, policy, and content for website tests.
		setupWebsiteBucket := func(t *testing.T) (httpClient *http.Client, listenerAddr string) {
			s3Client, addr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			ctx := context.Background()

			// Create bucket
			_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				t.Fatalf("CreateBucket failed: %v", err)
			}

			// Configure website hosting
			_, err = s3Client.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{
						Suffix: aws.String("index.html"),
					},
					ErrorDocument: &types.ErrorDocument{
						Key: aws.String("error.html"),
					},
				},
			})
			if err != nil {
				t.Fatalf("PutBucketWebsite failed: %v", err)
			}

			// Upload index.html
			_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      bucketName,
				Key:         aws.String("index.html"),
				Body:        bytes.NewReader([]byte("<html><body>Welcome</body></html>")),
				ContentType: aws.String("text/html"),
			})
			if err != nil {
				t.Fatalf("PutObject (index.html) failed: %v", err)
			}

			// Upload error.html
			_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      bucketName,
				Key:         aws.String("error.html"),
				Body:        bytes.NewReader([]byte("<html><body>Custom Error Page</body></html>")),
				ContentType: aws.String("text/html"),
			})
			if err != nil {
				t.Fatalf("PutObject (error.html) failed: %v", err)
			}

			// Upload subdir/index.html
			_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      bucketName,
				Key:         aws.String("subdir/index.html"),
				Body:        bytes.NewReader([]byte("<html><body>Subdirectory</body></html>")),
				ContentType: aws.String("text/html"),
			})
			if err != nil {
				t.Fatalf("PutObject (subdir/index.html) failed: %v", err)
			}

			// Upload style.css
			_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      bucketName,
				Key:         aws.String("style.css"),
				Body:        bytes.NewReader([]byte("body { color: red; }")),
				ContentType: aws.String("text/css"),
			})
			if err != nil {
				t.Fatalf("PutObject (style.css) failed: %v", err)
			}

			return buildWebsiteHttpClient(addr), addr
		}

		setupWebsiteRedirectBucket := func(t *testing.T, websiteConfig *types.WebsiteConfiguration) (httpClient *http.Client, listenerAddr string, s3Client *s3.Client) {
			s3Client, addr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			ctx := context.Background()
			_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)
			_, err = s3Client.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{
				Bucket:               bucketName,
				WebsiteConfiguration: websiteConfig,
			})
			require.NoError(t, err)

			return buildWebsiteHttpClientNoRedirect(addr), addr, s3Client
		}

		t.Run("it should serve index document at root"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr := setupWebsiteBucket(t)

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s.%s:%d/", *bucketName, testWebsiteEndpoint, addr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("GET / failed: %v", err)
			}
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			bodyBytes, _ := io.ReadAll(resp.Body)
			assert.Contains(t, string(bodyBytes), "Welcome")
			assert.Equal(t, "text/html", resp.Header.Get("Content-Type"))
		})

		t.Run("it should serve subdirectory index document"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr := setupWebsiteBucket(t)

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s.%s:%d/subdir/", *bucketName, testWebsiteEndpoint, addr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("GET /subdir/ failed: %v", err)
			}
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			bodyBytes, _ := io.ReadAll(resp.Body)
			assert.Contains(t, string(bodyBytes), "Subdirectory")
		})

		t.Run("it should redirect directory requests to trailing slash when index exists"+testSuffix, func(t *testing.T) {
			_, listenerAddr := setupWebsiteBucket(t)
			httpClient := buildWebsiteHttpClientNoRedirect(listenerAddr)

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s.%s:%d/subdir", *bucketName, testWebsiteEndpoint, addr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusFound, resp.StatusCode)
			assert.Equal(t, "/subdir/", resp.Header.Get("Location"))
		})

		t.Run("it should redirect directory HEAD requests to trailing slash when index exists"+testSuffix, func(t *testing.T) {
			_, listenerAddr := setupWebsiteBucket(t)
			httpClient := buildWebsiteHttpClientNoRedirect(listenerAddr)

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s.%s:%d/subdir", *bucketName, testWebsiteEndpoint, addr.Port)
			req, _ := http.NewRequest("HEAD", url, nil)
			resp, err := httpClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusFound, resp.StatusCode)
			assert.Equal(t, "/subdir/", resp.Header.Get("Location"))
		})

		t.Run("it should serve direct object access"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr := setupWebsiteBucket(t)

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s.%s:%d/style.css", *bucketName, testWebsiteEndpoint, addr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("GET /style.css failed: %v", err)
			}
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			bodyBytes, _ := io.ReadAll(resp.Body)
			assert.Contains(t, string(bodyBytes), "body { color: red; }")
			assert.Equal(t, "text/css", resp.Header.Get("Content-Type"))
		})

		t.Run("it should serve error document on 404"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr := setupWebsiteBucket(t)

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s.%s:%d/nonexistent.html", *bucketName, testWebsiteEndpoint, addr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("GET /nonexistent.html failed: %v", err)
			}
			defer resp.Body.Close()

			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
			bodyBytes, _ := io.ReadAll(resp.Body)
			assert.Contains(t, string(bodyBytes), "Custom Error Page")
		})

		t.Run("it should return default HTML 404 when no error document configured"+testSuffix, func(t *testing.T) {
			s3Client, addr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			ctx := context.Background()

			// Create bucket with website config but NO error document
			_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				t.Fatalf("CreateBucket failed: %v", err)
			}

			_, err = s3Client.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{
						Suffix: aws.String("index.html"),
					},
				},
			})
			if err != nil {
				t.Fatalf("PutBucketWebsite failed: %v", err)
			}

			httpClient := buildWebsiteHttpClient(addr)
			tcpAddr, _ := net.ResolveTCPAddr("tcp", addr)
			url := fmt.Sprintf("http://%s.%s:%d/nonexistent.html", *bucketName, testWebsiteEndpoint, tcpAddr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("GET /nonexistent.html failed: %v", err)
			}
			defer resp.Body.Close()

			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
			bodyBytes, _ := io.ReadAll(resp.Body)
			bodyStr := string(bodyBytes)
			assert.Contains(t, bodyStr, "404")
			assert.Contains(t, bodyStr, "NoSuchKey")
			assert.Contains(t, resp.Header.Get("Content-Type"), "text/html")
		})

		t.Run("it should return 401 for anonymous request denied by authorizer"+testSuffix, func(t *testing.T) {
			// Use a deny-anonymous authorizer: anonymous requests are always denied,
			// authenticated requests are always allowed. This simulates a private bucket.
			denyAnonAuthorizer, err := lua.NewLuaAuthorizer(`
			function authorizeRequest(request)
			  return not request:isAnonymous()
			end
			`)
			if err != nil {
				t.Fatalf("Could not create deny-anon authorizer: %v", err)
			}
			s3Client, addr, cleanup := setupTestServerWithAuthorizer(denyAnonAuthorizer, dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			ctx := context.Background()

			// Create bucket with website config
			_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				t.Fatalf("CreateBucket failed: %v", err)
			}

			_, err = s3Client.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{
				Bucket: bucketName,
				WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{
						Suffix: aws.String("index.html"),
					},
				},
			})
			if err != nil {
				t.Fatalf("PutBucketWebsite failed: %v", err)
			}

			// Upload an object so we can verify the 401 isn't just a 404
			_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      bucketName,
				Key:         aws.String("index.html"),
				Body:        bytes.NewReader([]byte("<html><body>Welcome</body></html>")),
				ContentType: aws.String("text/html"),
			})
			if err != nil {
				t.Fatalf("PutObject (index.html) failed: %v", err)
			}

			httpClient := buildWebsiteHttpClient(addr)
			tcpAddr, _ := net.ResolveTCPAddr("tcp", addr)
			url := fmt.Sprintf("http://%s.%s:%d/", *bucketName, testWebsiteEndpoint, tcpAddr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("GET / failed: %v", err)
			}
			defer resp.Body.Close()

			// Anonymous request denied by authorizer must return 401
			assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		})

		t.Run("it should return error for bucket without website config"+testSuffix, func(t *testing.T) {
			s3Client, addr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			ctx := context.Background()

			// Create bucket but do NOT configure website
			_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				t.Fatalf("CreateBucket failed: %v", err)
			}

			httpClient := buildWebsiteHttpClient(addr)
			tcpAddr, _ := net.ResolveTCPAddr("tcp", addr)
			url := fmt.Sprintf("http://%s.%s:%d/", *bucketName, testWebsiteEndpoint, tcpAddr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("GET / failed: %v", err)
			}
			defer resp.Body.Close()

			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
		})

		t.Run("it should handle HEAD requests"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr := setupWebsiteBucket(t)

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s.%s:%d/index.html", *bucketName, testWebsiteEndpoint, addr.Port)
			req, _ := http.NewRequest("HEAD", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("HEAD /index.html failed: %v", err)
			}
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, "text/html", resp.Header.Get("Content-Type"))
			assert.NotEmpty(t, resp.Header.Get("ETag"))
			assert.NotEmpty(t, resp.Header.Get("Last-Modified"))
		})

		t.Run("it should reject POST requests"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr := setupWebsiteBucket(t)

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s.%s:%d/", *bucketName, testWebsiteEndpoint, addr.Port)
			req, _ := http.NewRequest("POST", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("POST / failed: %v", err)
			}
			defer resp.Body.Close()

			assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
		})

		t.Run("it should serve via custom domain fallback"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr := setupWebsiteBucket(t)

			// Use a custom domain that matches the bucket name.
			// Since our bucket is "test", we use "test" as the Host header.
			// The routing handler treats any hostname that doesn't match the API
			// or website endpoints as a custom domain, using the full hostname
			// as the bucket name.
			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s:%d/", *bucketName, addr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("GET / via custom domain failed: %v", err)
			}
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			bodyBytes, _ := io.ReadAll(resp.Body)
			assert.Contains(t, string(bodyBytes), "Welcome")
		})

		t.Run("it should redirect all website requests"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr, _ := setupWebsiteRedirectBucket(t, &types.WebsiteConfiguration{
				RedirectAllRequestsTo: &types.RedirectAllRequestsTo{
					HostName: aws.String("www.example.com"),
					Protocol: types.ProtocolHttps,
				},
			})
			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)

			rootURL := fmt.Sprintf("http://%s.%s:%d/", *bucketName, testWebsiteEndpoint, addr.Port)
			rootReq, _ := http.NewRequest("GET", rootURL, nil)
			rootResp, err := httpClient.Do(rootReq)
			require.NoError(t, err)
			defer rootResp.Body.Close()
			assert.Equal(t, http.StatusMovedPermanently, rootResp.StatusCode)
			assert.Equal(t, "https://www.example.com/", rootResp.Header.Get("Location"))

			nestedURL := fmt.Sprintf("http://%s.%s:%d/docs/page.html", *bucketName, testWebsiteEndpoint, addr.Port)
			nestedReq, _ := http.NewRequest("GET", nestedURL, nil)
			nestedResp, err := httpClient.Do(nestedReq)
			require.NoError(t, err)
			defer nestedResp.Body.Close()
			assert.Equal(t, http.StatusMovedPermanently, nestedResp.StatusCode)
			assert.Equal(t, "https://www.example.com/docs/page.html", nestedResp.Header.Get("Location"))

			headReq, _ := http.NewRequest("HEAD", nestedURL, nil)
			headResp, err := httpClient.Do(headReq)
			require.NoError(t, err)
			defer headResp.Body.Close()
			assert.Equal(t, http.StatusMovedPermanently, headResp.StatusCode)
			assert.Equal(t, "https://www.example.com/docs/page.html", headResp.Header.Get("Location"))

			customDomainURL := fmt.Sprintf("http://%s:%d/custom.html", *bucketName, addr.Port)
			customReq, _ := http.NewRequest("GET", customDomainURL, nil)
			customResp, err := httpClient.Do(customReq)
			require.NoError(t, err)
			defer customResp.Body.Close()
			assert.Equal(t, http.StatusMovedPermanently, customResp.StatusCode)
			assert.Equal(t, "https://www.example.com/custom.html", customResp.Header.Get("Location"))
		})

		t.Run("it should apply prefix routing rules before object lookup"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr, s3Client := setupWebsiteRedirectBucket(t, &types.WebsiteConfiguration{
				IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
				RoutingRules: []types.RoutingRule{{
					Condition: &types.Condition{KeyPrefixEquals: aws.String("docs/")},
					Redirect: &types.Redirect{
						ReplaceKeyPrefixWith: aws.String("documents/"),
						HttpRedirectCode:     aws.String("302"),
					},
				}},
			})
			_, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    aws.String("docs/page.html"),
				Body:   bytes.NewReader([]byte("would be served without redirect")),
			})
			require.NoError(t, err)

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s.%s:%d/docs/page.html", *bucketName, testWebsiteEndpoint, addr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusFound, resp.StatusCode)
			assert.Equal(t, "/documents/page.html", resp.Header.Get("Location"))
		})

		t.Run("it should apply 404 routing rules after failed object lookup"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr, _ := setupWebsiteRedirectBucket(t, &types.WebsiteConfiguration{
				IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
				RoutingRules: []types.RoutingRule{{
					Condition: &types.Condition{HttpErrorCodeReturnedEquals: aws.String("404")},
					Redirect: &types.Redirect{
						HostName:         aws.String("errors.example.com"),
						ReplaceKeyWith:   aws.String("not-found.html"),
						HttpRedirectCode: aws.String("307"),
					},
				}},
			})

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s.%s:%d/missing.html", *bucketName, testWebsiteEndpoint, addr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusTemporaryRedirect, resp.StatusCode)
			assert.Equal(t, "http://errors.example.com/not-found.html", resp.Header.Get("Location"))
		})

		t.Run("it should apply directory redirects before 404 routing rules"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr, s3Client := setupWebsiteRedirectBucket(t, &types.WebsiteConfiguration{
				IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
				RoutingRules: []types.RoutingRule{{
					Condition: &types.Condition{HttpErrorCodeReturnedEquals: aws.String("404")},
					Redirect: &types.Redirect{
						HostName:         aws.String("errors.example.com"),
						ReplaceKeyWith:   aws.String("not-found.html"),
						HttpRedirectCode: aws.String("307"),
					},
				}},
			})
			_, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    aws.String("docs/index.html"),
				Body:   bytes.NewReader([]byte("docs")),
			})
			require.NoError(t, err)

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s.%s:%d/docs", *bucketName, testWebsiteEndpoint, addr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusFound, resp.StatusCode)
			assert.Equal(t, "/docs/", resp.Header.Get("Location"))
		})

		t.Run("it should require both prefix and error code routing conditions"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr, s3Client := setupWebsiteRedirectBucket(t, &types.WebsiteConfiguration{
				IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
				RoutingRules: []types.RoutingRule{{
					Condition: &types.Condition{
						KeyPrefixEquals:             aws.String("docs/"),
						HttpErrorCodeReturnedEquals: aws.String("404"),
					},
					Redirect: &types.Redirect{
						ReplaceKeyWith:   aws.String("docs-missing.html"),
						HttpRedirectCode: aws.String("308"),
					},
				}},
			})
			_, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    aws.String("docs/existing.html"),
				Body:   bytes.NewReader([]byte("existing")),
			})
			require.NoError(t, err)

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			existingURL := fmt.Sprintf("http://%s.%s:%d/docs/existing.html", *bucketName, testWebsiteEndpoint, addr.Port)
			existingReq, _ := http.NewRequest("GET", existingURL, nil)
			existingResp, err := httpClient.Do(existingReq)
			require.NoError(t, err)
			defer existingResp.Body.Close()
			assert.Equal(t, http.StatusOK, existingResp.StatusCode)

			outsideURL := fmt.Sprintf("http://%s.%s:%d/other/missing.html", *bucketName, testWebsiteEndpoint, addr.Port)
			outsideReq, _ := http.NewRequest("GET", outsideURL, nil)
			outsideResp, err := httpClient.Do(outsideReq)
			require.NoError(t, err)
			defer outsideResp.Body.Close()
			assert.Equal(t, http.StatusNotFound, outsideResp.StatusCode)

			missingURL := fmt.Sprintf("http://%s.%s:%d/docs/missing.html", *bucketName, testWebsiteEndpoint, addr.Port)
			missingReq, _ := http.NewRequest("GET", missingURL, nil)
			missingResp, err := httpClient.Do(missingReq)
			require.NoError(t, err)
			defer missingResp.Body.Close()
			assert.Equal(t, http.StatusPermanentRedirect, missingResp.StatusCode)
			assert.Equal(t, "/docs-missing.html", missingResp.Header.Get("Location"))
		})

		t.Run("it should serve object level website redirects when no routing rule intercepts"+testSuffix, func(t *testing.T) {
			httpClient, listenerAddr, s3Client := setupWebsiteRedirectBucket(t, &types.WebsiteConfiguration{
				IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
				RoutingRules: []types.RoutingRule{{
					Condition: &types.Condition{KeyPrefixEquals: aws.String("docs/")},
					Redirect: &types.Redirect{
						ReplaceKeyPrefixWith: aws.String("documents/"),
						HttpRedirectCode:     aws.String("302"),
					},
				}},
			})
			_, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:                  bucketName,
				Key:                     aws.String("legacy.html"),
				Body:                    bytes.NewReader([]byte("legacy")),
				WebsiteRedirectLocation: aws.String("/new.html"),
			})
			require.NoError(t, err)

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			url := fmt.Sprintf("http://%s.%s:%d/legacy.html", *bucketName, testWebsiteEndpoint, addr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusMovedPermanently, resp.StatusCode)
			assert.Equal(t, "/new.html", resp.Header.Get("Location"))
		})

		t.Run("it should return 404 for nonexistent bucket via website endpoint"+testSuffix, func(t *testing.T) {
			_, addr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			httpClient := buildWebsiteHttpClient(addr)
			tcpAddr, _ := net.ResolveTCPAddr("tcp", addr)
			url := fmt.Sprintf("http://nonexistent.%s:%d/", testWebsiteEndpoint, tcpAddr.Port)
			req, _ := http.NewRequest("GET", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("GET / for nonexistent bucket failed: %v", err)
			}
			defer resp.Body.Close()

			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
		})
	})
}
