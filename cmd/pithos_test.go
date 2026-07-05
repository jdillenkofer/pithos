package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/pgx"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/lifecyclereconciler"
	prometheusStorageMiddleware "github.com/jdillenkofer/pithos/internal/storage/middlewares/prometheus"
	"github.com/jdillenkofer/pithos/internal/storage/outbox"
	"github.com/jdillenkofer/pithos/internal/storage/replication"
	"github.com/jdillenkofer/pithos/internal/storage/s3client"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const accessKeyId = "AKIAIOSFODNN7EXAMPLE"
const secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
const region = "eu-central-1"
const partStoreEncryptionPassword = "test"
const defaultPgContainerPoolSize = 10
const testAPIEndpoint = "s3.localhost"
const testWebsiteEndpoint = "s3-website.localhost"

var (
	bucketName  = aws.String("test")
	bucketName2 = aws.String("test2")
	keyPrefix   = aws.String("my/test/key")
	key         = aws.String(*keyPrefix + "/hello_world.txt")
	key2        = aws.String(*keyPrefix + "/hello_world2.txt")
	body        = []byte("Hello, world!")
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func customDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	// The AWS SDK sends requests to hostnames like "test.s3.localhost:PORT" (virtual host)
	// or "s3.localhost:PORT" (path style). The test server listens on 127.0.0.1:PORT.
	// We extract just the port and dial to 127.0.0.1:PORT.
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	return net.Dial(network, "127.0.0.1:"+port)
}

func buildAwsHttpClient() *awshttp.BuildableClient {
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.DialContext = customDialContext
	})
	return httpClient
}

func buildHttpClient() *http.Client {
	client := http.Client{
		Transport: &http.Transport{
			DialContext:           customDialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	return &client
}

// buildWebsiteHttpClient creates an HTTP client that always dials to the given
// listener address, regardless of the hostname in the URL. This is necessary for
// website hosting tests where the hostname (e.g., test.s3-website.localhost or
// www.example.com) must be preserved in the Host header but the connection must
// go to the test server's actual listener address.
func buildWebsiteHttpClient(listenerAddr string) *http.Client {
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial(network, listenerAddr)
			},
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	return &client
}

func buildWebsiteHttpClientNoRedirect(listenerAddr string) *http.Client {
	client := buildWebsiteHttpClient(listenerAddr)
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return client
}

func mustNoErr(err error, message string) {
	if err != nil {
		slog.Error(fmt.Sprintf("%s: %s", message, err))
		os.Exit(1)
	}
}

func mustRequestAuthorizer() authorization.RequestAuthorizer {
	authorizationCode := `
	function authorizeRequest(request)
	  return true
	end
	`
	requestAuthorizer, err := lua.NewLuaAuthorizer(authorizationCode)
	mustNoErr(err, "Could not create LuaAuthorizer")
	return requestAuthorizer
}

func setupS3Client(baseEndpoint string, listenerAddr string, usePathStyle bool) *s3.Client {
	httpClient := buildAwsHttpClient()

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region), config.WithHTTPClient(httpClient), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyId, secretAccessKey, "")))
	mustNoErr(err, "Could not loadDefaultConfig")
	addr, err := net.ResolveTCPAddr("tcp", listenerAddr)
	mustNoErr(err, "Could not resolveTcpAddr")
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s:%d", baseEndpoint, addr.Port))
	})
	return s3Client
}

func newHTTPTestServer(baseEndpoint string, requestAuthorizer authorization.RequestAuthorizer, store storage.Storage) *httptest.Server {
	credentials := []settings.Credentials{
		{
			AccessKeyId:     accessKeyId,
			SecretAccessKey: secretAccessKey,
		},
	}
	return httptest.NewServer(server.SetupServer(credentials, region, baseEndpoint, testWebsiteEndpoint, requestAuthorizer, store))
}

func setupPostgresContainer(ctx context.Context) (*postgres.PostgresContainer, error) {
	username := "postgres"
	password := "postgres"
	dbname := "postgres"
	postgresContainer, err := postgres.Run(ctx, "postgres:18.4-alpine3.24@sha256:1b1689b20d16a014a3d195653381cf2caa75a41a92d93b255a9d6ea29fd353aa",
		postgres.WithUsername(username),
		postgres.WithPassword(password),
		postgres.WithDatabase(dbname),
		postgres.BasicWaitStrategies())
	if err != nil {
		return nil, err
	}
	return postgresContainer, nil
}

type PgContainerPool struct {
	available []*postgres.PostgresContainer
	all       []*postgres.PostgresContainer
	mu        sync.Mutex
	cond      *sync.Cond
}

// NewPgContainerPool starts n containers and returns a pool whose Checkout blocks until one is available.
func NewPgContainerPool(ctx context.Context, n int) (*PgContainerPool, error) {
	p := &PgContainerPool{
		available: make([]*postgres.PostgresContainer, 0, n),
		all:       make([]*postgres.PostgresContainer, 0, n),
	}
	p.cond = sync.NewCond(&p.mu)

	for i := 0; i < n; i++ {
		pc, err := setupPostgresContainer(ctx)
		if err != nil {
			p.TerminateAll(ctx)
			return nil, err
		}
		p.all = append(p.all, pc)
		p.available = append(p.available, pc)
	}

	return p, nil
}

// Checkout blocks until a container is available or ctx is done.
func (p *PgContainerPool) Checkout(ctx context.Context) (*postgres.PostgresContainer, error) {
	pcs, err := p.CheckoutN(ctx, 1)
	if err != nil {
		return nil, err
	}
	return pcs[0], nil
}

// CheckoutN atomically reserves n containers.
// It avoids hold-and-wait deadlocks by reserving all requested containers
// in one critical section.
func (p *PgContainerPool) CheckoutN(ctx context.Context, n int) ([]*postgres.PostgresContainer, error) {
	if n <= 0 {
		return nil, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	stopCancelWatcher := make(chan struct{})
	defer close(stopCancelWatcher)
	go func() {
		select {
		case <-ctx.Done():
			p.mu.Lock()
			p.cond.Broadcast()
			p.mu.Unlock()
		case <-stopCancelWatcher:
		}
	}()

	for len(p.available) < n {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.cond.Wait()
	}

	reserved := append([]*postgres.PostgresContainer(nil), p.available[:n]...)
	p.available = p.available[n:]
	return reserved, nil
}

// Return puts a container back into the pool.
func (p *PgContainerPool) Return(pc *postgres.PostgresContainer) error {
	// Basic safety: don't return nil
	if pc == nil {
		return errors.New("nil container")
	}
	p.mu.Lock()
	p.available = append(p.available, pc)
	p.mu.Unlock()
	p.cond.Broadcast()
	return nil
}

// TerminateAll stops all containers.
func (p *PgContainerPool) TerminateAll(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pc := range p.all {
		_ = pc.Container.Terminate(ctx)
	}
	p.available = nil
	p.cond.Broadcast()
}

// create a db pool with max 5 connections
var pgContainerPool *PgContainerPool
var pgContainerPoolOnce sync.Once
var pgContainerPoolErr error

func getPgContainerPool() (*PgContainerPool, error) {
	pgContainerPoolOnce.Do(func() {
		pgContainerPool, pgContainerPoolErr = NewPgContainerPool(context.Background(), defaultPgContainerPoolSize)
	})
	return pgContainerPool, pgContainerPoolErr
}

func cleanPublicDatabaseSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	_, err = tx.Exec("DROP SCHEMA public CASCADE")
	if err != nil {
		return err
	}
	_, err = tx.Exec("CREATE SCHEMA public AUTHORIZATION postgres")
	if err != nil {
		return err
	}
	_, err = tx.Exec("GRANT ALL ON SCHEMA public TO postgres")
	if err != nil {
		return err
	}
	_, err = tx.Exec("GRANT ALL ON SCHEMA public TO public")
	if err != nil {
		return err
	}
	_, err = tx.Exec("COMMENT ON SCHEMA public IS 'standard public schema'")
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

type postgresContainerLease struct {
	pool      *PgContainerPool
	container *postgres.PostgresContainer
}

func checkoutPostgresContainerLease(ctx context.Context) (*postgresContainerLease, error) {
	pool, err := getPgContainerPool()
	if err != nil {
		return nil, err
	}
	container, err := pool.Checkout(ctx)
	if err != nil {
		return nil, err
	}
	return &postgresContainerLease{
		pool:      pool,
		container: container,
	}, nil
}

func (l *postgresContainerLease) returnToPool() {
	err := l.pool.Return(l.container)
	if err != nil {
		slog.Error("Could not return pgContainer to pool", slog.String("error", err.Error()))
	}
}

func (l *postgresContainerLease) openDatabase(ctx context.Context) (database.Database, error) {
	dbURL, err := l.container.ConnectionString(ctx)
	if err != nil {
		return nil, err
	}
	return pgx.OpenDatabase(dbURL, nil)
}

func (l *postgresContainerLease) cleanup(ctx context.Context) {
	defer l.returnToPool()

	dbURL, err := l.container.ConnectionString(ctx)
	if err != nil {
		slog.Error("Could not get connection string from pgContainer", slog.String("error", err.Error()))
		return
	}
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		slog.Error("Could not open db to clean public schema", slog.String("error", err.Error()))
		return
	}
	defer func() {
		err := db.Close()
		if err != nil {
			slog.Error("Could not close db after cleaning public schema", slog.String("error", err.Error()))
		}
	}()

	err = cleanPublicDatabaseSchema(ctx, db)
	if err != nil {
		slog.Error("Could not clean public schema", slog.String("error", err.Error()))
	}
}

func setupDatabaseFromPostgresContainer(ctx context.Context, pgContainer *postgres.PostgresContainer) (db database.Database, cleanup func(), err error) {
	lease := &postgresContainerLease{
		pool:      pgContainerPool,
		container: pgContainer,
	}

	db, err = lease.openDatabase(ctx)
	if err != nil {
		lease.returnToPool()
		return nil, func() {}, err
	}
	cleanup = func() {
		lease.cleanup(ctx)
	}
	return db, cleanup, err
}

func setupDatabase(ctx context.Context, dbType database.DatabaseType, storagePath string) (db database.Database, cleanup func(), err error) {
	switch dbType {
	case database.DB_TYPE_SQLITE:
		dbPath := filepath.Join(storagePath, "pithos.db")
		cleanup = func() {}
		db, err = sqlite.OpenDatabase(dbPath)
		return db, cleanup, err
	case database.DB_TYPE_POSTGRES:
		lease, err := checkoutPostgresContainerLease(ctx)
		if err != nil {
			return nil, cleanup, err
		}
		db, err = lease.openDatabase(ctx)
		if err != nil {
			lease.returnToPool()
			return nil, cleanup, err
		}
		cleanup = func() {
			lease.cleanup(ctx)
		}
		return db, cleanup, nil
	}
	return nil, cleanup, errors.ErrUnsupported
}

type testDatabases struct {
	primary          database.Database
	primaryCleanup   func()
	secondary        database.Database
	secondaryCleanup func()
}

type cleanupRegistrar func(func())

func addDatabaseCleanup(add cleanupRegistrar, db database.Database, dbCleanup func(), closeErrMessage string) {
	add(func() {
		err := db.Close()
		mustNoErr(err, closeErrMessage)
		if dbCleanup != nil {
			dbCleanup()
		}
	})
}

func mustTempDir(add cleanupRegistrar, pattern string) string {
	path, err := os.MkdirTemp("", pattern)
	mustNoErr(err, "Could not create temp directory")
	add(func() {
		err := os.RemoveAll(path)
		mustNoErr(err, fmt.Sprintf("Could not remove storagePath %s", path))
	})
	return path
}

func setupTestDatabases(ctx context.Context, dbType database.DatabaseType, useReplication bool, storagePath string, storagePath2 string) (testDatabases, error) {
	result := testDatabases{}

	if dbType == database.DB_TYPE_POSTGRES && useReplication {
		pgContainerPool, err := getPgContainerPool()
		if err != nil {
			return result, err
		}

		containers, err := pgContainerPool.CheckoutN(ctx, 2)
		if err != nil {
			return result, err
		}

		result.primary, result.primaryCleanup, err = setupDatabaseFromPostgresContainer(ctx, containers[0])
		if err != nil {
			return result, err
		}

		result.secondary, result.secondaryCleanup, err = setupDatabaseFromPostgresContainer(ctx, containers[1])
		if err != nil {
			return result, err
		}
		return result, nil
	}

	var err error
	result.primary, result.primaryCleanup, err = setupDatabase(ctx, dbType, storagePath)
	if err != nil {
		return result, err
	}

	if useReplication {
		result.secondary, result.secondaryCleanup, err = setupDatabase(ctx, dbType, storagePath2)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

func setupReplicatedStorage(ctx context.Context, registry *prometheus.Registry, baseEndpoint string, primaryListenerAddr string, usePathStyle bool, db2 database.Database, storagePath2 string, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, encryptionPassword string, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) (storage.Storage, error) {
	localStore := storageFactory.CreateStorage(storagePath2, db2, useFilesystemPartStore, usePartStoreCompression, encryptionType, encryptionPassword, wrapPartStoreWithOutbox, registry)

	primaryS3Client := setupS3Client(baseEndpoint, primaryListenerAddr, usePathStyle)
	s3ClientStorage, err := s3client.NewStorage(primaryS3Client)
	if err != nil {
		return nil, err
	}

	storageOutboxEntryRepository, err := repositoryFactory.NewStorageOutboxEntryRepository(db2)
	if err != nil {
		return nil, err
	}

	outboxStorage, err := outbox.NewStorage(db2, "default", s3ClientStorage, storageOutboxEntryRepository, registry, 30*time.Second)
	if err != nil {
		return nil, err
	}

	store2, err := replication.NewStorage(localStore, outboxStorage)
	if err != nil {
		return nil, err
	}

	store2, err = prometheusStorageMiddleware.NewStorageMiddleware(store2, registry)
	if err != nil {
		return nil, err
	}

	err = store2.Start(ctx)
	if err != nil {
		return nil, err
	}

	return store2, nil
}

func setupTestServer(dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) (s3Client *s3.Client, listenerAddr string, cleanup func()) {
	return setupTestServerWithAuthorizer(mustRequestAuthorizer(), dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
}

func setupTestServerWithAuthorizer(requestAuthorizer authorization.RequestAuthorizer, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) (s3Client *s3.Client, listenerAddr string, cleanup func()) {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	cleanups := make([]func(), 0, 8)
	addCleanup := func(fn func()) {
		cleanups = append(cleanups, fn)
	}

	storagePath := mustTempDir(addCleanup, "pithos-test-data-")

	storagePath2 := ""
	if useReplication {
		storagePath2 = mustTempDir(addCleanup, "pithos-test-data-")
	}

	baseEndpoint := testAPIEndpoint

	dbs, err := setupTestDatabases(ctx, dbType, useReplication, storagePath, storagePath2)
	mustNoErr(err, "Couldn't set up test databases")

	addDatabaseCleanup(addCleanup, dbs.primary, dbs.primaryCleanup, "Couldn't close database")

	encryptionPassword := ""
	if encryptionType != storageFactory.EncryptionTypeNone {
		encryptionPassword = partStoreEncryptionPassword
	}
	store := storageFactory.CreateStorage(storagePath, dbs.primary, useFilesystemPartStore, usePartStoreCompression, encryptionType, encryptionPassword, wrapPartStoreWithOutbox, registry)

	if !useReplication {
		store, err = prometheusStorageMiddleware.NewStorageMiddleware(store, registry)
		mustNoErr(err, "Could not create prometheusStorageMiddleware")
	}

	err = store.Start(ctx)
	mustNoErr(err, "Couldn't start storage")
	addCleanup(func() {
		err := store.Stop(ctx)
		mustNoErr(err, "Couldn't stop storage")
	})

	primaryTS := newHTTPTestServer(baseEndpoint, requestAuthorizer, store)
	ts := primaryTS

	if useReplication {
		addCleanup(func() {
			primaryTS.Close()
		})

		addDatabaseCleanup(addCleanup, dbs.secondary, dbs.secondaryCleanup, "Couldn't close secondary database")

		store2, err := setupReplicatedStorage(ctx, registry, baseEndpoint, primaryTS.Listener.Addr().String(), usePathStyle, dbs.secondary, storagePath2, useFilesystemPartStore, encryptionType, encryptionPassword, wrapPartStoreWithOutbox, usePartStoreCompression)
		mustNoErr(err, "Couldn't set up replicated storage")
		addCleanup(func() {
			err := store2.Stop(ctx)
			mustNoErr(err, "Couldn't stop replicated storage")
		})

		ts = newHTTPTestServer(baseEndpoint, requestAuthorizer, store2)
	}
	addCleanup(func() {
		ts.Close()
	})

	listenerAddr = ts.Listener.Addr().String()
	s3Client = setupS3Client(baseEndpoint, listenerAddr, usePathStyle)

	cleanup = func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	return
}

func runIntegrationTest(t *testing.T, testFunc func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool)) {
	testutils.SkipIfNotIntegration(t)

	// Determine selected DB type
	var selectedDBType database.DatabaseType
	dbTypeSuffix := ""
	switch *testutils.DBType {
	case "postgres":
		selectedDBType = database.DB_TYPE_POSTGRES
		dbTypeSuffix = " using postgres"
		// Only skip on Windows in GitHub Actions if Postgres is used
		testutils.SkipOnWindowsInGitHubActions(t)
		testutils.SkipOnMacOSInGitHubActions(t)
	default:
		selectedDBType = database.DB_TYPE_SQLITE
		dbTypeSuffix = " using sqlite"
	}

	// Determine path style
	var usePathStyle bool
	pathStyleSuffix := ""
	switch *testutils.PathStyle {
	case "path":
		usePathStyle = true
		pathStyleSuffix = " using path style"
	default:
		usePathStyle = false
		pathStyleSuffix = " using host style"
	}

	// Determine replication
	var useReplication bool
	replicationSuffix := ""
	switch *testutils.ReplMode {
	case "replicated":
		useReplication = true
		replicationSuffix = " replicated"
	default:
		useReplication = false
		replicationSuffix = ""
	}

	// Determine part store
	var useFilesystemPartStore bool
	partStoreSuffix := ""
	switch *testutils.PartStore {
	case "filesystem":
		useFilesystemPartStore = true
		partStoreSuffix = " with filesystemPartStore"
	default:
		useFilesystemPartStore = false
		partStoreSuffix = " with sqlPartStore"
	}

	// Determine encryption type
	var encryptionType storageFactory.EncryptionType
	encryptSuffix := ""
	switch *testutils.PartStoreEncryption {
	case "tink":
		encryptionType = storageFactory.EncryptionTypeTink
		encryptSuffix = " (tink encryption)"
	default:
		encryptionType = storageFactory.EncryptionTypeNone
		encryptSuffix = ""
	}

	baseSuffix := dbTypeSuffix + pathStyleSuffix + replicationSuffix + partStoreSuffix + encryptSuffix

	usePartStoreCompression := encryptionType != storageFactory.EncryptionTypeNone

	for _, wrapPartStoreWithOutbox := range []bool{false, true} {
		testSuffix := baseSuffix
		if wrapPartStoreWithOutbox {
			testSuffix += " (using transactional outbox)"
		}
		testFunc(t, testSuffix, selectedDBType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
	}
}

func TestCreateBucket(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should create a bucket"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			assert.Equal(t, bucketName, createBucketResult.Location)
		})

		t.Run("it should not be able to create the same bucket twice"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})

			if err == nil {
				assert.Fail(t, "CreateBucket should failed when reusing the same bucket name")
			}

			var bucketAlreadyExistsError *types.BucketAlreadyExists
			if !errors.As(err, &bucketAlreadyExistsError) {
				assert.Fail(t, "Expected aws error BucketAlreadyExists", "err %v", err)
			}
		})
	})
}

func TestHeadBucket(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should be able to see an existing bucket"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}

			assert.NotNil(t, createBucketResult)
			headBucketResult, err := s3Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
				Bucket: bucketName,
			})

			if err != nil {
				assert.Fail(t, "HeadBucket failed", "err %v", err)
			}
			assert.NotNil(t, headBucketResult)
		})
	})
}

func TestListBuckets(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should be able to list all buckets"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			listBucketsResult, err := s3Client.ListBuckets(context.Background(), &s3.ListBucketsInput{})

			if err != nil {
				assert.Fail(t, "ListBuckets failed", "err %v", err)
			}

			assert.Len(t, listBucketsResult.Buckets, 1)
			assert.Equal(t, bucketName, listBucketsResult.Buckets[0].Name)
			assert.NotNil(t, listBucketsResult.Buckets[0].CreationDate)
			assert.True(t, listBucketsResult.Buckets[0].CreationDate.Before(time.Now()))
		})

		t.Run("it should list all buckets"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createBucketResult2, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName2,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult2)

			listBucketResult, err := s3Client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
			if err != nil {
				assert.Fail(t, "ListBuckets failed", "err %v", err)
			}
			assert.Len(t, listBucketResult.Buckets, 2)
		})
	})
}

func TestPutObject(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should allow uploading an object"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
		})

		for _, checksumAlgorithm := range []types.ChecksumAlgorithm{"CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"} {
			t.Run("it should allow uploading an object with checksumAlgorithm "+string(checksumAlgorithm)+testSuffix, func(t *testing.T) {
				s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
				t.Cleanup(cleanup)
				createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
					Bucket: bucketName,
				})
				if err != nil {
					assert.Fail(t, "CreateBucket failed", "err %v", err)
				}
				assert.NotNil(t, createBucketResult)

				putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
					Bucket:            bucketName,
					Body:              bytes.NewReader([]byte("Hello, first object!")),
					Key:               key,
					ChecksumAlgorithm: checksumAlgorithm,
				})
				if err != nil {
					assert.Fail(t, "PutObject failed", "err %v", err)
				}
				assert.NotNil(t, putObjectResult)
				assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *putObjectResult.ETag)
				assert.Equal(t, "Bjck3A==", *putObjectResult.ChecksumCRC32)
				assert.Equal(t, "H7cZCA==", *putObjectResult.ChecksumCRC32C)
				assert.Equal(t, "4SEgZkEEyhY=", *putObjectResult.ChecksumCRC64NVME)
				assert.Equal(t, "lMCBYNtqPCnP3avKVUtqfrThqHo=", *putObjectResult.ChecksumSHA1)
				assert.Equal(t, "sctyzI/H+7x/oVR7Gwt7NiQ7kop4Ua/7SrVraELVDpI=", *putObjectResult.ChecksumSHA256)
				assert.Equal(t, types.ChecksumTypeFullObject, putObjectResult.ChecksumType)
			})
		}

		t.Run("it should allow uploading an object with a presigned url"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			presignClient := s3.NewPresignClient(s3Client)
			body := []byte("Hello, first object!")
			presignedRequest, err := presignClient.PresignPutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)

			httpClient := buildHttpClient()
			request, err := http.NewRequest(presignedRequest.Method, presignedRequest.URL, bytes.NewReader(body))
			assert.Nil(t, err)
			putObjectResult, err := httpClient.Do(request)
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
			assert.Equal(t, 200, putObjectResult.StatusCode)
		})

		t.Run("it should allow uploading an object with a presigned url and preprovided body"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			presignClient := s3.NewPresignClient(s3Client)
			body := []byte("Hello, first object!")
			presignedRequest, err := presignClient.PresignPutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			assert.Nil(t, err)

			httpClient := buildHttpClient()
			request, err := http.NewRequest(presignedRequest.Method, presignedRequest.URL, bytes.NewReader(body))
			request.Header.Add("Content-Type", presignedRequest.SignedHeader.Get("Content-Type"))
			assert.Nil(t, err)
			putObjectResult, err := httpClient.Do(request)
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
			assert.Equal(t, 200, putObjectResult.StatusCode)
		})

		t.Run("it should allow uploading an object a second time"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
		})

		t.Run("it should allow uploads with if-none-match when object is absent"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:      bucketName,
				Body:        bytes.NewReader([]byte("Hello, first object!")),
				Key:         key,
				IfNoneMatch: aws.String("*"),
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
		})

		t.Run("it should reject uploads with if-none-match when object already exists"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:      bucketName,
				Body:        bytes.NewReader([]byte("Hello, first object!")),
				Key:         key,
				IfNoneMatch: aws.String("*"),
			})

			if err == nil {
				assert.Fail(t, "PutObject should fail when If-None-Match is set and object exists")
			}

			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "PreconditionFailed" {
				assert.Fail(t, "Expected aws error PreconditionFailed", "err %v", err)
			}
		})

		t.Run("it should reject non-star if-none-match values"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:      bucketName,
				Body:        bytes.NewReader(body),
				Key:         key,
				IfNoneMatch: aws.String("\"invalid-etag\""),
			})

			if err == nil {
				assert.Fail(t, "PutObject should fail when If-None-Match is not '*'")
			}

			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "InvalidRequest" {
				assert.Fail(t, "Expected aws error InvalidRequest", "err %v", err)
			}
		})

		t.Run("it should allow at most one concurrent create-if-absent upload"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			var wg sync.WaitGroup
			start := make(chan struct{})
			results := make([]*s3.PutObjectOutput, 2)
			errs := make([]error, 2)

			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					results[i], errs[i] = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
						Bucket:      bucketName,
						Body:        bytes.NewReader(body),
						Key:         key,
						IfNoneMatch: aws.String("*"),
					})
				}(i)
			}

			close(start)
			wg.Wait()

			successCount := 0
			for i := range 2 {
				if errs[i] == nil {
					successCount++
					assert.NotNil(t, results[i])
					continue
				}

				var apiErr smithy.APIError
				if !errors.As(errs[i], &apiErr) {
					assert.Fail(t, "Expected smithy.APIError", "err %v", errs[i])
					continue
				}
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}

			assert.LessOrEqual(t, successCount, 1, "at most one concurrent create-if-absent upload may succeed")
		})

		t.Run("it should allow if-match when etag matches"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			initialPutObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, initialPutObjectResult)
			assert.NotNil(t, initialPutObjectResult.ETag)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Body:    bytes.NewReader(body),
				Key:     key,
				IfMatch: initialPutObjectResult.ETag,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
		})

		t.Run("it should reject if-match when etag mismatches"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Body:    bytes.NewReader(body),
				Key:     key,
				IfMatch: aws.String("\"does-not-match\""),
			})

			if err == nil {
				assert.Fail(t, "PutObject should fail when If-Match does not match current object ETag")
			}

			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "PreconditionFailed" {
				assert.Fail(t, "Expected aws error PreconditionFailed", "err %v", err)
			}
		})

		t.Run("it should reject if-match when object is missing"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Body:    bytes.NewReader(body),
				Key:     key,
				IfMatch: aws.String("\"missing-etag\""),
			})

			if err == nil {
				assert.Fail(t, "PutObject should fail when If-Match is set but object does not exist")
			}

			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "PreconditionFailed" {
				assert.Fail(t, "Expected aws error PreconditionFailed", "err %v", err)
			}
		})

		t.Run("it should reject stale if-match updates during concurrent writes"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			initialPutObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, initialPutObjectResult)
			assert.NotNil(t, initialPutObjectResult.ETag)

			var wg sync.WaitGroup
			start := make(chan struct{})
			results := make([]*s3.PutObjectOutput, 2)
			errs := make([]error, 2)

			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					results[i], errs[i] = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
						Bucket:  bucketName,
						Body:    bytes.NewReader(body),
						Key:     key,
						IfMatch: initialPutObjectResult.ETag,
					})
				}(i)
			}

			close(start)
			wg.Wait()

			successCount := 0
			for i := range 2 {
				if errs[i] == nil {
					successCount++
					assert.NotNil(t, results[i])
					continue
				}

				var apiErr smithy.APIError
				if !errors.As(errs[i], &apiErr) {
					assert.Fail(t, "Expected smithy.APIError", "err %v", errs[i])
					continue
				}
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}

			assert.LessOrEqual(t, successCount, 1, "at most one concurrent If-Match update with the same stale ETag may succeed")
		})

		t.Run("it should reject putobject when if-match and if-none-match are both set"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:      bucketName,
				Body:        bytes.NewReader(body),
				Key:         key,
				IfMatch:     aws.String("\"etag\""),
				IfNoneMatch: aws.String("*"),
			})

			if err == nil {
				assert.Fail(t, "PutObject should fail when If-Match and If-None-Match are both set")
			}

			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "InvalidRequest" {
				assert.Fail(t, "Expected aws error InvalidRequest", "err %v", err)
			}
		})

		t.Run("it should hit the upload limit when uploading an object that is too large"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			// Use a LimitReader over a repeating source to simulate a large payload without allocating 5GB of RAM
			largePayload := io.LimitReader(ioutils.NewRepeatingReader([]byte("huge")), storage.MaxEntitySize+1)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   largePayload,
				Key:    key,
			})
			assert.Nil(t, putObjectResult)
			if err == nil {
				assert.Fail(t, "PutObject should have failed due to size limit")
			}
			var smithyOperationError *smithy.OperationError
			if !errors.As(err, &smithyOperationError) {
				assert.Fail(t, "Expected error smithy.OperationError", "err %v", err)
			}
		})
	})
}

func TestPutObjectWithTrailingChecksum(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		payload := []byte("Hello, trailing checksum!")
		checksumBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(checksumBytes, crc32.ChecksumIEEE(payload))
		payloadChecksum := base64.StdEncoding.EncodeToString(checksumBytes)

		// buildTrailerBody encodes the payload in the aws-chunked format used
		// with STREAMING-UNSIGNED-PAYLOAD-TRAILER: a single unsigned chunk,
		// the zero-length chunk, and the checksum trailer.
		buildTrailerBody := func(trailerName string, trailerValue string) string {
			return fmt.Sprintf("%x\r\n%s\r\n0\r\n%s:%s\r\n\r\n", len(payload), payload, trailerName, trailerValue)
		}

		putWithTrailer := func(t *testing.T, listenerAddr string, declaredTrailer string, body string) *http.Response {
			addr, err := net.ResolveTCPAddr("tcp", listenerAddr)
			require.NoError(t, err)
			url := fmt.Sprintf("http://%s:%d/%s/%s", testAPIEndpoint, addr.Port, *bucketName, *key)
			request, err := http.NewRequest(http.MethodPut, url, strings.NewReader(body))
			require.NoError(t, err)
			request.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
			request.Header.Set("content-encoding", "aws-chunked")
			request.Header.Set("x-amz-decoded-content-length", strconv.Itoa(len(payload)))
			request.Header.Set("x-amz-trailer", declaredTrailer)

			signer := v4.NewSigner()
			err = signer.SignHTTP(context.Background(), aws.Credentials{AccessKeyID: accessKeyId, SecretAccessKey: secretAccessKey}, request, "STREAMING-UNSIGNED-PAYLOAD-TRAILER", "s3", region, time.Now().UTC())
			require.NoError(t, err)

			response, err := buildHttpClient().Do(request)
			require.NoError(t, err)
			return response
		}

		t.Run("it should accept an aws-chunked upload with a valid trailing checksum"+testSuffix, func(t *testing.T) {
			s3Client, listenerAddr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			require.NoError(t, err)

			response := putWithTrailer(t, listenerAddr, "x-amz-checksum-crc32", buildTrailerBody("x-amz-checksum-crc32", payloadChecksum))
			assert.Equal(t, 200, response.StatusCode)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			require.NoError(t, err)
			data, err := io.ReadAll(getObjectResult.Body)
			require.NoError(t, err)
			assert.Equal(t, payload, data)
		})

		t.Run("it should reject an aws-chunked upload with a corrupted trailing checksum"+testSuffix, func(t *testing.T) {
			s3Client, listenerAddr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			require.NoError(t, err)

			response := putWithTrailer(t, listenerAddr, "x-amz-checksum-crc32", buildTrailerBody("x-amz-checksum-crc32", "AAAAAA=="))
			assert.Equal(t, 400, response.StatusCode)
			responseBody, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			assert.Contains(t, string(responseBody), "BadDigest")
		})

		t.Run("it should reject an aws-chunked upload whose trailer does not match the declared x-amz-trailer"+testSuffix, func(t *testing.T) {
			s3Client, listenerAddr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			require.NoError(t, err)

			response := putWithTrailer(t, listenerAddr, "x-amz-checksum-crc32", buildTrailerBody("x-amz-checksum-sha256", payloadChecksum))
			assert.Equal(t, 400, response.StatusCode)
			responseBody, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			assert.Contains(t, string(responseBody), "MalformedTrailerError")
		})
	})
}

func TestCompleteMultipartUploadPartVerification(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		// setupUpload creates a bucket and a multipart upload with two
		// uploaded parts and returns the parts as the client would declare
		// them in CompleteMultipartUpload.
		setupUpload := func(t *testing.T) (*s3.Client, *string, []types.CompletedPart) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			require.NoError(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})
			require.NoError(t, err)

			completedParts := make([]types.CompletedPart, 0, 2)
			for partNumber := int32(1); partNumber <= 2; partNumber++ {
				uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
					Bucket:     bucketName,
					Key:        key,
					UploadId:   createResult.UploadId,
					PartNumber: aws.Int32(partNumber),
					Body:       bytes.NewReader([]byte(fmt.Sprintf("part %d content", partNumber))),
				})
				require.NoError(t, err)
				completedParts = append(completedParts, types.CompletedPart{
					ETag:       uploadPartResult.ETag,
					PartNumber: aws.Int32(partNumber),
				})
			}
			return s3Client, createResult.UploadId, completedParts
		}

		completeUpload := func(s3Client *s3.Client, uploadId *string, parts []types.CompletedPart) error {
			_, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: parts,
				},
			})
			return err
		}

		assertApiError := func(t *testing.T, err error, expectedCode string) {
			assert.Error(t, err)
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) {
				assert.Equal(t, expectedCode, apiErr.ErrorCode())
			} else {
				assert.Fail(t, "Expected API error", "err %v", err)
			}
		}

		t.Run("it should reject completing a multipart upload with a wrong part etag"+testSuffix, func(t *testing.T) {
			s3Client, uploadId, completedParts := setupUpload(t)
			completedParts[1].ETag = aws.String("\"00000000000000000000000000000000\"")
			err := completeUpload(s3Client, uploadId, completedParts)
			assertApiError(t, err, "InvalidPart")
		})

		t.Run("it should reject completing a multipart upload with a wrong part checksum"+testSuffix, func(t *testing.T) {
			s3Client, uploadId, completedParts := setupUpload(t)
			completedParts[1].ChecksumCRC32 = aws.String("AAAAAA==")
			err := completeUpload(s3Client, uploadId, completedParts)
			assertApiError(t, err, "InvalidPart")
		})

		t.Run("it should reject completing a multipart upload referencing a part that was never uploaded"+testSuffix, func(t *testing.T) {
			s3Client, uploadId, completedParts := setupUpload(t)
			completedParts[1].PartNumber = aws.Int32(5)
			err := completeUpload(s3Client, uploadId, completedParts)
			assertApiError(t, err, "InvalidPart")
		})

		t.Run("it should reject completing a multipart upload that omits an uploaded part"+testSuffix, func(t *testing.T) {
			s3Client, uploadId, completedParts := setupUpload(t)
			err := completeUpload(s3Client, uploadId, completedParts[:1])
			assertApiError(t, err, "InvalidPart")
		})

		t.Run("it should reject completing a multipart upload with parts out of order"+testSuffix, func(t *testing.T) {
			s3Client, uploadId, completedParts := setupUpload(t)
			err := completeUpload(s3Client, uploadId, []types.CompletedPart{completedParts[1], completedParts[0]})
			assertApiError(t, err, "InvalidPartOrder")
		})

		t.Run("it should complete a multipart upload with matching etags and checksums"+testSuffix, func(t *testing.T) {
			s3Client, uploadId, completedParts := setupUpload(t)
			err := completeUpload(s3Client, uploadId, completedParts)
			assert.NoError(t, err)
		})
	})
}

func TestMultipartUpload(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should allow multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)

			completeMultipartUploadResult, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult)
			assert.Equal(t, bucketName, completeMultipartUploadResult.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult.Key)

			listObjectResult, err = s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, key, listObjectResult.Contents[0].Key)
			assert.True(t, strings.HasSuffix(*completeMultipartUploadResult.ETag, "-"+strconv.Itoa(1)+"\""))
			assert.Equal(t, completeMultipartUploadResult.ETag, listObjectResult.Contents[0].ETag)
			assert.False(t, *listObjectResult.IsTruncated)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		// We need a large enough payload to force a multipart upload
		var largePayload []byte = make([]byte, manager.MinUploadPartSize*2)
		r := rand.New(rand.NewSource(int64(1337)))
		r.Read(largePayload)

		for _, checksumAlgorithm := range []types.ChecksumAlgorithm{"CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"} {
			t.Run("it should allow multipart uploads using checksumAlgorithm "+string(checksumAlgorithm)+testSuffix, func(t *testing.T) {
				s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
				t.Cleanup(cleanup)
				createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
					Bucket: bucketName,
				})
				if err != nil {
					assert.Fail(t, "CreateBucket failed", "err %v", err)
				}
				assert.NotNil(t, createBucketResult)

				uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
					u.Concurrency = 1
					u.PartSize = manager.MinUploadPartSize
				})
				uploadOutput, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
					Bucket:            bucketName,
					Key:               key,
					Body:              bytes.NewReader(largePayload),
					ChecksumAlgorithm: checksumAlgorithm,
				})
				if err != nil {
					assert.Fail(t, "Upload failed", "err %v", err)
				}
				assert.NotNil(t, uploadOutput)
				assert.Equal(t, key, uploadOutput.Key)
				assert.Equal(t, 2, len(uploadOutput.CompletedParts))

				firstPart := uploadOutput.CompletedParts[0]
				assert.Equal(t, "\"32571c347a10c52443114d7adccdda7e\"", *firstPart.ETag)
				assert.Equal(t, "ofCoFg==", *firstPart.ChecksumCRC32)
				assert.Equal(t, "Abkn5A==", *firstPart.ChecksumCRC32C)
				assert.Equal(t, "aNFz0eBLamw=", *firstPart.ChecksumCRC64NVME)
				assert.Equal(t, "vaP4YsHAheW1JNwC9QjaFR+Mgxs=", *firstPart.ChecksumSHA1)
				assert.Equal(t, "U6bAqRNZMqgKtspwYKBPvggn3jo8VmDB1YzZ+0Vf+KM=", *firstPart.ChecksumSHA256)
				assert.Equal(t, int32(1), *firstPart.PartNumber)

				secondPart := uploadOutput.CompletedParts[1]
				assert.Equal(t, "\"ddcce51e31a68a3268905f2794b0bbb4\"", *secondPart.ETag)
				assert.Equal(t, "HUzpnQ==", *secondPart.ChecksumCRC32)
				assert.Equal(t, "2FK6Vg==", *secondPart.ChecksumCRC32C)
				assert.Equal(t, "gBCc/dz5qRU=", *secondPart.ChecksumCRC64NVME)
				assert.Equal(t, "/OkGPn2ayY9pA2v/JEo+B0hsUpg=", *secondPart.ChecksumSHA1)
				assert.Equal(t, "drUohCpA8EjWtWwhvmxeNp6G/cb8/Y3X6h6FnCZs3Bk=", *secondPart.ChecksumSHA256)
				assert.Equal(t, int32(2), *secondPart.PartNumber)

				assert.Equal(t, "\"b676ed737ae82cda0bc622cd80116002-2\"", *uploadOutput.ETag)
				assert.Equal(t, "ICnSTA==", *uploadOutput.ChecksumCRC32)
				assert.Equal(t, "wHOQSg==", *uploadOutput.ChecksumCRC32C)
				assert.Equal(t, "hJdk5JLZLJk=", *uploadOutput.ChecksumCRC64NVME)
				assert.Nil(t, uploadOutput.ChecksumSHA1)
				assert.Nil(t, uploadOutput.ChecksumSHA256)

				getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
					Bucket: bucketName,
					Key:    key,
				})
				if err != nil {
					assert.Fail(t, "GetObject failed", "err %v", err)
				}
				assert.NotNil(t, getObjectResult)
				assert.NotNil(t, getObjectResult.Body)
				objectBytes, err := io.ReadAll(getObjectResult.Body)
				assert.Nil(t, err)
				assert.Equal(t, largePayload, objectBytes)
			})
		}

		t.Run("it should allow listing multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			listMultipartUploadsResult, err := s3Client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
				Bucket: bucketName,
				Prefix: keyPrefix,
			})
			if err != nil {
				assert.Fail(t, "ListMultipartUploads failed", "err %v", err)
			}

			assert.NotNil(t, listMultipartUploadsResult)
			assert.Equal(t, *bucketName, *listMultipartUploadsResult.Bucket)
			assert.Equal(t, *keyPrefix, *listMultipartUploadsResult.Prefix)
			assert.Len(t, listMultipartUploadsResult.Uploads, 1)
			firstUpload := listMultipartUploadsResult.Uploads[0]
			assert.Equal(t, *key, *firstUpload.Key)
			assert.Equal(t, *uploadId, *firstUpload.UploadId)
			assert.NotNil(t, firstUpload.Initiated)
		})

		t.Run("it should allow listing two multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			createMultiPartUploadResult2, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key2,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload2 failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult2)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult2.Bucket)
			assert.Equal(t, *key2, *createMultiPartUploadResult2.Key)
			uploadId2 := createMultiPartUploadResult2.UploadId
			assert.NotNil(t, uploadId2)

			listMultipartUploadsResult, err := s3Client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
				Bucket: bucketName,
				Prefix: keyPrefix,
			})
			if err != nil {
				assert.Fail(t, "ListMultipartUploads failed", "err %v", err)
			}

			assert.NotNil(t, listMultipartUploadsResult)
			assert.Equal(t, *bucketName, *listMultipartUploadsResult.Bucket)
			assert.Equal(t, *keyPrefix, *listMultipartUploadsResult.Prefix)
			assert.Len(t, listMultipartUploadsResult.Uploads, 2)
			firstUpload := listMultipartUploadsResult.Uploads[0]
			assert.Equal(t, *key, *firstUpload.Key)
			assert.Equal(t, *uploadId, *firstUpload.UploadId)
			assert.NotNil(t, firstUpload.Initiated)
			secondUpload := listMultipartUploadsResult.Uploads[1]
			assert.Equal(t, *key2, *secondUpload.Key)
			assert.Equal(t, *uploadId2, *secondUpload.UploadId)
			assert.NotNil(t, secondUpload.Initiated)
		})

		t.Run("it should filter listed multipart uploads via authorizer hook"+testSuffix, func(t *testing.T) {
			authorizationCode := `
			function authorizeRequest(request)
			  return true
			end

			function authorizeListMultipartUpload(request, key, uploadId)
			  return string.find(key, "allowed/") == 1
			end
			`
			requestAuthorizer, err := lua.NewLuaAuthorizer(authorizationCode)
			if err != nil {
				t.Fatalf("Could not create LuaAuthorizer: %v", err)
			}

			s3Client, _, cleanup := setupTestServerWithAuthorizer(requestAuthorizer, dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: aws.String("allowed/file.txt")})
			assert.Nil(t, err)
			_, err = s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: aws.String("denied/file.txt")})
			assert.Nil(t, err)

			listMultipartUploadsResult, err := s3Client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Len(t, listMultipartUploadsResult.Uploads, 1)
			assert.Equal(t, "allowed/file.txt", *listMultipartUploadsResult.Uploads[0].Key)
		})

		t.Run("it should allow multipart uploads with two parts"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			completeMultipartUploadResult, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult)
			assert.Equal(t, bucketName, completeMultipartUploadResult.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult.Key)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, key, listObjectResult.Contents[0].Key)
			assert.True(t, strings.HasSuffix(*completeMultipartUploadResult.ETag, "-"+strconv.Itoa(2)+"\""))
			assert.Equal(t, completeMultipartUploadResult.ETag, listObjectResult.Contents[0].ETag)
			assert.False(t, *listObjectResult.IsTruncated)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should allow listing parts of multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			// listParts (but empty)
			listPartsResult, err := s3Client.ListParts(context.Background(), &s3.ListPartsInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "ListParts failed", "err %v", err)
			}
			assert.NotNil(t, listPartsResult)
			assert.Equal(t, *bucketName, *listPartsResult.Bucket)
			assert.Equal(t, *key, *listPartsResult.Key)
			assert.Equal(t, *uploadId, *listPartsResult.UploadId)
			assert.Len(t, listPartsResult.Parts, 0)
			assert.False(t, *listPartsResult.IsTruncated)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			listPartsResult, err = s3Client.ListParts(context.Background(), &s3.ListPartsInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
				MaxParts: aws.Int32(1),
			})
			if err != nil {
				assert.Fail(t, "ListParts failed", "err %v", err)
			}
			assert.NotNil(t, listPartsResult)
			assert.Equal(t, *bucketName, *listPartsResult.Bucket)
			assert.Equal(t, *key, *listPartsResult.Key)
			assert.Equal(t, *uploadId, *listPartsResult.UploadId)
			assert.Equal(t, int32(1), *listPartsResult.MaxParts)
			assert.Len(t, listPartsResult.Parts, 1)
			firstPart := listPartsResult.Parts[0]
			assert.Equal(t, int32(2), *firstPart.PartNumber)
			assert.Equal(t, int64(len(body[2:])), *firstPart.Size)
			assert.Equal(t, "\"ea0cfed76183b9faf2e87ca949d9c4b8\"", *firstPart.ETag)
			assert.Equal(t, "2GpoVg==", *firstPart.ChecksumCRC32)
			assert.Equal(t, "80iBtg==", *firstPart.ChecksumCRC32C)
			assert.Equal(t, "Iy5Z/rXq8uI=", *firstPart.ChecksumCRC64NVME)
			assert.Equal(t, "h1jfWItGBQfcDNMMI6FANuZJam4=", *firstPart.ChecksumSHA1)
			assert.Equal(t, "QzRXKSwYas0BDNnAMkDZMLlliJd9xDozckIiOuCoaao=", *firstPart.ChecksumSHA256)
			assert.False(t, *listPartsResult.IsTruncated)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			// listParts with limit
			listPartsResult, err = s3Client.ListParts(context.Background(), &s3.ListPartsInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
				MaxParts: aws.Int32(1),
			})
			if err != nil {
				assert.Fail(t, "ListParts failed", "err %v", err)
			}
			assert.NotNil(t, listPartsResult)
			assert.Equal(t, *bucketName, *listPartsResult.Bucket)
			assert.Equal(t, *key, *listPartsResult.Key)
			assert.Equal(t, *uploadId, *listPartsResult.UploadId)
			assert.Equal(t, int32(1), *listPartsResult.MaxParts)
			assert.Len(t, listPartsResult.Parts, 1)
			firstPart = listPartsResult.Parts[0]
			assert.Equal(t, int32(1), *firstPart.PartNumber)
			assert.Equal(t, int64(len(body[0:2])), *firstPart.Size)
			assert.Equal(t, "\"a64cf5823262686e1a28b2245be34ce0\"", *firstPart.ETag)
			assert.Equal(t, "RKFCJQ==", *firstPart.ChecksumCRC32)
			assert.Equal(t, "x1X1EA==", *firstPart.ChecksumCRC32C)
			assert.Equal(t, "MhUdjJvefpY=", *firstPart.ChecksumCRC64NVME)
			assert.Equal(t, "U6QXeWx3eFEAOz8kMeju9WJewVs=", *firstPart.ChecksumSHA1)
			assert.Equal(t, "MO/ftS/2f4Dat8uJ3P4O7IQSlmz+WDJJk2dLRhbWvRE=", *firstPart.ChecksumSHA256)
			assert.True(t, *listPartsResult.IsTruncated)

			// listParts with partNumberMarker offset
			listPartsResult, err = s3Client.ListParts(context.Background(), &s3.ListPartsInput{
				Bucket:           bucketName,
				Key:              key,
				UploadId:         uploadId,
				PartNumberMarker: aws.String("1"),
				MaxParts:         aws.Int32(1),
			})
			if err != nil {
				assert.Fail(t, "ListParts failed", "err %v", err)
			}
			assert.NotNil(t, listPartsResult)
			assert.Equal(t, *bucketName, *listPartsResult.Bucket)
			assert.Equal(t, *key, *listPartsResult.Key)
			assert.Equal(t, *uploadId, *listPartsResult.UploadId)
			assert.Equal(t, "1", *listPartsResult.PartNumberMarker)
			assert.Equal(t, int32(1), *listPartsResult.MaxParts)
			assert.Len(t, listPartsResult.Parts, 1)
			firstPart = listPartsResult.Parts[0]
			assert.Equal(t, int32(2), *firstPart.PartNumber)
			assert.Equal(t, int64(len(body[2:])), *firstPart.Size)
			assert.Equal(t, "\"ea0cfed76183b9faf2e87ca949d9c4b8\"", *firstPart.ETag)
			assert.Equal(t, "2GpoVg==", *firstPart.ChecksumCRC32)
			assert.Equal(t, "80iBtg==", *firstPart.ChecksumCRC32C)
			assert.Equal(t, "Iy5Z/rXq8uI=", *firstPart.ChecksumCRC64NVME)
			assert.Equal(t, "h1jfWItGBQfcDNMMI6FANuZJam4=", *firstPart.ChecksumSHA1)
			assert.Equal(t, "QzRXKSwYas0BDNnAMkDZMLlliJd9xDozckIiOuCoaao=", *firstPart.ChecksumSHA256)
			assert.False(t, *listPartsResult.IsTruncated)

			// listParts (all)
			listPartsResult, err = s3Client.ListParts(context.Background(), &s3.ListPartsInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
				MaxParts: aws.Int32(2),
			})
			if err != nil {
				assert.Fail(t, "ListParts failed", "err %v", err)
			}
			assert.NotNil(t, listPartsResult)
			assert.Equal(t, *bucketName, *listPartsResult.Bucket)
			assert.Equal(t, *key, *listPartsResult.Key)
			assert.Equal(t, *uploadId, *listPartsResult.UploadId)
			assert.Equal(t, int32(2), *listPartsResult.MaxParts)
			assert.Len(t, listPartsResult.Parts, 2)
			assert.False(t, *listPartsResult.IsTruncated)
			firstPart = listPartsResult.Parts[0]
			assert.Equal(t, int32(1), *firstPart.PartNumber)
			assert.Equal(t, int64(len(body[0:2])), *firstPart.Size)
			assert.Equal(t, "\"a64cf5823262686e1a28b2245be34ce0\"", *firstPart.ETag)
			assert.Equal(t, "RKFCJQ==", *firstPart.ChecksumCRC32)
			assert.Equal(t, "x1X1EA==", *firstPart.ChecksumCRC32C)
			assert.Equal(t, "MhUdjJvefpY=", *firstPart.ChecksumCRC64NVME)
			assert.Equal(t, "U6QXeWx3eFEAOz8kMeju9WJewVs=", *firstPart.ChecksumSHA1)
			assert.Equal(t, "MO/ftS/2f4Dat8uJ3P4O7IQSlmz+WDJJk2dLRhbWvRE=", *firstPart.ChecksumSHA256)
			secondPart := listPartsResult.Parts[1]
			assert.Equal(t, int32(2), *secondPart.PartNumber)
			assert.Equal(t, int64(len(body[2:])), *secondPart.Size)
			assert.Equal(t, "\"ea0cfed76183b9faf2e87ca949d9c4b8\"", *secondPart.ETag)
			assert.Equal(t, "2GpoVg==", *secondPart.ChecksumCRC32)
			assert.Equal(t, "80iBtg==", *secondPart.ChecksumCRC32C)
			assert.Equal(t, "Iy5Z/rXq8uI=", *secondPart.ChecksumCRC64NVME)
			assert.Equal(t, "h1jfWItGBQfcDNMMI6FANuZJam4=", *secondPart.ChecksumSHA1)
			assert.Equal(t, "QzRXKSwYas0BDNnAMkDZMLlliJd9xDozckIiOuCoaao=", *secondPart.ChecksumSHA256)
		})

		t.Run("it should filter listed parts via authorizer hook"+testSuffix, func(t *testing.T) {
			authorizationCode := `
			function authorizeRequest(request)
			  return true
			end

			function authorizeListPart(request, partNumber)
			  return partNumber == 2
			end
			`
			requestAuthorizer, err := lua.NewLuaAuthorizer(authorizationCode)
			if err != nil {
				t.Fatalf("Could not create LuaAuthorizer: %v", err)
			}

			s3Client, _, cleanup := setupTestServerWithAuthorizer(requestAuthorizer, dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			createMultipartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			uploadId := createMultipartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			_, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: bucketName, Key: key, UploadId: uploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("part-1"))})
			assert.Nil(t, err)
			_, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: bucketName, Key: key, UploadId: uploadId, PartNumber: aws.Int32(2), Body: bytes.NewReader([]byte("part-2"))})
			assert.Nil(t, err)

			listPartsResult, err := s3Client.ListParts(context.Background(), &s3.ListPartsInput{Bucket: bucketName, Key: key, UploadId: uploadId})
			assert.Nil(t, err)
			assert.Len(t, listPartsResult.Parts, 1)
			assert.Equal(t, int32(2), *listPartsResult.Parts[0].PartNumber)
		})

		t.Run("it should allow cancellation of multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			abortMultipartUpload, err := s3Client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "AbortMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, abortMultipartUpload)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should allow two multipart uploads on same key after complete first"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			completeMultipartUploadResult, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult)
			assert.Equal(t, bucketName, completeMultipartUploadResult.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult.Key)

			createMultiPartUploadResult2, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult2)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult2.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult2.Key)
			uploadId2 := createMultiPartUploadResult2.UploadId
			assert.NotNil(t, uploadId2)

			uploadPartResult2, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId2,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult2)

			uploadPartResult2, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId2,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult2)

			completeMultipartUploadResult2, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId2,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult2)
			assert.Equal(t, bucketName, completeMultipartUploadResult2.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult2.Key)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, key, listObjectResult.Contents[0].Key)
			assert.True(t, strings.HasSuffix(*completeMultipartUploadResult2.ETag, "-"+strconv.Itoa(2)+"\""))
			assert.Equal(t, completeMultipartUploadResult2.ETag, listObjectResult.Contents[0].ETag)
			assert.False(t, *listObjectResult.IsTruncated)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should allow two multipart uploads on same key after abort first"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			abortMultipartUploadResult, err := s3Client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "AbortMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, abortMultipartUploadResult)

			createMultiPartUploadResult2, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult2)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult2.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult2.Key)
			uploadId2 := createMultiPartUploadResult2.UploadId
			assert.NotNil(t, uploadId2)

			uploadPartResult2, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId2,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult2)

			uploadPartResult2, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId2,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult2)

			completeMultipartUploadResult2, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId2,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult2)
			assert.Equal(t, bucketName, completeMultipartUploadResult2.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult2.Key)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, key, listObjectResult.Contents[0].Key)
			assert.True(t, strings.HasSuffix(*completeMultipartUploadResult2.ETag, "-"+strconv.Itoa(2)+"\""))
			assert.Equal(t, completeMultipartUploadResult2.ETag, listObjectResult.Contents[0].ETag)
			assert.False(t, *listObjectResult.IsTruncated)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should reject stale If-Match CompleteMultipartUpload during concurrent completes"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			initialPut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("initial-state")),
			})
			assert.Nil(t, err)
			assert.NotNil(t, initialPut.ETag)

			prepareUpload := func(payload []byte) *string {
				createOut, createErr := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
					Bucket: bucketName,
					Key:    key,
				})
				if !assert.Nil(t, createErr) {
					return nil
				}
				if !assert.NotNil(t, createOut.UploadId) {
					return nil
				}

				_, uploadErr := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
					Bucket:     bucketName,
					Key:        key,
					UploadId:   createOut.UploadId,
					PartNumber: aws.Int32(1),
					Body:       bytes.NewReader(payload),
				})
				if !assert.Nil(t, uploadErr) {
					return nil
				}

				return createOut.UploadId
			}

			uploadIDs := []*string{
				prepareUpload([]byte("payload-a")),
				prepareUpload([]byte("payload-b")),
			}
			if !assert.NotNil(t, uploadIDs[0]) || !assert.NotNil(t, uploadIDs[1]) {
				return
			}

			var wg sync.WaitGroup
			start := make(chan struct{})
			errs := make([]error, 2)

			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					_, errs[i] = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
						Bucket:   bucketName,
						Key:      key,
						UploadId: uploadIDs[i],
						IfMatch:  initialPut.ETag,
					})
				}(i)
			}

			close(start)
			wg.Wait()

			successCount := 0
			for i := range 2 {
				if errs[i] == nil {
					successCount++
					continue
				}

				var apiErr smithy.APIError
				if !errors.As(errs[i], &apiErr) {
					assert.Fail(t, "Expected smithy.APIError", "err %v", errs[i])
					continue
				}
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}

			assert.LessOrEqual(t, successCount, 1, "at most one concurrent stale If-Match CompleteMultipartUpload may succeed")
		})

		t.Run("it should allow at most one concurrent create-if-absent CompleteMultipartUpload"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			prepareUpload := func(payload []byte) *string {
				createOut, createErr := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
					Bucket: bucketName,
					Key:    key,
				})
				if !assert.Nil(t, createErr) {
					return nil
				}
				if !assert.NotNil(t, createOut.UploadId) {
					return nil
				}

				_, uploadErr := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
					Bucket:     bucketName,
					Key:        key,
					UploadId:   createOut.UploadId,
					PartNumber: aws.Int32(1),
					Body:       bytes.NewReader(payload),
				})
				if !assert.Nil(t, uploadErr) {
					return nil
				}

				return createOut.UploadId
			}

			uploadIDs := []*string{
				prepareUpload([]byte("payload-1")),
				prepareUpload([]byte("payload-2")),
			}
			if !assert.NotNil(t, uploadIDs[0]) || !assert.NotNil(t, uploadIDs[1]) {
				return
			}

			var wg sync.WaitGroup
			start := make(chan struct{})
			errs := make([]error, 2)

			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					_, errs[i] = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
						Bucket:      bucketName,
						Key:         key,
						UploadId:    uploadIDs[i],
						IfNoneMatch: aws.String("*"),
					})
				}(i)
			}

			close(start)
			wg.Wait()

			successCount := 0
			for i := range 2 {
				if errs[i] == nil {
					successCount++
					continue
				}

				var apiErr smithy.APIError
				if !errors.As(errs[i], &apiErr) {
					assert.Fail(t, "Expected smithy.APIError", "err %v", errs[i])
					continue
				}
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}

			assert.LessOrEqual(t, successCount, 1, "at most one concurrent If-None-Match CompleteMultipartUpload may succeed")
		})

		t.Run("it should hit the upload limit when uploading a part that is too large"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			// Use a LimitReader over a repeating source to simulate a large payload without allocating 5GB of RAM
			largePayload := io.LimitReader(ioutils.NewRepeatingReader([]byte("huge")), storage.MaxEntitySize+1)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       largePayload,
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			assert.Nil(t, uploadPartResult)
			if err == nil {
				assert.Fail(t, "UploadPart should have failed due to size limit")
			}
			var smithyOperationError *smithy.OperationError
			if !errors.As(err, &smithyOperationError) {
				assert.Fail(t, "Expected error smithy.OperationError", "err %v", err)
			}
		})
	})
}

func TestGetObject(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should allow downloading the object"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
			assert.Equal(t, "\"6cd3556deb0da54bca060b4c39479839\"", *getObjectResult.ETag)
			assert.Equal(t, "6+bG5g==", *getObjectResult.ChecksumCRC32)
			assert.Equal(t, "yKEG5Q==", *getObjectResult.ChecksumCRC32C)
			assert.Equal(t, "n3hVaQAaPTQ=", *getObjectResult.ChecksumCRC64NVME)
			assert.Equal(t, "lDpwLQbzRZmu4fjajvn3KWAx1pk=", *getObjectResult.ChecksumSHA1)
			assert.Equal(t, "MV9b23bQeMQ7isAGTkoBZGErH853yGk0W/yUx1iU7dM=", *getObjectResult.ChecksumSHA256)
			assert.Equal(t, types.ChecksumTypeFullObject, getObjectResult.ChecksumType)
		})

		t.Run("it should allow downloading the object with a presigned url"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			presignClient := s3.NewPresignClient(s3Client)
			presignedRequest, err := presignClient.PresignGetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)

			httpClient := buildHttpClient()
			request, err := http.NewRequest(presignedRequest.Method, presignedRequest.URL, nil)
			assert.Nil(t, err)
			getObjectResult, err := httpClient.Do(request)
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.Equal(t, 200, getObjectResult.StatusCode)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should allow downloading the object with byte range"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=1-4"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body[1:5], objectBytes)
		})

		t.Run("it should allow downloading the object with byte range without end"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=1-"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body[1:], objectBytes)
		})

		t.Run("it should allow downloading the object with suffix byte range"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=-6"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body[7:], objectBytes)
		})

		t.Run("it should return 206 and clamp range end when it exceeds object size"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			// Request a range whose end far exceeds the object size.
			// Per RFC 7233 §2.1 the server must return 206 with the available bytes,
			// not 416 Range Not Satisfiable.
			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=0-5242879"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should allow downloading the object with multi byte range"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=1-4, 5-6"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.ContentType)
			assert.Contains(t, *getObjectResult.ContentType, "multipart/byteranges; boundary=")
			boundary, _ := strings.CutPrefix(*getObjectResult.ContentType, "multipart/byteranges; boundary=")
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)

			newContent := []byte{}
			/*
				+ 00000000  2d 2d 30 31 48 58 45 52  50 59 56 4a 39 41 47 56  |--01HXERPYVJ9AGV|
				+ 00000010  44 42 34 57 4a 41 44 4a  43 56 59 4e 0d 0a 43 6f  |DB4WJADJCVYN..Co|
				+ 00000020  6e 74 65 6e 74 2d 52 61  6e 67 65 3a 20 62 79 74  |ntent-Range: byt|
				+ 00000030  65 73 20 31 2d 34 2f 31  33 0d 0a 0d 0a 65 6c 6c  |es 1-4/13....ell|
				+ 00000040  6f 0d 0a 2d 2d 30 31 48  58 45 52 50 59 56 4a 39  |o..--01HXERPYVJ9|
				+ 00000050  41 47 56 44 42 34 57 4a  41 44 4a 43 56 59 4e 0d  |AGVDB4WJADJCVYN.|
				+ 00000060  0a 43 6f 6e 74 65 6e 74  2d 52 61 6e 67 65 3a 20  |.Content-Range: |
				+ 00000070  62 79 74 65 73 20 35 2d  36 2f 31 33 0d 0a 0d 0a  |bytes 5-6/13....|
				+ 00000080  2c 20 0d 0a 2d 2d 30 31  48 58 45 52 50 59 56 4a  |, ..--01HXERPYVJ|
				+ 00000090  39 41 47 56 44 42 34 57  4a 41 44 4a 43 56 59 4e  |9AGVDB4WJADJCVYN|
				+ 000000a0  2d 2d 0d 0a                                       |--..|
			*/

			newContent = append(newContent, []byte("--")...)
			newContent = append(newContent, []byte(boundary)...)
			newContent = append(newContent, []byte("\r\nContent-Range: bytes 1-4/13\r\n\r\n")...)
			newContent = append(newContent, body[1:5]...)
			newContent = append(newContent, []byte("\r\n--")...)
			newContent = append(newContent, []byte(boundary)...)
			newContent = append(newContent, []byte("\r\nContent-Range: bytes 5-6/13\r\n\r\n")...)
			newContent = append(newContent, body[5:7]...)
			newContent = append(newContent, []byte("\r\n--")...)
			newContent = append(newContent, []byte(boundary)...)
			newContent = append(newContent, []byte("--\r\n")...)
			assert.Equal(t, newContent, objectBytes)
		})

		t.Run("it should return 200 for GetObject with matching If-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader(body),
			})
			assert.Nil(t, err)
			assert.NotNil(t, putResult.ETag)

			_, err = s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket:  bucketName,
				Key:     key,
				IfMatch: putResult.ETag,
			})
			assert.Nil(t, err)
		})

		t.Run("it should return 412 for GetObject with mismatched If-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader(body),
			})
			assert.Nil(t, err)

			_, err = s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket:  bucketName,
				Key:     key,
				IfMatch: aws.String("\"does-not-match\""),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}
		})

		t.Run("it should return an error for GetObject with matching If-None-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader(body),
			})
			assert.Nil(t, err)
			assert.NotNil(t, putResult.ETag)

			_, err = s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket:      bucketName,
				Key:         key,
				IfNoneMatch: putResult.ETag,
			})
			// 304 Not Modified surfaces as an error from the SDK (no body to deserialize).
			assert.NotNil(t, err)
			var httpErr *awshttp.ResponseError
			if assert.ErrorAs(t, err, &httpErr) {
				assert.Equal(t, 304, httpErr.Response.StatusCode)
			}
		})

		t.Run("it should return 200 for GetObject with non-matching If-None-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader(body),
			})
			assert.Nil(t, err)

			_, err = s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket:      bucketName,
				Key:         key,
				IfNoneMatch: aws.String("\"does-not-match\""),
			})
			assert.Nil(t, err)
		})
	})
}

func TestCopyObject(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		setupSourceObject := func(s3Client *s3.Client, contentType *string) {
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:      bucketName,
				Key:         key,
				Body:        bytes.NewReader(body),
				ContentType: contentType,
			})
			assert.Nil(t, err)
		}

		t.Run("it should copy an object preserving content, etag and content type"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, aws.String("text/plain"))

			copyResult, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        key2,
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.Nil(t, err)
			assert.NotNil(t, copyResult.CopyObjectResult)
			assert.Equal(t, "\"6cd3556deb0da54bca060b4c39479839\"", *copyResult.CopyObjectResult.ETag)
			assert.NotNil(t, copyResult.CopyObjectResult.LastModified)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			objectBytes, err := io.ReadAll(getResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
			assert.Equal(t, "\"6cd3556deb0da54bca060b4c39479839\"", *getResult.ETag)
			assert.Equal(t, "text/plain", *getResult.ContentType)
		})

		t.Run("it should copy an object across buckets"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, nil)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName2})
			assert.Nil(t, err)

			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName2,
				Key:        key,
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName2, Key: key})
			assert.Nil(t, err)
			objectBytes, err := io.ReadAll(getResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should replace the content type when metadata directive is REPLACE"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, aws.String("text/plain"))

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:            bucketName,
				Key:               key2,
				CopySource:        aws.String(*bucketName + "/" + *key),
				MetadataDirective: types.MetadataDirectiveReplace,
				ContentType:       aws.String("application/json"),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			objectBytes, err := io.ReadAll(getResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
			assert.Equal(t, "application/json", *getResult.ContentType)
		})

		t.Run("it should honor copy-source-if-match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, nil)

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:            bucketName,
				Key:               key2,
				CopySource:        aws.String(*bucketName + "/" + *key),
				CopySourceIfMatch: aws.String("\"6cd3556deb0da54bca060b4c39479839\""),
			})
			assert.Nil(t, err)

			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:            bucketName,
				Key:               key2,
				CopySource:        aws.String(*bucketName + "/" + *key),
				CopySourceIfMatch: aws.String("\"does-not-match\""),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}
		})

		t.Run("it should fail copy-source-if-none-match when the etag matches"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, nil)

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:                bucketName,
				Key:                   key2,
				CopySource:            aws.String(*bucketName + "/" + *key),
				CopySourceIfNoneMatch: aws.String("\"6cd3556deb0da54bca060b4c39479839\""),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}
		})

		t.Run("it should return NoSuchKey when the source does not exist"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        key2,
				CopySource: aws.String(*bucketName + "/does-not-exist"),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "NoSuchKey", apiErr.ErrorCode())
			}
		})

		t.Run("it should reject a no-op self copy"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, nil)

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        key,
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "InvalidRequest", apiErr.ErrorCode())
			}
		})

		t.Run("it should allow a self copy that replaces metadata"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, aws.String("text/plain"))

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:            bucketName,
				Key:               key,
				CopySource:        aws.String(*bucketName + "/" + *key),
				MetadataDirective: types.MetadataDirectiveReplace,
				ContentType:       aws.String("application/json"),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			objectBytes, err := io.ReadAll(getResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
			assert.Equal(t, "application/json", *getResult.ContentType)
		})

		t.Run("it should forbid a copy when the destination write is denied"+testSuffix, func(t *testing.T) {
			// Allow everything except the copy operation itself.
			authorizationCode := `
			function authorizeRequest(request)
			  return request.operation ~= "CopyObject"
			end
			`
			requestAuthorizer, err := lua.NewLuaAuthorizer(authorizationCode)
			if err != nil {
				t.Fatalf("Could not create LuaAuthorizer: %v", err)
			}
			s3Client, _, cleanup := setupTestServerWithAuthorizer(requestAuthorizer, dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			// Authorization is checked before any storage access, so the copy is
			// rejected with 403 regardless of whether the source exists.
			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        key2,
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.NotNil(t, err)
			var httpErr *awshttp.ResponseError
			if assert.ErrorAs(t, err, &httpErr) {
				assert.Equal(t, 403, httpErr.Response.StatusCode)
			}
		})
	})
}

func TestObjectTagging(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		setupObject := func(s3Client *s3.Client) {
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader(body),
			})
			assert.Nil(t, err)
		}

		tagsToMap := func(tagSet []types.Tag) map[string]string {
			m := map[string]string{}
			for _, tag := range tagSet {
				m[*tag.Key] = *tag.Value
			}
			return m
		}

		t.Run("it should put and get an object tag set"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupObject(s3Client)

			_, err := s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket: bucketName,
				Key:    key,
				Tagging: &types.Tagging{TagSet: []types.Tag{
					{Key: aws.String("env"), Value: aws.String("prod")},
					{Key: aws.String("team"), Value: aws.String("storage")},
				}},
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.Equal(t, map[string]string{"env": "prod", "team": "storage"}, tagsToMap(getResult.TagSet))
		})

		t.Run("it should replace an existing tag set on a subsequent put"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupObject(s3Client)

			_, err := s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("a"), Value: aws.String("1")}}},
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("b"), Value: aws.String("2")}}},
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.Equal(t, map[string]string{"b": "2"}, tagsToMap(getResult.TagSet))
		})

		t.Run("it should clear tags when putting an empty tag set"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupObject(s3Client)

			_, err := s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("a"), Value: aws.String("1")}}},
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{}},
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.Len(t, getResult.TagSet, 0)
		})

		t.Run("it should delete the tag set"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupObject(s3Client)

			_, err := s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("a"), Value: aws.String("1")}}},
			})
			assert.Nil(t, err)

			_, err = s3Client.DeleteObjectTagging(context.Background(), &s3.DeleteObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.Len(t, getResult.TagSet, 0)
		})

		t.Run("it should return NoSuchKey for tagging of a missing object"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
		})

		t.Run("it should set tags from the x-amz-tagging header on PutObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Key:     key,
				Body:    bytes.NewReader(body),
				Tagging: aws.String("env=prod&team=storage"),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.Equal(t, map[string]string{"env": "prod", "team": "storage"}, tagsToMap(getResult.TagSet))
		})

		t.Run("it should expose the tag count on GetObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Key:     key,
				Body:    bytes.NewReader(body),
				Tagging: aws.String("a=1&b=2"),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			io.ReadAll(getResult.Body)
			assert.NotNil(t, getResult.TagCount)
			assert.Equal(t, int32(2), *getResult.TagCount)
		})

		t.Run("it should copy tags by default on CopyObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Key:     key,
				Body:    bytes.NewReader(body),
				Tagging: aws.String("env=prod"),
			})
			assert.Nil(t, err)

			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        key2,
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			assert.Equal(t, map[string]string{"env": "prod"}, tagsToMap(getResult.TagSet))
		})

		t.Run("it should replace tags when tagging directive is REPLACE on CopyObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Key:     key,
				Body:    bytes.NewReader(body),
				Tagging: aws.String("env=prod"),
			})
			assert.Nil(t, err)

			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:           bucketName,
				Key:              key2,
				CopySource:       aws.String(*bucketName + "/" + *key),
				TaggingDirective: types.TaggingDirectiveReplace,
				Tagging:          aws.String("env=staging&region=eu"),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			assert.Equal(t, map[string]string{"env": "staging", "region": "eu"}, tagsToMap(getResult.TagSet))
		})
	})
}

func TestObjectMetadata(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		expires := time.Date(2026, time.October, 21, 7, 28, 0, 0, time.UTC)
		putObjectWithMetadata := func(s3Client *s3.Client) {
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:                  bucketName,
				Key:                     key,
				Body:                    bytes.NewReader(body),
				ContentType:             aws.String("text/plain"),
				CacheControl:            aws.String("max-age=3600"),
				ContentDisposition:      aws.String(`attachment; filename="hello.txt"`),
				ContentEncoding:         aws.String("identity"),
				ContentLanguage:         aws.String("en-US"),
				Expires:                 aws.Time(expires),
				WebsiteRedirectLocation: aws.String("/redirected.html"),
				Metadata:                map[string]string{"Purpose": "testing", "owner": "storage-team"},
			})
			assert.Nil(t, err)
		}

		assertMetadata := func(t *testing.T, cacheControl, contentDisposition, contentEncoding, contentLanguage, expiresString, websiteRedirectLocation *string, userMetadata map[string]string) {
			assert.NotNil(t, cacheControl)
			assert.Equal(t, "max-age=3600", *cacheControl)
			assert.NotNil(t, contentDisposition)
			assert.Equal(t, `attachment; filename="hello.txt"`, *contentDisposition)
			assert.NotNil(t, contentEncoding)
			assert.Equal(t, "identity", *contentEncoding)
			assert.NotNil(t, contentLanguage)
			assert.Equal(t, "en-US", *contentLanguage)
			assert.NotNil(t, expiresString)
			parsedExpires, err := http.ParseTime(*expiresString)
			assert.Nil(t, err)
			assert.True(t, expires.Equal(parsedExpires))
			assert.NotNil(t, websiteRedirectLocation)
			assert.Equal(t, "/redirected.html", *websiteRedirectLocation)
			// S3 stores user metadata keys lowercase.
			assert.Equal(t, map[string]string{"purpose": "testing", "owner": "storage-team"}, userMetadata)
		}

		t.Run("it should persist and return metadata on HeadObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			putObjectWithMetadata(s3Client)

			headResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assertMetadata(t, headResult.CacheControl, headResult.ContentDisposition, headResult.ContentEncoding, headResult.ContentLanguage, headResult.ExpiresString, headResult.WebsiteRedirectLocation, headResult.Metadata)
		})

		t.Run("it should return metadata on GetObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			putObjectWithMetadata(s3Client)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			io.ReadAll(getResult.Body)
			getResult.Body.Close()
			assertMetadata(t, getResult.CacheControl, getResult.ContentDisposition, getResult.ContentEncoding, getResult.ContentLanguage, getResult.ExpiresString, getResult.WebsiteRedirectLocation, getResult.Metadata)
		})

		t.Run("it should replace metadata when the object is overwritten"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			putObjectWithMetadata(s3Client)

			_, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader(body),
			})
			assert.Nil(t, err)

			headResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.Nil(t, headResult.CacheControl)
			assert.Nil(t, headResult.ContentDisposition)
			assert.Nil(t, headResult.ContentEncoding)
			assert.Nil(t, headResult.ContentLanguage)
			assert.Nil(t, headResult.ExpiresString)
			assert.Nil(t, headResult.WebsiteRedirectLocation)
			assert.Len(t, headResult.Metadata, 0)
		})

		t.Run("it should preserve metadata across multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket:                  bucketName,
				Key:                     key,
				CacheControl:            aws.String("max-age=3600"),
				ContentDisposition:      aws.String(`attachment; filename="hello.txt"`),
				ContentEncoding:         aws.String("identity"),
				ContentLanguage:         aws.String("en-US"),
				Expires:                 aws.Time(expires),
				WebsiteRedirectLocation: aws.String("/redirected.html"),
				Metadata:                map[string]string{"purpose": "testing", "owner": "storage-team"},
			})
			assert.Nil(t, err)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Key:        key,
				UploadId:   createResult.UploadId,
				PartNumber: aws.Int32(1),
				Body:       bytes.NewReader(body),
			})
			assert.Nil(t, err)

			_, err = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: createResult.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{{ETag: uploadPartResult.ETag, PartNumber: aws.Int32(1)}},
				},
			})
			assert.Nil(t, err)

			headResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assertMetadata(t, headResult.CacheControl, headResult.ContentDisposition, headResult.ContentEncoding, headResult.ContentLanguage, headResult.ExpiresString, headResult.WebsiteRedirectLocation, headResult.Metadata)
		})

		t.Run("it should copy metadata by default on CopyObject except the website redirect location"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			putObjectWithMetadata(s3Client)

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        key2,
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.Nil(t, err)

			headResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			assert.NotNil(t, headResult.CacheControl)
			assert.Equal(t, "max-age=3600", *headResult.CacheControl)
			assert.Equal(t, map[string]string{"purpose": "testing", "owner": "storage-team"}, headResult.Metadata)
			// S3 never carries x-amz-website-redirect-location over to the copy.
			assert.Nil(t, headResult.WebsiteRedirectLocation)
		})

		t.Run("it should replace metadata when metadata directive is REPLACE on CopyObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			putObjectWithMetadata(s3Client)

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:            bucketName,
				Key:               key2,
				CopySource:        aws.String(*bucketName + "/" + *key),
				MetadataDirective: types.MetadataDirectiveReplace,
				CacheControl:      aws.String("no-store"),
				Metadata:          map[string]string{"replaced": "yes"},
			})
			assert.Nil(t, err)

			headResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			assert.NotNil(t, headResult.CacheControl)
			assert.Equal(t, "no-store", *headResult.CacheControl)
			assert.Nil(t, headResult.ContentDisposition)
			assert.Equal(t, map[string]string{"replaced": "yes"}, headResult.Metadata)
		})

		t.Run("it should reject user metadata over the 2 KB limit"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:   bucketName,
				Key:      key,
				Body:     bytes.NewReader(body),
				Metadata: map[string]string{"big": strings.Repeat("v", storage.MaxUserMetadataSize)},
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			assert.True(t, errors.As(err, &apiErr))
			assert.Equal(t, "MetadataTooLarge", apiErr.ErrorCode())
		})
	})
}

func TestObjectStorageClass(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should round-trip the storage class through put, head, get and listings"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:       bucketName,
				Key:          key,
				Body:         bytes.NewReader([]byte("cold data")),
				StorageClass: types.StorageClassGlacier,
			})
			assert.Nil(t, err)

			headObjectResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClassGlacier, headObjectResult.StorageClass)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)
			body, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			getObjectResult.Body.Close()
			assert.Equal(t, []byte("cold data"), body)
			assert.Equal(t, types.StorageClassGlacier, getObjectResult.StorageClass)

			listObjectsV2Result, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			assert.Nil(t, err)
			assert.Len(t, listObjectsV2Result.Contents, 1)
			assert.Equal(t, types.ObjectStorageClassGlacier, listObjectsV2Result.Contents[0].StorageClass)

			listObjectsResult, err := s3Client.ListObjects(context.Background(), &s3.ListObjectsInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)
			assert.Len(t, listObjectsResult.Contents, 1)
			assert.Equal(t, types.ObjectStorageClassGlacier, listObjectsResult.Contents[0].StorageClass)

			listObjectVersionsResult, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)
			assert.Len(t, listObjectVersionsResult.Versions, 1)
			assert.Equal(t, types.ObjectVersionStorageClass("GLACIER"), listObjectVersionsResult.Versions[0].StorageClass)
		})

		t.Run("it should default to STANDARD and omit the response header"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("plain data")),
			})
			assert.Nil(t, err)

			// AWS omits x-amz-storage-class for STANDARD objects, which the SDK
			// surfaces as the enum zero value.
			headObjectResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClass(""), headObjectResult.StorageClass)

			listObjectsV2Result, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			assert.Nil(t, err)
			assert.Len(t, listObjectsV2Result.Contents, 1)
			assert.Equal(t, types.ObjectStorageClassStandard, listObjectsV2Result.Contents[0].StorageClass)
		})

		t.Run("it should reject an invalid storage class"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:       bucketName,
				Key:          key,
				Body:         bytes.NewReader([]byte("data")),
				StorageClass: types.StorageClass("BOGUS_CLASS"),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			assert.True(t, errors.As(err, &apiErr))
			assert.Equal(t, "InvalidStorageClass", apiErr.ErrorCode())
		})

		t.Run("it should carry the storage class from CreateMultipartUpload to the completed object"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket:       bucketName,
				Key:          key,
				StorageClass: types.StorageClassStandardIa,
			})
			assert.Nil(t, err)

			listUploadsResult, err := s3Client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)
			assert.Len(t, listUploadsResult.Uploads, 1)
			assert.Equal(t, types.StorageClassStandardIa, listUploadsResult.Uploads[0].StorageClass)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Key:        key,
				UploadId:   createResult.UploadId,
				PartNumber: aws.Int32(1),
				Body:       bytes.NewReader([]byte("multipart data")),
			})
			assert.Nil(t, err)

			listPartsResult, err := s3Client.ListParts(context.Background(), &s3.ListPartsInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: createResult.UploadId,
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClassStandardIa, listPartsResult.StorageClass)

			_, err = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: createResult.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{
						{ETag: uploadPartResult.ETag, PartNumber: aws.Int32(1)},
					},
				},
			})
			assert.Nil(t, err)

			headObjectResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClassStandardIa, headObjectResult.StorageClass)
		})

		t.Run("it should apply the storage class of the copy request instead of the source's"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:       bucketName,
				Key:          key,
				Body:         bytes.NewReader([]byte("source data")),
				StorageClass: types.StorageClassGlacier,
			})
			assert.Nil(t, err)

			// Copy with an explicit class: destination gets that class.
			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:       bucketName,
				Key:          aws.String("copy-with-class"),
				CopySource:   aws.String(*bucketName + "/" + *key),
				StorageClass: types.StorageClassOnezoneIa,
			})
			assert.Nil(t, err)
			headObjectResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    aws.String("copy-with-class"),
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClassOnezoneIa, headObjectResult.StorageClass)

			// Copy without a class: destination defaults to STANDARD even though
			// the source is GLACIER (matching AWS).
			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        aws.String("copy-without-class"),
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.Nil(t, err)
			headObjectResult, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    aws.String("copy-without-class"),
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClass(""), headObjectResult.StorageClass)

			// A self copy that only changes the storage class is allowed.
			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:       bucketName,
				Key:          key,
				CopySource:   aws.String(*bucketName + "/" + *key),
				StorageClass: types.StorageClassStandardIa,
			})
			assert.Nil(t, err)
			headObjectResult, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClassStandardIa, headObjectResult.StorageClass)
		})
	})
}

func TestTagBasedAuthorization(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		authorizationCode := `
		function authorizeRequest(request)
		  if request:isOperation("PutObjectTagging") then
		    return request:requestTagEquals("team", "storage")
		  end
		  if request:isOperation("GetObject") then
		    return request:objectTagEquals("team", "storage")
		  end
		  return true
		end

		function authorizeListObject(request, key)
		  return request:objectTagEquals("team", "storage")
		end
		`
		newAuthorizer := func() authorization.RequestAuthorizer {
			ra, err := lua.NewLuaAuthorizer(authorizationCode)
			if err != nil {
				t.Fatalf("Could not create LuaAuthorizer: %v", err)
			}
			return ra
		}

		assertForbidden := func(t *testing.T, err error) {
			assert.NotNil(t, err)
			var httpErr *awshttp.ResponseError
			if assert.ErrorAs(t, err, &httpErr) {
				assert.Equal(t, 403, httpErr.Response.StatusCode)
			}
		}

		t.Run("GetObject is allowed only for the matching existing object tag"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServerWithAuthorizer(newAuthorizer(), dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body), Tagging: aws.String("team=storage")})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key2, Body: bytes.NewReader(body), Tagging: aws.String("team=other")})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			io.ReadAll(getResult.Body)

			_, err = s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key2})
			assertForbidden(t, err)
		})

		t.Run("ListObjects is filtered by existing object tag"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServerWithAuthorizer(newAuthorizer(), dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body), Tagging: aws.String("team=storage")})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key2, Body: bytes.NewReader(body), Tagging: aws.String("team=other")})
			assert.Nil(t, err)

			list, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Len(t, list.Contents, 1)
			if len(list.Contents) == 1 {
				assert.Equal(t, *key, *list.Contents[0].Key)
			}
		})

		t.Run("PutObjectTagging is gated by the request tag being set"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServerWithAuthorizer(newAuthorizer(), dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body)})
			assert.Nil(t, err)

			_, err = s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("team"), Value: aws.String("storage")}}},
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("team"), Value: aws.String("other")}}},
			})
			assertForbidden(t, err)
		})
	})
}

func TestUploadPartCopy(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should copy a whole source object into a multipart part"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body)})
			assert.Nil(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			uploadId := createResult.UploadId

			copyPartResult, err := s3Client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
				Bucket:     bucketName,
				Key:        key2,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.Nil(t, err)
			assert.NotNil(t, copyPartResult.CopyPartResult)
			assert.NotNil(t, copyPartResult.CopyPartResult.ETag)

			_, err = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key2,
				UploadId: uploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{
						{ETag: copyPartResult.CopyPartResult.ETag, PartNumber: aws.Int32(1)},
					},
				},
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			objectBytes, err := io.ReadAll(getResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should copy a byte range of the source into a multipart part"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body)})
			assert.Nil(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			uploadId := createResult.UploadId

			// Copy "Hello," (bytes 0-5) as part 1 and "world!" (bytes 7-12) as part 2.
			part1, err := s3Client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
				Bucket:          bucketName,
				Key:             key2,
				UploadId:        uploadId,
				PartNumber:      aws.Int32(1),
				CopySource:      aws.String(*bucketName + "/" + *key),
				CopySourceRange: aws.String("bytes=0-5"),
			})
			assert.Nil(t, err)
			part2, err := s3Client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
				Bucket:          bucketName,
				Key:             key2,
				UploadId:        uploadId,
				PartNumber:      aws.Int32(2),
				CopySource:      aws.String(*bucketName + "/" + *key),
				CopySourceRange: aws.String("bytes=7-12"),
			})
			assert.Nil(t, err)

			_, err = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key2,
				UploadId: uploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{
						{ETag: part1.CopyPartResult.ETag, PartNumber: aws.Int32(1)},
						{ETag: part2.CopyPartResult.ETag, PartNumber: aws.Int32(2)},
					},
				},
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			objectBytes, err := io.ReadAll(getResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, []byte("Hello,world!"), objectBytes)
		})

		t.Run("it should honor copy-source-if-match for UploadPartCopy"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body)})
			assert.Nil(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			uploadId := createResult.UploadId

			_, err = s3Client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
				Bucket:            bucketName,
				Key:               key2,
				UploadId:          uploadId,
				PartNumber:        aws.Int32(1),
				CopySource:        aws.String(*bucketName + "/" + *key),
				CopySourceIfMatch: aws.String("\"does-not-match\""),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}
		})
	})
}

func TestDeleteObject(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should allow deleting an object"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			deleteObjectResult, err := s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "DeleteObject failed", "err %v", err)
			}
			assert.NotNil(t, deleteObjectResult)
		})

		t.Run("it should allow deleting an object with a presigned url"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			presignClient := s3.NewPresignClient(s3Client)
			presignedRequest, err := presignClient.PresignDeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)

			httpClient := buildHttpClient()
			request, err := http.NewRequest(presignedRequest.Method, presignedRequest.URL, nil)
			assert.Nil(t, err)
			deleteObjectResult, err := httpClient.Do(request)
			if err != nil {
				assert.Fail(t, "DeleteObject failed", "err %v", err)
			}
			assert.NotNil(t, deleteObjectResult)
			assert.Equal(t, 204, deleteObjectResult.StatusCode)
		})

		t.Run("it should return 204 for DeleteObject with matching If-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
			})
			assert.Nil(t, err)
			assert.NotNil(t, putResult.ETag)

			_, err = s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket:  bucketName,
				Key:     key,
				IfMatch: putResult.ETag,
			})
			assert.Nil(t, err)

			// Verify the object is gone.
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
		})

		t.Run("it should return 412 for DeleteObject with mismatched If-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
			})
			assert.Nil(t, err)

			_, err = s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket:  bucketName,
				Key:     key,
				IfMatch: aws.String("\"does-not-match\""),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}

			// Verify the object still exists.
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
		})

		t.Run("it should reject stale If-Match deletes during concurrent writes"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
			})
			assert.Nil(t, err)
			assert.NotNil(t, putResult.ETag)

			var wg sync.WaitGroup
			start := make(chan struct{})
			errs := make([]error, 2)

			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					_, errs[i] = s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
						Bucket:  bucketName,
						Key:     key,
						IfMatch: putResult.ETag,
					})
				}(i)
			}

			close(start)
			wg.Wait()

			successCount := 0
			for i := range 2 {
				if errs[i] == nil {
					successCount++
					continue
				}

				var apiErr smithy.APIError
				if !errors.As(errs[i], &apiErr) {
					assert.Fail(t, "Expected smithy.APIError", "err %v", errs[i])
					continue
				}
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}

			assert.LessOrEqual(t, successCount, 1, "at most one concurrent If-Match delete may succeed")
		})
	})
}

func TestHeadObject(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should allow head an object"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			headObjectResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "HeadObject failed", "err %v", err)
			}
			assert.NotNil(t, headObjectResult)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *headObjectResult.ETag)
			assert.Equal(t, "Bjck3A==", *headObjectResult.ChecksumCRC32)
			assert.Equal(t, "H7cZCA==", *headObjectResult.ChecksumCRC32C)
			assert.Equal(t, "4SEgZkEEyhY=", *headObjectResult.ChecksumCRC64NVME)
			assert.Equal(t, "lMCBYNtqPCnP3avKVUtqfrThqHo=", *headObjectResult.ChecksumSHA1)
			assert.Equal(t, "sctyzI/H+7x/oVR7Gwt7NiQ7kop4Ua/7SrVraELVDpI=", *headObjectResult.ChecksumSHA256)
			assert.Equal(t, types.ChecksumTypeFullObject, headObjectResult.ChecksumType)
		})

		t.Run("it should return 200 for HeadObject with matching If-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
			})
			assert.Nil(t, err)
			assert.NotNil(t, putResult.ETag)

			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket:  bucketName,
				Key:     key,
				IfMatch: putResult.ETag,
			})
			assert.Nil(t, err)
		})

		t.Run("it should return 412 for HeadObject with mismatched If-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
			})
			assert.Nil(t, err)

			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket:  bucketName,
				Key:     key,
				IfMatch: aws.String("\"does-not-match\""),
			})
			// HeadObject 412 has no XML body, so the SDK surfaces it as an HTTP response error only.
			assert.NotNil(t, err)
			var httpErr *awshttp.ResponseError
			if assert.ErrorAs(t, err, &httpErr) {
				assert.Equal(t, 412, httpErr.Response.StatusCode)
			}
		})

		t.Run("it should return an error for HeadObject with matching If-None-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
			})
			assert.Nil(t, err)
			assert.NotNil(t, putResult.ETag)

			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket:      bucketName,
				Key:         key,
				IfNoneMatch: putResult.ETag,
			})
			// 304 Not Modified surfaces as an error from the SDK (no body to deserialize).
			assert.NotNil(t, err)
			var httpErr *awshttp.ResponseError
			if assert.ErrorAs(t, err, &httpErr) {
				assert.Equal(t, 304, httpErr.Response.StatusCode)
			}
		})

		t.Run("it should return 200 for HeadObject with non-matching If-None-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
			})
			assert.Nil(t, err)

			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket:      bucketName,
				Key:         key,
				IfNoneMatch: aws.String("\"does-not-match\""),
			})
			assert.Nil(t, err)
		})
	})
}

func TestDeleteBucket(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should delete an existing bucket"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			deleteBucketResult, err := s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "DeleteBucket failed", "err %v", err)
			}
			assert.NotNil(t, deleteBucketResult)
		})

		t.Run("it should delete an existing bucket with a presigned url"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			presignClient := s3.NewPresignClient(s3Client)
			presignedRequest, err := presignClient.PresignDeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)

			httpClient := buildHttpClient()
			request, err := http.NewRequest(presignedRequest.Method, presignedRequest.URL, nil)
			assert.Nil(t, err)
			deleteBucketResult, err := httpClient.Do(request)
			if err != nil {
				assert.Fail(t, "DeleteBucket failed", "err %v", err)
			}
			assert.NotNil(t, deleteBucketResult)
			assert.Equal(t, 204, deleteBucketResult.StatusCode)
		})

		t.Run("it should fail when deleting non existing bucket"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: aws.String("test2"),
			})

			if err == nil {
				assert.Fail(t, "DeleteBucket should fail when using non existing bucket name")
			}

			var ae smithy.APIError
			if !errors.As(err, &ae) || ae.ErrorCode() != "NoSuchBucket" {
				assert.Fail(t, "Expected aws error NoSuchBucket", "err %v", err)
			}
		})

		t.Run("it should not see the bucket after deletion anymore"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			deleteBucketResult, err := s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "DeleteBucket failed", "err %v", err)
			}
			assert.NotNil(t, deleteBucketResult)

			_, err = s3Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
				Bucket: bucketName,
			})

			var notFoundError *types.NotFound
			if !errors.As(err, &notFoundError) {
				assert.Fail(t, "Expected aws error NotFound", "err %v", err)
			}
		})

		t.Run("it should not allow deleting a bucket with objects in it"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			_, err = s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			if err == nil {
				assert.Fail(t, "DeleteBucket should fail when using non existing bucket name")
			}

			var ae smithy.APIError
			if !errors.As(err, &ae) || ae.ErrorCode() != "BucketNotEmpty" {
				assert.Fail(t, "Expected aws error BucketNotEmpty", "err %v", err)
			}
		})
	})
}

func TestListObjects(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		// Test ListObjectsV1 (deprecated but still supported)
		t.Run("it should list no objects V1"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			listObjectResult, err := s3Client.ListObjects(context.Background(), &s3.ListObjectsInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V1 failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should list a single object V1"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjects(context.Background(), &s3.ListObjectsInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V1 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)

			object := listObjectResult.Contents[0]
			assert.Equal(t, *key, *object.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object.StorageClass)
			assert.Equal(t, int64(20), *object.Size)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *object.ETag)

			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should truncate when listing objects V1"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    key2,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			// Test pagination with MaxKeys=1
			listObjectResult, err := s3Client.ListObjects(context.Background(), &s3.ListObjectsInput{
				Bucket:  bucketName,
				MaxKeys: aws.Int32(1),
			})
			if err != nil {
				assert.Fail(t, "ListObjects V1 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.True(t, *listObjectResult.IsTruncated)

			// For ListObjects V1, NextMarker should be set when truncated
			assert.NotNil(t, listObjectResult.NextMarker)
			assert.NotEmpty(t, *listObjectResult.NextMarker)

			// Get next page using Marker
			listObjectResult2, err := s3Client.ListObjects(context.Background(), &s3.ListObjectsInput{
				Bucket: bucketName,
				Marker: listObjectResult.NextMarker,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V1 pagination failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult2.Name)
			assert.Len(t, listObjectResult2.CommonPrefixes, 0)
			assert.Len(t, listObjectResult2.Contents, 1)
			assert.False(t, *listObjectResult2.IsTruncated)

			// Verify we got different objects
			assert.NotEqual(t, *listObjectResult.Contents[0].Key, *listObjectResult2.Contents[0].Key)
		})

		t.Run("it should list a single object V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, int32(1), *listObjectResult.KeyCount)

			object := listObjectResult.Contents[0]
			assert.Equal(t, *key, *object.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object.StorageClass)
			assert.Equal(t, int64(20), *object.Size)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *object.ETag)

			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should truncate and paginate with continuation token V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			// Create multiple objects for pagination testing
			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    key2,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			// Create third object for pagination
			key3 := aws.String(*keyPrefix + "/hello_world3.txt")
			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, third object!")),
				Key:    key3,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			// Test pagination with MaxKeys=2
			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:  bucketName,
				MaxKeys: aws.Int32(2),
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 2)
			assert.Equal(t, int32(2), *listObjectResult.KeyCount)
			assert.True(t, *listObjectResult.IsTruncated)
			assert.NotEmpty(t, *listObjectResult.NextContinuationToken)

			// Get next page using ContinuationToken
			listObjectResult2, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:            bucketName,
				ContinuationToken: listObjectResult.NextContinuationToken,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 pagination failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult2.Name)
			assert.Len(t, listObjectResult2.CommonPrefixes, 0)
			assert.Len(t, listObjectResult2.Contents, 1)
			assert.Equal(t, int32(1), *listObjectResult2.KeyCount)
			assert.False(t, *listObjectResult2.IsTruncated)
			assert.Equal(t, *listObjectResult.NextContinuationToken, *listObjectResult2.ContinuationToken)

			// Verify all objects are unique
			allKeys := make(map[string]bool)
			for _, obj := range listObjectResult.Contents {
				allKeys[*obj.Key] = true
			}
			for _, obj := range listObjectResult2.Contents {
				if allKeys[*obj.Key] {
					assert.Fail(t, "Duplicate key found in pagination", "key: %s", *obj.Key)
				}
				allKeys[*obj.Key] = true
			}
			assert.Len(t, allKeys, 3) // Should have 3 unique keys
		})

		t.Run("it should paginate filtered list objects via authorizer hook V2"+testSuffix, func(t *testing.T) {
			authorizationCode := `
			function authorizeRequest(request)
			  return true
			end

			function authorizeListObject(request, key)
			  return string.find(key, "allowed/") == 1
			end
			`
			requestAuthorizer, err := lua.NewLuaAuthorizer(authorizationCode)
			if err != nil {
				t.Fatalf("Could not create LuaAuthorizer: %v", err)
			}

			s3Client, _, cleanup := setupTestServerWithAuthorizer(requestAuthorizer, dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}

			keys := []string{
				"allowed/a.txt",
				"denied/a.txt",
				"allowed/b.txt",
				"denied/b.txt",
				"allowed/c.txt",
				"denied/c.txt",
			}
			for i, keyName := range keys {
				_, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
					Bucket: bucketName,
					Body:   bytes.NewReader([]byte(fmt.Sprintf("obj-%d", i))),
					Key:    aws.String(keyName),
				})
				if err != nil {
					assert.Fail(t, "PutObject failed", "err %v", err)
				}
			}

			page1, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:  bucketName,
				MaxKeys: aws.Int32(2),
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 page1 failed", "err %v", err)
			}
			assert.Len(t, page1.Contents, 2)
			assert.True(t, *page1.IsTruncated)
			assert.NotNil(t, page1.NextContinuationToken)

			for _, object := range page1.Contents {
				assert.True(t, strings.HasPrefix(*object.Key, "allowed/"))
			}

			page2, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:            bucketName,
				ContinuationToken: page1.NextContinuationToken,
				MaxKeys:           aws.Int32(2),
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 page2 failed", "err %v", err)
			}
			assert.Len(t, page2.Contents, 1)
			assert.False(t, *page2.IsTruncated)

			for _, object := range page2.Contents {
				assert.True(t, strings.HasPrefix(*object.Key, "allowed/"))
			}

			allKeys := map[string]bool{}
			for _, object := range page1.Contents {
				allKeys[*object.Key] = true
			}
			for _, object := range page2.Contents {
				allKeys[*object.Key] = true
			}
			assert.Len(t, allKeys, 3)
			assert.True(t, allKeys["allowed/a.txt"])
			assert.True(t, allKeys["allowed/b.txt"])
			assert.True(t, allKeys["allowed/c.txt"])
		})

		t.Run("it should handle StartAfter parameter V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			// Create objects with predictable lexicographic order
			keys := []string{
				"my/test/key/a.txt",
				"my/test/key/b.txt",
				"my/test/key/c.txt",
			}

			for i, keyName := range keys {
				putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
					Bucket: bucketName,
					Body:   bytes.NewReader([]byte(fmt.Sprintf("Hello, object %d!", i+1))),
					Key:    aws.String(keyName),
				})
				if err != nil {
					assert.Fail(t, "PutObject failed", "err %v", err)
				}
				assert.NotNil(t, putObjectResult)
			}

			// Test StartAfter functionality
			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:     bucketName,
				StartAfter: aws.String("my/test/key/a.txt"), // Should skip a.txt
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 with StartAfter failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.Contents, 2) // Should get b.txt and c.txt
			assert.Equal(t, int32(2), *listObjectResult.KeyCount)
			assert.False(t, *listObjectResult.IsTruncated)

			// Verify the returned objects are correct
			assert.Equal(t, "my/test/key/b.txt", *listObjectResult.Contents[0].Key)
			assert.Equal(t, "my/test/key/c.txt", *listObjectResult.Contents[1].Key)
		})

		// Continue with existing tests for prefixes and delimiters...
		t.Run("it should list objects with prefix and delimiter V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    aws.String("my.txt"),
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:    bucketName,
				Delimiter: aws.String("/"),
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 1)
			commonPrefix := *listObjectResult.CommonPrefixes[0].Prefix
			assert.Equal(t, "my/", commonPrefix)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, "my.txt", *listObjectResult.Contents[0].Key)
			assert.False(t, *listObjectResult.IsTruncated)
		})

		// Test ListObjectsV2 (recommended)
		t.Run("it should list no objects V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)
			assert.Equal(t, int32(0), *listObjectResult.KeyCount)
		})

		t.Run("it should list a single object V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, int32(1), *listObjectResult.KeyCount)

			object := listObjectResult.Contents[0]
			assert.Equal(t, *key, *object.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object.StorageClass)
			assert.Equal(t, int64(20), *object.Size)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *object.ETag)

			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should truncate and paginate with continuation token V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			// Create multiple objects for pagination testing
			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    key2,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			// Create third object for pagination
			key3 := aws.String(*keyPrefix + "/hello_world3.txt")
			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, third object!")),
				Key:    key3,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			// Test pagination with MaxKeys=2
			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:  bucketName,
				MaxKeys: aws.Int32(2),
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 2)
			assert.Equal(t, int32(2), *listObjectResult.KeyCount)
			assert.True(t, *listObjectResult.IsTruncated)
			assert.NotEmpty(t, *listObjectResult.NextContinuationToken)

			// Get next page using ContinuationToken
			listObjectResult2, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:            bucketName,
				ContinuationToken: listObjectResult.NextContinuationToken,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 pagination failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult2.Name)
			assert.Len(t, listObjectResult2.CommonPrefixes, 0)
			assert.Len(t, listObjectResult2.Contents, 1)
			assert.Equal(t, int32(1), *listObjectResult2.KeyCount)
			assert.False(t, *listObjectResult2.IsTruncated)
			assert.Equal(t, *listObjectResult.NextContinuationToken, *listObjectResult2.ContinuationToken)

			// Verify all objects are unique
			allKeys := make(map[string]bool)
			for _, obj := range listObjectResult.Contents {
				allKeys[*obj.Key] = true
			}
			for _, obj := range listObjectResult2.Contents {
				if allKeys[*obj.Key] {
					assert.Fail(t, "Duplicate key found in pagination", "key: %s", *obj.Key)
				}
				allKeys[*obj.Key] = true
			}
			assert.Len(t, allKeys, 3) // Should have 3 unique keys
		})

		t.Run("it should handle StartAfter parameter V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			// Create objects with predictable lexicographic order
			keys := []string{
				"my/test/key/a.txt",
				"my/test/key/b.txt",
				"my/test/key/c.txt",
			}

			for i, keyName := range keys {
				putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
					Bucket: bucketName,
					Body:   bytes.NewReader([]byte(fmt.Sprintf("Hello, object %d!", i+1))),
					Key:    aws.String(keyName),
				})
				if err != nil {
					assert.Fail(t, "PutObject failed", "err %v", err)
				}
				assert.NotNil(t, putObjectResult)
			}

			// Test StartAfter functionality
			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:     bucketName,
				StartAfter: aws.String("my/test/key/a.txt"), // Should skip a.txt
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 with StartAfter failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.Contents, 2) // Should get b.txt and c.txt
			assert.Equal(t, int32(2), *listObjectResult.KeyCount)
			assert.False(t, *listObjectResult.IsTruncated)

			// Verify the returned objects are correct
			assert.Equal(t, "my/test/key/b.txt", *listObjectResult.Contents[0].Key)
			assert.Equal(t, "my/test/key/c.txt", *listObjectResult.Contents[1].Key)
		})
	})
}

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
					},
				},
			})
			require.NoError(t, err)

			getResult, err := s3Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: bucketName})
			require.NoError(t, err)
			require.Len(t, getResult.Rules, 3)

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

			// Transition actions require storage classes and are not supported.
			expectErrorCode(t, putLifecycle(types.LifecycleRule{
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Transitions: []types.Transition{{
					Days:         aws.Int32(30),
					StorageClass: types.TransitionStorageClassGlacier,
				}},
			}), "NotImplemented")

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

func TestDeleteObjects(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should delete multiple existing objects"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key2, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key},
						{Key: key2},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 2)
			assert.Len(t, result.Errors, 0)

			// Objects must no longer exist
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key2})
			assert.NotNil(t, err)
		})

		t.Run("it should apply per-entry delete authorization hook"+testSuffix, func(t *testing.T) {
			authorizationCode := `
			function authorizeRequest(request)
			  return true
			end

			function authorizeDeleteObjectEntry(request, key)
			  return key ~= "blocked.txt"
			end
			`
			requestAuthorizer, err := lua.NewLuaAuthorizer(authorizationCode)
			if err != nil {
				t.Fatalf("Could not create LuaAuthorizer: %v", err)
			}

			s3Client, _, cleanup := setupTestServerWithAuthorizer(requestAuthorizer, dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			allowedKey := aws.String("allowed.txt")
			blockedKey := aws.String("blocked.txt")

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: allowedKey, Body: bytes.NewReader(body)})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: blockedKey, Body: bytes.NewReader(body)})
			assert.Nil(t, err)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: allowedKey}, {Key: blockedKey}}},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 1)
			assert.Len(t, result.Errors, 1)
			assert.Equal(t, "blocked.txt", *result.Errors[0].Key)
			assert.Equal(t, "AccessDenied", *result.Errors[0].Code)

			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: allowedKey})
			assert.NotNil(t, err)
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: blockedKey})
			assert.Nil(t, err)
		})

		t.Run("it should treat a missing key as successfully deleted"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			nonExistentKey := aws.String("does/not/exist.txt")
			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: nonExistentKey},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 1)
			assert.Len(t, result.Errors, 0)
		})

		t.Run("it should suppress deleted entries in quiet mode"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)

			quiet := true
			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key},
					},
					Quiet: &quiet,
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 0)
			assert.Len(t, result.Errors, 0)

			// Object must still have been deleted
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
		})

		t.Run("it should delete an explicit object VersionId"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket: bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			})
			assert.Nil(t, err)

			firstPut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)
			assert.NotNil(t, firstPut.VersionId)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("second")),
			})
			assert.Nil(t, err)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key, VersionId: firstPut.VersionId},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 1)
			assert.Len(t, result.Errors, 0)
			assert.Equal(t, *key, *result.Deleted[0].Key)
			assert.NotNil(t, result.Deleted[0].VersionId)
			assert.Equal(t, *firstPut.VersionId, *result.Deleted[0].VersionId)

			// Latest version must still exist.
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
		})

		t.Run("it should return DeleteMarkerVersionId for key-only deletes on versioned buckets"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket: bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 1)
			assert.Len(t, result.Errors, 0)
			assert.Equal(t, *key, *result.Deleted[0].Key)
			if assert.NotNil(t, result.Deleted[0].DeleteMarker) {
				assert.True(t, *result.Deleted[0].DeleteMarker)
			}
			assert.Nil(t, result.Deleted[0].VersionId)
			if assert.NotNil(t, result.Deleted[0].DeleteMarkerVersionId) {
				assert.NotEmpty(t, *result.Deleted[0].DeleteMarkerVersionId)
			}

			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
			var responseErr *awshttp.ResponseError
			if assert.True(t, errors.As(err, &responseErr)) {
				assert.Equal(t, http.StatusNotFound, responseErr.HTTPStatusCode())
				assert.Equal(t, "true", responseErr.HTTPResponse().Header.Get("x-amz-delete-marker"))
				assert.Equal(t, aws.ToString(result.Deleted[0].DeleteMarkerVersionId), responseErr.HTTPResponse().Header.Get("x-amz-version-id"))
			}
		})

		t.Run("it should handle duplicate keys with different VersionIds"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket: bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			})
			assert.Nil(t, err)

			put1, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v1"))})
			assert.Nil(t, err)
			assert.NotNil(t, put1.VersionId)

			put2, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v2"))})
			assert.Nil(t, err)
			assert.NotNil(t, put2.VersionId)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: key, VersionId: put1.VersionId}, {Key: key, VersionId: put2.VersionId}}},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Errors, 0)
			assert.Len(t, result.Deleted, 2)

			deletedVersions := map[string]bool{}
			for _, deleted := range result.Deleted {
				if deleted.VersionId != nil {
					deletedVersions[*deleted.VersionId] = true
				}
			}
			assert.True(t, deletedVersions[*put1.VersionId])
			assert.True(t, deletedVersions[*put2.VersionId])
		})

		t.Run("it should return 404 when the bucket does not exist"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key},
					},
				},
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			assert.True(t, errors.As(err, &apiErr))
			assert.Equal(t, "NoSuchBucket", apiErr.ErrorCode())
		})

		t.Run("it should handle an empty delete list"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 0)
			assert.Len(t, result.Errors, 0)
		})

		t.Run("it should partially succeed when some keys exist and some do not"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)

			nonExistentKey := aws.String("does/not/exist.txt")
			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key},
						{Key: nonExistentKey},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 2)
			assert.Len(t, result.Errors, 0)

			// Existing object must be gone
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
		})

		t.Run("it should delete object when ETag matches in DeleteObjects"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)
			assert.NotNil(t, putResult.ETag)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key, ETag: putResult.ETag},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 1)
			assert.Len(t, result.Errors, 0)

			// Object must be gone.
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
		})

		t.Run("it should return an error entry when ETag mismatches in DeleteObjects"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key, ETag: aws.String("\"does-not-match\"")},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 0)
			assert.Len(t, result.Errors, 1)
			assert.Equal(t, *key, *result.Errors[0].Key)
			assert.Equal(t, "PreconditionFailed", *result.Errors[0].Code)

			// Object must still exist.
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
		})
	})
}

func TestBucketVersioning(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should keep prior versions and reuse null version after suspend"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			versioningOut, err := s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusEnabled, versioningOut.Status)

			putV1, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v1"))})
			assert.Nil(t, err)
			assert.NotNil(t, putV1.VersionId)
			assert.NotEmpty(t, aws.ToString(putV1.VersionId))

			putV2, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v2"))})
			assert.Nil(t, err)
			assert.NotNil(t, putV2.VersionId)
			assert.NotEmpty(t, aws.ToString(putV2.VersionId))
			assert.NotEqual(t, aws.ToString(putV1.VersionId), aws.ToString(putV2.VersionId))

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
			})
			assert.Nil(t, err)

			versioningOut, err = s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusSuspended, versioningOut.Status)

			putNull1, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v3"))})
			assert.Nil(t, err)
			assert.Equal(t, "null", aws.ToString(putNull1.VersionId))

			putNull2, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v4"))})
			assert.Nil(t, err)
			assert.Equal(t, "null", aws.ToString(putNull2.VersionId))

			versionsOut, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, Prefix: key})
			assert.Nil(t, err)

			versionIDs := map[string]bool{}
			nullVersionCount := 0
			nullLatest := false
			for _, version := range versionsOut.Versions {
				if aws.ToString(version.Key) != aws.ToString(key) {
					continue
				}
				versionID := aws.ToString(version.VersionId)
				versionIDs[versionID] = true
				if versionID == "null" {
					nullVersionCount++
					nullLatest = aws.ToBool(version.IsLatest)
				}
			}

			assert.True(t, versionIDs[aws.ToString(putV1.VersionId)])
			assert.True(t, versionIDs[aws.ToString(putV2.VersionId)])
			assert.True(t, versionIDs["null"])
			assert.Equal(t, 1, nullVersionCount)
			assert.True(t, nullLatest)
		})

		t.Run("it should resume unique version ids after re-enabling versioning"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
			})
			assert.Nil(t, err)

			versioningOut, err := s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusSuspended, versioningOut.Status)

			putNull1, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("sv1"))})
			assert.Nil(t, err)
			assert.Equal(t, "null", aws.ToString(putNull1.VersionId))

			putNull2, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("sv2"))})
			assert.Nil(t, err)
			assert.Equal(t, "null", aws.ToString(putNull2.VersionId))

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			versioningOut, err = s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusEnabled, versioningOut.Status)

			putV1, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("ev1"))})
			assert.Nil(t, err)
			assert.NotNil(t, putV1.VersionId)
			assert.NotEmpty(t, aws.ToString(putV1.VersionId))
			assert.NotEqual(t, "null", aws.ToString(putV1.VersionId))

			putV2, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("ev2"))})
			assert.Nil(t, err)
			assert.NotNil(t, putV2.VersionId)
			assert.NotEmpty(t, aws.ToString(putV2.VersionId))
			assert.NotEqual(t, "null", aws.ToString(putV2.VersionId))
			assert.NotEqual(t, aws.ToString(putV1.VersionId), aws.ToString(putV2.VersionId))

			versionsOut, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, Prefix: key})
			assert.Nil(t, err)

			versionIDs := map[string]bool{}
			latestByID := map[string]bool{}
			for _, version := range versionsOut.Versions {
				if aws.ToString(version.Key) != aws.ToString(key) {
					continue
				}
				versionID := aws.ToString(version.VersionId)
				versionIDs[versionID] = true
				latestByID[versionID] = aws.ToBool(version.IsLatest)
			}

			assert.True(t, versionIDs["null"])
			assert.True(t, versionIDs[aws.ToString(putV1.VersionId)])
			assert.True(t, versionIDs[aws.ToString(putV2.VersionId)])
			assert.True(t, latestByID[aws.ToString(putV2.VersionId)])
		})

		t.Run("it should keep api status and put semantics across enable suspend re-enable transitions"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			versioningOut, err := s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusEnabled, versioningOut.Status)

			enabledPut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("enabled"))})
			assert.Nil(t, err)
			assert.NotNil(t, enabledPut.VersionId)
			assert.NotEmpty(t, aws.ToString(enabledPut.VersionId))
			assert.NotEqual(t, "null", aws.ToString(enabledPut.VersionId))

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
			})
			assert.Nil(t, err)

			versioningOut, err = s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusSuspended, versioningOut.Status)

			suspendedPut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("suspended"))})
			assert.Nil(t, err)
			assert.Equal(t, "null", aws.ToString(suspendedPut.VersionId))

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			versioningOut, err = s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusEnabled, versioningOut.Status)

			reenabledPut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("reenabled"))})
			assert.Nil(t, err)
			assert.NotNil(t, reenabledPut.VersionId)
			assert.NotEmpty(t, aws.ToString(reenabledPut.VersionId))
			assert.NotEqual(t, "null", aws.ToString(reenabledPut.VersionId))
			assert.NotEqual(t, aws.ToString(enabledPut.VersionId), aws.ToString(reenabledPut.VersionId))
		})

		t.Run("it should return delete-marker semantics for current and explicit versions"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, listenerAddr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			putOut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body)})
			assert.Nil(t, err)
			assert.NotNil(t, putOut.VersionId)
			assert.NotEmpty(t, aws.ToString(putOut.VersionId))

			deleteOut, err := s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: bucketName, Key: key})
			if err != nil {
				t.Fatalf("DeleteObject failed: %v", err)
			}
			markerVersionID := ""
			if deleteOut != nil && deleteOut.VersionId != nil {
				markerVersionID = *deleteOut.VersionId
			}

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			httpClient := buildHttpClient()
			objectURL := fmt.Sprintf("http://%s:%d/%s/%s", testAPIEndpoint, addr.Port, *bucketName, *key)

			headReq, _ := http.NewRequest(http.MethodHead, objectURL, nil)
			headResp, err := httpClient.Do(headReq)
			assert.Nil(t, err)
			defer headResp.Body.Close()
			assert.Equal(t, http.StatusNotFound, headResp.StatusCode)
			assert.Equal(t, "true", headResp.Header.Get("x-amz-delete-marker"))
			if markerVersionID == "" {
				markerVersionID = headResp.Header.Get("x-amz-version-id")
			}
			if markerVersionID == "" {
				t.Fatalf("missing delete marker version id in DeleteObject output and HEAD response")
			}
			assert.Equal(t, markerVersionID, headResp.Header.Get("x-amz-version-id"))

			getReq, _ := http.NewRequest(http.MethodGet, objectURL, nil)
			getResp, err := httpClient.Do(getReq)
			assert.Nil(t, err)
			defer getResp.Body.Close()
			assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
			assert.Equal(t, "true", getResp.Header.Get("x-amz-delete-marker"))
			assert.Equal(t, markerVersionID, getResp.Header.Get("x-amz-version-id"))

			headVersionReq, _ := http.NewRequest(http.MethodHead, objectURL+"?versionId="+markerVersionID, nil)
			headVersionResp, err := httpClient.Do(headVersionReq)
			assert.Nil(t, err)
			defer headVersionResp.Body.Close()
			assert.Equal(t, http.StatusMethodNotAllowed, headVersionResp.StatusCode)
			assert.Equal(t, "true", headVersionResp.Header.Get("x-amz-delete-marker"))
			assert.Equal(t, markerVersionID, headVersionResp.Header.Get("x-amz-version-id"))
			assert.NotEmpty(t, headVersionResp.Header.Get("Last-Modified"))

			getVersionReq, _ := http.NewRequest(http.MethodGet, objectURL+"?versionId="+markerVersionID, nil)
			getVersionResp, err := httpClient.Do(getVersionReq)
			assert.Nil(t, err)
			defer getVersionResp.Body.Close()
			assert.Equal(t, http.StatusMethodNotAllowed, getVersionResp.StatusCode)
			assert.Equal(t, "true", getVersionResp.Header.Get("x-amz-delete-marker"))
			assert.Equal(t, markerVersionID, getVersionResp.Header.Get("x-amz-version-id"))
			assert.NotEmpty(t, getVersionResp.Header.Get("Last-Modified"))
		})

		t.Run("it should return version id for completed multipart upload in versioned bucket"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			createOut, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.NotNil(t, createOut)
			assert.NotNil(t, createOut.UploadId)

			_, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Key:        key,
				UploadId:   createOut.UploadId,
				PartNumber: aws.Int32(1),
				Body:       bytes.NewReader(body),
			})
			assert.Nil(t, err)

			completeOut, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: createOut.UploadId,
			})
			assert.Nil(t, err)
			assert.NotNil(t, completeOut)
			assert.NotNil(t, completeOut.VersionId)
			assert.NotEmpty(t, aws.ToString(completeOut.VersionId))

			versionsOut, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, Prefix: key})
			assert.Nil(t, err)
			assert.NotEmpty(t, versionsOut.Versions)

			versionFound := false
			for _, version := range versionsOut.Versions {
				if aws.ToString(version.Key) == aws.ToString(key) && aws.ToString(version.VersionId) == aws.ToString(completeOut.VersionId) {
					versionFound = true
					assert.True(t, aws.ToBool(version.IsLatest))
					break
				}
			}
			assert.True(t, versionFound)
		})

		t.Run("it should paginate ListObjectVersions with key and version markers"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			putV1, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v1"))})
			assert.Nil(t, err)
			assert.NotNil(t, putV1.VersionId)
			assert.NotEmpty(t, aws.ToString(putV1.VersionId))
			putV2, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v2"))})
			assert.Nil(t, err)
			assert.NotNil(t, putV2.VersionId)
			assert.NotEmpty(t, aws.ToString(putV2.VersionId))
			assert.NotEqual(t, aws.ToString(putV1.VersionId), aws.ToString(putV2.VersionId))

			page1, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, MaxKeys: aws.Int32(1)})
			assert.Nil(t, err)
			assert.True(t, aws.ToBool(page1.IsTruncated))
			assert.NotNil(t, page1.NextKeyMarker)
			assert.NotNil(t, page1.NextVersionIdMarker)
			assert.Len(t, page1.Versions, 1)

			page2, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, MaxKeys: aws.Int32(1), KeyMarker: page1.NextKeyMarker, VersionIdMarker: page1.NextVersionIdMarker})
			assert.Nil(t, err)
			assert.Len(t, page2.Versions, 1)
			assert.NotEqual(t, aws.ToString(page1.Versions[0].VersionId), aws.ToString(page2.Versions[0].VersionId))
		})

		t.Run("it should count common prefixes in ListObjectVersions pagination"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			putA, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: aws.String("a/one.txt"), Body: bytes.NewReader(body)})
			assert.Nil(t, err)
			assert.NotNil(t, putA.VersionId)
			assert.NotEmpty(t, aws.ToString(putA.VersionId))
			putB, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: aws.String("b/two.txt"), Body: bytes.NewReader(body)})
			assert.Nil(t, err)
			assert.NotNil(t, putB.VersionId)
			assert.NotEmpty(t, aws.ToString(putB.VersionId))

			page1, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, Delimiter: aws.String("/"), MaxKeys: aws.Int32(1)})
			assert.Nil(t, err)
			assert.True(t, aws.ToBool(page1.IsTruncated))
			assert.Len(t, page1.CommonPrefixes, 1)

			page2, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, Delimiter: aws.String("/"), MaxKeys: aws.Int32(1), KeyMarker: page1.NextKeyMarker, VersionIdMarker: page1.NextVersionIdMarker})
			assert.Nil(t, err)
			assert.Len(t, page2.CommonPrefixes, 1)
			assert.NotEqual(t, aws.ToString(page1.CommonPrefixes[0].Prefix), aws.ToString(page2.CommonPrefixes[0].Prefix))
		})
	})
}
