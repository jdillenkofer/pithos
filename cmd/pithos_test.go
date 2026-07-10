package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/pgx"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	prometheusStorageMiddleware "github.com/jdillenkofer/pithos/internal/storage/middlewares/prometheus"
	"github.com/jdillenkofer/pithos/internal/storage/outbox"
	"github.com/jdillenkofer/pithos/internal/storage/replication"
	"github.com/jdillenkofer/pithos/internal/storage/s3client"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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

const tempDirCleanupAttempts = 50
const tempDirCleanupRetryDelay = 100 * time.Millisecond

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
		var err error
		for attempt := 0; attempt < tempDirCleanupAttempts; attempt++ {
			err = os.RemoveAll(path)
			if err == nil {
				break
			}
			if attempt+1 < tempDirCleanupAttempts {
				time.Sleep(tempDirCleanupRetryDelay)
			}
		}
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
