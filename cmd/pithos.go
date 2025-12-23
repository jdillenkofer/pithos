package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"reflect"
	"bufio"
	"strings"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/benchmark"
	storageConfig "github.com/jdillenkofer/pithos/internal/storage/config"
	"github.com/jdillenkofer/pithos/internal/storage/integrity"
	"github.com/jdillenkofer/pithos/internal/storage/migrator"
	"github.com/jdillenkofer/pithos/internal/telemetry"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	"github.com/jdillenkofer/pithos/internal/auditlog/tool"
)

const defaultStorageConfig = `
{
  "type": "MetadataPartStorage",
  "db": {
    "type": "RegisterDatabaseReference",
	"refName": "db",
	"db": {
      "type": "SqliteDatabase",
	  "dbPath": "./data/pithos.db"
	}
  },
  "metadataStore": {
    "type": "SqlMetadataStore",
	"db": {
	  "type": "DatabaseReference",
	  "refName": "db"
	}
  },
  "partStore": {
    "type": "SqlPartStore",
	"db": {
	  "type": "DatabaseReference",
	  "refName": "db"
	}
  }
}
`

const defaultAuthorizationCode = `
function authorizeRequest(request)
  return true
end
`

const subcommandServe = "serve"
const subcommandMigrateStorage = "migrate-storage"
const subcommandBenchmarkStorage = "benchmark-storage"
const subcommandValidateStorage = "validate-storage"
const subcommandAuditLog = "audit-log"

func main() {
	ctx := context.Background()
	if len(os.Args) < 2 {
		slog.Info(fmt.Sprintf("Usage: %s %s|%s|%s|%s|%s [options]", os.Args[0], subcommandServe, subcommandMigrateStorage, subcommandBenchmarkStorage, subcommandValidateStorage, subcommandAuditLog))
		os.Exit(1)
	}

	logLevelVar := setupLogging()

	subcommand := os.Args[1]
	switch subcommand {
	case subcommandServe:
		serve(ctx, logLevelVar)
	case subcommandMigrateStorage:
		migrateStorage(ctx)
	case subcommandBenchmarkStorage:
		benchmarkStorage(ctx)
	case subcommandValidateStorage:
		validateStorage(ctx)
	case subcommandAuditLog:
		auditLogTool()
	default:
		slog.Error(fmt.Sprintf("Invalid subcommand: %s. Expected one of '%s', '%s', '%s', '%s', '%s'.", subcommand, subcommandServe, subcommandMigrateStorage, subcommandBenchmarkStorage, subcommandValidateStorage, subcommandAuditLog))
		os.Exit(1)
	}
}

func setupLogging() *slog.LevelVar {
	var logLevelVar = new(slog.LevelVar)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     logLevelVar,
	}))
	slog.SetDefault(logger)
	return logLevelVar
}

func serve(ctx context.Context, logLevelVar *slog.LevelVar) {
	settings, err := settings.LoadSettings(os.Args[2:])
	if err != nil {
		slog.Error(fmt.Sprint("Error while loading settings: ", err))
		os.Exit(1)
	}

	// Set up OpenTelemetry.
	if settings.OtelEnabled() {
		otelShutdown, err := telemetry.SetupOTelSDK(ctx, settings)
		if err != nil {
			slog.Error(fmt.Sprint("Error setting up OpenTelemetry: ", err))
			os.Exit(1)
		}
		// Handle shutdown properly so nothing leaks.
		defer func() {
			if err := otelShutdown(context.Background()); err != nil {
				slog.Error(fmt.Sprint("Error shutting down OpenTelemetry: ", err))
			}
		}()
	}

	logLevel := settings.LogLevel()
	logLevelVar.Set(logLevel)

	dbContainer, store := loadStorageConfiguration(settings.StorageJsonPath(), prometheus.DefaultRegisterer)

	dbs := dbContainer.Dbs()

	err = store.Start(ctx)
	if err != nil {
		slog.Error(fmt.Sprint("Couldn't start storage: ", err))
		os.Exit(1)
	}

	defer func() {
		err := store.Stop(ctx)
		if err != nil {
			slog.Error(fmt.Sprint("Couldn't stop storage: ", err))
			os.Exit(1)
		}
		for _, db := range dbs {
			err = db.Close()
			if err != nil {
				slog.Error(fmt.Sprint("Couldn't close database: ", err))
				os.Exit(1)
			}
		}
	}()

	requestAuthorizer, err := loadRequestAuthorizer(settings.AuthorizerPath())
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create LuaAuthorizer: %s", err))
	}

	handler := server.SetupServer(settings.Credentials(), settings.Region(), settings.Domain(), requestAuthorizer, store)
	addr := fmt.Sprintf("%v:%v", settings.BindAddress(), settings.Port())
	httpServer := &http.Server{
		BaseContext: func(net.Listener) context.Context { return ctx },
		Addr:        addr,
		Handler:     handler,
	}

	if settings.MonitoringPortEnabled() {
		monitoringHandler := server.SetupMonitoringServer(dbs)
		monitoringAddr := fmt.Sprintf("%v:%v", settings.BindAddress(), settings.MonitoringPort())
		httpMonitoringServer := &http.Server{
			BaseContext: func(net.Listener) context.Context { return ctx },
			Addr:        monitoringAddr,
			Handler:     monitoringHandler,
		}
		go (func() {
			slog.Info(fmt.Sprintf("Listening with monitoring api on http://%v", monitoringAddr))
			httpMonitoringServer.ListenAndServe()
		})()
	}

	slog.Info(fmt.Sprintf("Listening with s3 api on http://%v", addr))
	err = httpServer.ListenAndServe()
	if err != nil {
		slog.Error(fmt.Sprintf("Error while starting http server: %s", err))
		os.Exit(1)
	}
}

func loadRequestAuthorizer(authorizerPath string) (*lua.LuaAuthorizer, error) {
	authorizerCode, err := os.ReadFile(authorizerPath)
	if err != nil {
		slog.Warn(fmt.Sprint("Couldn't load authorizer: ", err))
		slog.Warn("Using defaultAuthorizationCode (which allows every operation) as fallback")
		authorizerCode = []byte(defaultAuthorizationCode)
	}
	return lua.NewLuaAuthorizer(string(authorizerCode))
}

func loadStorageConfiguration(storageJsonPath string, prometheusRegisterer prometheus.Registerer) (*config.DbContainer, storage.Storage) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		slog.Error(fmt.Sprint("Error while creating diContainer: ", err))
		os.Exit(1)
	}
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*prometheus.Registerer)(nil)), prometheusRegisterer)
	if err != nil {
		slog.Error(fmt.Sprint("Error while registering prometheus.Registerer in diContainer: ", err))
		os.Exit(1)
	}

	dbContainer := config.NewDbContainer()
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*config.DbContainer)(nil)), dbContainer)
	if err != nil {
		slog.Error(fmt.Sprint("Error while registering dbContainer in diContainer: ", err))
		os.Exit(1)
	}

	storageJsonConfig, err := os.ReadFile(storageJsonPath)
	if err != nil {
		slog.Warn(fmt.Sprint("Couldn't load storageJson: ", err))
		slog.Warn("Using defaultStorageConfig as fallback")
		storageJsonConfig = []byte(defaultStorageConfig)
	}

	storageInstantiator, err := storageConfig.CreateStorageInstantiatorFromJson(storageJsonConfig)
	if err != nil {
		slog.Error(fmt.Sprint("Error while creating storageInstantiator from json: ", err))
		os.Exit(1)
	}
	err = storageInstantiator.RegisterReferences(diContainer)
	if err != nil {
		slog.Error(fmt.Sprint("Error while registering references: ", err))
		os.Exit(1)
	}
	store, err := storageInstantiator.Instantiate(diContainer)
	if err != nil {
		slog.Error(fmt.Sprint("Error while instantiating storage: ", err))
		os.Exit(1)
	}
	return dbContainer, store
}

func migrateStorage(ctx context.Context) {
	if len(os.Args) < 4 {
		slog.Info(fmt.Sprintf("Usage: %s %s [source-config.json] [destination-config.json]", os.Args[0], subcommandMigrateStorage))
		os.Exit(1)
	}
	sourceStorageConfig := os.Args[2]
	destinationStorageConfig := os.Args[3]

	sourceDbContainer, sourceStorage := loadStorageConfiguration(sourceStorageConfig, prometheus.NewRegistry())

	sourceDbs := sourceDbContainer.Dbs()

	err := sourceStorage.Start(ctx)
	if err != nil {
		slog.Error(fmt.Sprint("Couldn't start storage: ", err))
		os.Exit(1)
	}

	defer func() {
		err := sourceStorage.Stop(ctx)
		if err != nil {
			slog.Error(fmt.Sprint("Couldn't stop storage: ", err))
			os.Exit(1)
		}
		for _, db := range sourceDbs {
			err = db.Close()
			if err != nil {
				slog.Error(fmt.Sprint("Couldn't close database:", err))
				os.Exit(1)
			}
		}
	}()

	destinationDbContainer, destinationStorage := loadStorageConfiguration(destinationStorageConfig, prometheus.NewRegistry())

	destinationDbs := destinationDbContainer.Dbs()

	err = destinationStorage.Start(ctx)
	if err != nil {
		slog.Error(fmt.Sprint("Couldn't start storage: ", err))
		os.Exit(1)
	}

	defer func() {
		err := destinationStorage.Stop(ctx)
		if err != nil {
			slog.Error(fmt.Sprint("Couldn't stop storage: ", err))
			os.Exit(1)
		}
		for _, db := range destinationDbs {
			err = db.Close()
			if err != nil {
				slog.Error(fmt.Sprint("Couldn't close database: ", err))
				os.Exit(1)
			}
		}
	}()

	slog.Info("Storage migration started!")
	err = migrator.MigrateStorage(ctx, sourceStorage, destinationStorage)
	if err != nil {
		slog.Error(fmt.Sprint("Could not migrate storage: ", err))
		os.Exit(1)
	}
	slog.Info("Storage migration successfully completed!")
}

func benchmarkStorage(ctx context.Context) {
	if len(os.Args) < 3 {
		slog.Info(fmt.Sprintf("Usage: %s %s [config.json]", os.Args[0], subcommandBenchmarkStorage))
		os.Exit(1)
	}
	storageConfig := os.Args[2]

	dbContainer, storage := loadStorageConfiguration(storageConfig, prometheus.NewRegistry())

	dbs := dbContainer.Dbs()

	err := storage.Start(ctx)
	if err != nil {
		slog.Error(fmt.Sprint("Couldn't start storage: ", err))
		os.Exit(1)
	}

	defer func() {
		err := storage.Stop(ctx)
		if err != nil {
			slog.Error(fmt.Sprint("Couldn't stop storage: ", err))
			os.Exit(1)
		}
		for _, db := range dbs {
			err = db.Close()
			if err != nil {
				slog.Error(fmt.Sprint("Couldn't close database:", err))
				os.Exit(1)
			}
		}
	}()

	slog.Info("Storage benchmark started!")
	benchmarkResult, err := benchmark.BenchmarkStorage(ctx, storage)
	if err != nil {
		slog.Error(fmt.Sprint("Could not benchmark storage: ", err))
		os.Exit(1)
	}
	slog.Info("Storage benchmark successfully completed!")
	for _, sb := range benchmarkResult.SizeBenchmarks {
		sizeStr := formatSize(sb.SizeBytes)
		slog.Info(fmt.Sprintf("%s objects - Upload: %s, Download: %s",
			sizeStr,
			formatSpeed(sb.UploadSpeedBytesPerSecond),
			formatSpeed(sb.DownloadSpeedBytesPerSecond)))
	}
}

func formatSpeed(bytesPerSecond float64) string {
	units := []string{"B/s", "KB/s", "MB/s", "GB/s", "TB/s"}
	size := bytesPerSecond
	unitIndex := 0

	for size >= 1000 && unitIndex < len(units)-1 {
		size /= 1000
		unitIndex++
	}

	return fmt.Sprintf("%.2f %s", size, units[unitIndex])
}

func formatSize(bytes int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	size := float64(bytes)
	unitIndex := 0

	for size >= 1000 && unitIndex < len(units)-1 {
		size /= 1000
		unitIndex++
	}

	return fmt.Sprintf("%.0f %s", size, units[unitIndex])
}

func validateStorage(ctx context.Context) {
	// Define flags
	fs := flag.NewFlagSet(subcommandValidateStorage, flag.ExitOnError)
	deleteCorrupted := fs.Bool("delete-corrupted", false, "Delete corrupted objects")
	force := fs.Bool("force", false, "Force deletion without confirmation")
	jsonOutput := fs.Bool("json", false, "Output results in JSON format")
	outputPath := fs.String("output", "", "Path to write validation report (optional)")

	// Parse flags
	// os.Args[0] is program name, os.Args[1] is subcommand
	// We need to parse starting from os.Args[2]
	if len(os.Args) < 3 {
		slog.Info(fmt.Sprintf("Usage: %s %s [config.json] [options]", os.Args[0], subcommandValidateStorage))
		fs.PrintDefaults()
		os.Exit(1)
	}

	// The config file is the first argument after subcommand
	storageConfigPath := os.Args[2]

	// Parse remaining flags
	if len(os.Args) > 3 {
		fs.Parse(os.Args[3:])
	}

	// Load storage
	dbContainer, storage := loadStorageConfiguration(storageConfigPath, prometheus.NewRegistry())

	dbs := dbContainer.Dbs()

	err := storage.Start(ctx)
	if err != nil {
		slog.Error(fmt.Sprint("Couldn't start storage: ", err))
		os.Exit(1)
	}

	defer func() {
		err := storage.Stop(ctx)
		if err != nil {
			slog.Error(fmt.Sprint("Couldn't stop storage: ", err))
			os.Exit(1)
		}
		for _, db := range dbs {
			err = db.Close()
			if err != nil {
				slog.Error(fmt.Sprint("Couldn't close database:", err))
				os.Exit(1)
			}
		}
	}()

	slog.Info("Storage integrity validation started!")

	validator := integrity.NewValidator(storage, dbContainer, *deleteCorrupted, *force)
	report, err := validator.ValidateAll(ctx)
	if err != nil {
		slog.Error(fmt.Sprintf("Validation failed: %v", err))
		os.Exit(1)
	}

	// Output results
	var outputWriter io.Writer = os.Stdout
	if *outputPath != "" {
		f, err := os.Create(*outputPath)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to create output file: %v", err))
			os.Exit(1)
		}
		defer f.Close()
		outputWriter = f
	}

	if *jsonOutput {
		err = integrity.OutputJSON(report, outputWriter)
	} else {
		err = integrity.OutputHumanReadable(report, outputWriter)
	}

	if err != nil {
		slog.Error(fmt.Sprintf("Failed to write report: %v", err))
		os.Exit(1)
	}

	if report.HasFailures() {
		slog.Error("Integrity validation found failures.")
		os.Exit(1)
	}

	slog.Info("Storage integrity validation successfully completed!")
}

func auditLogTool() {
	if len(os.Args) < 3 {
		slog.Info(fmt.Sprintf("Usage: %s %s [verify|dump|stats|keygen] [options]", os.Args[0], subcommandAuditLog))
		os.Exit(1)
	}

	sub := os.Args[2]
	if sub == "keygen" {
		fs := flag.NewFlagSet(subcommandAuditLog+" "+sub, flag.ExitOnError)
		outputBase := fs.String("f", "", "Output base filename (e.g. 'audit_key')")
		fs.Parse(os.Args[3:])

		auditTool := tool.NewAuditLogTool("", nil, nil)
		if *outputBase == "" {
			err := auditTool.Keygen(os.Stdout)
			if err != nil {
				slog.Error(fmt.Sprintf("Key generation failed: %v", err))
				os.Exit(1)
			}
			return
		}

		// File-based keygen
		keys, err := auditTool.GenerateAuditKeys()
		if err != nil {
			slog.Error(fmt.Sprintf("Key generation failed: %v", err))
			os.Exit(1)
		}

		edBase := *outputBase + "_ed25519"
		mlBase := *outputBase + "_mldsa"
		files := []string{edBase, edBase + ".pub", mlBase, mlBase + ".pub"}

		existing := []string{}
		for _, f := range files {
			if _, err := os.Stat(f); err == nil {
				existing = append(existing, f)
			}
		}

		if len(existing) > 0 {
			fmt.Printf("The following files already exist:\n")
			for _, f := range existing {
				fmt.Printf("  %s\n", f)
			}
			fmt.Print("Overwrite (y/n)? ")
			var response string
			fmt.Scanln(&response)
			if strings.ToLower(response) != "y" {
				fmt.Println("Key generation aborted.")
				return
			}
		}

		if err := auditTool.WriteKeypair(edBase, keys.Ed25519Priv, keys.Ed25519Pub); err != nil {
			slog.Error(fmt.Sprintf("Failed to write Ed25519 keys: %v", err))
			os.Exit(1)
		}
		if err := auditTool.WriteKeypair(mlBase, keys.MlDsaPriv, keys.MlDsaPub); err != nil {
			slog.Error(fmt.Sprintf("Failed to write ML-DSA keys: %v", err))
			os.Exit(1)
		}

		fmt.Printf("Keys saved to:\n")
		for _, f := range files {
			fmt.Printf("  %s\n", f)
		}
		return
	}

	fs := flag.NewFlagSet(subcommandAuditLog+" "+sub, flag.ExitOnError)
	
	inputFilePath := fs.String("input-file", "", "Path to the audit log file")
	inputFormat := fs.String("input-format", "bin", "Input format (bin, json)")
	ed25519PubKeyStr := fs.String("ed25519-public-key", "", "Base64 encoded Ed25519 public key or path to key file")
	mlDsaPubKeyStr := fs.String("ml-dsa-public-key", "", "Base64 encoded ML-DSA public key or path to key file")
	outputFormat := fs.String("output-format", "json", "Output format (json, text, bin) - only for dump")
	outputFilePath := fs.String("output-file", "-", "Output path (use '-' for stdout)")

	fs.Parse(os.Args[3:])

	if *inputFilePath == "" {
		slog.Error("Input file path is required")
		fs.PrintDefaults()
		os.Exit(1)
	}

	if *ed25519PubKeyStr == "" {
		slog.Error("Ed25519 public key is required")
		fs.PrintDefaults()
		os.Exit(1)
	}

	edPubKey, err := signing.LoadEd25519PublicKey(*ed25519PubKeyStr)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to load Ed25519 public key: %v", err))
		os.Exit(1)
	}

	var mlDsaVerifier signing.Verifier
	if *mlDsaPubKeyStr != "" {
		mlPub, err := signing.LoadMlDsaPublicKey(*mlDsaPubKeyStr)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to load ML-DSA public key: %v", err))
			os.Exit(1)
		}
		mlDsaVerifier = signing.NewMlDsaVerifier(mlPub)
	}

	var out io.Writer = os.Stdout
	if *outputFilePath != "-" {
		f, err := os.Create(*outputFilePath)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to create output file: %v", err))
			os.Exit(1)
		}
		defer f.Close()
		out = f
	}

	bw := bufio.NewWriter(out)
	defer bw.Flush()

	auditTool := tool.NewAuditLogTool(*inputFilePath, signing.NewEd25519Verifier(edPubKey), mlDsaVerifier)

	switch sub {
	case "verify":
		err = auditTool.Verify(*inputFormat)
		if err == nil {
			fmt.Fprintln(os.Stderr, "Audit log verification successful!")
		}
	case "dump":
		err = auditTool.Dump(*inputFormat, *outputFormat, bw)
	case "stats":
		err = auditTool.PrintStats(*inputFormat, bw)
	default:
		slog.Error(fmt.Sprintf("Unknown audit-log subcommand: %s", sub))
		os.Exit(1)
	}

	if err != nil {
		slog.Error(fmt.Sprintf("Audit log operation failed: %v", err))
		os.Exit(1)
	}
}
