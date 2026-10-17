package session

import (
	"errors"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/pgx"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestPostgresSessions(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := t.Context()
	container, err := postgres.Run(ctx, testutils.PostgresImage, postgres.WithUsername("postgres"), postgres.WithPassword("postgres"), postgres.WithDatabase("postgres"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	defer container.Terminate(ctx)
	connection, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	db, err := pgx.OpenDatabase(connection, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo, err := NewRepository(ctx, db, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	record := Record{IDHash: Hash("token"), AccessKeyID: "admin", AccountID: "account", PrincipalID: "principal", CredentialHash: "fingerprint", CSRFToken: "csrf", CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := repo.Create(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRepository(ctx, db, 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	found, err := repo.GetAndTouch(ctx, record.IDHash, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if found.AccessKeyID != "admin" {
		t.Fatal("session identity lost")
	}
	if _, err := repo.GetAndTouch(ctx, record.IDHash, now.Add(32*time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("idle expiry: %v", err)
	}
	if err := repo.Cleanup(ctx, now.Add(32*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, record.IDHash); err != nil {
		t.Fatal(err)
	}
}
