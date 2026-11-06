package session

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	_ "github.com/mattn/go-sqlite3"
)

func TestRepositoryLifetimeAndRevocation(t *testing.T) {
	ctx := context.Background()
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "session.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo, err := NewRepository(ctx, db, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// Reapplying migrations must preserve existing sessions.
	if _, err := NewRepository(ctx, db, 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	token, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	if len(token) != 64 || Hash(token) == token {
		t.Fatal("invalid token/hash")
	}
	record := Record{IDHash: Hash(token), AccessKeyID: "key", AccountID: "account", PrincipalID: "principal", CredentialHash: "fingerprint", CSRFToken: "csrf", CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(12 * time.Hour), ClientIP: "127.0.0.1", UserAgent: "test"}
	if err := repo.Create(ctx, record); err != nil {
		t.Fatal(err)
	}
	found, err := repo.GetAndTouch(ctx, Hash(token), now.Add(29*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if found.AccessKeyID != "key" || found.CredentialHash != "fingerprint" || !found.LastSeenAt.Equal(now.Add(29*time.Minute)) {
		t.Fatalf("unexpected session: %+v", found)
	}
	if _, err := repo.GetAndTouch(ctx, Hash(token), now.Add(60*time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("idle expiry: %v", err)
	}
	if err := repo.Cleanup(ctx, now.Add(60*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetAndTouch(ctx, Hash(token), now); !errors.Is(err, ErrExpired) {
		t.Fatal("cleanup left expired session")
	}
	record.IDHash = Hash("second")
	record.LastSeenAt = now.Add(12*time.Hour - time.Minute)
	if err := repo.Create(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetAndTouch(ctx, record.IDHash, now.Add(12*time.Hour)); !errors.Is(err, ErrExpired) {
		t.Fatalf("maximum expiry: %v", err)
	}
	if err := repo.Delete(ctx, record.IDHash); err != nil {
		t.Fatal(err)
	}
}
