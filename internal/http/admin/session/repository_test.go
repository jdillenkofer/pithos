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
func TestLoginLimiter(t *testing.T) {
	limiter := NewLoginLimiter()
	now := time.Now()
	limiter.now = func() time.Time { return now }
	for i := 0; i < 10; i++ {
		if !limiter.Allow("127.0.0.1", "key") {
			t.Fatalf("denied attempt %d", i)
		}
	}
	if limiter.Allow("different-ip", "key") {
		t.Fatal("access-key limit missing")
	}
	for i := 0; i < 10; i++ {
		if !limiter.Allow("127.0.0.1", "other-key") {
			t.Fatal("unexpected denial")
		}
	}
	if limiter.Allow("127.0.0.1", "third-key") {
		t.Fatal("IP limit missing")
	}
	now = now.Add(time.Minute)
	if !limiter.Allow("127.0.0.1", "key") {
		t.Fatal("window did not reset")
	}
	for i := 0; i < 10000; i++ {
		limiter.windows[time.Unix(int64(i), 0).String()] = attemptWindow{reset: now.Add(time.Minute)}
	}
	if limiter.Allow("new-ip", "new-key") {
		t.Fatal("memory bound missing")
	}
}
