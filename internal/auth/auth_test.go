package auth

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"freud/internal/db"
)

func TestPasswordAndValidation(t *testing.T) {
	if _, err := ValidateUsername("a"); err == nil {
		t.Fatalf("expected short username error")
	}
	if _, err := ValidateUsername("bad user!"); err == nil {
		t.Fatalf("expected invalid char username error")
	}
	u, err := ValidateUsername("alice_01")
	if err != nil || u != "alice_01" {
		t.Fatalf("unexpected valid username error: %v", err)
	}

	if _, err := HashPassword("12345"); err == nil {
		t.Fatalf("expected short password error")
	}
	hash, err := HashPassword("s3cret-pass")
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}
	if !CheckPassword(hash, "s3cret-pass") || CheckPassword(hash, "wrong") {
		t.Fatalf("CheckPassword mismatch")
	}

	tok, err := NewSessionToken()
	if err != nil || len(tok) != 64 {
		t.Fatalf("unexpected session token: %q err=%v", tok, err)
	}
}

func TestLoginLimiterAndEnsureAdminFromEnv(t *testing.T) {
	lim := NewLoginLimiter(2, time.Minute)
	if !lim.Allow("1.2.3.4") {
		t.Fatalf("expected initial attempt allowed")
	}
	lim.RecordFailure("1.2.3.4")
	lim.RecordFailure("1.2.3.4")
	if lim.Allow("1.2.3.4") {
		t.Fatalf("expected attempt blocked after max failures")
	}
	lim.Reset("1.2.3.4")
	if !lim.Allow("1.2.3.4") {
		t.Fatalf("expected reset to allow attempts again")
	}

	store, err := db.Open(filepath.Join(t.TempDir(), "auth.db"))
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	defer store.Close()

	created, err := EnsureAdminFromEnv(context.Background(), store, "admin1234")
	if err != nil || !created {
		t.Fatalf("expected admin created from env, created=%v err=%v", created, err)
	}
	again, err := EnsureAdminFromEnv(context.Background(), store, "admin1234")
	if err != nil || again {
		t.Fatalf("expected second EnsureAdminFromEnv no-op, again=%v err=%v", again, err)
	}
}
