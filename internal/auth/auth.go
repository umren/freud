package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os/user"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"

	"freud/internal/db"
)

const (
	minPasswordLen = 6
	maxPasswordLen = 128
	sessionTTL     = 7 * 24 * time.Hour
)

var usernamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{1,31}$`)

// ValidateUsername checks that username is 2-32 safe characters.
func ValidateUsername(username string) (string, error) {
	u := strings.TrimSpace(username)
	if !usernamePattern.MatchString(u) {
		return "", fmt.Errorf("username must be 2-32 characters (letters, digits, '.', '_', '-', starting with letter/digit)")
	}
	return u, nil
}

// ValidatePassword ensures password meets minimum security length bounds.
func ValidatePassword(password string) error {
	if len(password) < minPasswordLen {
		return fmt.Errorf("password must be at least %d characters", minPasswordLen)
	}
	if len(password) > maxPasswordLen {
		return fmt.Errorf("password must be at most %d characters", maxPasswordLen)
	}
	return nil
}

// HashPassword hashes a plaintext password with bcrypt.
func HashPassword(password string) (string, error) {
	if err := ValidatePassword(password); err != nil {
		return "", err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(hash), nil
}

// CheckPassword verifies a plaintext password against a stored bcrypt hash.
func CheckPassword(hash, password string) bool {
	if hash == "" || password == "" {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// NewSessionToken generates a cryptographically random 32-byte hex session token.
func NewSessionToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate session token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// ValidateTokenName checks that an API token label is 1-64 characters after trimming.
func ValidateTokenName(name string) (string, error) {
	n := strings.TrimSpace(name)
	if len(n) == 0 || len(n) > 64 {
		return "", fmt.Errorf("token name must be 1-64 characters")
	}
	return n, nil
}

// NewAPIToken generates a new raw API token ("frd_<hex>"), its display prefix, and its SHA-256 hex digest.
func NewAPIToken() (raw, prefix, hash string, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", "", fmt.Errorf("generate api token: %w", err)
	}
	raw = "frd_" + hex.EncodeToString(buf)
	prefix = raw[:12]
	hash = HashAPIToken(raw)
	return raw, prefix, hash, nil
}

// HashAPIToken computes the SHA-256 hex digest used to store and look up API tokens.
func HashAPIToken(raw string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(raw)))
	return hex.EncodeToString(sum[:])
}

// SessionExpiry returns the standard expiration timestamp for a newly created session.
func SessionExpiry() time.Time {
	return time.Now().UTC().Add(sessionTTL)
}

// LocalActor returns the current OS username (falling back to "local") for trusted CLI/TUI audit entries.
func LocalActor() string {
	if u, err := user.Current(); err == nil && strings.TrimSpace(u.Username) != "" {
		return u.Username
	}
	return "local"
}

// EnsureAdminFromEnv creates the initial "admin" user if no admin exists yet and passwordEnv is non-empty.
func EnsureAdminFromEnv(ctx context.Context, store *db.Store, passwordEnv string) (bool, error) {
	if strings.TrimSpace(passwordEnv) == "" {
		return false, nil
	}
	hasAdmin, err := store.HasAdmin(ctx)
	if err != nil || hasAdmin {
		return false, err
	}
	hash, err := HashPassword(passwordEnv)
	if err != nil {
		return false, fmt.Errorf("invalid FREUD_ADMIN_PASSWORD: %w", err)
	}
	if _, err := store.CreateUser(ctx, "admin", hash, true); err != nil {
		return false, err
	}
	_ = store.RecordAudit(ctx, "admin", "SETUP_ADMIN", "admin", "env", "Initialized via FREUD_ADMIN_PASSWORD")
	return true, nil
}

// LoginLimiter provides lightweight in-memory brute-force protection for /login.
type LoginLimiter struct {
	mu       sync.Mutex
	attempts map[string][]time.Time
	window   time.Duration
	maxFails int
}

// NewLoginLimiter creates a rate limiter permitting up to maxFails failures per key within window.
func NewLoginLimiter(maxFails int, window time.Duration) *LoginLimiter {
	return &LoginLimiter{
		attempts: make(map[string][]time.Time),
		window:   window,
		maxFails: maxFails,
	}
}

// Allow reports whether another login attempt is allowed for key.
func (l *LoginLimiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.pruneLocked(key, now)
	return len(l.attempts[key]) < l.maxFails
}

// RecordFailure records a failed login attempt for key.
func (l *LoginLimiter) RecordFailure(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.pruneLocked(key, now)
	l.attempts[key] = append(l.attempts[key], now)
}

// Reset clears failed attempts for key after a successful login.
func (l *LoginLimiter) Reset(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.attempts, key)
}

func (l *LoginLimiter) pruneLocked(key string, now time.Time) {
	cutoff := now.Add(-l.window)
	list := l.attempts[key]
	kept := list[:0]
	for _, ts := range list {
		if ts.After(cutoff) {
			kept = append(kept, ts)
		}
	}
	if len(kept) == 0 {
		delete(l.attempts, key)
		return
	}
	l.attempts[key] = kept
}
