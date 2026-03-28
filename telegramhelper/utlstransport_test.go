package telegramhelper

import (
	"testing"
	"time"
)

func TestNewValidatorHTTPClientWithProxy_EmptyAddr(t *testing.T) {
	// Empty proxy address should fall back to direct connection (no error).
	client, err := NewValidatorHTTPClientWithProxy("", "", "", 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error for empty proxy addr: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client for empty proxy addr")
	}
}

func TestNewValidatorHTTPClientWithProxy_ValidAddr(t *testing.T) {
	// Construction should succeed even though the proxy isn't actually listening —
	// SOCKS5 dialer creation is lazy (connects on first request, not at build time).
	client, err := NewValidatorHTTPClientWithProxy("127.0.0.1:1080", "user", "pass", 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error creating proxy client: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.Timeout != 5*time.Second {
		t.Errorf("timeout = %v, want 5s", client.Timeout)
	}
}

func TestNewValidatorHTTPClientWithProxy_NoAuth(t *testing.T) {
	// Empty user/pass should still succeed (no auth).
	client, err := NewValidatorHTTPClientWithProxy("127.0.0.1:1080", "", "", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}
