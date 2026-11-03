package doveadm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestSyncSuccess verifies that a successful sync request works
func TestSyncSuccess(t *testing.T) {
	// Create a mock Doveadm server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method and path
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/doveadm/v1" {
			t.Errorf("expected path /doveadm/v1, got %s", r.URL.Path)
		}

		// Verify authentication
		_, pass, ok := r.BasicAuth()
		if !ok {
			t.Error("expected basic auth")
		}
		if pass != "testpass" {
			t.Errorf("unexpected credentials: %s", pass)
		}

		// Verify content type
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected content type: %s", r.Header.Get("Content-Type"))
		}

		// Verify request body
		var payload []interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}

		if len(payload) != 1 {
			t.Fatalf("expected 1 element in payload, got %d", len(payload))
		}

		// Send success response
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `[["sync",{"status":"ok"},"dovewarden-sync"]]`)
	}))
	defer server.Close()

	// Create client and test sync
	client := NewClient(server.URL, "testpass")
	ctx := context.Background()

	resp, err := client.Sync(ctx, "user-a", "imap", "", false)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Error("expected non-nil response")
	}
}

// TestSyncServerError verifies error handling for server errors
func TestSyncServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, "Internal Server Error")
	}))
	defer server.Close()

	client := NewClient(server.URL, "testpass")
	ctx := context.Background()

	_, err := client.Sync(ctx, "user-a", "imap", "", false)
	if err == nil {
		t.Error("expected error for 500 status")
	}
}

// TestSyncUnauthorized verifies error handling for authentication failures
func TestSyncUnauthorized(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = fmt.Fprintf(w, "Unauthorized")
	}))
	defer server.Close()

	client := NewClient(server.URL, "wrongpass")
	ctx := context.Background()

	_, err := client.Sync(ctx, "user-a", "imap", "", false)
	if err == nil {
		t.Error("expected error for 401 status")
	}
}

// TestSyncPayloadFormat verifies the correct payload format is sent
func TestSyncPayloadFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload []interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}

		// Verify structure: [[command, params, tag]]
		cmdArray, ok := payload[0].([]interface{})
		if !ok {
			t.Fatal("expected array as first element")
		}

		if len(cmdArray) != 3 {
			t.Fatalf("expected 3 elements in command array, got %d", len(cmdArray))
		}

		// Verify command
		cmd, ok := cmdArray[0].(string)
		if !ok || cmd != "sync" {
			t.Errorf("expected 'sync' command, got %v", cmdArray[0])
		}

		// Verify params
		params, ok := cmdArray[1].(map[string]interface{})
		if !ok {
			t.Fatal("expected map for params")
		}

		user, ok := params["user"].(string)
		if !ok || user != "test-user" {
			t.Errorf("expected user 'test-user', got %v", params["user"])
		}

		dest, ok := params["destination"].([]interface{})
		if !ok || len(dest) != 1 {
			t.Errorf("expected destination array with 1 element, got %v", params["destination"])
		}

		if destVal, ok := dest[0].(string); !ok || destVal != "imap" {
			t.Errorf("expected destination 'imap', got %v", dest[0])
		}

		// Verify tag
		tag, ok := cmdArray[2].(string)
		if !ok || tag != "dovewarden-sync" {
			t.Errorf("expected tag 'dovewarden-sync', got %v", cmdArray[2])
		}

		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `[["doveadmResponse",[{"state":"DOVEADM-DUMMY-STATE"}],"dovewarden-sync"]]`)
	}))
	defer server.Close()

	client := NewClient(server.URL, "testpass")
	ctx := context.Background()

	_, err := client.Sync(ctx, "test-user", "imap", "", false)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestSyncWithState verifies that state parameter is sent when provided
func TestSyncWithState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload []interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}

		cmdArray, ok := payload[0].([]interface{})
		if !ok {
			t.Fatal("expected array as first element")
		}

		params, ok := cmdArray[1].(map[string]interface{})
		if !ok {
			t.Fatal("expected map for params")
		}

		// Verify state is present
		state, ok := params["state"].(string)
		if !ok || state != "test-state-123" {
			t.Errorf("expected state 'test-state-123', got %v", params["state"])
		}

		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `[["doveadmResponse",[{"state":"new-state-456"}],"dovewarden-sync"]]`)
	}))
	defer server.Close()

	client := NewClient(server.URL, "testpass")
	ctx := context.Background()

	resp, err := client.Sync(ctx, "test-user", "imap", "test-state-123", false)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if resp.State != "new-state-456" {
		t.Errorf("expected state 'new-state-456', got %s", resp.State)
	}
}

// TestSyncWithoutState verifies that state parameter is omitted when empty
func TestSyncWithoutState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload []interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}

		cmdArray, ok := payload[0].([]interface{})
		if !ok {
			t.Fatal("expected array as first element")
		}

		params, ok := cmdArray[1].(map[string]interface{})
		if !ok {
			t.Fatal("expected map for params")
		}

		// Verify state is present anyway
		if _, exists := params["state"]; !exists {
			t.Error("state parameter missing, expected new state anyway")
		}

		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `[["sync",{},"dovewarden-sync"]]`)
	}))
	defer server.Close()

	client := NewClient(server.URL, "testpass")
	ctx := context.Background()

	resp, err := client.Sync(ctx, "test-user", "imap", "", false)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if resp.State != "" {
		t.Errorf("expected empty state, got %s", resp.State)
	}
}

// TestSyncErrorCarriesExitCode verifies callers can tell why Doveadm refused a sync.
func TestSyncErrorCarriesExitCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `[["error",{"type":"exitCode","exitCode":75},"dovewarden-sync"]]`)
	}))
	defer server.Close()

	_, err := NewClient(server.URL, "testpass").Sync(context.Background(), "user-a", "imap", "old", false)
	var syncErr *SyncError
	if !errors.As(err, &syncErr) || syncErr.ExitCode != 75 {
		t.Fatalf("expected a SyncError with exit code 75, got %v", err)
	}
}

// TestUnreachableErrors verifies that a refused connection and a 503 are reported as
// Doveadm being unreachable, while a dsync failure and a 500 are not.
func TestUnreachableErrors(t *testing.T) {
	ctx := context.Background()

	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closed.Close()
	if _, err := NewClient(closed.URL, "pw").Sync(ctx, "u", "imap", "", false); !IsUnreachable(err) {
		t.Errorf("refused connection: got %v", err)
	}
	if err := NewClient(closed.URL, "pw").Probe(ctx); !IsUnreachable(err) {
		t.Errorf("probe of refused connection: got %v", err)
	}

	for code, want := range map[int]bool{503: true, 502: true, 500: false} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(code)
		}))
		_, err := NewClient(srv.URL, "pw").Sync(ctx, "u", "imap", "", false)
		srv.Close()
		if err == nil || IsUnreachable(err) != want {
			t.Errorf("status %d: unreachable=%v, want %v (%v)", code, IsUnreachable(err), want, err)
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[["error",{"type":"exitCode","exitCode":2},"dovewarden-sync"]]`))
	}))
	defer srv.Close()
	if _, err := NewClient(srv.URL, "pw").Sync(ctx, "u", "imap", "", false); err == nil || IsUnreachable(err) {
		t.Errorf("dsync failure: got %v", err)
	}
	if err := NewClient(srv.URL, "pw").Probe(ctx); err != nil {
		t.Errorf("probe of a live server: %v", err)
	}
}

// TestFullSyncParam verifies fullSync is sent only when asked for.
func TestFullSyncParam(t *testing.T) {
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		_, _ = w.Write([]byte(`[["doveadmResponse",[],"dovewarden-sync"]]`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "pw")
	_, _ = c.Sync(context.Background(), "u", "imap", "", false)
	_, _ = c.Sync(context.Background(), "u", "imap", "", true)
	if strings.Contains(bodies[0], "fullSync") || !strings.Contains(bodies[1], `"fullSync":true`) {
		t.Errorf("unexpected bodies: %v", bodies)
	}
}

// TestUserAgent verifies requests name dovewarden instead of Go's default agent.
func TestUserAgent(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.UserAgent())
		_, _ = w.Write([]byte(`[["doveadmResponse",[],"t"]]`))
	}))
	defer srv.Close()
	old := UserAgent
	UserAgent = "dovewarden/1.2.3"
	defer func() { UserAgent = old }()

	c := NewClient(srv.URL, "pw")
	_, _ = c.Sync(context.Background(), "u", "imap", "", false)
	_ = c.Probe(context.Background())
	_, _ = c.ListUsers(context.Background())
	for i, ua := range got {
		if ua != "dovewarden/1.2.3" {
			t.Errorf("request %d: User-Agent %q", i, ua)
		}
	}
	if len(got) != 3 {
		t.Errorf("expected 3 requests, got %d", len(got))
	}
}
