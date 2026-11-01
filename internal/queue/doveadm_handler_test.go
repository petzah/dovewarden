package queue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeDoveadm answers sync requests with queued bodies and records the state it was sent.
type fakeDoveadm struct {
	mu     sync.Mutex
	bodies []string
	sent   []string
}

func (f *fakeDoveadm) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, string(b))
	_, _ = fmt.Fprint(w, f.bodies[0])
	f.bodies = f.bodies[1:]
}

func newTestHandler(t *testing.T, f *fakeDoveadm) (*DoveadmEventHandler, *InMemoryQueue) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	server := httptest.NewServer(f)
	t.Cleanup(func() {
		server.Close()
		_ = q.Close()
	})
	return NewDoveadmEventHandler(server.URL, "pw", "tcp:b:12345", logger, q), q
}

func syncFailure(code int) string {
	return fmt.Sprintf(`[["error",{"type":"exitCode","exitCode":%d},"dovewarden-sync"]]`, code)
}

func syncSuccess(state string) string {
	return fmt.Sprintf(`[["doveadmResponse",[{"state":%q}],"dovewarden-sync"]]`, state)
}

// A user whose state dsync rejects must recover with a full sync, not retry it forever.
// dsync reports stale state with either exit code.
func TestStaleStateIsDroppedOnSyncFailure(t *testing.T) {
	for _, code := range []int{75, 2} {
		t.Run(fmt.Sprintf("exit %d", code), func(t *testing.T) {
			f := &fakeDoveadm{bodies: []string{syncFailure(code), syncSuccess("fresh")}}
			h, q := newTestHandler(t, f)
			ctx := context.Background()
			if err := q.SetReplicationState(ctx, "u", "stale"); err != nil {
				t.Fatal(err)
			}

			if err := h.Handle(ctx, "u"); err == nil {
				t.Fatal("expected the failed sync to return an error, so that it is retried")
			}
			if err := h.Handle(ctx, "u"); err != nil {
				t.Fatalf("retry failed: %v", err)
			}

			if !strings.Contains(f.sent[0], `"state":"stale"`) {
				t.Errorf("first sync should send the stored state: %s", f.sent[0])
			}
			if !strings.Contains(f.sent[1], `"state":""`) {
				t.Errorf("retry should be a full sync: %s", f.sent[1])
			}
			if got, _ := q.GetReplicationState(ctx, "u"); got != "fresh" {
				t.Errorf("state = %q, want fresh", got)
			}
		})
	}
}

// An HTTP failure says nothing about the state, so incremental sync resumes afterwards.
func TestStateSurvivesHTTPFailure(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(func() {
		server.Close()
		_ = q.Close()
	})
	h := NewDoveadmEventHandler(server.URL, "pw", "tcp:b:12345", logger, q)
	ctx := context.Background()
	if err := q.SetReplicationState(ctx, "u", "kept"); err != nil {
		t.Fatal(err)
	}

	if err := h.Handle(ctx, "u"); err == nil {
		t.Fatal("expected an error")
	}
	if got, _ := q.GetReplicationState(ctx, "u"); got != "kept" {
		t.Errorf("state = %q, want kept", got)
	}
}

// State Doveadm would refuse must not be sent back on the next sync.
func TestOversizedStateIsNotStored(t *testing.T) {
	f := &fakeDoveadm{bodies: []string{syncSuccess(strings.Repeat("x", maxStateSize+1))}}
	h, q := newTestHandler(t, f)
	ctx := context.Background()
	if err := q.SetReplicationState(ctx, "u", "old"); err != nil {
		t.Fatal(err)
	}

	if err := h.Handle(ctx, "u"); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if got, _ := q.GetReplicationState(ctx, "u"); got != "" {
		t.Errorf("state has %d bytes, want none", len(got))
	}
}
