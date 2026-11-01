package queue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestEverySyncIsFull verifies no state is sent, even after Doveadm has returned some or
// a state was left stored: incremental state can diverge without any error.
func TestEverySyncIsFull(t *testing.T) {
	var sent []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		sent = append(sent, string(b))
		_, _ = fmt.Fprint(w, `[["doveadmResponse",[{"state":"from-doveadm"}],"dovewarden-sync"]]`)
	}))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	t.Cleanup(func() {
		server.Close()
		_ = q.Close()
	})
	h := NewDoveadmEventHandler(server.URL, "pw", "tcp:b:12345", logger, q)
	ctx := context.Background()
	if err := q.SetReplicationState(ctx, "u", "left-over"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if err := h.Handle(ctx, "u"); err != nil {
			t.Fatalf("sync %d failed: %v", i, err)
		}
	}

	for i, body := range sent {
		if !strings.Contains(body, `"state":""`) {
			t.Errorf("sync %d was not full: %s", i, body)
		}
	}
	if last, _ := q.GetLastReplicationTime(ctx, "u"); last.IsZero() {
		t.Error("a sync should record the time")
	}
}

// TestFailedSyncReturnsError verifies a failure reaches the worker, which retries it.
func TestFailedSyncReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `[["error",{"type":"exitCode","exitCode":75},"dovewarden-sync"]]`)
	}))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	t.Cleanup(func() {
		server.Close()
		_ = q.Close()
	})

	h := NewDoveadmEventHandler(server.URL, "pw", "tcp:b:12345", logger, q)
	if err := h.Handle(context.Background(), "u"); err == nil {
		t.Fatal("expected an error")
	}
}
