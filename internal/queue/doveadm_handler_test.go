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
	"time"

	"github.com/dovewarden/dovewarden/internal/doveadm"
	"github.com/dovewarden/dovewarden/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// newTestHandler returns a handler talking to a fake Doveadm that answers with reply and
// records every request body.
func newTestHandler(t *testing.T, reply func() string) (*DoveadmEventHandler, Queue, *[]string) {
	t.Helper()
	var sent []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		sent = append(sent, string(b))
		_, _ = fmt.Fprint(w, reply())
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
	return NewDoveadmEventHandler(server.URL, "pw", "tcp:b:12345", logger, q), q, &sent
}

const syncOK = `[["doveadmResponse",[{"state":"from-doveadm"}],"dovewarden-sync"]]`

// TestNoStateIsKept verifies no state is sent, even after Doveadm has returned some or
// a state was left stored: incremental state can diverge without any error.
func TestNoStateIsKept(t *testing.T) {
	h, q, sent := newTestHandler(t, func() string { return syncOK })
	ctx := context.Background()
	if err := q.SetReplicationState(ctx, "u", "left-over"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if err := h.Handle(ctx, "u"); err != nil {
			t.Fatalf("sync %d failed: %v", i, err)
		}
	}

	for i, body := range *sent {
		if !strings.Contains(body, `"state":""`) {
			t.Errorf("sync %d sent a state: %s", i, body)
		}
	}
	if last, _ := q.GetLastReplicationTime(ctx, "u"); last.IsZero() {
		t.Error("a sync should record the time")
	}
}

// TestEverySyncIsFullByDefault verifies the default: equal counters on both sides can
// hide different mail, and only a full sync compares it.
func TestEverySyncIsFullByDefault(t *testing.T) {
	h, _, sent := newTestHandler(t, func() string { return syncOK })
	for i := 0; i < 3; i++ {
		_ = h.Handle(context.Background(), "u")
	}
	for i, body := range *sent {
		if !strings.Contains(body, `"fullSync":true`) {
			t.Errorf("sync %d was not full: %s", i, body)
		}
	}
}

// TestFullSyncWhenDivergenceIsLikely verifies that with an interval set, a full sync is
// sent on the first sync of a user, after a failure, and once the interval has passed,
// and a normal one otherwise.
func TestFullSyncWhenDivergenceIsLikely(t *testing.T) {
	fail := false
	h, _, sent := newTestHandler(t, func() string {
		if fail {
			return `[["error",{"type":"exitCode","exitCode":2},"dovewarden-sync"]]`
		}
		return syncOK
	})
	m := metrics.New(prometheus.NewRegistry())
	h.SetMetrics(m)
	h.SetFullSyncInterval(time.Hour)
	ctx := context.Background()
	isFull := func(i int) bool { return strings.Contains((*sent)[i], `"fullSync":true`) }

	_ = h.Handle(ctx, "u") // 0: first since start
	_ = h.Handle(ctx, "u") // 1: nothing to suspect
	fail = true
	_ = h.Handle(ctx, "u") // 2: fails
	fail = false
	_ = h.Handle(ctx, "u") // 3: after the failure
	_ = h.Handle(ctx, "u") // 4: recovered
	h.SetFullSyncInterval(time.Nanosecond)
	_ = h.Handle(ctx, "u") // 5: interval passed

	want := []bool{true, false, false, true, false, true}
	for i, w := range want {
		if got := isFull(i); got != w {
			t.Errorf("sync %d: full=%v, want %v: %s", i, got, w, (*sent)[i])
		}
	}

	count := func(result, full string) float64 {
		var d dto.Metric
		_ = m.Syncs.WithLabelValues(result, full).Write(&d)
		return d.GetCounter().GetValue()
	}
	if count("ok", "true") != 3 || count("ok", "false") != 2 || count("failed", "false") != 1 {
		t.Errorf("unexpected sync counts: ok/full %v, ok/normal %v, failed/normal %v",
			count("ok", "true"), count("ok", "false"), count("failed", "false"))
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

// TestRemovedUserIsNotFailing verifies a user Doveadm reports as unknown (exit code 67)
// is counted as nouser and is not left on record as failing.
func TestRemovedUserIsNotFailing(t *testing.T) {
	h, _, _ := newTestHandler(t, func() string {
		return `[["error",{"type":"exitCode","exitCode":67},"dovewarden-sync"]]`
	})
	m := metrics.New(prometheus.NewRegistry())
	h.SetMetrics(m)
	err := h.Handle(context.Background(), "gone")
	if !doveadm.IsNoUser(err) {
		t.Fatalf("expected a no-user error, got %v", err)
	}
	if len(h.failed) != 0 {
		t.Errorf("a removed user should not be on record as failing")
	}
	var d dto.Metric
	_ = m.Syncs.WithLabelValues("nouser", "true").Write(&d)
	if d.GetCounter().GetValue() != 1 {
		t.Errorf("expected one nouser sync, got %v", d.GetCounter().GetValue())
	}
}
