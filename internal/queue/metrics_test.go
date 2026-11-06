package queue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dovewarden/dovewarden/internal/doveadm"
	"github.com/dovewarden/dovewarden/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// value reads a gauge or counter.
func value(t *testing.T, c prometheus.Metric) float64 {
	t.Helper()
	var d dto.Metric
	if err := c.Write(&d); err != nil {
		t.Fatal(err)
	}
	switch {
	case d.Gauge != nil:
		return d.Gauge.GetValue()
	case d.Counter != nil:
		return d.Counter.GetValue()
	}
	t.Fatal("not a gauge or counter")
	return 0
}

// TestSyncMetrics verifies each dsync run is timed by result and failing users counted.
func TestSyncMetrics(t *testing.T) {
	fail := true
	h, _, _ := newTestHandler(t, func() string {
		if fail {
			return `[["error",{"type":"exitCode","exitCode":75},"dovewarden-sync"]]`
		}
		return syncOK
	})
	m := metrics.New(prometheus.NewRegistry())
	h.SetMetrics(m)
	ctx := context.Background()

	_ = h.Handle(ctx, "a")
	_ = h.Handle(ctx, "b")
	if got := value(t, m.UsersFailing); got != 2 {
		t.Fatalf("expected 2 failing users, got %v", got)
	}
	fail = false
	_ = h.Handle(ctx, "a")
	if got := value(t, m.UsersFailing); got != 1 {
		t.Fatalf("expected 1 failing user after a success, got %v", got)
	}

	var d dto.Metric
	if err := m.SyncDuration.WithLabelValues("failed").(prometheus.Histogram).Write(&d); err != nil {
		t.Fatal(err)
	}
	if d.Histogram.GetSampleCount() != 2 {
		t.Fatalf("expected 2 timed failures, got %d", d.Histogram.GetSampleCount())
	}
	if err := m.SyncDuration.WithLabelValues("ok").(prometheus.Histogram).Write(&d); err != nil {
		t.Fatal(err)
	}
	if d.Histogram.GetSampleCount() != 1 {
		t.Fatalf("expected 1 timed success, got %d", d.Histogram.GetSampleCount())
	}
}

// TestQueueDepthAndErrors verifies the depth counts users once, and store errors are
// counted while a missing key is not.
func TestQueueDepthAndErrors(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New(prometheus.NewRegistry())
	q.SetErrorCounter(m.RedisErrors)
	ctx := context.Background()

	for _, u := range []string{"a", "b", "a"} {
		if err := q.Enqueue(ctx, u, 1.0); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := q.Depth(ctx); err != nil || n != 2 {
		t.Fatalf("expected depth 2, got %d (%v)", n, err)
	}
	if _, err := q.GetLastReplicationTime(ctx, "nobody"); err != nil {
		t.Fatal(err)
	}
	if got := value(t, m.RedisErrors); got != 0 {
		t.Fatalf("a missing key is not an error, got %v errors", got)
	}

	q.server.Close()
	if _, err := q.Dequeue(ctx); err == nil {
		t.Fatal("expected an error from a closed store")
	}
	if err := q.Enqueue(ctx, "c", 1.0); err == nil {
		t.Fatal("expected an error from a closed store")
	}
	if got := value(t, m.RedisErrors); got != 2 {
		t.Fatalf("expected 2 store errors, got %v", got)
	}
	_ = q.client.Close()
}

// TestRetryingGauge verifies users in backoff are counted, and forgotten on success.
func TestRetryingGauge(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	m := metrics.New(prometheus.NewRegistry())
	wp := NewWorkerPool(q, 1, logger)
	wp.SetMetrics(m)
	wp.SetRetryDelays(time.Hour, time.Hour)
	ctx := context.Background()

	wp.scheduleRetry(ctx, "a")
	wp.scheduleRetry(ctx, "a")
	wp.scheduleRetry(ctx, "b")
	if got := value(t, m.UsersRetrying); got != 2 {
		t.Fatalf("expected 2 users retrying, got %v", got)
	}
	wp.clearRetry("a")
	if got := value(t, m.UsersRetrying); got != 1 {
		t.Fatalf("expected 1 user retrying, got %v", got)
	}
	_ = wp.Stop(ctx)
}

// TestBackgroundPassMetrics verifies a pass reports the users, the stale ones and the
// oldest sync, and that a failed listing counts as a failed pass and changes nothing else.
func TestBackgroundPassMetrics(t *testing.T) {
	listFails := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if listFails {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprint(w, `[["doveadmResponse",{"userList":["fresh","old","never"]},"dovewarden-list-users"]]`)
	}))
	defer server.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	ctx := context.Background()

	threshold := 10 * time.Minute
	s := NewBackgroundReplicationService(doveadm.NewClient(server.URL, "pw"), q, logger, time.Hour, threshold)
	m := metrics.New(prometheus.NewRegistry())
	s.SetMetrics(m)
	s.started = time.Now().Add(-5 * time.Minute)
	old := time.Now().Add(-time.Hour)
	_ = q.SetLastReplicationTime(ctx, "fresh", time.Now())
	_ = q.SetLastReplicationTime(ctx, "old", old)

	if err := s.runReplication(ctx); err != nil {
		t.Fatal(err)
	}
	if got := value(t, m.UsersTotal); got != 3 {
		t.Fatalf("expected 3 users, got %v", got)
	}
	// "never" counts from the start, 5 minutes ago: within the threshold.
	if got := value(t, m.UsersStale); got != 1 {
		t.Fatalf("expected 1 stale user, got %v", got)
	}
	if got := value(t, m.OldestUserSyncTimestamp); got != float64(old.Unix()) {
		t.Fatalf("expected the oldest sync at %d, got %v", old.Unix(), got)
	}
	if got := value(t, m.BackgroundRuns.WithLabelValues("ok")); got != 1 {
		t.Fatalf("expected 1 good pass, got %v", got)
	}
	last := value(t, m.BackgroundLastSuccess)

	listFails = true
	if err := s.runReplication(ctx); err == nil {
		t.Fatal("expected the pass to fail")
	}
	if got := value(t, m.BackgroundRuns.WithLabelValues("failed")); got != 1 {
		t.Fatalf("expected 1 failed pass, got %v", got)
	}
	if got := value(t, m.BackgroundLastSuccess); got != last {
		t.Fatal("a failed pass must not move the last success")
	}
}
