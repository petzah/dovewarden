package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dovewarden/dovewarden/internal/doveadm"
)

// TestWorkerPoolDequeue verifies that workers dequeue events from the queue.
func TestWorkerPoolDequeue(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer func() {
		if cerr := q.Close(); cerr != nil {
			t.Fatalf("failed to close queue: %v", cerr)
		}
	}()

	ctx := context.Background()
	// Enqueue a few events
	if err := q.Enqueue(ctx, "user-a", 1.0); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if err := q.Enqueue(ctx, "user-b", 1.0); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Create worker pool and start
	wp := NewWorkerPool(q, 2, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.Start(ctx)

	// Wait a bit for workers to process
	time.Sleep(1 * time.Second)

	// Queue should be empty now (events were processed)
	dequeued, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if dequeued != "" {
		t.Fatalf("expected empty queue, got user: %s", dequeued)
	}

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := wp.Stop(shutdownCtx); err != nil {
		t.Fatalf("failed to stop worker pool: %v", err)
	}
}

// TestWorkerPoolRequeueOnError verifies that failed events are requeued.
func TestWorkerPoolRequeueOnError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer func() {
		if cerr := q.Close(); cerr != nil {
			t.Fatalf("failed to close queue: %v", cerr)
		}
	}()

	ctx := context.Background()
	// Enqueue one event
	if err := q.Enqueue(ctx, "user-a", 1.0); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Create worker pool
	wp := NewWorkerPool(q, 1, logger)

	// Create a handler that fails once then succeeds
	failCount := int32(0)
	handler := &TestHandler{
		failOnce: true,
		onHandle: func(username string) error {
			current := atomic.AddInt32(&failCount, 1)
			if current == 1 {
				return errors.New("simulated handler failure")
			}
			return nil
		},
	}
	wp.SetHandler(handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.Start(ctx)
	time.Sleep(2 * time.Second)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := wp.Stop(shutdownCtx); err != nil {
		t.Fatalf("failed to stop worker pool: %v", err)
	}

	// Should have been called at least twice (fail then succeed)
	if atomic.LoadInt32(&failCount) < 2 {
		t.Fatalf("expected handler to be called at least twice, got %d", atomic.LoadInt32(&failCount))
	}
}

// TestGracefulShutdown verifies that shutdown waits for active tasks.
func TestGracefulShutdown(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer func() {
		if cerr := q.Close(); cerr != nil {
			t.Fatalf("failed to close queue: %v", cerr)
		}
	}()

	ctx := context.Background()
	if err := q.Enqueue(ctx, "user-a", 1.0); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Create worker pool with handler that takes a bit to process
	wp := NewWorkerPool(q, 1, logger)
	handler := &TestHandler{
		delay: 100 * time.Millisecond,
	}
	wp.SetHandler(handler)
	wpCtx, wpCancel := context.WithCancel(context.Background())
	defer wpCancel()
	wp.Start(wpCtx)

	time.Sleep(500 * time.Millisecond)

	// Graceful shutdown should complete
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	if err := wp.Stop(shutdownCtx); err != nil {
		t.Fatalf("failed to stop worker pool: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Fatalf("shutdown took too long: %v", elapsed)
	}

	// Active count should be zero
	if wp.ActiveCount() != 0 {
		t.Fatalf("expected 0 active workers, got %d", wp.ActiveCount())
	}
}

// TestHandler is a mock event handler for testing.
type TestHandler struct {
	delay    time.Duration
	failOnce bool
	onHandle func(username string) error
}

func (h *TestHandler) Handle(ctx context.Context, username string) error {
	if h.delay > 0 {
		select {
		case <-time.After(h.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if h.onHandle != nil {
		return h.onHandle(username)
	}
	return nil
}

// runFailing counts the handler calls for one user within the given window, and returns
// the stopped pool.
func runFailing(t *testing.T, window time.Duration, onHandle func(attempt int32) error) (int32, *WorkerPool) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Enqueue(ctx, "user-a", 1.0); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	var attempts int32
	wp := NewWorkerPool(q, 4, logger)
	wp.SetRetryDelays(100*time.Millisecond, 400*time.Millisecond)
	wp.SetHandler(&TestHandler{onHandle: func(string) error {
		return onHandle(atomic.AddInt32(&attempts, 1))
	}})
	wp.Start(ctx)

	// New events during a backoff must not bring the next attempt forward.
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if err := q.Enqueue(ctx, "user-a", 1.0); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := wp.Stop(stopCtx); err != nil {
		t.Fatalf("failed to stop worker pool: %v", err)
	}
	return atomic.LoadInt32(&attempts), wp
}

// TestRetryBacksOff verifies a failing user is not hammered, even while events keep coming.
func TestRetryBacksOff(t *testing.T) {
	attempts, _ := runFailing(t, 2*time.Second, func(int32) error { return errors.New("down") })
	// Delays 100, 200, 400, 400... allow about 6 attempts in 2 s, a fetch loop hundreds.
	if attempts < 3 || attempts > 9 {
		t.Fatalf("expected a backed-off number of attempts, got %d", attempts)
	}
}

// TestRetryResetsAfterSuccess verifies a recovered user leaves backoff.
func TestRetryResetsAfterSuccess(t *testing.T) {
	attempts, wp := runFailing(t, 2*time.Second, func(attempt int32) error {
		if attempt == 1 {
			return errors.New("down once")
		}
		return nil
	})
	if attempts < 2 {
		t.Fatalf("expected a retry, got %d attempts", attempts)
	}
	if len(wp.retries) != 0 {
		t.Fatalf("expected no failures on record after recovery, got %d", len(wp.retries))
	}
}

// TestOneSyncPerUser verifies syncs of one user never overlap, and that an event arriving
// during a sync causes another one, since the running sync may have missed it.
func TestOneSyncPerUser(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var running, overlaps, finished int32
	started := make(chan struct{}, 100)
	wp := NewWorkerPool(q, 4, logger)
	wp.SetHandler(&TestHandler{onHandle: func(string) error {
		if atomic.AddInt32(&running, 1) > 1 {
			atomic.AddInt32(&overlaps, 1)
		}
		started <- struct{}{}
		time.Sleep(400 * time.Millisecond)
		atomic.AddInt32(&running, -1)
		atomic.AddInt32(&finished, 1)
		return nil
	}})
	wp.Start(ctx)

	if err := q.Enqueue(ctx, "user-a", 1.0); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	<-started
	// Arrives mid-sync, so one more sync must follow the running one.
	for i := 0; i < 3; i++ {
		if err := q.Enqueue(ctx, "user-a", 1.0); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(1500 * time.Millisecond)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := wp.Stop(stopCtx); err != nil {
		t.Fatalf("failed to stop worker pool: %v", err)
	}
	if got := atomic.LoadInt32(&overlaps); got != 0 {
		t.Errorf("%d syncs overlapped", got)
	}
	if got := atomic.LoadInt32(&finished); got < 2 {
		t.Errorf("expected a second sync for the mid-sync event, got %d sync(s)", got)
	}
}

// outageHandler fails every sync as unreachable while down is set, and probes the same way.
type outageHandler struct {
	down     atomic.Bool
	attempts atomic.Int32
	mu       sync.Mutex
	synced   map[string]bool
}

func (h *outageHandler) Handle(_ context.Context, username string) error {
	h.attempts.Add(1)
	if h.down.Load() {
		return &doveadm.UnreachableError{Err: errors.New("connection refused")}
	}
	h.mu.Lock()
	h.synced[username] = true
	h.mu.Unlock()
	return nil
}

func (h *outageHandler) Probe(context.Context) error {
	if h.down.Load() {
		return &doveadm.UnreachableError{Err: errors.New("connection refused")}
	}
	return nil
}

// TestOutagePausesAndResumes verifies that while Doveadm is unreachable the pool stops
// trying users instead of retrying each one, and that every waiting user syncs as soon as
// it answers again rather than at the end of its own backoff.
func TestOutagePausesAndResumes(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer func() { _ = q.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := &outageHandler{synced: make(map[string]bool)}
	h.down.Store(true)
	wp := NewWorkerPool(q, 4, logger)
	wp.SetHandler(h)
	wp.SetProbeInterval(100 * time.Millisecond)
	wp.Start(ctx)

	users := []string{"u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8", "u9", "u10"}
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		for _, u := range users {
			if err := q.Enqueue(ctx, u, 1.0); err != nil {
				t.Fatalf("enqueue failed: %v", err)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Only the jobs already taken when the pool paused may fail: the workers and the
	// one buffered in the pipe.
	if got := h.attempts.Load(); got > 6 {
		t.Errorf("expected the pool to pause, got %d attempts while down", got)
	}

	h.down.Store(false)
	time.Sleep(700 * time.Millisecond)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := wp.Stop(stopCtx); err != nil {
		t.Fatalf("failed to stop worker pool: %v", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, u := range users {
		if !h.synced[u] {
			t.Errorf("%s was not synced after Doveadm came back", u)
		}
	}
}

// TestRemovedUserIsNotRetried verifies a user that no longer exists is dropped instead of
// being retried for good: no retry would ever succeed.
func TestRemovedUserIsNotRetried(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	q, err := NewInMemoryQueue("test", "", logger)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer func() { _ = q.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts int32
	wp := NewWorkerPool(q, 2, logger)
	wp.SetRetryDelays(50*time.Millisecond, 100*time.Millisecond)
	wp.SetHandler(&TestHandler{onHandle: func(string) error {
		atomic.AddInt32(&attempts, 1)
		return &doveadm.SyncError{Tag: "t", Type: "exitCode", ExitCode: doveadm.ExitNoUser}
	}})
	wp.Start(ctx)
	if err := q.Enqueue(ctx, "gone", 1.0); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	time.Sleep(time.Second)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := wp.Stop(stopCtx); err != nil {
		t.Fatalf("failed to stop worker pool: %v", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Errorf("expected one attempt, got %d", got)
	}
	if wp.retryPending("gone") {
		t.Error("a removed user should have no retry pending")
	}
}
