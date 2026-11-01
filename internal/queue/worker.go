package queue

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// EventHandler is the interface for handling dequeued events.
type EventHandler interface {
	// Handle processes an event for the given username.
	// Returns error if handling failed (event will be requeued).
	Handle(ctx context.Context, username string) error
}

// DefaultEventHandler is a placeholder implementation that just logs the username.
type DefaultEventHandler struct {
	logger *slog.Logger
}

// Handle logs the username (placeholder for actual handling).
func (h *DefaultEventHandler) Handle(ctx context.Context, username string) error {
	h.logger.Info("Handling event", "username", username)
	return nil
}

// Delays between retries of a user whose sync keeps failing. The maximum bounds how
// long a user waits after the other side comes back.
const (
	defaultRetryBaseDelay = time.Second
	defaultRetryMaxDelay  = 30 * time.Second
)

// retryState tracks a user whose last sync failed.
type retryState struct {
	failures int
	timer    *time.Timer // non-nil while a retry is pending
}

// WorkerPool manages a pool of workers that dequeue and process events.
type WorkerPool struct {
	queue      Queue
	numWorkers int
	handler    EventHandler
	logger     *slog.Logger

	// Channels for coordination
	stopCh chan struct{}
	wg     sync.WaitGroup

	// internal pipe for jobs
	jobsCh chan string

	activeCount int32

	retryBaseDelay time.Duration
	retryMaxDelay  time.Duration
	retryMu        sync.Mutex
	retries        map[string]*retryState
}

// NewWorkerPool creates a new worker pool with the specified number of workers.
func NewWorkerPool(q Queue, numWorkers int, logger *slog.Logger) *WorkerPool {
	return &WorkerPool{
		queue:      q,
		numWorkers: numWorkers,
		handler:    &DefaultEventHandler{logger: logger},
		logger:     logger,
		stopCh:     make(chan struct{}),
		jobsCh:     make(chan string, 1),

		retryBaseDelay: defaultRetryBaseDelay,
		retryMaxDelay:  defaultRetryMaxDelay,
		retries:        make(map[string]*retryState),
	}
}

// SetRetryDelays overrides the delays between retries of a failing user.
func (wp *WorkerPool) SetRetryDelays(base, max time.Duration) {
	wp.retryMu.Lock()
	defer wp.retryMu.Unlock()
	wp.retryBaseDelay = base
	wp.retryMaxDelay = max
}

// SetHandler sets a custom event handler for the worker pool.
func (wp *WorkerPool) SetHandler(handler EventHandler) {
	wp.handler = handler
}

// Start begins processing events from the queue with the configured number of workers.
func (wp *WorkerPool) Start(ctx context.Context) {
	// Start fetcher goroutine that pulls from Redis and pushes into jobsCh
	wp.wg.Add(1)
	go wp.fetcher(ctx)

	// Start worker goroutines that consume from jobsCh
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}
	wp.logger.Info("Worker pool started", "num_workers", wp.numWorkers)
}

// fetcher continuously dequeues from the backend and pushes into jobsCh
func (wp *WorkerPool) fetcher(ctx context.Context) {
	defer wp.wg.Done()
	for {
		select {
		case <-wp.stopCh:
			// stop fetching new jobs
			close(wp.jobsCh) // signal no more jobs
			wp.logger.Debug("Fetcher stopping")
			return
		default:
		}

		// Try to dequeue with timeout
		dequeueCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		username, err := wp.queue.Dequeue(dequeueCtx)
		cancel()

		if err != nil {
			wp.logger.Error("Failed to dequeue", "error", err)
			// brief backoff
			select {
			case <-wp.stopCh:
				close(wp.jobsCh)
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		if username == "" {
			// empty queue, wait a bit
			select {
			case <-wp.stopCh:
				close(wp.jobsCh)
				return
			case <-time.After(300 * time.Millisecond):
			}
			continue
		}

		// push job into pipe; block if workers are busy (provides backpressure)
		select {
		case <-wp.stopCh:
			close(wp.jobsCh)
			return
		case wp.jobsCh <- username:
		}
	}
}

// worker processes events from jobsCh until it is closed or stop requested.
func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()
	for {
		select {
		case <-wp.stopCh:
			// don't exit immediately; drain any already received job via default select below
			// fallthrough to default to check jobsCh
		default:
		}

		username, ok := wp.takeJob()
		if !ok {
			// jobsCh closed and drained
			wp.logger.Debug("Worker stopping", "worker_id", id)
			return
		}

		// A pending retry covers whatever this event was about.
		if wp.retryPending(username) {
			wp.logger.Debug("Skipping user in backoff", "worker_id", id, "username", username)
			continue
		}

		// mark active
		atomic.AddInt32(&wp.activeCount, 1)
		wp.logger.Debug("Processing event", "worker_id", id, "username", username)

		// Handle the event
		if err := wp.handler.Handle(ctx, username); err != nil {
			delay := wp.scheduleRetry(ctx, username)
			wp.logger.Error("Handler failed, requeuing", "worker_id", id, "username", username, "delay", delay, "error", err)
		} else {
			wp.clearRetry(username)
		}

		// mark inactive
		atomic.AddInt32(&wp.activeCount, -1)
	}
}

// retryPending reports whether a retry is already scheduled for the user.
func (wp *WorkerPool) retryPending(username string) bool {
	wp.retryMu.Lock()
	defer wp.retryMu.Unlock()
	r, ok := wp.retries[username]
	return ok && r.timer != nil
}

// scheduleRetry requeues the user after a delay that doubles with each failure in a row.
func (wp *WorkerPool) scheduleRetry(ctx context.Context, username string) time.Duration {
	wp.retryMu.Lock()
	defer wp.retryMu.Unlock()

	r, ok := wp.retries[username]
	if !ok {
		r = &retryState{}
		wp.retries[username] = r
	}
	r.failures++

	delay := wp.retryBaseDelay
	for i := 1; i < r.failures && delay < wp.retryMaxDelay; i++ {
		delay *= 2
	}
	if delay > wp.retryMaxDelay {
		delay = wp.retryMaxDelay
	}

	// Two workers can fail on one user at once; one retry is enough.
	if r.timer != nil {
		return delay
	}
	r.timer = time.AfterFunc(delay, func() {
		wp.retryMu.Lock()
		r.timer = nil
		wp.retryMu.Unlock()
		if err := wp.queue.Enqueue(ctx, username, 1.0); err != nil {
			wp.logger.Error("Failed to requeue", "username", username, "error", err)
		}
	})
	return delay
}

// clearRetry forgets a user's failures after a successful sync.
func (wp *WorkerPool) clearRetry(username string) {
	wp.retryMu.Lock()
	defer wp.retryMu.Unlock()
	delete(wp.retries, username)
}

// takeJob reads a single job from jobsCh, blocking until available or channel closed.
func (wp *WorkerPool) takeJob() (string, bool) {
	username, ok := <-wp.jobsCh
	return username, ok
}

// Stop gracefully shuts down the worker pool.
// It stops accepting new tasks and waits for all active tasks to complete.
func (wp *WorkerPool) Stop(ctx context.Context) error {
	wp.logger.Info("Stopping worker pool")
	wp.retryMu.Lock()
	for _, r := range wp.retries {
		if r.timer != nil {
			r.timer.Stop()
		}
	}
	wp.retryMu.Unlock()
	// signal to stop
	select {
	case <-wp.stopCh:
		// already closed
	default:
		close(wp.stopCh)
	}

	// wait for fetcher+workers to exit
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		wp.logger.Info("Worker pool stopped gracefully")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ActiveCount returns the number of workers currently processing tasks.
func (wp *WorkerPool) ActiveCount() int32 {
	return atomic.LoadInt32(&wp.activeCount)
}
