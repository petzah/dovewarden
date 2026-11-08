package queue

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dovewarden/dovewarden/internal/doveadm"
	"github.com/dovewarden/dovewarden/internal/metrics"
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

// Prober is implemented by a handler that can tell whether its backend answers at all.
// With one, the pool pauses while the backend is unreachable instead of retrying every
// user against it.
type Prober interface {
	Probe(ctx context.Context) error
}

// Delays between retries of a user whose sync keeps failing. The maximum bounds how
// long a user waits after the other side comes back.
const (
	defaultRetryBaseDelay = time.Second
	defaultRetryMaxDelay  = 30 * time.Second
	defaultProbeInterval  = 2 * time.Second
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

	// Users being synced; the value records that an event arrived meanwhile.
	inFlightMu sync.Mutex
	inFlight   map[string]bool

	// While Doveadm is unreachable the fetcher waits on resumeCh, and users whose sync
	// failed for that reason wait in parked until a probe succeeds.
	probeInterval time.Duration
	pauseMu       sync.Mutex
	resumeCh      chan struct{} // non-nil while paused
	parked        map[string]struct{}

	metrics *metrics.Metrics // optional
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

		inFlight: make(map[string]bool),

		probeInterval: defaultProbeInterval,
		parked:        make(map[string]struct{}),
	}
}

// SetMetrics makes the pool report when it is paused.
func (wp *WorkerPool) SetMetrics(m *metrics.Metrics) {
	wp.metrics = m
}

// setRetryingGauge must be called with retryMu held.
func (wp *WorkerPool) setRetryingGauge() {
	if wp.metrics != nil {
		wp.metrics.UsersRetrying.Set(float64(len(wp.retries)))
	}
}

func (wp *WorkerPool) setPausedGauge(v float64) {
	if wp.metrics != nil {
		wp.metrics.DoveadmPaused.Set(v)
	}
}

// SetProbeInterval overrides how often an unreachable Doveadm is checked.
func (wp *WorkerPool) SetProbeInterval(d time.Duration) {
	wp.pauseMu.Lock()
	defer wp.pauseMu.Unlock()
	wp.probeInterval = d
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

		// Hold off while Doveadm is unreachable; the queue keeps every event meanwhile.
		if resume := wp.pausedCh(); resume != nil {
			select {
			case <-wp.stopCh:
				close(wp.jobsCh)
				return
			case <-resume:
			}
			continue
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

		if !wp.claim(username) {
			wp.logger.Debug("User already syncing, will rerun", "worker_id", id, "username", username)
			continue
		}

		// mark active
		atomic.AddInt32(&wp.activeCount, 1)
		wp.logger.Debug("Processing event", "worker_id", id, "username", username)

		// Handle the event
		if err := wp.handler.Handle(ctx, username); err != nil {
			if doveadm.IsNoUser(err) {
				// A removed user: retrying cannot help, and a new event queues the user
				// again once it exists.
				wp.clearRetry(username)
				wp.logger.Warn("User does not exist, not retrying", "worker_id", id, "username", username)
			} else if wp.pauseFor(ctx, username, err) {
				wp.logger.Warn("Doveadm unreachable, user waits for it", "worker_id", id, "username", username, "error", err)
			} else {
				delay := wp.scheduleRetry(ctx, username)
				wp.logger.Error("Handler failed, requeuing", "worker_id", id, "username", username, "delay", delay, "error", err)
			}
		} else {
			wp.clearRetry(username)
		}
		wp.release(ctx, username)

		// mark inactive
		atomic.AddInt32(&wp.activeCount, -1)
	}
}

// pausedCh returns the channel closed on resume, or nil when not paused.
func (wp *WorkerPool) pausedCh() chan struct{} {
	wp.pauseMu.Lock()
	defer wp.pauseMu.Unlock()
	return wp.resumeCh
}

// pauseFor parks the user and pauses the pool if err means Doveadm is unreachable and
// the handler can probe it. It reports whether it did.
func (wp *WorkerPool) pauseFor(ctx context.Context, username string, err error) bool {
	prober, ok := wp.handler.(Prober)
	if !ok || !doveadm.IsUnreachable(err) {
		return false
	}
	wp.pauseMu.Lock()
	defer wp.pauseMu.Unlock()
	wp.parked[username] = struct{}{}
	if wp.resumeCh == nil {
		wp.resumeCh = make(chan struct{})
		wp.logger.Error("Doveadm unreachable, pausing all syncs until it answers", "probe_interval", wp.probeInterval, "error", err)
		wp.setPausedGauge(1)
		go wp.probeUntilUp(ctx, prober, wp.probeInterval)
	}
	return true
}

// probeUntilUp checks Doveadm until it answers, then resumes the pool.
func (wp *WorkerPool) probeUntilUp(ctx context.Context, prober Prober, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	start := time.Now()
	for {
		select {
		case <-wp.stopCh:
			return
		case <-ticker.C:
		}
		probeCtx, cancel := context.WithTimeout(ctx, interval)
		err := prober.Probe(probeCtx)
		cancel()
		if err != nil {
			wp.logger.Debug("Doveadm still unreachable", "error", err)
			continue
		}
		wp.resume(ctx, time.Since(start))
		return
	}
}

// resume restarts the pool and syncs every waiting user now: the parked ones, and those
// in backoff, whose failures were most likely the outage too.
func (wp *WorkerPool) resume(ctx context.Context, down time.Duration) {
	wp.retryMu.Lock()
	users := make(map[string]struct{}, len(wp.retries))
	for u, r := range wp.retries {
		if r.timer != nil && r.timer.Stop() {
			users[u] = struct{}{}
		}
	}
	wp.retries = make(map[string]*retryState)
	wp.setRetryingGauge()
	wp.retryMu.Unlock()

	wp.pauseMu.Lock()
	for u := range wp.parked {
		users[u] = struct{}{}
	}
	wp.parked = make(map[string]struct{})
	close(wp.resumeCh)
	wp.resumeCh = nil
	wp.pauseMu.Unlock()
	wp.setPausedGauge(0)

	wp.logger.Info("Doveadm answers again, resuming syncs", "paused_for", down, "users", len(users))
	for u := range users {
		if err := wp.queue.Enqueue(ctx, u, 1.0); err != nil {
			wp.logger.Error("Failed to requeue", "username", u, "error", err)
		}
	}
}

// claim reserves the user for this worker. Two syncs of one user must not overlap: the
// state of one can claim mail the other has not copied, and every later incremental
// sync then skips that mail while still reporting success.
func (wp *WorkerPool) claim(username string) bool {
	wp.inFlightMu.Lock()
	defer wp.inFlightMu.Unlock()
	if _, busy := wp.inFlight[username]; busy {
		wp.inFlight[username] = true
		return false
	}
	wp.inFlight[username] = false
	return true
}

// release frees the user and requeues it if an event arrived during the sync, which
// that sync may have missed.
func (wp *WorkerPool) release(ctx context.Context, username string) {
	wp.inFlightMu.Lock()
	rerun := wp.inFlight[username]
	delete(wp.inFlight, username)
	wp.inFlightMu.Unlock()
	if rerun {
		if err := wp.queue.Enqueue(ctx, username, 1.0); err != nil {
			wp.logger.Error("Failed to requeue", "username", username, "error", err)
		}
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
		wp.setRetryingGauge()
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
	wp.setRetryingGauge()
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
