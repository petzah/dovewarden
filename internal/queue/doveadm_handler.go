package queue

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/dovewarden/dovewarden/internal/doveadm"
	"github.com/dovewarden/dovewarden/internal/metrics"
)

// DoveadmEventHandler handles events by sending dsync requests to Doveadm
type DoveadmEventHandler struct {
	client      *doveadm.Client
	destination string
	logger      *slog.Logger
	queue       Queue
	metrics     *metrics.Metrics // optional

	// fullEvery is how often each user gets a full sync when nothing failed. Zero means
	// every sync is full.
	fullEvery time.Duration
	mu        sync.Mutex
	lastFull  map[string]time.Time
	failed    map[string]bool
}

// DefaultFullSyncInterval is how often a user gets a full sync when nothing has failed.
// Zero makes every sync full: one delivery or flag change on each side between two syncs
// leaves the counters equal, and only a full sync notices that.
const DefaultFullSyncInterval = 0

// NewDoveadmEventHandler creates a new handler for Doveadm sync operations
func NewDoveadmEventHandler(baseURL, password, destination string, logger *slog.Logger, queue Queue) *DoveadmEventHandler {
	return &DoveadmEventHandler{
		client:      doveadm.NewClient(baseURL, password),
		destination: destination,
		logger:      logger,
		queue:       queue,
		fullEvery:   DefaultFullSyncInterval,
		lastFull:    make(map[string]time.Time),
		failed:      make(map[string]bool),
	}
}

// SetFullSyncInterval sets how often each user gets a full sync when nothing has failed;
// zero makes every sync full.
func (h *DoveadmEventHandler) SetFullSyncInterval(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fullEvery = d
}

// SetMetrics makes the handler count its syncs.
func (h *DoveadmEventHandler) SetMetrics(m *metrics.Metrics) {
	h.metrics = m
}

func (h *DoveadmEventHandler) countSync(full bool, err error, took time.Duration) {
	if h.metrics == nil {
		return
	}
	result := "ok"
	if doveadm.IsUnreachable(err) {
		result = "unreachable"
	} else if doveadm.IsNoUser(err) {
		result = "nouser"
	} else if err != nil {
		result = "failed"
	}
	h.metrics.Syncs.WithLabelValues(result, strconv.FormatBool(full)).Inc()
	h.metrics.SyncDuration.WithLabelValues(result).Observe(took.Seconds())
}

// Probe reports whether Doveadm answers at all.
func (h *DoveadmEventHandler) Probe(ctx context.Context) error {
	return h.client.Probe(ctx)
}

// needsFull reports whether the next sync of the user must be a full one: always when
// fullEvery is zero, otherwise after any failure, before the first full sync since start,
// and once fullEvery has passed.
func (h *DoveadmEventHandler) needsFull(username string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failed[username] {
		return true
	}
	if h.fullEvery <= 0 {
		return true
	}
	last, ok := h.lastFull[username]
	return !ok || time.Since(last) >= h.fullEvery
}

func (h *DoveadmEventHandler) recordResult(username string, full bool, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer func() {
		if h.metrics != nil {
			h.metrics.UsersFailing.Set(float64(len(h.failed)))
		}
	}()
	if doveadm.IsNoUser(err) {
		// Removed users are forgotten, so they are not counted as failing for good.
		delete(h.failed, username)
		delete(h.lastFull, username)
		return
	}
	if err != nil {
		h.failed[username] = true
		return
	}
	delete(h.failed, username)
	if full {
		h.lastFull[username] = time.Now()
	}
}

// Handle sends a dsync request to Doveadm for the given username.
//
// No incremental state is kept: it can go wrong with no signal, after which dsync finds
// nothing to do and exits 0 while the two sides stay different. An empty state is still
// not a full sync, though. dsync skips a mailbox whose message count, uidnext and
// highestmodseq match on both sides, so mail that differs behind equal counters stays
// different: it takes no more than one delivery or flag change on each side between two
// syncs. So by default every sync is a full one, which compares every mailbox. With
// fullEvery set, a full sync is sent only after a failure, on the first sync of each user
// since start, and every fullEvery.
func (h *DoveadmEventHandler) Handle(ctx context.Context, username string) error {
	full := h.needsFull(username)
	h.logger.Info("Syncing user via dsync", "username", username, "destination", h.destination, "full", full)

	start := time.Now()
	_, err := h.client.Sync(ctx, username, h.destination, "", full)
	h.recordResult(username, full, err)
	h.countSync(full, err, time.Since(start))
	if doveadm.IsNoUser(err) {
		h.logger.Warn("dsync failed: user does not exist", "username", username, "full", full, "error", err)
		return err
	}
	if err != nil {
		h.logger.Error("dsync failed", "username", username, "full", full, "error", err)
		return err
	}

	// Record the timestamp of this successful replication
	if err := h.queue.SetLastReplicationTime(ctx, username, time.Now()); err != nil {
		h.logger.Warn("Failed to store last replication time", "username", username, "error", err)
		// Don't fail the sync operation if timestamp storage fails
	}

	h.logger.Info("dsync completed", "username", username, "full", full)
	return nil
}
