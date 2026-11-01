package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/dovewarden/dovewarden/internal/doveadm"
)

// DoveadmEventHandler handles events by sending dsync requests to Doveadm
type DoveadmEventHandler struct {
	client      *doveadm.Client
	destination string
	logger      *slog.Logger
	queue       Queue
}

// NewDoveadmEventHandler creates a new handler for Doveadm sync operations
func NewDoveadmEventHandler(baseURL, password, destination string, logger *slog.Logger, queue Queue) *DoveadmEventHandler {
	return &DoveadmEventHandler{
		client:      doveadm.NewClient(baseURL, password),
		destination: destination,
		logger:      logger,
		queue:       queue,
	}
}

// Handle sends a dsync request to Doveadm for the given username.
//
// Every sync is a full one, on purpose. Incremental state can go wrong with no signal:
// dsync then finds nothing to do and exits 0 while the two sides stay different, and a
// delete it never saw can bring mail back. A full sync costs about 150 ms on a mailbox of
// a thousand messages, so the state is not worth that risk here.
func (h *DoveadmEventHandler) Handle(ctx context.Context, username string) error {
	h.logger.Info("Syncing user via dsync", "username", username, "destination", h.destination)

	if _, err := h.client.Sync(ctx, username, h.destination, ""); err != nil {
		h.logger.Error("dsync failed", "username", username, "error", err)
		return err
	}

	// Record the timestamp of this successful replication
	if err := h.queue.SetLastReplicationTime(ctx, username, time.Now()); err != nil {
		h.logger.Warn("Failed to store last replication time", "username", username, "error", err)
		// Don't fail the sync operation if timestamp storage fails
	}

	h.logger.Info("dsync completed", "username", username)
	return nil
}
