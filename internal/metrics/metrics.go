package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds all Prometheus metrics for the application.
type Metrics struct {
	EventsReceived prometheus.Counter
	EventsFiltered prometheus.Counter
	EventsEnqueued prometheus.Counter
	EnqueueErrors  prometheus.Counter
	RedisErrors    prometheus.Counter

	// Syncs counts dsync runs by result (ok, failed, unreachable) and by whether they
	// were full.
	Syncs *prometheus.CounterVec
	// DoveadmPaused is 1 while all syncs are paused because Doveadm does not answer.
	DoveadmPaused prometheus.Gauge

	// SyncDuration times each dsync run, by result.
	SyncDuration *prometheus.HistogramVec
	// UsersFailing counts users whose last sync failed.
	UsersFailing prometheus.Gauge
	// UsersRetrying counts users waiting for a retry after failures.
	UsersRetrying prometheus.Gauge

	// The background pass: its runs, and what the last successful one saw. Usernames are
	// never labels.
	BackgroundRuns          *prometheus.CounterVec
	BackgroundLastSuccess   prometheus.Gauge
	UsersTotal              prometheus.Gauge
	UsersStale              prometheus.Gauge
	OldestUserSyncTimestamp prometheus.Gauge
}

// New creates and registers all metrics.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsReceived: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "dovewarden_events_received_total",
				Help: "Total number of events received from Dovecot",
			},
		),
		EventsFiltered: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "dovewarden_events_filtered_total",
				Help: "Total number of events that passed the filter (those that can change mail)",
			},
		),
		EventsEnqueued: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "dovewarden_events_enqueued_total",
				Help: "Total number of events successfully enqueued",
			},
		),
		EnqueueErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "dovewarden_enqueue_errors_total",
				Help: "Total number of enqueue errors",
			},
		),
		RedisErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "dovewarden_redis_errors_total",
				Help: "Total number of errors from the queue's Redis store",
			},
		),
		Syncs: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dovewarden_syncs_total",
				Help: "Total number of dsync runs, by result (ok, failed, unreachable) and full (true, false)",
			},
			[]string{"result", "full"},
		),
		DoveadmPaused: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "dovewarden_doveadm_paused",
				Help: "1 while syncs are paused because Doveadm does not answer, else 0",
			},
		),
		SyncDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "dovewarden_sync_duration_seconds",
				Help:    "Duration of dsync runs, by result (ok, failed, unreachable)",
				// A catch-up after an outage took over 8 minutes on dev, so the buckets go to 30 minutes.
				Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200, 1800},
			},
			[]string{"result"},
		),
		UsersFailing: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "dovewarden_users_failing",
				Help: "Number of users whose last dsync run failed",
			},
		),
		UsersRetrying: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "dovewarden_users_retrying",
				Help: "Number of users in retry backoff after failed syncs",
			},
		),
		BackgroundRuns: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dovewarden_background_runs_total",
				Help: "Total number of background replication passes, by result (ok, failed)",
			},
			[]string{"result"},
		),
		BackgroundLastSuccess: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "dovewarden_background_last_success_timestamp_seconds",
				Help: "Unix time of the last background pass that listed every user",
			},
		),
		UsersTotal: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "dovewarden_users_total",
				Help: "Number of users Doveadm listed in the last successful background pass",
			},
		),
		UsersStale: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "dovewarden_users_stale",
				Help: "Users not synced within the background threshold, as of the last successful pass",
			},
		),
		OldestUserSyncTimestamp: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "dovewarden_oldest_user_sync_timestamp_seconds",
				Help: "Unix time of the least recent successful sync of any user, as of the last " +
					"successful background pass; process start for a user not synced since",
			},
		),
	}

	reg.MustRegister(
		m.EventsReceived,
		m.EventsFiltered,
		m.EventsEnqueued,
		m.EnqueueErrors,
		m.RedisErrors,
		m.Syncs,
		m.DoveadmPaused,
		m.SyncDuration,
		m.UsersFailing,
		m.UsersRetrying,
		m.BackgroundRuns,
		m.BackgroundLastSuccess,
		m.UsersTotal,
		m.UsersStale,
		m.OldestUserSyncTimestamp,
	)

	return m
}

// RegisterQueueDepth exports the queue's length, read at every scrape.
func RegisterQueueDepth(reg prometheus.Registerer, depth func() float64) {
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dovewarden_queue_depth",
			Help: "Number of users waiting in the sync queue",
		},
		depth,
	))
}
