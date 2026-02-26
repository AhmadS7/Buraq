package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// TasksProcessedTotal tracks the total number of tasks successfully processed.
	TasksProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "horus_tasks_processed_total",
			Help: "Total number of tasks processed by Horus consumers",
		},
		[]string{"task_type", "status"}, // Status can be "success" or "error"
	)

	// TaskDurationSeconds tracks the duration of task processing.
	TaskDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "horus_task_duration_seconds",
			Help:    "Histogram of task processing duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"task_type"},
	)
)
