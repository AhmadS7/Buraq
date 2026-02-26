package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// TasksProcessedTotal tracks the total number of tasks successfully processed.
	TasksProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "buraq_tasks_processed_total",
			Help: "Total number of tasks processed by Buraq consumers",
		},
		[]string{"task_type", "status"}, // Status can be "success" or "error"
	)

	// TasksFailedTotal tracks the number of times a task processing attempt failed.
	TasksFailedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "buraq_tasks_failed_total",
			Help: "Total number of task processing failures (retries)",
		},
		[]string{"task_type"},
	)

	// TasksDLQTotal tracks the total number of tasks moved to the Dead-Letter Queue.
	TasksDLQTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "buraq_tasks_dlq_total",
			Help: "Total number of tasks moved to the DLQ stream after exhausted retries",
		},
		[]string{"task_type"},
	)

	// TaskDurationSeconds tracks the duration of task processing.
	TaskDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "buraq_task_duration_seconds",
			Help:    "Histogram of task processing duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"task_type"},
	)
)
