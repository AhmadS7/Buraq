package worker

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

var (
	tasksProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tasks_processed_total",
			Help: "Total number of tasks processed, labeled by status",
		},
		[]string{"status"},
	)
	tasksFailedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tasks_failed_total",
			Help: "Total number of tasks that failed and moved to DLQ",
		},
	)
	taskProcessingDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "task_processing_duration_seconds",
			Help:    "Histogram of task processing durations in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)
)

// Task models the payload enqueued to Buraq
type Task struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
	Retries int             `json:"retries"`
}

// Worker represents a hardened Buraq Task Consumer
type Worker struct {
	client     *redis.Client
	stream     string
	group      string
	consumerID string
}

func NewWorker(client *redis.Client, stream, group, consumerID string) *Worker {
	return &Worker{
		client:     client,
		stream:     stream,
		group:      group,
		consumerID: consumerID,
	}
}

// Start listens to the stream and gracefully shuts down on context cancellation.
// It also spins up an HTTP server on the given metricsAddr to export Prometheus metrics natively.
func (w *Worker) Start(ctx context.Context, workersCount int, metricsAddr string) error {
	w.client.XGroupCreateMkStream(ctx, w.stream, w.group, "0")

	// Prometheus HTTP server
	if metricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())

			server := &http.Server{
				Addr:    metricsAddr,
				Handler: mux,
			}

			go func() {
				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.Fatalf("Metrics server failed: %v", err)
				}
			}()

			<-ctx.Done()

			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			server.Shutdown(shutCtx)
		}()
	}

	taskChannel := make(chan redis.XMessage)
	var wg sync.WaitGroup

	// Spawn Goroutines for the worker pool
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func(workerNodeID int) {
			defer wg.Done()
			for msg := range taskChannel {
				// Use context.Background() in the handler to prevent aborting mid-task
				// during shutdown logic
				w.processMessage(context.Background(), msg)
			}
			log.Printf("Worker node %d gracefully shut down", workerNodeID)
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(taskChannel) // Signifies no more tasks will be fetched; pool can drain

		for {
			select {
			case <-ctx.Done():
				log.Println("Context cancelled. Stop fetching, allowing workers to drain...")
				return
			default:
				res, err := w.client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    w.group,
					Consumer: w.consumerID,
					Streams:  []string{w.stream, ">"},
					Count:    10,
					Block:    2 * time.Second,
				}).Result()

				if err != nil && err != redis.Nil && err != context.Canceled {
					log.Printf("Error reading stream: %v", err)
					time.Sleep(time.Second)
					continue
				}

				if ctx.Err() != nil {
					return
				}

				for _, stream := range res {
					for _, msg := range stream.Messages {
						// Blocks until a worker is free. Not selected against ctx.Done
						// to ensure fetched messages are safely held in channel
						taskChannel <- msg
					}
				}
			}
		}
	}()

	log.Println("Worker pool started. Waiting for context cancellation.")
	wg.Wait()
	return nil
}

func (w *Worker) processMessage(ctx context.Context, msg redis.XMessage) {
	startTime := time.Now()
	var t Task
	payloadStr, ok := msg.Values["payload"].(string)

	if ok {
		_ = json.Unmarshal([]byte(payloadStr), &t)
	}

	err := w.simulateWork(&t)

	duration := time.Since(startTime).Seconds()
	taskProcessingDurationSeconds.Observe(duration)

	if err != nil {
		t.Retries++
		if t.Retries >= 3 {
			w.moveToDLQ(ctx, &t, msg.ID)
		} else {
			// Re-queue
			w.requeueTask(ctx, &t)
			w.ack(ctx, msg.ID)
		}
		tasksProcessedTotal.WithLabelValues("error").Inc()
		return
	}

	tasksProcessedTotal.WithLabelValues("success").Inc()
	w.ack(ctx, msg.ID)
}

func (w *Worker) simulateWork(t *Task) error {
	// Simulated. In real-world, we process the task.
	time.Sleep(10 * time.Millisecond)
	return nil
}

func (w *Worker) moveToDLQ(ctx context.Context, t *Task, originalMsgID string) {
	data, _ := json.Marshal(t)
	err := w.client.XAdd(ctx, &redis.XAddArgs{
		Stream: "buraq:dlq",
		Values: map[string]interface{}{"payload": data},
	}).Err()

	if err == nil {
		w.ack(ctx, originalMsgID)
		tasksFailedTotal.Inc()
		log.Printf("Moved task %s to DLQ", t.ID)
	}
}

func (w *Worker) requeueTask(ctx context.Context, t *Task) {
	data, _ := json.Marshal(t)
	w.client.XAdd(ctx, &redis.XAddArgs{
		Stream: w.stream,
		Values: map[string]interface{}{"payload": data},
	})
}

func (w *Worker) ack(ctx context.Context, msgID string) {
	w.client.XAck(ctx, w.stream, w.group, msgID)
}
