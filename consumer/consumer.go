package consumer

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"buraq/metrics"
	"buraq/task"

	"github.com/redis/go-redis/v9"
)

// Consumer manages fetching tasks from Redis and processing them via a worker pool.
type Consumer struct {
	client      *redis.Client
	stream      string
	group       string
	consumer    string
	workerCount int
}

// New creates a new Consumer instance.
func New(client *redis.Client, stream, group, consumer string, workerCount int) *Consumer {
	return &Consumer{
		client:      client,
		stream:      stream,
		group:       group,
		consumer:    consumer,
		workerCount: workerCount,
	}
}

// Start creates the consumer group (if necessary) and begins fetching/processing tasks.
func (c *Consumer) Start(ctx context.Context) error {
	// Create consumer group, ignoring error if it already exists
	err := c.client.XGroupCreateMkStream(ctx, c.stream, c.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	tasksCh := make(chan redis.XMessage)
	var wg sync.WaitGroup

	// Start worker pool
	for i := 1; i <= c.workerCount; i++ {
		wg.Add(1)
		go c.worker(ctx, &wg, tasksCh, i)
	}

	// Start task fetcher
	wg.Add(1)
	go c.fetchTasks(ctx, &wg, tasksCh)

	// Wait for fetcher and workers to shut down gracefully
	wg.Wait()
	return nil
}

// fetchTasks continuously reads messages from the Redis Stream and sends them to workers.
func (c *Consumer) fetchTasks(ctx context.Context, wg *sync.WaitGroup, tasksCh chan<- redis.XMessage) {
	defer wg.Done()
	defer close(tasksCh) // Close channel to signal workers to stop once fetcher is done

	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, consumer fetcher stopping.")
			return
		default:
			args := &redis.XReadGroupArgs{
				Group:    c.group,
				Consumer: c.consumer,
				Streams:  []string{c.stream, ">"},
				Count:    10, // Fetch up to 10 tasks at a time
				Block:    2 * time.Second,
			}

			streams, err := c.client.XReadGroup(ctx, args).Result()
			if err != nil {
				if err != redis.Nil {
					log.Printf("Error reading from Redis Stream: %v", err)
					time.Sleep(1 * time.Second) // Backoff on error
				}
				continue
			}

			// Distribute tasks to workers via channel
			for _, stream := range streams {
				for _, msg := range stream.Messages {
					select {
					case tasksCh <- msg:
					case <-ctx.Done():
						log.Println("Context cancelled while distributing tasks.")
						return
					}
				}
			}
		}
	}
}

// worker represents a single goroutine in the worker pool processing tasks.
func (c *Consumer) worker(ctx context.Context, wg *sync.WaitGroup, tasksCh <-chan redis.XMessage, id int) {
	defer wg.Done()
	log.Printf("Worker %d: started", id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d: stopped (context cancelled)", id)
			return
		case msg, ok := <-tasksCh:
			if !ok {
				log.Printf("Worker %d: stopped (channel closed)", id)
				return
			}
			c.processMessage(ctx, id, msg)
		}
	}
}

// processMessage extracts the task payload, processes it, records metrics, and acknowledges it.
// If processing fails, it handles retries or routes the task to the DLQ.
func (c *Consumer) processMessage(ctx context.Context, workerID int, msg redis.XMessage) {
	startTime := time.Now()

	payload, ok := msg.Values["payload"].(string)
	if !ok {
		log.Printf("Worker %d: Invalid payload format in message %s", workerID, msg.ID)
		c.acknowledge(context.Background(), msg.ID) // use background ctx for ack so it succeeds even during shutdown
		return
	}

	t, err := task.Unmarshal([]byte(payload))
	if err != nil {
		log.Printf("Worker %d: Failed to unmarshal task %s: %v", workerID, msg.ID, err)
		c.acknowledge(context.Background(), msg.ID)
		return
	}

	log.Printf("Worker %d: Processing task %s (Type: %s, Retry: %d/%d)", workerID, t.ID, t.Type, t.CurrentRetries, t.MaxRetries)

	err = c.simulateWork(t)
	duration := time.Since(startTime).Seconds()
	metrics.TaskDurationSeconds.WithLabelValues(t.Type).Observe(duration)

	if err != nil {
		log.Printf("Worker %d: Task %s failed: %v", workerID, t.ID, err)
		metrics.TasksProcessedTotal.WithLabelValues(t.Type, "error").Inc()
		metrics.TasksFailedTotal.WithLabelValues(t.Type).Inc()

		c.handleTaskFailure(context.Background(), workerID, t, msg.ID, err)
		return
	}

	// Success
	metrics.TasksProcessedTotal.WithLabelValues(t.Type, "success").Inc()
	log.Printf("Worker %d: Completed task %s in %.3fs", workerID, t.ID, duration)

	// Acknowledge task inside background context to ensure it acks if main ctx is cancelled right after process
	c.acknowledge(context.Background(), msg.ID)
}

func (c *Consumer) handleTaskFailure(ctx context.Context, workerID int, t *task.Task, originalMsgID string, processErr error) {
	t.Error = processErr.Error()

	if t.CurrentRetries < t.MaxRetries {
		t.CurrentRetries++
		log.Printf("Worker %d: Re-queueing task %s (Retry %d/%d)", workerID, t.ID, t.CurrentRetries, t.MaxRetries)

		err := c.enqueueTask(ctx, c.stream, t)
		if err != nil {
			log.Printf("Worker %d: Failed to re-queue task %s: %v", workerID, t.ID, err)
			return // Don't ack original if we failed to re-queue, let it sit in PEL
		}
	} else {
		log.Printf("Worker %d: Task %s exceeded max retries. Moving to DLQ.", workerID, t.ID)
		metrics.TasksDLQTotal.WithLabelValues(t.Type).Inc()

		dlqStream := fmt.Sprintf("%s_dlq", c.stream)
		err := c.enqueueTask(ctx, dlqStream, t)
		if err != nil {
			log.Printf("Worker %d: Failed to move task %s to DLQ: %v", workerID, t.ID, err)
			return // Don't ack original if we failed to DLQ, let it sit in PEL
		}
	}

	// Always ACK the original message if we successfully moved it (either retry or DLQ)
	c.acknowledge(ctx, originalMsgID)
}

func (c *Consumer) enqueueTask(ctx context.Context, destStream string, t *task.Task) error {
	data, err := t.Marshal()
	if err != nil {
		return err
	}

	args := &redis.XAddArgs{
		Stream: destStream,
		Values: map[string]interface{}{
			"payload": data,
		},
	}

	return c.client.XAdd(ctx, args).Err()
}

func (c *Consumer) simulateWork(t *task.Task) error {
	time.Sleep(500 * time.Millisecond)

	// Simulate a 5% failure rate to demonstrate retry and DLQ logic
	if rand.New(rand.NewSource(time.Now().UnixNano())).Float32() < 0.05 {
		return fmt.Errorf("simulated network timeout while calling external API")
	}

	return nil
}

func (c *Consumer) acknowledge(ctx context.Context, msgID string) {
	err := c.client.XAck(ctx, c.stream, c.group, msgID).Err()
	if err != nil {
		log.Printf("Failed to acknowledge message %s: %v", msgID, err)
	}
}
