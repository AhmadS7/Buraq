package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"buraq/api"
	"buraq/consumer"
	"buraq/producer"
	"buraq/task"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	// Initialize context that listens to OS signals for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT and SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received signal %v, initiating graceful shutdown...", sig)
		cancel()
	}()

	// Start Prometheus metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Starting Prometheus metrics server on :2112")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Fatalf("Metrics server failed: %v", err)
		}
	}()

	// Initialize Redis Client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("Could not connect to Redis (is it running?): %v", err)
		log.Println("Continuing anyway, but commands will fail until Redis is available.")
	} else {
		log.Println("Connected to Redis successfully.")
	}

	streamName := "buraq_tasks"
	groupName := "buraq_workers"
	consumerName := "worker_node_1"

	// Initialize Producer
	p := producer.New(rdb, streamName)

	// API Server
	apiSrv := api.NewServer(rdb, streamName)
	go func() {
		if err := apiSrv.Start(":8080"); err != nil {
			log.Fatalf("API server failed: %v", err)
		}
	}()

	// Start a goroutine to continuously produce mock tasks
	go produceMockTasks(ctx, p)

	// Initialize and start Consumer with 5 workers
	c := consumer.New(rdb, streamName, groupName, consumerName, 5)

	log.Println("Starting Buraq Consumer...")
	if err := c.Start(ctx); err != nil {
		log.Fatalf("Consumer stopped with error: %v", err)
	}

	log.Println("Buraq shutdown gracefully.")
}

func produceMockTasks(ctx context.Context, p *producer.Producer) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	taskCounter := 1

	for {
		select {
		case <-ctx.Done():
			log.Println("Mock producer stopping...")
			return
		case <-ticker.C:
			t := &task.Task{
				ID:         fmt.Sprintf("task-%d", taskCounter),
				Type:       "email_notification",
				Payload:    json.RawMessage(`{"user_id": 123, "template": "welcome"}`),
				CreatedAt:  time.Now().UTC(),
				MaxRetries: 3,
			}

			msgID, err := p.Produce(ctx, t)
			if err != nil {
				log.Printf("Failed to produce task %d: %v", taskCounter, err)
			} else {
				log.Printf("Produced task %d as message ID %s", taskCounter, msgID)
			}
			taskCounter++
		}
	}
}
