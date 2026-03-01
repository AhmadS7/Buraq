package api

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"

	"buraq/task"

	"github.com/redis/go-redis/v9"
)

type Server struct {
	client *redis.Client
	stream string
}

func NewServer(client *redis.Client, stream string) *Server {
	return &Server{
		client: client,
		stream: stream,
	}
}

func (s *Server) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/stream", s.handleStream)
	mux.HandleFunc("/api/workers", s.handleWorkers)
	mux.HandleFunc("/api/retry-dlq", s.handleRetryDLQ)

	handler := corsMiddleware(mux)

	log.Printf("Starting API server on %s", addr)
	return http.ListenAndServe(addr, handler)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	pubsub := s.client.Subscribe(ctx, "buraq_events")
	defer pubsub.Close()
	ch := pubsub.Channel()

	// Send an initial heartbeat to establish connection
	fmt.Fprintf(w, "data: {\"type\":\"ping\"}\n\n")
	flusher.Flush()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", msg.Payload)
			flusher.Flush()
		}
	}
}

func (s *Server) handleWorkers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Mock some worker data for React Flow
	type Worker struct {
		ID     string  `json:"id"`
		CPU    float64 `json:"cpu"`
		Memory float64 `json:"memory"`
		Status string  `json:"status"`
	}

	workers := []Worker{
		{ID: "worker_node_1-1", CPU: rand.Float64() * 100, Memory: 256 + rand.Float64()*512, Status: "active"},
		{ID: "worker_node_1-2", CPU: rand.Float64() * 100, Memory: 256 + rand.Float64()*512, Status: "active"},
		{ID: "worker_node_1-3", CPU: rand.Float64() * 100, Memory: 256 + rand.Float64()*512, Status: "active"},
		{ID: "worker_node_1-4", CPU: rand.Float64() * 100, Memory: 256 + rand.Float64()*512, Status: "active"},
		{ID: "worker_node_1-5", CPU: rand.Float64() * 100, Memory: 256 + rand.Float64()*512, Status: "active"},
	}

	json.NewEncoder(w).Encode(workers)
}

func (s *Server) handleRetryDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" && r.Method != "OPTIONS" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	dlqStream := s.stream + "_dlq"

	// Fetch all tasks from DLQ
	messages, err := s.client.XRange(ctx, dlqStream, "-", "+").Result()
	if err != nil {
		http.Error(w, "Failed to read DLQ", http.StatusInternalServerError)
		return
	}

	retried := 0
	for _, msg := range messages {
		payload, ok := msg.Values["payload"].(string)
		if !ok {
			continue
		}

		t, err := task.Unmarshal([]byte(payload))
		if err != nil {
			continue
		}

		// Reset retries
		t.CurrentRetries = 0

		data, err := t.Marshal()
		if err != nil {
			continue
		}

		// Add back to main stream
		err = s.client.XAdd(ctx, &redis.XAddArgs{
			Stream: s.stream,
			Values: map[string]interface{}{
				"payload": data,
			},
		}).Err()

		if err == nil {
			// Delete from DLQ
			s.client.XDel(ctx, dlqStream, msg.ID)
			retried++

			// Publish event that it's back to Pending
			e := task.Event{
				Type:   "Pending",
				TaskID: t.ID,
			}
			b, _ := json.Marshal(e)
			s.client.Publish(ctx, "buraq_events", string(b))
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"retried": retried,
	})
}
