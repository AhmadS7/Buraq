package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// BenchmarkBuraqQueue simulates 10,000 concurrent task enqueues and measures P99 latency.
func BenchmarkBuraqQueue(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		b.Skip("Redis not available, skipping benchmark:", err)
	}

	stream := fmt.Sprintf("buraq_bench_p99_%d", time.Now().UnixNano())
	group := stream + "_group"

	// Pre-start consumer
	w := NewWorker(rdb, stream, group, "bench_consumer_1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx, 50, "") // 50 concurrent internal workers, no metrics server needed for bench

	b.ResetTimer()

	// 10,000 Tasks
	const totalTasks = 10000
	var wg sync.WaitGroup
	var mu sync.Mutex
	latencies := make([]float64, 0, totalTasks)

	// In real-world b.N goes to an arbitrary big number, but prompt requested simulating 10,000 tasks
	b.Logf("Enqueuing %d concurrent tasks...", totalTasks)
	startTotal := time.Now()

	for i := 0; i < totalTasks; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			startReq := time.Now()

			t := Task{ID: fmt.Sprintf("task-%d", id)}
			data, _ := json.Marshal(t)

			err := rdb.XAdd(context.Background(), &redis.XAddArgs{
				Stream: stream,
				Values: map[string]interface{}{"payload": data},
			}).Err()

			if err == nil {
				dur := float64(time.Since(startReq).Milliseconds())
				mu.Lock()
				latencies = append(latencies, dur)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	totalDur := time.Since(startTotal).Seconds()

	// Calculate P99 Latency of exact enqueue times
	sort.Float64s(latencies)
	var p99 float64
	if len(latencies) > 0 {
		p99Idx := int(float64(len(latencies)) * 0.99)
		if p99Idx >= len(latencies) {
			p99Idx = len(latencies) - 1
		}
		p99 = latencies[p99Idx]
	}

	b.ReportMetric(totalDur, "total_seconds")
	b.ReportMetric(p99, "p99_latency_ms")
	b.ReportMetric(float64(totalTasks)/totalDur, "TPS")

	// Clean up Redis stream
	cancel()
	rdb.Del(context.Background(), stream)
}
