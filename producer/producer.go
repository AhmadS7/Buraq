package producer

import (
	"context"
	"encoding/json"

	"buraq/task"

	"github.com/redis/go-redis/v9"
)

// Producer handles enqueueing tasks to a Redis Stream.
type Producer struct {
	client *redis.Client
	stream string
}

// New creates a new Producer instance.
func New(client *redis.Client, stream string) *Producer {
	return &Producer{
		client: client,
		stream: stream,
	}
}

// Produce serializes the task and adds it to the Redis stream.
func (p *Producer) Produce(ctx context.Context, t *task.Task) (string, error) {
	data, err := t.Marshal()
	if err != nil {
		return "", err
	}

	args := &redis.XAddArgs{
		Stream: p.stream,
		Values: map[string]interface{}{
			"payload": data,
		},
	}

	res, err := p.client.XAdd(ctx, args).Result()
	if err == nil {
		e := task.Event{
			Type:   "Pending",
			TaskID: t.ID,
		}
		b, _ := json.Marshal(e)
		p.client.Publish(ctx, "buraq_events", string(b))
	}
	return res, err
}
