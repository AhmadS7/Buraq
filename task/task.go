package task

import (
	"encoding/json"
	"time"
)

// Task represents a unit of work in Buraq.
type Task struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

// Marshal converts the Task into a JSON byte slice.
func (t *Task) Marshal() ([]byte, error) {
	return json.Marshal(t)
}

// Unmarshal parses a JSON byte slice into a Task.
func Unmarshal(data []byte) (*Task, error) {
	var t Task
	err := json.Unmarshal(data, &t)
	return &t, err
}
