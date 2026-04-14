// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

package pgque

import "time"

// Event is a message to publish to a queue.
// Payload is JSON-marshaled before being sent.
type Event struct {
	Type    string
	Payload any
}

// Message is a message received from a queue.
// Payload is the raw JSON string as stored in PostgreSQL.
type Message struct {
	MsgID      int64     `json:"msg_id"`
	BatchID    int64     `json:"batch_id"`
	Type       string    `json:"type"`
	Payload    string    `json:"payload"` // raw JSON
	RetryCount *int      `json:"retry_count"`
	CreatedAt  time.Time `json:"created_at"`
	Extra1     *string   `json:"extra1"`
	Extra2     *string   `json:"extra2"`
	Extra3     *string   `json:"extra3"`
	Extra4     *string   `json:"extra4"`
}
