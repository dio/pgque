// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

// Package pgque is a Go client for PgQue — a zero-bloat PostgreSQL message
// queue built on top of PgQ.
//
// Quick start:
//
//	client, err := pgque.Connect(ctx, dsn)
//	// produce
//	id, err := client.Send(ctx, "orders", pgque.Event{Type: "order.created", Payload: order})
//	// consume (high-level)
//	c := client.NewConsumer("orders", "processor")
//	c.Handle("order.created", func(ctx context.Context, msg pgque.Message) error { ... })
//	c.Start(ctx)
package pgque

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Client is the main pgque client. It wraps a pgx connection pool and is
// safe for concurrent use.
type Client struct {
	pool *pgxpool.Pool
}

// Connect opens a new Client using the given PostgreSQL DSN.
// The caller must call Close when done.
func Connect(ctx context.Context, dsn string) (*Client, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgque: connect: %w", err)
	}
	return &Client{pool: pool}, nil
}

// Close releases all connections in the pool.
func (c *Client) Close() { c.pool.Close() }

// Pool returns the underlying pgxpool for direct SQL access (e.g. DDL like
// create_queue, register_consumer).
func (c *Client) Pool() *pgxpool.Pool { return c.pool }

// Send publishes an event to the named queue and returns the assigned event ID.
// ev.Type defaults to "default" when empty.
func (c *Client) Send(ctx context.Context, queue string, ev Event) (int64, error) {
	payload, err := json.Marshal(ev.Payload)
	if err != nil {
		return 0, fmt.Errorf("pgque: marshal payload: %w", err)
	}
	typ := ev.Type
	if typ == "" {
		typ = "default"
	}
	var eid int64
	err = c.pool.QueryRow(ctx,
		`SELECT pgque.send($1, $2, $3::jsonb)`, queue, typ, string(payload),
	).Scan(&eid)
	if err != nil {
		return 0, fmt.Errorf("pgque: send: %w", err)
	}
	return eid, nil
}

// Receive fetches up to maxMessages from the next available batch for the
// named consumer. Returns an empty slice when no batch is ready.
func (c *Client) Receive(ctx context.Context, queue, consumer string, maxMessages int) ([]Message, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT * FROM pgque.receive($1, $2, $3)`, queue, consumer, maxMessages)
	if err != nil {
		return nil, fmt.Errorf("pgque: receive: %w", err)
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		var createdAt time.Time
		if err := rows.Scan(
			&m.MsgID, &m.BatchID, &m.Type, &m.Payload,
			&m.RetryCount, &createdAt,
			&m.Extra1, &m.Extra2, &m.Extra3, &m.Extra4,
		); err != nil {
			return nil, fmt.Errorf("pgque: scan message: %w", err)
		}
		m.CreatedAt = createdAt
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgque: receive rows: %w", err)
	}
	return msgs, nil
}

// Ack finishes a batch, advancing the consumer's position in the queue.
// batchID comes from Message.BatchID.
func (c *Client) Ack(ctx context.Context, batchID int64) error {
	if _, err := c.pool.Exec(ctx, `SELECT pgque.ack($1)`, batchID); err != nil {
		return fmt.Errorf("pgque: ack batch %d: %w", batchID, err)
	}
	return nil
}

// Nack re-queues a single message for later redelivery. If the message has
// exceeded the queue's max_retries, it is routed to the dead-letter table
// instead. retryAfterSecs controls the delay before the next delivery attempt.
// reason is optional (pass "" for none).
//
// After nacking individual messages, call Ack to finish the batch.
func (c *Client) Nack(ctx context.Context, batchID int64, msg Message, retryAfterSecs int, reason string) error {
	interval := fmt.Sprintf("%d seconds", retryAfterSecs)
	var reasonArg *string
	if reason != "" {
		reasonArg = &reason
	}
	_, err := c.pool.Exec(ctx,
		`SELECT pgque.nack(
			$1,
			ROW($2,$3,$4,$5,$6,$7,$8,$9,$10,$11)::pgque.message,
			$12::interval,
			$13
		)`,
		batchID,
		msg.MsgID, msg.BatchID, msg.Type, msg.Payload, msg.RetryCount, msg.CreatedAt,
		msg.Extra1, msg.Extra2, msg.Extra3, msg.Extra4,
		interval,
		reasonArg,
	)
	if err != nil {
		return fmt.Errorf("pgque: nack msg %d: %w", msg.MsgID, err)
	}
	return nil
}

// NewConsumer creates a Consumer for the given queue and consumer name.
// Use the returned Consumer's Handle method to register handlers, then call
// Start to begin consuming.
func (c *Client) NewConsumer(queue, name string, opts ...Option) *Consumer {
	con := &Consumer{
		client:         c,
		queue:          queue,
		name:           name,
		pollInterval:   30 * time.Second,
		maxMessages:    100,
		retryAfterSecs: 60,
		handlers:       make(map[string]HandlerFunc),
	}
	for _, o := range opts {
		o(con)
	}
	return con
}
