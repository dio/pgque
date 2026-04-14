// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

package pgque

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// HandlerFunc processes a single message. Return a non-nil error to nack the
// message; return nil to indicate successful processing.
type HandlerFunc func(ctx context.Context, msg Message) error

// Consumer polls a pgque queue and dispatches messages to registered
// handlers. It uses LISTEN/NOTIFY to wake up immediately when new events
// are ticked, falling back to a configurable poll interval when no
// notification arrives.
type Consumer struct {
	client         *Client
	queue          string
	name           string
	pollInterval   time.Duration
	maxMessages    int
	retryAfterSecs int
	handlers       map[string]HandlerFunc
	catchAll       HandlerFunc
}

// Handle registers a handler for the given event type.
// Use "*" to register a catch-all handler for any unregistered type.
func (c *Consumer) Handle(eventType string, fn HandlerFunc) {
	if eventType == "*" {
		c.catchAll = fn
	} else {
		c.handlers[eventType] = fn
	}
}

// Start begins the consume loop, blocking until ctx is cancelled.
// It opens a dedicated connection for LISTEN/NOTIFY (separate from the pool).
// Callers that need signal-based shutdown should pass a context created with
// signal.NotifyContext.
func (c *Consumer) Start(ctx context.Context) error {
	listenConn, err := pgx.ConnectConfig(ctx, c.client.pool.Config().ConnConfig)
	if err != nil {
		return fmt.Errorf("pgque: open listen conn: %w", err)
	}
	defer listenConn.Close(context.Background()) //nolint:contextcheck

	channel := pgx.Identifier{"pgque_" + c.queue}.Sanitize()
	if _, err := listenConn.Exec(ctx, "LISTEN "+channel); err != nil {
		return fmt.Errorf("pgque: LISTEN %s: %w", channel, err)
	}
	slog.InfoContext(ctx, "pgque: consumer started",
		"queue", c.queue, "consumer", c.name,
		"poll_interval", c.pollInterval,
	)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		gotMsgs, err := c.pollOnce(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "pgque: poll error", "queue", c.queue, "error", err)
			// Wait before retrying to avoid tight error loops.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.pollInterval):
			}
			continue
		}

		if !gotMsgs {
			// Block until Postgres notifies us (ticker fired) or the
			// fallback interval elapses — whichever comes first.
			waitCtx, cancel := context.WithTimeout(ctx, c.pollInterval)
			_, _ = listenConn.WaitForNotification(waitCtx)
			cancel()
		}
		// Got messages — loop immediately, there may be more batches.
	}
}

// pollOnce fetches one batch and dispatches all messages.
// Returns (true, nil) on a non-empty batch, (false, nil) when the queue is
// empty, or (false, err) on a hard error.
func (c *Consumer) pollOnce(ctx context.Context) (bool, error) {
	msgs, err := c.client.Receive(ctx, c.queue, c.name, c.maxMessages)
	if err != nil {
		return false, err
	}
	if len(msgs) == 0 {
		return false, nil
	}

	batchID := msgs[0].BatchID
	slog.DebugContext(ctx, "pgque: batch received",
		"queue", c.queue, "batch_id", batchID, "count", len(msgs),
	)

	for _, msg := range msgs {
		handler := c.handlers[msg.Type]
		if handler == nil {
			handler = c.catchAll
		}
		if handler == nil {
			slog.WarnContext(ctx, "pgque: no handler registered",
				"type", msg.Type, "msg_id", msg.MsgID,
			)
			continue
		}

		if herr := handler(ctx, msg); herr != nil {
			slog.ErrorContext(ctx, "pgque: handler error",
				"type", msg.Type, "msg_id", msg.MsgID, "error", herr,
			)
			if nerr := c.client.Nack(ctx, batchID, msg, c.retryAfterSecs, herr.Error()); nerr != nil {
				slog.ErrorContext(ctx, "pgque: nack failed",
					"msg_id", msg.MsgID, "error", nerr,
				)
			}
		}
	}

	if err := c.client.Ack(ctx, batchID); err != nil {
		return true, err
	}
	return true, nil
}
