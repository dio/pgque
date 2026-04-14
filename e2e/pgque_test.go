// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

package e2e_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pgque "github.com/dio/pgque"
)

// setupQueue creates a queue + consumer and registers cleanup.
func setupQueue(t *testing.T, client *pgque.Client, queue, consumer string) {
	t.Helper()
	ctx := context.Background()
	if _, err := client.Pool().Exec(ctx, "SELECT pgque.create_queue($1)", queue); err != nil {
		t.Fatalf("create_queue: %v", err)
	}
	if _, err := client.Pool().Exec(ctx, "SELECT pgque.register_consumer($1, $2)", queue, consumer); err != nil {
		t.Fatalf("register_consumer: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		client.Pool().Exec(ctx, "SELECT pgque.unregister_consumer($1, $2)", queue, consumer) //nolint:errcheck
		client.Pool().Exec(ctx, "SELECT pgque.drop_queue($1)", queue)                        //nolint:errcheck
	})
}

// ticker manually advances the queue (replaces pg_cron in tests).
func ticker(t *testing.T, client *pgque.Client) {
	t.Helper()
	if _, err := client.Pool().Exec(context.Background(), "SELECT pgque.ticker()"); err != nil {
		t.Fatalf("ticker: %v", err)
	}
}

func newClient(t *testing.T) *pgque.Client {
	t.Helper()
	client, err := pgque.Connect(context.Background(), testDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func TestSend(t *testing.T) {
	client := newClient(t)
	setupQueue(t, client, "test_send", "c1")

	eid, err := client.Send(context.Background(), "test_send", pgque.Event{
		Type:    "order.created",
		Payload: map[string]any{"order_id": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if eid == 0 {
		t.Fatal("expected non-zero event ID")
	}
}

func TestSendDefaultType(t *testing.T) {
	client := newClient(t)
	setupQueue(t, client, "test_default_type", "c1")

	eid, err := client.Send(context.Background(), "test_default_type", pgque.Event{
		Payload: map[string]any{"x": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if eid == 0 {
		t.Fatal("expected non-zero event ID")
	}
}

func TestSendReceiveAck(t *testing.T) {
	client := newClient(t)
	setupQueue(t, client, "test_sra", "c1")
	ctx := context.Background()

	_, err := client.Send(ctx, "test_sra", pgque.Event{
		Type:    "ping",
		Payload: map[string]any{"val": 42},
	})
	if err != nil {
		t.Fatal(err)
	}
	ticker(t, client)

	msgs, err := client.Receive(ctx, "test_sra", "c1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("want 1 message, got %d", len(msgs))
	}

	msg := msgs[0]
	if msg.Type != "ping" {
		t.Fatalf("want type=ping, got %q", msg.Type)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["val"] != float64(42) {
		t.Fatalf("want val=42, got %v", payload["val"])
	}

	if err := client.Ack(ctx, msg.BatchID); err != nil {
		t.Fatal(err)
	}

	// Queue should be empty after ack.
	ticker(t, client)
	msgs2, err := client.Receive(ctx, "test_sra", "c1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs2) != 0 {
		t.Fatalf("want empty queue after ack, got %d messages", len(msgs2))
	}
}

func TestNack(t *testing.T) {
	client := newClient(t)
	setupQueue(t, client, "test_nack", "c1")
	ctx := context.Background()

	_, err := client.Send(ctx, "test_nack", pgque.Event{
		Type:    "job",
		Payload: map[string]any{"attempt": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	ticker(t, client)

	msgs, err := client.Receive(ctx, "test_nack", "c1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("want 1 message, got %d", len(msgs))
	}

	// Nack the message with a 0-second retry delay so it's immediately eligible.
	if err := client.Nack(ctx, msgs[0].BatchID, msgs[0], 0, "simulated failure"); err != nil {
		t.Fatal(err)
	}
	// Still ack the batch to advance the consumer.
	if err := client.Ack(ctx, msgs[0].BatchID); err != nil {
		t.Fatal(err)
	}

	// Run maintenance to move the retry back into the event table.
	if _, err := client.Pool().Exec(ctx, "SELECT pgque.maint()"); err != nil {
		t.Fatalf("maint: %v", err)
	}
	ticker(t, client)

	retried, err := client.Receive(ctx, "test_nack", "c1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(retried) != 1 {
		t.Fatalf("want 1 retried message, got %d", len(retried))
	}
	if retried[0].RetryCount == nil || *retried[0].RetryCount < 1 {
		t.Fatalf("want retry_count >= 1, got %v", retried[0].RetryCount)
	}

	if err := client.Ack(ctx, retried[0].BatchID); err != nil {
		t.Fatal(err)
	}
}

func TestConsumerDispatch(t *testing.T) {
	client := newClient(t)
	setupQueue(t, client, "test_dispatch", "c1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.Send(ctx, "test_dispatch", pgque.Event{
		Type:    "greet",
		Payload: map[string]any{"name": "pgque"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ticker(t, client)

	received := make(chan pgque.Message, 1)
	c := client.NewConsumer("test_dispatch", "c1",
		pgque.WithPollInterval(100*time.Millisecond),
	)
	c.Handle("greet", func(_ context.Context, msg pgque.Message) error {
		received <- msg
		return nil
	})
	go c.Start(ctx) //nolint:errcheck

	select {
	case msg := <-received:
		if msg.Type != "greet" {
			t.Fatalf("want type=greet, got %q", msg.Type)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestConsumerCatchAll(t *testing.T) {
	client := newClient(t)
	setupQueue(t, client, "test_catchall", "c1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.Send(ctx, "test_catchall", pgque.Event{
		Type:    "unknown.type",
		Payload: map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	ticker(t, client)

	received := make(chan pgque.Message, 1)
	c := client.NewConsumer("test_catchall", "c1",
		pgque.WithPollInterval(100*time.Millisecond),
	)
	c.Handle("*", func(_ context.Context, msg pgque.Message) error {
		received <- msg
		return nil
	})
	go c.Start(ctx) //nolint:errcheck

	select {
	case msg := <-received:
		if msg.Type != "unknown.type" {
			t.Fatalf("want type=unknown.type, got %q", msg.Type)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for catch-all message")
	}
}
