// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

package e2e_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pgque "github.com/dio/pgque"
)

// setupQueue creates a queue + consumer and registers cleanup.
// ticker_max_count=1 and ticker_max_lag=0 ensure ticker() always fires
// immediately in tests regardless of how quickly successive calls are made.
func setupQueue(t *testing.T, client *pgque.Client, queue, consumer string) {
	t.Helper()
	ctx := context.Background()
	_, err := client.Pool().Exec(ctx, "SELECT pgque.create_queue($1)", queue)
	require.NoError(t, err, "create_queue")

	_, err = client.Pool().Exec(ctx, "SELECT pgque.register_consumer($1, $2)", queue, consumer)
	require.NoError(t, err, "register_consumer")

	_, err = client.Pool().Exec(ctx, "SELECT pgque.set_queue_config($1, 'ticker_max_count', '1')", queue)
	require.NoError(t, err, "set ticker_max_count")

	_, err = client.Pool().Exec(ctx, "SELECT pgque.set_queue_config($1, 'ticker_max_lag', '0')", queue)
	require.NoError(t, err, "set ticker_max_lag")

	t.Cleanup(func() {
		ctx := context.Background()
		client.Pool().Exec(ctx, "SELECT pgque.unregister_consumer($1, $2)", queue, consumer) //nolint:errcheck
		client.Pool().Exec(ctx, "SELECT pgque.drop_queue($1)", queue)                        //nolint:errcheck
	})
}

// ticker manually advances the queue (replaces pg_cron in tests).
func ticker(t *testing.T, client *pgque.Client) {
	t.Helper()
	_, err := client.Pool().Exec(context.Background(), "SELECT pgque.ticker()")
	require.NoError(t, err, "ticker")
}

func newClient(t *testing.T) *pgque.Client {
	t.Helper()
	client, err := pgque.Connect(context.Background(), testDSN)
	require.NoError(t, err, "connect")
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
	require.NoError(t, err)
	require.NotZero(t, eid, "expected non-zero event ID")
}

func TestSendDefaultType(t *testing.T) {
	client := newClient(t)
	setupQueue(t, client, "test_default_type", "c1")

	eid, err := client.Send(context.Background(), "test_default_type", pgque.Event{
		Payload: map[string]any{"x": 1},
	})
	require.NoError(t, err)
	require.NotZero(t, eid)
}

func TestSendReceiveAck(t *testing.T) {
	client := newClient(t)
	setupQueue(t, client, "test_sra", "c1")
	ctx := context.Background()

	_, err := client.Send(ctx, "test_sra", pgque.Event{
		Type:    "ping",
		Payload: map[string]any{"val": 42},
	})
	require.NoError(t, err)
	ticker(t, client)

	msgs, err := client.Receive(ctx, "test_sra", "c1", 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "ping", msgs[0].Type)

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(msgs[0].Payload), &payload))
	require.Equal(t, float64(42), payload["val"])

	require.NoError(t, client.Ack(ctx, msgs[0].BatchID))

	// Queue should be empty after ack.
	ticker(t, client)
	msgs2, err := client.Receive(ctx, "test_sra", "c1", 10)
	require.NoError(t, err)
	require.Empty(t, msgs2)
}

func TestNack(t *testing.T) {
	client := newClient(t)
	setupQueue(t, client, "test_nack", "c1")
	ctx := context.Background()

	_, err := client.Send(ctx, "test_nack", pgque.Event{
		Type:    "job",
		Payload: map[string]any{"attempt": 1},
	})
	require.NoError(t, err)
	ticker(t, client)

	msgs, err := client.Receive(ctx, "test_nack", "c1", 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	require.NoError(t, client.Nack(ctx, msgs[0].BatchID, msgs[0], 0, "simulated failure"))
	require.NoError(t, client.Ack(ctx, msgs[0].BatchID))

	// maint_retry_events moves the event from retry_queue back into the event
	// table. Note: pgque.maint() does NOT call maint_retry_events — that
	// function is invoked separately (e.g. by the maint job via pg_cron).
	_, err = client.Pool().Exec(ctx, "SELECT pgque.maint_retry_events()")
	require.NoError(t, err)

	// Ticker may throttle — poll until the retried message appears.
	require.Eventually(t, func() bool {
		ticker(t, client)
		retried, err := client.Receive(ctx, "test_nack", "c1", 10)
		require.NoError(t, err)
		if len(retried) == 0 {
			return false
		}
		require.NotNil(t, retried[0].RetryCount)
		require.GreaterOrEqual(t, *retried[0].RetryCount, 1)
		require.NoError(t, client.Ack(ctx, retried[0].BatchID))
		return true
	}, 5*time.Second, 100*time.Millisecond)
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
	require.NoError(t, err)
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
		require.Equal(t, "greet", msg.Type)
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
	require.NoError(t, err)
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
		require.Equal(t, "unknown.type", msg.Type)
	case <-ctx.Done():
		t.Fatal("timeout waiting for catch-all message")
	}
}
