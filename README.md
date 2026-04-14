# pgque

Go client for [pgque](https://github.com/NikolayS/pgque), a zero-bloat PostgreSQL message queue built on [PgQ](https://pgq.github.io/extension/pgq/files/external-sql.html).

## Prerequisites

pgque must be installed in your Postgres database. Follow the [upstream instructions](https://github.com/NikolayS/pgque?tab=readme-ov-file#installation), then create a queue and register at least one consumer:

```sql
SELECT pgque.create_queue('orders');
SELECT pgque.register_consumer('orders', 'processor');
```

## Install

```
go get github.com/dio/pgque
```

Requires Go 1.26+ and pgx v5.

## Quick start

```go
client, err := pgque.Connect(ctx, "postgres://user:pass@localhost/mydb")
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Publish
id, err := client.Send(ctx, "orders", pgque.Event{
    Type:    "order.created",
    Payload: map[string]any{"order_id": 42},
})

// Consume
c := client.NewConsumer("orders", "processor")
c.Handle("order.created", func(ctx context.Context, msg pgque.Message) error {
    var payload map[string]any
    json.Unmarshal([]byte(msg.Payload), &payload)
    fmt.Println("got order:", payload["order_id"])
    return nil
})
log.Fatal(c.Start(ctx))
```

## Consumer

`Consumer.Start` blocks until the context is cancelled. It opens a dedicated connection for `LISTEN/NOTIFY` so it wakes up as soon as `pgque.ticker()` fires, without constant polling. The fallback poll interval (default 30s) only kicks in when no notification arrives.

Returning an error from a handler nacks that message for redelivery. The batch is always acked when the handler loop finishes.

### Catch-all handler

Register `"*"` to handle event types that have no explicit handler:

```go
c.Handle("*", func(ctx context.Context, msg pgque.Message) error {
    log.Printf("unhandled type %s", msg.Type)
    return nil
})
```

### Options

| Option | Default | Description |
|---|---|---|
| `WithPollInterval(d)` | 30s | Fallback sleep when queue is empty and no NOTIFY arrives |
| `WithMaxMessages(n)` | 100 | Max messages per `Receive` call |
| `WithRetryAfter(secs)` | 60 | Delay before a nacked message is redelivered |

## Low-level API

`Send`, `Receive`, `Ack`, and `Nack` are also available directly if you prefer to manage the consume loop yourself.

```go
// Send returns the assigned event ID.
eid, err := client.Send(ctx, "orders", pgque.Event{...})

// Receive fetches the next batch (empty slice = no batch ready).
msgs, err := client.Receive(ctx, "orders", "processor", 100)

// Ack finishes the batch.
err = client.Ack(ctx, msgs[0].BatchID)

// Nack re-queues a single message for retry.
err = client.Nack(ctx, msgs[0].BatchID, msgs[0], 60, "transient error")
```

`Nack` per message, then `Ack` the batch when done.

## Testing

### Unit tests

```
go test -race ./...
```

### E2E tests

The e2e tests live in a separate Go module under `e2e/` so that
`embedded-postgres` and other test-only deps never appear in the main module's
dependency graph. A `go.work` file ties them together for local development.

The tests spin up an embedded Postgres instance (port 5454, no external
installation needed) and install the pgque schema into it at startup. The
schema is fetched from a pinned upstream commit and is not checked in:

```
make fetch-schema   # downloads e2e/testdata/pgque.sql
make test.e2e       # runs the e2e suite (implies fetch-schema)
```

If `e2e/testdata/pgque.sql` is missing, the suite exits 0 with a message
rather than failing, so CI without the fetch step does not break the build.

#### How the tests replace pg_cron

In production, `pgque.ticker()` is called by pg_cron on a schedule. In tests
there is no pg_cron, so each test calls `ticker()` directly after publishing
an event. Two queue config knobs make this reliable regardless of timing:

- `ticker_max_count=1` -- tick fires after a single new event (not a batch)
- `ticker_max_lag=0` -- tick fires even when lag is zero (no minimum wait)

These are set per queue in `setupQueue` and cleaned up after each test.

For retry flows (`TestNack`), the test calls `pgque.maint_retry_events()`
directly. That function moves events from the retry queue back into the event
table; it is intentionally separate from `pgque.maint()`, which only does
table rotation.

## License

Apache 2.0. See [LICENSE](LICENSE).
