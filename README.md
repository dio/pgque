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

## License

Apache 2.0. See [LICENSE](LICENSE).
