// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

package pgque

import "time"

// Option configures a Consumer.
type Option func(*Consumer)

// WithPollInterval sets the fallback sleep duration between poll cycles
// when the queue is empty and no NOTIFY arrives. Defaults to 30s.
func WithPollInterval(d time.Duration) Option {
	return func(c *Consumer) { c.pollInterval = d }
}

// WithMaxMessages sets the maximum number of messages fetched per Receive
// call. Defaults to 100.
func WithMaxMessages(n int) Option {
	return func(c *Consumer) { c.maxMessages = n }
}

// WithRetryAfter sets the number of seconds before a nacked message is
// redelivered. Defaults to 60.
func WithRetryAfter(secs int) Option {
	return func(c *Consumer) { c.retryAfterSecs = secs }
}
