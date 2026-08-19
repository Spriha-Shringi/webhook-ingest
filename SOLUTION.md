# Solution

## What was broken

The duplicate check was a read-then-write race: concurrent retries could all
observe a missing `event_id` and each update the aggregate. Even distinct events
for a status or duration correction of the same `call_id` were counted as new
calls, so `call_count` and total duration drifted from the current calls table.
The fix uses one Postgres transaction: it inserts a delivery only once, locks
the call while replacing its current state, and applies the resulting delta to
the affected account totals. Existing affected databases retain duplicate raw
events for audit, mark surplus copies inactive, and rebuild the derived totals
from `calls` before enforcing active-event uniqueness.

Recording work used the request context after acknowledgement, ignored errors,
and existed only in a goroutine. That caused silent failures and loss of
in-flight work on deployment. Recording claims are now durable five-minute
leases, acquired with `FOR UPDATE SKIP LOCKED`; replicas therefore do not work
the same call. Four bounded workers process leased work concurrently, the
recovery loop resumes unfinished work after restart, and shutdown stops new
claims then drains active work until its deadline. The cache also lacked a
write lock and started empty after restart; it is now synchronized and hydrated
from durable `account_stats` at startup.

## Why Postgres owns deduplication

Postgres is already the durable source of truth. Its partial unique index and
`INSERT ... ON CONFLICT DO NOTHING` remain correct through races, crashes, and
multiple application replicas; the surrounding transaction keeps delivery,
call, and aggregate changes consistent. Redis locks or TTL keys could improve
throughput as an optional fast path, but eviction, expiration, and failure
between Redis and Postgres make them insufficient as the final correctness
boundary.

## At 10,000 webhooks/second

The current four-worker pool is deliberately bounded rather than unbounded; it
is a safe baseline, not a 10,000/sec recording processor. At that scale I would
keep the database idempotency constraint and transaction, partition the event
table, and publish recording jobs through a transactional outbox to a durable
queue. Horizontally scaled consumer fleets would then control recording
concurrency independently of HTTP ingestion. I would also use a distributed
stats read model or cache, shard hot aggregate writes, enforce backpressure,
and instrument queue depth, lease age, duplicate rate, and failed retries.
