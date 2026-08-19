# Solution

The duplicate-delivery check had a time-of-check/time-of-use race: concurrent
requests could all see that an `event_id` was absent, then each insert an event
and increment the account totals. There was also no unique database constraint
to stop that race, and the event, call, and aggregate writes were separate
operations, so a failure partway through could leave durable state inconsistent.
Recording processing inherited the HTTP request context, which is cancelled
after the response, discarded processing errors, and used only a goroutine;
that explains both silently unprocessed recordings and work disappearing during
deployments. Finally, cache writes were not synchronised and the in-memory
stats cache started empty after a restart despite Postgres having the totals.

I chose Postgres as the idempotency authority. A unique `events.event_id`
constraint, `INSERT ... ON CONFLICT DO NOTHING`, and a single transaction make
the event, call, and aggregate change atomic. A duplicate delivery is therefore
a successful no-op even when retries race across processes. I considered Redis
locks or TTL-based deduplication, but Redis eviction, expiry, and process
crashes make it unsuitable as the final correctness boundary. Redis can still
be useful as an optimisation, but Postgres remains the durable source of truth.

Recording recovery now scans persisted unfinished calls at startup and on a
bounded interval; the worker uses its own lifecycle context and logs failures.
At 10,000 webhooks per second, I would retain the database uniqueness
constraint and transactional write, then partition the events table, move
recording work to a durable outbox and horizontally scaled consumer fleet, and
replace the per-process cache with a distributed cache or durable read model.
