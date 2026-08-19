-- event_id is stable across provider retries. The database must be the final
-- authority for deduplication because HTTP deliveries can race across workers.
-- Preserve duplicate audit records from an existing affected database instead
-- of deleting them. Only the oldest delivery remains active for deduplication.
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS deduplicated_at TIMESTAMPTZ;

WITH ranked_events AS (
    SELECT id,
           row_number() OVER (PARTITION BY event_id ORDER BY id) AS delivery_rank
    FROM events
    WHERE deduplicated_at IS NULL
)
UPDATE events
SET deduplicated_at = now()
FROM ranked_events
WHERE events.id = ranked_events.id
  AND ranked_events.delivery_rank > 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_active_event_id
    ON events (event_id)
    WHERE deduplicated_at IS NULL;
