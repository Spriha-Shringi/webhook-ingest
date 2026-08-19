-- A lease prevents multiple service replicas from processing the same
-- unfinished recording at once. Stale leases are reclaimed by the worker.
ALTER TABLE calls
    ADD COLUMN IF NOT EXISTS recording_locked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_calls_recording_claim
    ON calls (recording_locked_at, updated_at)
    WHERE recording_processed = FALSE AND recording_url IS NOT NULL;
