-- The recording worker scans only unfinished calls. Keep restart recovery from
-- becoming a full-table scan as the calls table grows.
CREATE INDEX IF NOT EXISTS idx_calls_pending_recordings
    ON calls (updated_at)
    WHERE recording_processed = FALSE AND recording_url IS NOT NULL;
