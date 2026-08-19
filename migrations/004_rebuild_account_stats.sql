-- Earlier versions counted deliveries instead of the current state of calls.
-- Rebuild the durable aggregate once so existing accounts stop reporting drift.
UPDATE account_stats
SET call_count = 0,
    total_duration_sec = 0;

INSERT INTO account_stats (account_id, call_count, total_duration_sec)
SELECT account_id, count(*), coalesce(sum(duration_sec), 0)
FROM calls
GROUP BY account_id
ON CONFLICT (account_id) DO UPDATE SET
    call_count = EXCLUDED.call_count,
    total_duration_sec = EXCLUDED.total_duration_sec;
