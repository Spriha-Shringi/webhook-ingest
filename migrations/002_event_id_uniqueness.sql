-- event_id is stable across provider retries. The database must be the final
-- authority for deduplication because HTTP deliveries can race across workers.
ALTER TABLE events
    ADD CONSTRAINT events_event_id_key UNIQUE (event_id);
