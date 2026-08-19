// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const (
	recordingWork          = 50 * time.Millisecond
	recordingRetryInterval = time.Second
	recordingQueueSize     = 100
)

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	recordingCtx    context.Context
	cancelRecording context.CancelFunc
	recordingJobs   chan string
	workers         sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Start begins the durable recording worker. It is called after the service is
// fully wired so a process restart immediately resumes unfinished work.
func (s *Service) Start(ctx context.Context) error {
	durable, err := s.store.AllAccountStats(ctx)
	if err != nil {
		return err
	}
	totals := make(map[string]stats.AccountStats, len(durable))
	for accountID, total := range durable {
		totals[accountID] = stats.AccountStats{
			CallCount:        total.CallCount,
			TotalDurationSec: total.TotalDurationSec,
		}
	}
	s.cache.Replace(totals)

	s.recordingCtx, s.cancelRecording = context.WithCancel(context.Background())
	s.recordingJobs = make(chan string, recordingQueueSize)
	s.workers.Add(1)
	go s.recordingWorker()
	return nil
}

// Stop asks the recording worker to finish promptly. Calls not completed by
// then remain marked unprocessed and will be recovered by the next process.
func (s *Service) Stop() {
	if s.cancelRecording == nil {
		return
	}
	s.cancelRecording()
	s.workers.Wait()
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}
	inserted, err := s.store.ProcessDelivery(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		select {
		case s.recordingJobs <- rec.CallID:
		default:
			// The durable scan will recover work if the in-memory queue is full.
			s.log.Warn("recording queue full; will retry from database", "call_id", rec.CallID)
		}
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	select {
	case <-time.After(recordingWork):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}

func (s *Service) recordingWorker() {
	defer s.workers.Done()
	ticker := time.NewTicker(recordingRetryInterval)
	defer ticker.Stop()

	s.processPendingRecordings()
	for {
		select {
		case <-s.recordingCtx.Done():
			return
		case callID := <-s.recordingJobs:
			s.processOneRecording(callID)
		case <-ticker.C:
			s.processPendingRecordings()
		}
	}
}

func (s *Service) processPendingRecordings() {
	callIDs, err := s.store.PendingRecordingCallIDs(s.recordingCtx, recordingQueueSize)
	if err != nil {
		if s.recordingCtx.Err() == nil {
			s.log.Error("list pending recordings", "err", err)
		}
		return
	}
	for _, callID := range callIDs {
		s.processOneRecording(callID)
	}
}

func (s *Service) processOneRecording(callID string) {
	if err := s.processRecording(s.recordingCtx, store.Event{CallID: callID}); err != nil && s.recordingCtx.Err() == nil {
		s.log.Error("process recording", "call_id", callID, "err", err)
	}
}
