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
	recordingWorkerCount   = 4
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
	recordingWake   chan struct{}
	stopRecordings  chan struct{}
	stopOnce        sync.Once
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
	// An unbuffered channel limits leased work to active workers. Ingestion only
	// signals recovery, so it remains fast even when recording work is busy.
	s.recordingJobs = make(chan string)
	s.recordingWake = make(chan struct{}, 1)
	s.stopRecordings = make(chan struct{})
	s.workers.Add(recordingWorkerCount + 1)
	for range recordingWorkerCount {
		go s.recordingWorker()
	}
	go s.recordingRecoveryLoop()
	return nil
}

// Stop stops leasing new recordings and drains active work until ctx expires.
// If the deadline wins, released leases make unfinished work retryable.
func (s *Service) Stop(ctx context.Context) error {
	if s.cancelRecording == nil {
		return nil
	}
	s.stopOnce.Do(func() { close(s.stopRecordings) })
	done := make(chan struct{})
	go func() {
		s.workers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.cancelRecording()
		<-done
		return ctx.Err()
	}
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
	changes, inserted, err := s.store.ProcessDelivery(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}
	for _, change := range changes {
		s.cache.Apply(change.AccountID, change.CallCountDelta, change.DurationSecDelta)
	}

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		select {
		case s.recordingWake <- struct{}{}:
		default:
			// A recovery pass is already scheduled.
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
	for callID := range s.recordingJobs {
		if s.recordingCtx.Err() != nil {
			_ = s.store.ReleaseRecordingLease(context.Background(), callID)
			continue
		}
		s.processOneRecording(callID)
	}
}

func (s *Service) recordingRecoveryLoop() {
	defer s.workers.Done()
	defer close(s.recordingJobs)
	ticker := time.NewTicker(recordingRetryInterval)
	defer ticker.Stop()

	for {
		claimed, keepRunning := s.claimAndQueueRecordings()
		if !keepRunning {
			return
		}
		if claimed == recordingWorkerCount {
			// All workers received work, so claim the next batch immediately.
			continue
		}
		select {
		case <-s.stopRecordings:
			return
		case <-s.recordingCtx.Done():
			return
		case <-s.recordingWake:
		case <-ticker.C:
		}
	}
}

// claimAndQueueRecordings leases a bounded batch and waits for workers to
// accept it, avoiding an unbounded in-memory backlog of externally visible work.
func (s *Service) claimAndQueueRecordings() (int, bool) {
	callIDs, err := s.store.ClaimPendingRecordingCallIDs(s.recordingCtx, recordingWorkerCount)
	if err != nil {
		if s.recordingCtx.Err() == nil {
			s.log.Error("claim pending recordings", "err", err)
		}
		return 0, s.recordingCtx.Err() == nil
	}
	for _, callID := range callIDs {
		select {
		case s.recordingJobs <- callID:
		case <-s.stopRecordings:
			_ = s.store.ReleaseRecordingLease(context.Background(), callID)
			return 0, false
		case <-s.recordingCtx.Done():
			_ = s.store.ReleaseRecordingLease(context.Background(), callID)
			return 0, false
		}
	}
	return len(callIDs), true
}

func (s *Service) processOneRecording(callID string) {
	if err := s.processRecording(s.recordingCtx, store.Event{CallID: callID}); err != nil {
		if releaseErr := s.store.ReleaseRecordingLease(context.Background(), callID); releaseErr != nil {
			s.log.Error("release recording lease", "call_id", callID, "err", releaseErr)
		}
		if s.recordingCtx.Err() == nil {
			s.log.Error("process recording", "call_id", callID, "err", err)
		}
	}
}
