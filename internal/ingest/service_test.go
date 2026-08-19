package ingest_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return eventJSONWithDuration(eventID, callID, accountID, 143)
}

func eventJSONWithDuration(eventID, callID, accountID string, durationSec int) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  %d,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, durationSec, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	var storedEvents int
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID).Scan(&storedEvents); err != nil {
		t.Fatalf("count stored events: %v", err)
	}
	if storedEvents != 1 {
		t.Fatalf("stored %d events, want 1", storedEvents)
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestConcurrentDuplicateDeliveriesAreCountedOnce(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()
	body := eventJSON(eventID, callID, accountID)

	const deliveries = 20
	var wg sync.WaitGroup
	errs := make(chan error, deliveries)
	for range deliveries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("got HTTP %d, want 200", resp.StatusCode)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	var events int
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Fatalf("stored %d copies of %s, want 1", events, eventID)
	}

	stats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if stats.CallCount != 1 || stats.TotalDurationSec != 143 {
		t.Fatalf("stats = %+v, want one 143-second call", stats)
	}
}

func TestCallCorrectionReplacesItsStatsContribution(t *testing.T) {
	srv, st := testutil.NewServer(t)
	firstEventID, callID, accountID := testutil.IDs(t, st)
	secondEventID := firstEventID + "_correction"

	if resp := post(t, srv.URL+"/webhooks/calls", eventJSONWithDuration(firstEventID, callID, accountID, 100)); resp.StatusCode != http.StatusOK {
		t.Fatalf("first delivery: got %d, want 200", resp.StatusCode)
	}
	if resp := post(t, srv.URL+"/webhooks/calls", eventJSONWithDuration(secondEventID, callID, accountID, 143)); resp.StatusCode != http.StatusOK {
		t.Fatalf("correction: got %d, want 200", resp.StatusCode)
	}

	var calls int
	if err := st.Pool().QueryRow(context.Background(), `SELECT count(*) FROM calls WHERE call_id = $1`, callID).Scan(&calls); err != nil {
		t.Fatalf("count calls: %v", err)
	}
	if calls != 1 {
		t.Fatalf("stored %d calls, want 1", calls)
	}

	got, err := st.AccountStats(context.Background(), accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 143 {
		t.Fatalf("stats = %+v, want one 143-second call", got)
	}
}

func TestRecordingIsProcessedAfterWebhookAcknowledgement(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	if resp := post(t, srv.URL+"/webhooks/calls", eventJSON(eventID, callID, accountID)); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var processed bool
		err := st.Pool().QueryRow(context.Background(),
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID).Scan(&processed)
		if err == nil && processed {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("recording was not marked processed after the webhook response")
}

func TestServiceRecoversRecordingLeftUnprocessedBeforeStartup(t *testing.T) {
	st := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	if _, inserted, err := st.ProcessDelivery(context.Background(), store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID, Status: "completed", DurationSec: 143,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav", Payload: []byte(`{}`),
	}); err != nil || !inserted {
		t.Fatalf("seed unfinished recording: inserted:%t err:%v", inserted, err)
	}

	cfg := config.Load()
	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	svc := ingest.New(st, stats.NewCache(), rdb, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("start service: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var processed bool
		err := st.Pool().QueryRow(context.Background(),
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID).Scan(&processed)
		if err == nil && processed {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("recording left by the previous process was not recovered")
}

func TestServiceProcessesRecordingsWithBoundedWorkerPool(t *testing.T) {
	st := testutil.NewStore(t)
	_, firstCallID, accountID := testutil.IDs(t, st)
	const recordings = 8
	for i := range recordings {
		callID := fmt.Sprintf("%s_%d", firstCallID, i)
		if _, inserted, err := st.ProcessDelivery(context.Background(), store.Event{
			EventID: "evt_" + callID, CallID: callID, AccountID: accountID, Status: "completed", DurationSec: 1,
			RecordingURL: "https://recordings.example.com/" + callID + ".wav", Payload: []byte(`{}`),
		}); err != nil || !inserted {
			t.Fatalf("seed recording %d: inserted:%t err:%v", i, inserted, err)
		}
	}

	cfg := config.Load()
	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	svc := ingest.New(st, stats.NewCache(), rdb, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("start service: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })

	deadline := time.Now().Add(750 * time.Millisecond)
	for time.Now().Before(deadline) {
		var processed int
		err := st.Pool().QueryRow(context.Background(),
			`SELECT count(*) FROM calls WHERE account_id = $1 AND recording_processed = TRUE`, accountID).Scan(&processed)
		if err == nil && processed == recordings {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("worker pool did not finish %d recordings before deadline", recordings)
}

func TestServiceLoadsDurableStatsAtStartup(t *testing.T) {
	st := testutil.NewStore(t)
	_, _, accountID := testutil.IDs(t, st)
	if _, inserted, err := st.ProcessDelivery(context.Background(), store.Event{
		EventID: "evt_" + accountID, CallID: "call_" + accountID, AccountID: accountID,
		Status: "completed", DurationSec: 42, Payload: []byte(`{}`),
	}); err != nil || !inserted {
		t.Fatalf("seed stats: inserted:%t err:%v", inserted, err)
	}

	cfg := config.Load()
	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	svc := ingest.New(st, stats.NewCache(), rdb, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("start service: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })

	got := svc.Stats(accountID)
	if got.CallCount != 1 || got.TotalDurationSec != 42 {
		t.Fatalf("stats = %+v, want one 42-second call", got)
	}
}
