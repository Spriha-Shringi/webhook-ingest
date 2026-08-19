package store_test

import (
	"context"
	"testing"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestInsertEventThenExists(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10, Payload: []byte(`{}`),
	}

	exists, err := s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if exists {
		t.Fatal("expected event to be absent before insert")
	}

	if err := s.InsertEvent(ctx, evt); err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}

	exists, err = s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected event to exist after insert")
	}
}

func TestInsertEventRejectsDuplicateEventID(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10, Payload: []byte(`{}`),
	}

	if err := s.InsertEvent(context.Background(), evt); err != nil {
		t.Fatalf("first InsertEvent: %v", err)
	}
	if err := s.InsertEvent(context.Background(), evt); err == nil {
		t.Fatal("duplicate event_id insert succeeded, want unique constraint error")
	}
}

func TestIncrementAccountStatsAccumulates(t *testing.T) {
	s := testutil.NewStore(t)
	_, _, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	if err := s.IncrementAccountStats(ctx, accountID, 30); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}
	if err := s.IncrementAccountStats(ctx, accountID, 12); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}

	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("got %+v, want CallCount=2 TotalDurationSec=42", got)
	}
}

func TestUpsertCallThenMarkRecordingProcessed(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10,
		RecordingURL: "https://example.com/a.wav", Payload: []byte(`{}`),
	}
	if err := s.UpsertCall(ctx, evt); err != nil {
		t.Fatalf("UpsertCall: %v", err)
	}
	if err := s.MarkRecordingProcessed(ctx, callID); err != nil {
		t.Fatalf("MarkRecordingProcessed: %v", err)
	}

	var processed bool
	row := s.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true")
	}
}

func TestClaimPendingRecordingCallIDsLeasesEachCallOnce(t *testing.T) {
	s := testutil.NewStore(t)
	_, firstCallID, accountID := testutil.IDs(t, s)
	secondCallID := firstCallID + "_second"
	ctx := context.Background()
	for _, callID := range []string{firstCallID, secondCallID} {
		if err := s.UpsertCall(ctx, store.Event{
			CallID: callID, AccountID: accountID, Status: "completed", DurationSec: 10,
			RecordingURL: "https://example.com/" + callID + ".wav",
		}); err != nil {
			t.Fatalf("seed %s: %v", callID, err)
		}
	}

	first, err := s.ClaimPendingRecordingCallIDs(ctx, 1)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim = %v, %v; want one call", first, err)
	}
	second, err := s.ClaimPendingRecordingCallIDs(ctx, 1)
	if err != nil || len(second) != 1 {
		t.Fatalf("second claim = %v, %v; want one different call", second, err)
	}
	if first[0] == second[0] {
		t.Fatalf("same call %q was leased twice", first[0])
	}
	third, err := s.ClaimPendingRecordingCallIDs(ctx, 1)
	if err != nil {
		t.Fatalf("third claim: %v", err)
	}
	if len(third) != 0 {
		t.Fatalf("third claim = %v, want no unleased calls", third)
	}
}
