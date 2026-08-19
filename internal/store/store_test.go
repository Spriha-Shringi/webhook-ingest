package store_test

import (
	"context"
	"testing"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestProcessDeliveryStoresEventAndCall(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10, Payload: []byte(`{}`),
	}

	if _, inserted, err := s.ProcessDelivery(ctx, evt); err != nil || !inserted {
		t.Fatalf("ProcessDelivery = inserted:%t err:%v, want inserted", inserted, err)
	}
}

func TestProcessDeliveryIgnoresDuplicateEventID(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10, Payload: []byte(`{}`),
	}

	if _, inserted, err := s.ProcessDelivery(context.Background(), evt); err != nil || !inserted {
		t.Fatalf("first ProcessDelivery = inserted:%t err:%v", inserted, err)
	}
	if _, inserted, err := s.ProcessDelivery(context.Background(), evt); err != nil || inserted {
		t.Fatalf("duplicate ProcessDelivery = inserted:%t err:%v, want no-op", inserted, err)
	}
}

func TestProcessDeliveryUpdatesAccountStats(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	if _, inserted, err := s.ProcessDelivery(ctx, store.Event{EventID: eventID, CallID: callID, AccountID: accountID, Status: "completed", DurationSec: 42, Payload: []byte(`{}`)}); err != nil || !inserted {
		t.Fatalf("ProcessDelivery = inserted:%t err:%v", inserted, err)
	}

	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 42 {
		t.Fatalf("got %+v, want CallCount=1 TotalDurationSec=42", got)
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
	if _, inserted, err := s.ProcessDelivery(ctx, evt); err != nil || !inserted {
		t.Fatalf("ProcessDelivery = inserted:%t err:%v", inserted, err)
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
		if _, inserted, err := s.ProcessDelivery(ctx, store.Event{
			EventID: "evt_" + callID, CallID: callID, AccountID: accountID, Status: "completed", DurationSec: 10,
			RecordingURL: "https://example.com/" + callID + ".wav", Payload: []byte(`{}`),
		}); err != nil || !inserted {
			t.Fatalf("seed %s: inserted:%t err:%v", callID, inserted, err)
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
