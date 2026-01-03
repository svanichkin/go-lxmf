package lxmf

import "testing"

func TestPeerRoundTripSerialization(t *testing.T) {
	router := &LXMRouter{
		PropagationEntries: map[string]*propagationEntry{},
	}

	handledID := []byte("handled-id")
	unhandledID := []byte("unhandled-id")
	router.PropagationEntries[string(handledID)] = &propagationEntry{HandledPeers: []string{}, UnhandledPeers: []string{}}
	router.PropagationEntries[string(unhandledID)] = &propagationEntry{HandledPeers: []string{}, UnhandledPeers: []string{}}

	peer := NewLXMPeer(router, []byte("peerhash3"), PeerDefaultSyncStrategy)
	peer.Alive = true
	peer.LastHeard = 123.4
	peer.PeeringTimebase = 456.7
	peer.PropagationTransferLimit = 12
	peer.PropagationSyncLimit = 34
	peer.PropagationStampCost = 10
	peer.PropagationStampCostFlexibility = 2
	peer.PeeringCost = 18
	peer.SyncStrategy = PeerStrategyPersistent
	peer.Offered = 4
	peer.Outgoing = 2
	peer.Incoming = 3
	peer.RxBytes = 10
	peer.TxBytes = 11
	peer.LastSyncAttempt = 987.6
	peer.PeeringKey = []any{[]byte("key"), 3}
	peer.Metadata = map[any]any{"name": "peer"}

	peer.AddHandledMessage(handledID)
	peer.AddUnhandledMessage(unhandledID)

	raw, err := peer.ToBytes()
	if err != nil {
		t.Fatalf("to bytes: %v", err)
	}

	roundTrip, err := LXMPeerFromBytes(raw, router)
	if err != nil {
		t.Fatalf("from bytes: %v", err)
	}

	if roundTrip.UnhandledMessageCount() != 1 {
		t.Fatalf("expected 1 unhandled message, got %d", roundTrip.UnhandledMessageCount())
	}
	if roundTrip.HandledMessageCount() != 1 {
		t.Fatalf("expected 1 handled message, got %d", roundTrip.HandledMessageCount())
	}
	if rate := roundTrip.AcceptanceRate(); rate != 0.5 {
		t.Fatalf("expected acceptance rate 0.5, got %f", rate)
	}
}

