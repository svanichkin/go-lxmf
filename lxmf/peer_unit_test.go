package lxmf

import "testing"

func TestPeerQueueProcessing(t *testing.T) {
	router := &LXMRouter{
		PropagationEntries: map[string]*propagationEntry{},
	}

	transientID := []byte("message-1")
	entry := &propagationEntry{
		DestinationHash: []byte("dest"),
		HandledPeers:    []string{},
		UnhandledPeers:  []string{string([]byte("peerhash"))},
	}
	router.PropagationEntries[string(transientID)] = entry

	peer := NewLXMPeer(router, []byte("peerhash"), PeerDefaultSyncStrategy)

	peer.QueueHandledMessage(transientID)
	peer.ProcessQueues()

	if !containsString(entry.HandledPeers, string(peer.DestinationHash)) {
		t.Fatalf("expected handled peer entry to be added")
	}
	if containsString(entry.UnhandledPeers, string(peer.DestinationHash)) {
		t.Fatalf("expected unhandled peer entry to be removed")
	}
}

func TestPeerAcceptanceRateAndCounts(t *testing.T) {
	router := &LXMRouter{
		PropagationEntries: map[string]*propagationEntry{},
	}
	transientID := []byte("message-2")
	entry := &propagationEntry{
		DestinationHash: []byte("dest"),
		HandledPeers:    []string{},
		UnhandledPeers:  []string{},
	}
	router.PropagationEntries[string(transientID)] = entry

	peer := NewLXMPeer(router, []byte("peerhash2"), PeerDefaultSyncStrategy)
	peer.AddUnhandledMessage(transientID)
	peer.Offered = 10
	peer.Outgoing = 5

	if count := peer.UnhandledMessageCount(); count != 1 {
		t.Fatalf("expected 1 unhandled message, got %d", count)
	}
	if rate := peer.AcceptanceRate(); rate != 0.5 {
		t.Fatalf("expected acceptance rate 0.5, got %f", rate)
	}
}
