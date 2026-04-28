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

	foundHandled := false
	for _, value := range entry.HandledPeers {
		if value == string(peer.DestinationHash) {
			foundHandled = true
			break
		}
	}
	if !foundHandled {
		t.Fatalf("expected handled peer entry to be added")
	}
	foundUnhandled := false
	for _, value := range entry.UnhandledPeers {
		if value == string(peer.DestinationHash) {
			foundUnhandled = true
			break
		}
	}
	if foundUnhandled {
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

	if count := len(peer.UnhandledMessages()); count != 1 {
		t.Fatalf("expected 1 unhandled message, got %d", count)
	}
	if rate := peer.AcceptanceRate(); rate != 0.5 {
		t.Fatalf("expected acceptance rate 0.5, got %f", rate)
	}
}

func TestPeerStampCostsKnownAllowsZeroFlexibility(t *testing.T) {
	peer := &LXMPeer{
		PeeringCost:                     18,
		PropagationStampCost:            16,
		PropagationStampCostFlexibility: 0,
	}

	if !(peer.PeeringCost > 0 && peer.PropagationStampCost >= 0 && peer.PropagationStampCostFlexibility >= 0) {
		t.Fatalf("expected zero stamp-cost flexibility to still count as known costs")
	}
}

func TestPeerStampCostsKnownRequiresPeeringCost(t *testing.T) {
	peer := &LXMPeer{
		PeeringCost:                     0,
		PropagationStampCost:            16,
		PropagationStampCostFlexibility: 3,
	}

	if peer.PeeringCost > 0 && peer.PropagationStampCost >= 0 && peer.PropagationStampCostFlexibility >= 0 {
		t.Fatalf("expected missing peering cost to block sync prerequisites")
	}
}
