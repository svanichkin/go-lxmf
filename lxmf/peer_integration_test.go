package lxmf

import (
	"testing"

	"github.com/svanichkin/go-reticulum/rns"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

func restorePeerFromBytes(peerBytes []byte, router *LXMRouter) (*LXMPeer, error) {
	var dict map[any]any
	if err := umsgpack.Unpackb(peerBytes, &dict); err != nil {
		return nil, err
	}
	rawHash := []byte(nil)
	switch v := dict["destination_hash"].(type) {
	case []byte:
		rawHash = append([]byte(nil), v...)
	case string:
		rawHash = []byte(v)
	}
	peer := NewLXMPeer(router, rawHash, PeerDefaultSyncStrategy)

	peer.PeeringTimebase = func() float64 {
		switch t := dict["peering_timebase"].(type) {
		case float64:
			return t
		case int:
			return float64(t)
		case int64:
			return float64(t)
		case uint64:
			return float64(t)
		default:
			return 0
		}
	}()
	if alive, ok := dict["alive"].(bool); ok {
		peer.Alive = alive
	}
	peer.LastHeard = func() float64 {
		switch t := dict["last_heard"].(type) {
		case float64:
			return t
		case int:
			return float64(t)
		case int64:
			return float64(t)
		case uint64:
			return float64(t)
		default:
			return 0
		}
	}()
	peer.LinkEstablishmentRate = func() float64 {
		switch t := dict["link_establishment_rate"].(type) {
		case float64:
			return t
		case int:
			return float64(t)
		case int64:
			return float64(t)
		case uint64:
			return float64(t)
		default:
			return 0
		}
	}()
	peer.SyncTransferRate = func() float64 {
		switch t := dict["sync_transfer_rate"].(type) {
		case float64:
			return t
		case int:
			return float64(t)
		case int64:
			return float64(t)
		case uint64:
			return float64(t)
		default:
			return 0
		}
	}()
	if v, ok := dict["propagation_transfer_limit"]; ok {
		peer.PropagationTransferLimit = func() float64 {
			switch t := v.(type) {
			case float64:
				return t
			case int:
				return float64(t)
			case int64:
				return float64(t)
			case uint64:
				return float64(t)
			default:
				return 0
			}
		}()
	}
	if v, ok := dict["propagation_sync_limit"]; ok {
		peer.PropagationSyncLimit = func() float64 {
			switch t := v.(type) {
			case float64:
				return t
			case int:
				return float64(t)
			case int64:
				return float64(t)
			case uint64:
				return float64(t)
			default:
				return 0
			}
		}()
	} else {
		peer.PropagationSyncLimit = peer.PropagationTransferLimit
	}
	if v, ok := dict["propagation_stamp_cost"]; ok {
		peer.PropagationStampCost = int(func() float64 {
			switch t := v.(type) {
			case float64:
				return t
			case int:
				return float64(t)
			case int64:
				return float64(t)
			case uint64:
				return float64(t)
			default:
				return 0
			}
		}())
	}
	if v, ok := dict["propagation_stamp_cost_flexibility"]; ok {
		peer.PropagationStampCostFlexibility = int(func() float64 {
			switch t := v.(type) {
			case float64:
				return t
			case int:
				return float64(t)
			case int64:
				return float64(t)
			case uint64:
				return float64(t)
			default:
				return 0
			}
		}())
	}
	if v, ok := dict["peering_cost"]; ok {
		peer.PeeringCost = int(func() float64 {
			switch t := v.(type) {
			case float64:
				return t
			case int:
				return float64(t)
			case int64:
				return float64(t)
			case uint64:
				return float64(t)
			default:
				return 0
			}
		}())
	}
	if v, ok := dict["sync_strategy"]; ok {
		peer.SyncStrategy = int(func() float64 {
			switch t := v.(type) {
			case float64:
				return t
			case int:
				return float64(t)
			case int64:
				return float64(t)
			case uint64:
				return float64(t)
			default:
				return 0
			}
		}())
	}
	if v, ok := dict["offered"]; ok {
		peer.Offered = int(func() float64 {
			switch t := v.(type) {
			case float64:
				return t
			case int:
				return float64(t)
			case int64:
				return float64(t)
			case uint64:
				return float64(t)
			default:
				return 0
			}
		}())
	}
	if v, ok := dict["outgoing"]; ok {
		peer.Outgoing = int(func() float64 {
			switch t := v.(type) {
			case float64:
				return t
			case int:
				return float64(t)
			case int64:
				return float64(t)
			case uint64:
				return float64(t)
			default:
				return 0
			}
		}())
	}
	if v, ok := dict["incoming"]; ok {
		peer.Incoming = int(func() float64 {
			switch t := v.(type) {
			case float64:
				return t
			case int:
				return float64(t)
			case int64:
				return float64(t)
			case uint64:
				return float64(t)
			default:
				return 0
			}
		}())
	}
	if v, ok := dict["rx_bytes"]; ok {
		peer.RxBytes = int(func() float64 {
			switch t := v.(type) {
			case float64:
				return t
			case int:
				return float64(t)
			case int64:
				return float64(t)
			case uint64:
				return float64(t)
			default:
				return 0
			}
		}())
	}
	if v, ok := dict["tx_bytes"]; ok {
		peer.TxBytes = int(func() float64 {
			switch t := v.(type) {
			case float64:
				return t
			case int:
				return float64(t)
			case int64:
				return float64(t)
			case uint64:
				return float64(t)
			default:
				return 0
			}
		}())
	}
	if v, ok := dict["last_sync_attempt"]; ok {
		peer.LastSyncAttempt = func() float64 {
			switch t := v.(type) {
			case float64:
				return t
			case int:
				return float64(t)
			case int64:
				return float64(t)
			case uint64:
				return float64(t)
			default:
				return 0
			}
		}()
	}
	if v, ok := dict["peering_key"]; ok {
		if key, ok := v.([]any); ok {
			peer.PeeringKey = key
		}
	}
	if meta, ok := dict["metadata"].(map[any]any); ok {
		peer.Metadata = meta
	}

	decode := func(v any) [][]byte {
		list, ok := v.([]any)
		if !ok {
			return nil
		}
		out := make([][]byte, 0, len(list))
		for _, entry := range list {
			switch ids := entry.(type) {
			case []byte:
				if len(ids) > 0 {
					out = append(out, append([]byte(nil), ids...))
				}
			case string:
				if ids != "" {
					out = append(out, []byte(ids))
				}
			}
		}
		return out
	}

	hmCount := 0
	for _, id := range decode(dict["handled_ids"]) {
		if _, ok := router.PropagationEntries[string(id)]; ok {
			peer.AddHandledMessage(id)
			hmCount++
		}
	}

	umCount := 0
	for _, id := range decode(dict["unhandled_ids"]) {
		if _, ok := router.PropagationEntries[string(id)]; ok {
			peer.AddUnhandledMessage(id)
			umCount++
		}
	}

	peer.hmCount = hmCount
	peer.umCount = umCount
	peer.hmCountsSynced = true
	peer.umCountsSynced = true
	return peer, nil
}

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

	roundTrip, err := restorePeerFromBytes(raw, router)
	if err != nil {
		t.Fatalf("from bytes: %v", err)
	}

	if got := len(roundTrip.UnhandledMessages()); got != 1 {
		t.Fatalf("expected 1 unhandled message, got %d", got)
	}
	if got := len(roundTrip.HandledMessages()); got != 1 {
		t.Fatalf("expected 1 handled message, got %d", got)
	}
	if rate := roundTrip.AcceptanceRate(); rate != 0.5 {
		t.Fatalf("expected acceptance rate 0.5, got %f", rate)
	}
}

func TestPeerRoundTripSerializationPreservesZeroFlexibility(t *testing.T) {
	storage := t.TempDir()
	identity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	router, err := NewLXMRouter(identity, storage)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	peerIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("peer identity: %v", err)
	}
	peerDest, err := rns.NewDestination(peerIdentity, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "propagation")
	if err != nil {
		t.Fatalf("peer destination: %v", err)
	}

	rns.IdentityRemember([]byte("packet"), peerDest.Hash, peerIdentity.GetPublicKey(), nil)

	peer := NewLXMPeer(router, peerDest.Hash, PeerDefaultSyncStrategy)
	peer.PeeringCost = 18
	peer.PropagationStampCost = 16
	peer.PropagationStampCostFlexibility = 0

	raw, err := peer.ToBytes()
	if err != nil {
		t.Fatalf("to bytes: %v", err)
	}

	roundTrip, err := restorePeerFromBytes(raw, router)
	if err != nil {
		t.Fatalf("from bytes: %v", err)
	}

	if roundTrip.PropagationStampCostFlexibility != 0 {
		t.Fatalf("expected zero flexibility after round-trip, got %d", roundTrip.PropagationStampCostFlexibility)
	}
	if !(roundTrip.PeeringCost > 0 && roundTrip.PropagationStampCost >= 0 && roundTrip.PropagationStampCostFlexibility >= 0) {
		t.Fatalf("expected zero flexibility peer to remain sync-eligible after round-trip")
	}
}
