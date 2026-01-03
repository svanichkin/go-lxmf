package lxmf

import (
	"testing"

	"github.com/svanichkin/go-reticulum/rns"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

func TestDeliveryAnnounceHandlerAspectFilter(t *testing.T) {
	handler := NewDeliveryAnnounceHandler(&LXMRouter{})
	if got := handler.AspectFilter(); got != AppName+".delivery" {
		t.Fatalf("unexpected aspect filter: %s", got)
	}
}

func TestDeliveryAnnounceHandlerUpdatesStampCost(t *testing.T) {
	storage := t.TempDir()
	router := &LXMRouter{
		StoragePath:       storage,
		OutboundStampCosts: map[string]stampCostEntry{},
	}
	handler := NewDeliveryAnnounceHandler(router)

	appData, err := umsgpack.Packb([]any{nil, 12})
	if err != nil {
		t.Fatalf("pack app data: %v", err)
	}

	destHash := []byte("0123456789abcdef")
	handler.ReceivedAnnounce(destHash, nil, appData)

	entry, ok := router.OutboundStampCosts[string(destHash)]
	if !ok {
		t.Fatalf("expected outbound stamp cost entry to be stored")
	}
	if entry.Cost != 12 {
		t.Fatalf("expected cost 12, got %d", entry.Cost)
	}
}

func TestPropagationAnnounceHandlerAutoPeers(t *testing.T) {
	router := &LXMRouter{
		AutoPeer:         true,
		AutoPeerMaxDepth: rns.PathfinderMaxHops,
		MaxPeers:         10,
		MaxPeeringCost:   MaxPeeringCostDefault,
		Peers:            map[string]*LXMPeer{},
		DefaultSyncStrategy: PeerDefaultSyncStrategy,
	}
	handler := NewPropagationAnnounceHandler(router)

	appData, err := umsgpack.Packb([]any{
		false,
		12345,
		true,
		256,
		1024,
		[]any{16, 3, 18},
		map[any]any{},
	})
	if err != nil {
		t.Fatalf("pack app data: %v", err)
	}

	destHash := []byte("fedcba9876543210")
	handler.ReceivedAnnounce(destHash, nil, appData)

	if router.Peers[string(destHash)] == nil {
		t.Fatalf("expected peer to be added")
	}
}
