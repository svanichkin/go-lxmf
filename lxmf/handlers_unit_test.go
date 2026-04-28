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
	if !handler.ReceivePathResponses() {
		t.Fatalf("expected delivery announce handler to opt in to path responses")
	}
}

func TestDeliveryAnnounceHandlerUpdatesStampCost(t *testing.T) {
	storage := t.TempDir()
	router := &LXMRouter{
		StoragePath:        storage,
		OutboundStampCosts: map[string][]any{},
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
	if len(entry) < 2 {
		t.Fatalf("expected outbound stamp cost list, got %#v", entry)
	}
	if int(floatFromAny(entry[1])) != 12 {
		t.Fatalf("expected cost 12, got %#v", entry[1])
	}
}

func TestPropagationAnnounceHandlerAutoPeers(t *testing.T) {
	router := &LXMRouter{
		PropagationNode:     true,
		AutoPeer:            true,
		AutoPeerMaxDepth:    rns.PathfinderMaxHops,
		MaxPeers:            10,
		MaxPeeringCost:      MaxPeeringCostDefault,
		Peers:               map[string]*LXMPeer{},
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

func TestPropagationAnnounceHandlerOptsIntoPathResponses(t *testing.T) {
	handler := NewPropagationAnnounceHandler(&LXMRouter{})
	if !handler.ReceivePathResponses() {
		t.Fatalf("expected propagation announce handler to opt in to path responses")
	}
}

func TestPropagationAnnounceHandlerIgnoresWhenNotPropagationNode(t *testing.T) {
	router := &LXMRouter{
		AutoPeer:            true,
		AutoPeerMaxDepth:    rns.PathfinderMaxHops,
		MaxPeers:            10,
		MaxPeeringCost:      MaxPeeringCostDefault,
		Peers:               map[string]*LXMPeer{},
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

	if router.Peers[string(destHash)] != nil {
		t.Fatalf("expected peer announce to be ignored when router is not a propagation node")
	}
}

func TestPropagationAnnounceHandlerIgnoresPathResponseForAutopeer(t *testing.T) {
	router := &LXMRouter{
		PropagationNode:     true,
		AutoPeer:            true,
		AutoPeerMaxDepth:    rns.PathfinderMaxHops,
		MaxPeers:            10,
		MaxPeeringCost:      MaxPeeringCostDefault,
		Peers:               map[string]*LXMPeer{},
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

	destHash := []byte("fedcba9876543211")
	handler.ReceivedAnnounceWithPacketInfo(destHash, nil, appData, nil, true)

	if router.Peers[string(destHash)] != nil {
		t.Fatalf("expected auto-peering to ignore path responses")
	}
}

func TestPropagationAnnounceHandlerStaticPeerIgnoresPathResponseAfterFirstContact(t *testing.T) {
	router := &LXMRouter{
		PropagationNode:     true,
		AutoPeer:            true,
		AutoPeerMaxDepth:    rns.PathfinderMaxHops,
		MaxPeers:            10,
		MaxPeeringCost:      MaxPeeringCostDefault,
		Peers:               map[string]*LXMPeer{},
		StaticPeers:         [][]byte{[]byte("static-peer-1234")},
		DefaultSyncStrategy: PeerDefaultSyncStrategy,
	}
	destHash := []byte("static-peer-1234")
	router.Peers[string(destHash)] = &LXMPeer{
		Router:               router,
		DestinationHash:      copyBytes(destHash),
		LastHeard:            10,
		PeeringTimebase:      1,
		PropagationSyncLimit: 111,
		PeeringCost:          9,
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

	handler.ReceivedAnnounceWithPacketInfo(destHash, nil, appData, nil, true)

	peer := router.Peers[string(destHash)]
	if peer.PeeringTimebase != 1 {
		t.Fatalf("expected static peer path response to be ignored after first contact")
	}
	if peer.PeeringCost != 9 {
		t.Fatalf("expected static peer config to remain unchanged")
	}
}
