package lxmf

import (
	"testing"

	"github.com/svanichkin/go-reticulum/rns"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

func TestHandlersIntegrationWithRouter(t *testing.T) {
	storage := t.TempDir()
	identity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	router, err := NewLXMRouter(identity, storage)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	router.AutoPeer = true
	router.AutoPeerMaxDepth = rns.PathfinderMaxHops

	handler := NewPropagationAnnounceHandler(router)
	appData, err := umsgpack.Packb([]any{
		false,
		123,
		true,
		256,
		1024,
		[]any{16, 3, 18},
		map[any]any{},
	})
	if err != nil {
		t.Fatalf("pack app data: %v", err)
	}

	destHash := []byte("0011223344556677")
	handler.ReceivedAnnounce(destHash, nil, appData)

	if router.Peers[string(destHash)] == nil {
		t.Fatalf("expected peer to be added in integration handler test")
	}
}

