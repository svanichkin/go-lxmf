package lxmf

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/svanichkin/go-reticulum/rns"
)

func TestRouterEnablePropagationIndexesStore(t *testing.T) {
	storage := t.TempDir()
	identity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	router, err := NewLXMRouter(identity, storage)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	messageDir := filepath.Join(storage, "lxmf", "messagestore")
	if err := os.MkdirAll(messageDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	transientID := make([]byte, rns.HashLengthBytes)
	for i := range transientID {
		transientID[i] = byte(i)
	}
	received := time.Now().Unix()
	stampValue := 3
	filename := hex.EncodeToString(transientID) + "_" + strconv.FormatInt(received, 10) + "_" + strconv.Itoa(stampValue)
	path := filepath.Join(messageDir, filename)

	payload := make([]byte, DestinationLength)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write message: %v", err)
	}

	if err := router.EnablePropagation(); err != nil {
		t.Fatalf("enable propagation: %v", err)
	}

	if len(router.PropagationEntries) != 1 {
		t.Fatalf("expected 1 propagation entry, got %d", len(router.PropagationEntries))
	}
}

func TestRouterLXMDeliveryStampEnforcementOverrides(t *testing.T) {
	destID, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("dest identity: %v", err)
	}
	srcID, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("src identity: %v", err)
	}
	dest, err := rns.NewDestination(destID, rns.DestinationIN, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		t.Fatalf("dest destination: %v", err)
	}
	src, err := rns.NewDestination(srcID, rns.DestinationIN, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		t.Fatalf("src destination: %v", err)
	}

	cost := 2
	msg, err := NewLXMessage(dest, src, "content", "title", nil, MethodOpportunistic, nil, nil, &cost, false)
	if err != nil {
		t.Fatalf("new message: %v", err)
	}
	msg.DeferStamp = true
	if err := msg.Pack(false); err != nil {
		t.Fatalf("pack: %v", err)
	}

	router := &LXMRouter{
		DeliveryConfigs: map[string]*deliveryConfig{
			string(dest.Hash()): {StampCost: &cost},
		},
		enforceStamps: true,
		AvailableTickets: newAvailableTickets(),
		LocallyDelivered: map[string]int64{},
	}

	if ok := router.LXMDelivery(msg.Packed, rns.DestinationSINGLE, nil, nil, MethodOpportunistic, true, false); !ok {
		t.Fatalf("expected delivery to succeed with no_stamp_enforcement")
	}
	if ok := router.LXMDelivery(msg.Packed, rns.DestinationSINGLE, nil, nil, MethodOpportunistic, false, false); ok {
		t.Fatalf("expected delivery to fail with stamp enforcement")
	}
}
