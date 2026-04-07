package lxmf

import (
	"testing"
	"time"

	"github.com/svanichkin/go-reticulum/rns"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

func TestPNAnnounceRoundTrip(t *testing.T) {
	identity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	router, err := NewLXMRouter(identity, t.TempDir())
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	router.Name = "node"
	router.PropagationNode = true
	router.PropagationNodeStartTime = time.Now().Unix()

	appData := router.GetPropagationNodeAppData()
	if !PNAnnounceDataIsValid(appData) {
		t.Fatalf("expected announce data to be valid")
	}
	if name := PNNameFromAppData(appData); name != "node" {
		t.Fatalf("unexpected PN name: %s", name)
	}

	var decoded []any
	if err := umsgpack.Unpackb(appData, &decoded); err != nil || len(decoded) < 6 {
		t.Fatalf("unexpected announce payload")
	}
	if cost, ok := PNStampCostFromAppData(appData); !ok || cost != router.PropagationStampCost {
		t.Fatalf("unexpected stamp cost %d (ok=%v)", cost, ok)
	}
}

func TestDisplayNameFromAppDataLegacyRoundTripRejectsInvalidUTF8(t *testing.T) {
	if got := DisplayNameFromAppData([]byte("legacy-node")); got != "legacy-node" {
		t.Fatalf("unexpected legacy display name: %s", got)
	}
	if got := DisplayNameFromAppData([]byte{0xff, 0xfe, 0xfd}); got != "" {
		t.Fatalf("expected invalid legacy utf-8 to be rejected, got %q", got)
	}
}
