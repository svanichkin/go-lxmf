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
	router.PropagationNodeStartTime = float64(time.Now().UnixNano()) / 1e9

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
	if cost := PNStampCostFromAppData(appData); cost == nil {
		t.Fatalf("unexpected stamp cost: nil")
	} else {
		switch n := cost.(type) {
		case int:
			if n != router.PropagationStampCost {
				t.Fatalf("unexpected stamp cost %v", cost)
			}
		case int64:
			if int(n) != router.PropagationStampCost {
				t.Fatalf("unexpected stamp cost %v", cost)
			}
		default:
			t.Fatalf("unexpected stamp cost %T %v", cost, cost)
		}
	}
}

func TestDisplayNameFromAppDataLegacyRoundTripRejectsInvalidUTF8(t *testing.T) {
	if got := DisplayNameFromAppData([]byte("legacy-node")); got != "legacy-node" {
		t.Fatalf("unexpected legacy display name: %s", got)
	}
	defer func() {
		if recover() == nil {
			t.Fatalf("expected invalid legacy utf-8 to panic")
		}
	}()
	_ = DisplayNameFromAppData([]byte{0xff, 0xfe, 0xfd})
}
