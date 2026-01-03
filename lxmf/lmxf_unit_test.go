package lxmf

import (
	"testing"

	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

func TestDisplayNameFromAppData(t *testing.T) {
	appData, err := umsgpack.Packb([]any{[]byte("name"), nil})
	if err != nil {
		t.Fatalf("pack app data: %v", err)
	}
	if got := DisplayNameFromAppData(appData); got != "name" {
		t.Fatalf("unexpected display name: %s", got)
	}

	if got := DisplayNameFromAppData([]byte("legacy")); got != "legacy" {
		t.Fatalf("unexpected legacy display name: %s", got)
	}
}

func TestStampCostFromAppData(t *testing.T) {
	appData, err := umsgpack.Packb([]any{nil, 7})
	if err != nil {
		t.Fatalf("pack app data: %v", err)
	}
	if cost, ok := StampCostFromAppData(appData); !ok || cost != 7 {
		t.Fatalf("expected stamp cost 7, got %d (ok=%v)", cost, ok)
	}
}

func TestPNAnnounceHelpers(t *testing.T) {
	meta := map[any]any{PNMetaName: []byte("node")}
	appData, err := umsgpack.Packb([]any{
		false,
		123,
		true,
		256,
		1024,
		[]any{16, 3, 18},
		meta,
	})
	if err != nil {
		t.Fatalf("pack app data: %v", err)
	}

	if !PNAnnounceDataIsValid(appData) {
		t.Fatalf("expected PN announce data to be valid")
	}
	if name := PNNameFromAppData(appData); name != "node" {
		t.Fatalf("unexpected PN name: %s", name)
	}
	if cost, ok := PNStampCostFromAppData(appData); !ok || cost != 16 {
		t.Fatalf("unexpected PN stamp cost: %d (ok=%v)", cost, ok)
	}
}

