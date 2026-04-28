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

func TestDisplayNameFromAppDataRejectsInvalidLegacyUTF8(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected invalid legacy display name to panic")
		}
	}()
	_ = DisplayNameFromAppData([]byte{0xff, 0xfe})
}

func TestStampCostFromAppData(t *testing.T) {
	appData, err := umsgpack.Packb([]any{nil, 7})
	if err != nil {
		t.Fatalf("pack app data: %v", err)
	}
	if cost := StampCostFromAppData(appData); cost == nil {
		t.Fatalf("expected stamp cost 7, got nil")
	} else {
		switch n := cost.(type) {
		case int:
			if n != 7 {
				t.Fatalf("expected stamp cost 7, got %v", cost)
			}
		case int64:
			if n != 7 {
				t.Fatalf("expected stamp cost 7, got %v", cost)
			}
		default:
			t.Fatalf("expected stamp cost 7, got %T %v", cost, cost)
		}
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
	if cost := PNStampCostFromAppData(appData); cost == nil {
		t.Fatalf("unexpected PN stamp cost: nil")
	} else {
		switch n := cost.(type) {
		case int:
			if n != 16 {
				t.Fatalf("unexpected PN stamp cost: %v", cost)
			}
		case int64:
			if n != 16 {
				t.Fatalf("unexpected PN stamp cost: %v", cost)
			}
		default:
			t.Fatalf("unexpected PN stamp cost: %T %v", cost, cost)
		}
	}
}
