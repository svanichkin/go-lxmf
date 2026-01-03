package lxmf

import (
	"testing"

	"github.com/svanichkin/go-reticulum/rns"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

func newTestDest(t *testing.T, aspect string) *rns.Destination {
	t.Helper()
	identity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	dest, err := rns.NewDestination(identity, rns.DestinationIN, rns.DestinationSINGLE, AppName, aspect)
	if err != nil {
		t.Fatalf("new destination: %v", err)
	}
	return dest
}

func TestPackUnpackRoundTrip(t *testing.T) {
	dest := newTestDest(t, "delivery")
	src := newTestDest(t, "delivery")
	stampCost := 1
	fields := map[any]any{FieldThread: []byte("thread")}
	msg, err := NewLXMessage(dest, src, "hello", "title", fields, MethodDirect, nil, nil, &stampCost, false)
	if err != nil {
		t.Fatalf("new message: %v", err)
	}
	msg.DeferStamp = false
	if err := msg.Pack(false); err != nil {
		t.Fatalf("pack: %v", err)
	}
	if len(msg.Packed) == 0 {
		t.Fatalf("expected packed bytes")
	}

	unpacked, err := UnpackFromBytes(msg.Packed, msg.Method)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}
	if unpacked.TitleAsString() != "title" {
		t.Fatalf("unexpected title: %s", unpacked.TitleAsString())
	}
	if unpacked.ContentAsString() != "hello" {
		t.Fatalf("unexpected content: %s", unpacked.ContentAsString())
	}
	if len(unpacked.Stamp) == 0 {
		t.Fatalf("expected stamp to be present on unpacked message")
	}
}

func TestPackedContainer(t *testing.T) {
	dest := newTestDest(t, "delivery")
	src := newTestDest(t, "delivery")
	msg, err := NewLXMessage(dest, src, "hi", "t", nil, MethodDirect, nil, nil, nil, false)
	if err != nil {
		t.Fatalf("new message: %v", err)
	}
	if err := msg.Pack(false); err != nil {
		t.Fatalf("pack: %v", err)
	}
	container, err := msg.PackedContainer()
	if err != nil {
		t.Fatalf("packed container: %v", err)
	}
	var decoded map[any]any
	if err := umsgpack.Unpackb(container, &decoded); err != nil {
		t.Fatalf("unpack container: %v", err)
	}
	if _, ok := decoded["lxmf_bytes"]; !ok {
		t.Fatalf("expected lxmf_bytes in container")
	}
	if _, ok := decoded["method"]; !ok {
		t.Fatalf("expected method in container")
	}
}

func TestAsQRWithoutGenerator(t *testing.T) {
	dest := newTestDest(t, "delivery")
	src := newTestDest(t, "delivery")
	msg, err := NewLXMessage(dest, src, "hi", "t", nil, MethodPaper, nil, nil, nil, false)
	if err != nil {
		t.Fatalf("new message: %v", err)
	}
	if err := msg.Pack(false); err != nil {
		t.Fatalf("pack: %v", err)
	}
	if _, err := msg.AsQR(); err == nil {
		t.Fatalf("expected AsQR to fail without generator")
	}
}

