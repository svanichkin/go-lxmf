package lxmf

import (
	"strings"
	"testing"

	"github.com/svanichkin/go-reticulum/rns"
)

func TestMessagePaperURI(t *testing.T) {
	destIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	srcIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	dest, err := rns.NewDestination(destIdentity, rns.DestinationIN, rns.DestinationSINGLE, AppName, "paper")
	if err != nil {
		t.Fatalf("new dest: %v", err)
	}
	src, err := rns.NewDestination(srcIdentity, rns.DestinationIN, rns.DestinationSINGLE, AppName, "paper")
	if err != nil {
		t.Fatalf("new src: %v", err)
	}

	msg, err := NewLXMessage(dest, src, "payload", "paper", nil, MethodPaper, nil, nil, nil, false)
	if err != nil {
		t.Fatalf("new message: %v", err)
	}
	if err := msg.Pack(false); err != nil {
		t.Fatalf("pack: %v", err)
	}
	uri, err := msg.AsURI(true)
	if err != nil {
		t.Fatalf("as uri: %v", err)
	}
	if !strings.HasPrefix(uri, URISchema+"://") {
		t.Fatalf("unexpected uri prefix: %s", uri)
	}
	if msg.State != MethodPaper {
		t.Fatalf("expected paper message state, got %d", msg.State)
	}
	if msg.Progress != 1.0 {
		t.Fatalf("expected progress 1.0, got %f", msg.Progress)
	}
}

