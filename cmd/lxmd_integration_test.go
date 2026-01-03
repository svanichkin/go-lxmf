package main

import (
	"testing"
)

func TestHandleRemoteCommandsMissingConfig(t *testing.T) {
	err := handleRemoteCommands("nonexistent", "", "", "", false, false, "", "", 1, 0, 0)
	if err == nil || err.Error() == "" {
		t.Fatalf("expected error when config missing")
	}
}

func TestRequestSyncPeerInvalidHash(t *testing.T) {
	if err := requestSyncPeer("invalidhash", "", 1); err == nil {
		t.Fatalf("expected error when peer hash invalid")
	}
}
