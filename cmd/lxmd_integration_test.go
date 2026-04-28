package main

import (
	"testing"
)

func TestRequestSyncPeerInvalidHash(t *testing.T) {
	if err := requestSyncPeer("invalidhash", "", 1); err == nil {
		t.Fatalf("expected error when peer hash invalid")
	}
}
