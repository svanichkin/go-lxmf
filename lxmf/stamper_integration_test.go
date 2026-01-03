package lxmf

import (
	"bytes"
	"testing"

	"github.com/svanichkin/go-reticulum/rns"
)

func TestValidatePNStampRoundTrip(t *testing.T) {
	lxmData := make([]byte, LXMFOverhead+8)
	for i := range lxmData {
		lxmData[i] = byte(i)
	}
	transientID := rns.FullHash(lxmData)
	stamp, _ := GenerateStamp(transientID, 1, WorkblockExpandRoundsPN)
	if stamp == nil {
		t.Fatalf("expected stamp to be generated")
	}

	payload := append(append([]byte{}, lxmData...), stamp...)
	gotID, gotData, _, gotStamp := ValidatePNStamp(payload, 1)
	if !bytes.Equal(gotID, transientID) {
		t.Fatalf("transient id mismatch")
	}
	if !bytes.Equal(gotData, lxmData) {
		t.Fatalf("lxm data mismatch")
	}
	if !bytes.Equal(gotStamp, stamp) {
		t.Fatalf("stamp mismatch")
	}
}

func TestValidatePNStampsBatch(t *testing.T) {
	var payloads [][]byte
	for n := 0; n < 2; n++ {
		lxmData := make([]byte, LXMFOverhead+8)
		for i := range lxmData {
			lxmData[i] = byte(i + n)
		}
		transientID := rns.FullHash(lxmData)
		stamp, _ := GenerateStamp(transientID, 1, WorkblockExpandRoundsPN)
		if stamp == nil {
			t.Fatalf("expected stamp to be generated")
		}
		payloads = append(payloads, append(append([]byte{}, lxmData...), stamp...))
	}

	validated := ValidatePNStamps(payloads, 1)
	if len(validated) != len(payloads) {
		t.Fatalf("expected %d validated entries, got %d", len(payloads), len(validated))
	}
}

