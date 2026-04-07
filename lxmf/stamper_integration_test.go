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

func TestValidatePNStampsMultipMatchesSimple(t *testing.T) {
	payloads := make([][]byte, 0, PNValidationPoolMinSize+3)
	for n := 0; n < PNValidationPoolMinSize+3; n++ {
		payload := make([]byte, LXMFOverhead+StampSize+8)
		for i := range payload {
			payload[i] = byte(i + n)
		}
		payloads = append(payloads, payload)
	}

	simple := ValidatePNStampsJobSimple(payloads, 0)
	multip := ValidatePNStampsJobMultip(payloads, 0)

	if len(simple) != len(multip) {
		t.Fatalf("validated entry count mismatch: simple=%d multip=%d", len(simple), len(multip))
	}
	if len(simple) != len(payloads) {
		t.Fatalf("expected every payload to validate at zero cost, got %d", len(simple))
	}

	indexByID := make(map[string][]any, len(multip))
	for _, entry := range multip {
		indexByID[string(entry[0].([]byte))] = entry
	}

	for _, entry := range simple {
		id := string(entry[0].([]byte))
		got, ok := indexByID[id]
		if !ok {
			t.Fatalf("missing validated entry for transient id %x", entry[0].([]byte))
		}
		if !bytes.Equal(got[1].([]byte), entry[1].([]byte)) {
			t.Fatalf("lxm data mismatch for transient id %x", entry[0].([]byte))
		}
		if got[2].(int) != entry[2].(int) {
			t.Fatalf("stamp value mismatch for transient id %x", entry[0].([]byte))
		}
		if !bytes.Equal(got[3].([]byte), entry[3].([]byte)) {
			t.Fatalf("stamp data mismatch for transient id %x", entry[0].([]byte))
		}
	}
}
