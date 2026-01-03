package lxmf

import (
	"bytes"
	"testing"
)

func TestStampWorkblockLength(t *testing.T) {
	workblock := StampWorkblock([]byte("seed"), 2)
	if len(workblock) != 2*256 {
		t.Fatalf("expected workblock length 512, got %d", len(workblock))
	}
}

func TestStampValidZeroCost(t *testing.T) {
	if !StampValid([]byte("anything"), 0, []byte("workblock")) {
		t.Fatalf("expected StampValid to return true for cost 0")
	}
}

func TestValidatePeeringKey(t *testing.T) {
	peeringID := []byte("peer-a")
	cost := 1
	key, _ := GenerateStamp(peeringID, cost, WorkblockExpandRoundsPeering)
	if key == nil {
		t.Fatalf("expected peering key to be generated")
	}
	if !ValidatePeeringKey(peeringID, key, cost) {
		t.Fatalf("expected peering key to validate")
	}
}

func TestValidatePNStampTooShort(t *testing.T) {
	transientID, lxmData, value, stampData := ValidatePNStamp([]byte("short"), 1)
	if transientID != nil || lxmData != nil || value != 0 || stampData != nil {
		t.Fatalf("expected empty result for too-short payload")
	}
}

func TestStampValueDeterministic(t *testing.T) {
	workblock := StampWorkblock([]byte("seed"), 1)
	stamp := []byte("stamp")
	v1 := StampValue(workblock, stamp)
	v2 := StampValue(workblock, stamp)
	if v1 != v2 {
		t.Fatalf("expected stamp value to be deterministic")
	}
	if !bytes.Equal(workblock, StampWorkblock([]byte("seed"), 1)) {
		t.Fatalf("expected workblock to be deterministic")
	}
}

