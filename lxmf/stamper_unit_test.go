package lxmf

import (
	"bytes"
	"testing"
	"time"
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

func TestJobConcurrentReturnsStampWhenWorkerFindsResult(t *testing.T) {
	workblock := StampWorkblock([]byte("seed"), 1)

	for i := 0; i < 16; i++ {
		messageID := []byte{byte(i + 1)}
		stamp, rounds := jobConcurrent(0, workblock, messageID)
		if stamp == nil {
			t.Fatalf("expected stamp on iteration %d, got nil", i)
		}
		if !StampValid(stamp, 0, workblock) {
			t.Fatalf("expected returned stamp to validate on iteration %d", i)
		}
		if rounds != 0 {
			t.Fatalf("expected zero rounds for zero-cost stamp on iteration %d, got %d", i, rounds)
		}
	}
}

func TestGenerateStampWithTimeoutCancelsActiveWork(t *testing.T) {
	timeout := time.Millisecond
	messageID := []byte("timeout-cancel")

	stamp, value := generateStampWithTimeout(messageID, 255, 1, &timeout)
	if stamp != nil || value != 0 {
		t.Fatalf("expected timed out stamp generation to return nil/0")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		activeJobsMu.Lock()
		_, exists := activeJobs[string(messageID)]
		activeJobsMu.Unlock()
		if !exists {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("expected active stamp job to be cleaned up after cancellation")
}
