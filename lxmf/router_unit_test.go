package lxmf

import (
	"testing"
	"time"

	"github.com/svanichkin/go-reticulum/rns"
)

func TestRouterMessageStorageSize(t *testing.T) {
	router := &LXMRouter{
		PropagationNode:   true,
		PropagationEntries: map[string]*propagationEntry{},
	}
	router.PropagationEntries["a"] = &propagationEntry{Size: 10}
	router.PropagationEntries["b"] = &propagationEntry{Size: 5}
	if size := router.MessageStorageSize(); size != 15 {
		t.Fatalf("expected size 15, got %d", size)
	}
}

func TestRouterGetWeightAndStampValue(t *testing.T) {
	router := &LXMRouter{
		PropagationEntries: map[string]*propagationEntry{},
		PrioritisedList:    []string{},
	}
	id := []byte("id")
	router.PropagationEntries[string(id)] = &propagationEntry{
		DestinationHash: []byte("dest"),
		Received:        nowSeconds() - 10,
		Size:            100,
		StampValue:      7,
	}
	if val := router.GetStampValue(id); val != 7 {
		t.Fatalf("expected stamp value 7, got %d", val)
	}
	if weight := router.GetWeight(id); weight <= 0 {
		t.Fatalf("expected weight > 0, got %f", weight)
	}
}

func TestRouterAcknowledgeSyncCompletion(t *testing.T) {
	router := &LXMRouter{
		PropagationTransferState:    PRComplete,
		PropagationTransferProgress: 0.5,
		WantsDownloadOnPathAvailableFrom: []byte("from"),
	}
	router.AcknowledgeSyncCompletion(true, nil)
	if router.PropagationTransferState != PRIdle {
		t.Fatalf("expected state PRIdle, got %d", router.PropagationTransferState)
	}
	if router.PropagationTransferProgress != 0 {
		t.Fatalf("expected progress 0, got %f", router.PropagationTransferProgress)
	}
	if router.WantsDownloadOnPathAvailableFrom != nil {
		t.Fatalf("expected wants_download_on_path_available_from to be nil")
	}
}

func TestRouterCleanThrottledPeers(t *testing.T) {
	router := &LXMRouter{
		ThrottledPeers: map[string]int64{},
	}
	router.ThrottledPeers["a"] = time.Now().Unix() - 10
	router.ThrottledPeers["b"] = time.Now().Unix() + 10
	router.CleanThrottledPeers()
	if _, ok := router.ThrottledPeers["a"]; ok {
		t.Fatalf("expected expired throttle to be removed")
	}
	if _, ok := router.ThrottledPeers["b"]; !ok {
		t.Fatalf("expected active throttle to remain")
	}
}

func TestRouterProcessOutboundCancelledCallback(t *testing.T) {
	called := 0
	msg := &LXMessage{
		State: MessageCancelled,
		FailedCallback: func(*LXMessage) {
			called++
		},
	}
	router := &LXMRouter{
		PendingOutbound: []*LXMessage{msg},
	}
	router.ProcessOutbound()
	if called != 1 {
		t.Fatalf("expected failed callback to be called once, got %d", called)
	}
	if len(router.PendingOutbound) != 0 {
		t.Fatalf("expected pending outbound to be empty")
	}
}

func TestRouterCleanLinksClearsBackchannelIdentified(t *testing.T) {
	link := &rns.Link{Status: rns.LinkClosed, LinkID: []byte("link")}
	router := &LXMRouter{
		DirectLinks:           map[string]*rns.Link{"dest": link},
		ValidatedPeerLinks:    map[string]bool{string(link.LinkID): true},
		backchannelIdentified: map[*rns.Link]bool{link: true},
	}
	router.CleanLinks()
	if len(router.DirectLinks) != 0 {
		t.Fatalf("expected direct links to be cleared")
	}
	if _, ok := router.backchannelIdentified[link]; ok {
		t.Fatalf("expected backchannel identified entry to be cleared")
	}
}
