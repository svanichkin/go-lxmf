package lxmf

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/svanichkin/go-reticulum/rns"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
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

func TestRouterTryAutopeerFromSyncRequiresPropagationEnabled(t *testing.T) {
	remoteIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	remoteDest, err := rns.NewDestination(remoteIdentity, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "propagation")
	if err != nil {
		t.Fatalf("new destination: %v", err)
	}
	remoteHash := remoteDest.Hash()

	appData, err := umsgpack.Packb([]any{
		false,
		12345,
		false,
		256,
		1024,
		[]any{16, 3, 18},
		map[any]any{},
	})
	if err != nil {
		t.Fatalf("pack app data: %v", err)
	}

	if err := rns.IdentityRemember([]byte("packet"), remoteHash, remoteIdentity.GetPublicKey(), appData); err != nil {
		t.Fatalf("remember identity: %v", err)
	}

	router := &LXMRouter{
		AutoPeer:            true,
		AutoPeerMaxDepth:    rns.PathfinderMaxHops,
		MaxPeers:            10,
		MaxPeeringCost:      MaxPeeringCostDefault,
		Peers:               map[string]*LXMPeer{},
		DefaultSyncStrategy: PeerDefaultSyncStrategy,
	}

	if ok := router.tryAutopeerFromSync(remoteHash, "remote"); ok {
		t.Fatalf("expected auto-peering to be skipped when remote propagation is disabled")
	}
	if router.Peers[string(remoteHash)] != nil {
		t.Fatalf("expected no peer to be added when remote propagation is disabled")
	}
}

func TestRouterCleanMessageStoreIgnoresInformationStorageLimit(t *testing.T) {
	tmpDir := t.TempDir()
	transientID := make([]byte, rns.HashLengthBytes)
	for i := range transientID {
		transientID[i] = byte(i)
	}
	stampValue := 1
	received := nowSeconds()
	fileName := fmt.Sprintf("%s_%f_%d", rns.HexRep(transientID, false), received, stampValue)
	msgPath := filepath.Join(tmpDir, fileName)
	if err := os.WriteFile(msgPath, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write message file: %v", err)
	}

	infoLimit := 1
	router := &LXMRouter{
		MessagePath:             tmpDir,
		PropagationEntries:      map[string]*propagationEntry{},
		InformationStorageLimit: &infoLimit,
	}
	router.PropagationEntries[string(transientID)] = &propagationEntry{
		DestinationHash: []byte("dest"),
		FilePath:        msgPath,
		Received:        received,
		Size:            int64(len("payload")),
		HandledPeers:    []string{},
		UnhandledPeers:  []string{},
		StampValue:      stampValue,
	}

	router.CleanMessageStore()

	if len(router.PropagationEntries) != 1 {
		t.Fatalf("expected information storage limit to not cull message store entries")
	}
	if _, err := os.Stat(msgPath); err != nil {
		t.Fatalf("expected message file to remain, stat err: %v", err)
	}
}
