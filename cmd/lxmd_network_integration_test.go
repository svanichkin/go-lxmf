package main

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/svanichkin/go-lxmf/lxmf"
	"github.com/svanichkin/go-reticulum/rns"
)

type lxmdLoopbackTransport struct {
	ifc           *rns.Interface
	mu            sync.Mutex
	pendingByLink map[string][][]byte
}

func (t *lxmdLoopbackTransport) Outbound(p *rns.Packet) bool {
	if p == nil {
		return false
	}
	if !p.Packed {
		if err := p.Pack(); err != nil {
			return false
		}
	}
	if !p.Sent {
		p.Sent = true
		p.SentAt = time.Now()
	}
	if p.CreateReceipt && p.Receipt == nil {
		rc := rns.NewPacketReceipt(p)
		p.Receipt = rc
		rns.Receipts = append(rns.Receipts, rc)
	}

	in := rns.NewPacket(nil, p.Raw, 0, 0, 0, 0, nil, nil, false, 0)
	if in == nil || !in.Unpack() {
		return false
	}

	var deliver func(pkt *rns.Packet) bool
	deliver = func(pkt *rns.Packet) bool {
		if p.Link != nil && len(p.Link.LinkID) > 0 && string(pkt.DestinationHash) == string(p.Link.LinkID) {
			if p.Link.Initiator {
				if responder := findResponderLinkByID(pkt.DestinationHash); responder != nil {
					pkt.Link = responder
				}
			} else {
				if initiator := findInitiatorLinkByID(pkt.DestinationHash); initiator != nil {
					pkt.Link = initiator
				}
			}
		}

		if pkt.Link == nil {
			if pkt.DestinationType == byte(rns.DestinationLINK) || pkt.Context == rns.PacketCtxLRProof {
				var link *rns.Link
				if pkt.Context == rns.PacketCtxLRProof {
					link = findInitiatorLinkByID(pkt.DestinationHash)
				} else {
					link = findAnyLinkByID(pkt.DestinationHash)
				}
				if link != nil {
					pkt.Link = link
				} else if pkt.Context == rns.PacketCtxLRProof {
					rawCopy := append([]byte(nil), pkt.Raw...)
					t.mu.Lock()
					if t.pendingByLink == nil {
						t.pendingByLink = make(map[string][][]byte)
					}
					key := string(pkt.DestinationHash)
					t.pendingByLink[key] = append(t.pendingByLink[key], rawCopy)
					t.mu.Unlock()
					destHash := append([]byte(nil), pkt.DestinationHash...)
					go func() {
						deadline := time.Now().Add(200 * time.Millisecond)
						for time.Now().Before(deadline) {
							if findInitiatorLinkByID(destHash) != nil {
								retry := rns.NewPacket(nil, rawCopy, 0, 0, 0, 0, nil, nil, false, 0)
								if retry == nil || !retry.Unpack() {
									return
								}
								_ = deliver(retry)
								return
							}
							time.Sleep(2 * time.Millisecond)
						}
					}()
					return true
				}
			} else {
				pkt.Destination = findDestinationByHash(pkt.DestinationHash)
			}
		}

		if pkt.Link != nil {
			pkt.Link.Receive(pkt)
			return true
		}
		if pkt.Destination != nil {
			return pkt.Destination.Receive(pkt)
		}
		return false
	}

	_ = deliver(in)

	t.mu.Lock()
	pending := t.pendingByLink
	t.pendingByLink = nil
	t.mu.Unlock()
	for _, raws := range pending {
		for _, raw := range raws {
			pkt := rns.NewPacket(nil, raw, 0, 0, 0, 0, nil, nil, false, 0)
			if pkt == nil || !pkt.Unpack() {
				continue
			}
			_ = deliver(pkt)
		}
	}

	return true
}

func (*lxmdLoopbackTransport) HopsTo([]byte) int { return 1 }

func (*lxmdLoopbackTransport) GetFirstHopTimeout([]byte) time.Duration {
	return 10 * time.Millisecond
}

func (*lxmdLoopbackTransport) GetPacketRSSI([]byte) *float64 { return nil }
func (*lxmdLoopbackTransport) GetPacketSNR([]byte) *float64  { return nil }
func (*lxmdLoopbackTransport) GetPacketQ([]byte) *float64    { return nil }

func findDestinationByHash(hash []byte) *rns.Destination {
	var fallback *rns.Destination
	for _, d := range rns.Destinations {
		if d == nil || len(d.Hash) == 0 {
			continue
		}
		if string(d.Hash) == string(hash) {
			if d.Direction == rns.DestinationIN {
				return d
			}
			if fallback == nil {
				fallback = d
			}
		}
	}
	return fallback
}

func findAnyLinkByID(linkID []byte) *rns.Link {
	for _, l := range rns.ActiveLinks {
		if l != nil && l.Status != rns.LinkClosed && string(l.LinkID) == string(linkID) {
			return l
		}
	}
	for _, l := range rns.PendingLinks {
		if l != nil && l.Status != rns.LinkClosed && string(l.LinkID) == string(linkID) {
			return l
		}
	}
	return nil
}

func findInitiatorLinkByID(linkID []byte) *rns.Link {
	for _, l := range rns.ActiveLinks {
		if l != nil && l.Status != rns.LinkClosed && l.Initiator && string(l.LinkID) == string(linkID) {
			return l
		}
	}
	for _, l := range rns.PendingLinks {
		if l != nil && l.Status != rns.LinkClosed && l.Initiator && string(l.LinkID) == string(linkID) {
			return l
		}
	}
	return nil
}

func findResponderLinkByID(linkID []byte) *rns.Link {
	for _, l := range rns.ActiveLinks {
		if l != nil && l.Status != rns.LinkClosed && !l.Initiator && string(l.LinkID) == string(linkID) {
			return l
		}
	}
	for _, l := range rns.PendingLinks {
		if l != nil && l.Status != rns.LinkClosed && !l.Initiator && string(l.LinkID) == string(linkID) {
			return l
		}
	}
	return nil
}

type lxmdTestNode struct {
	identity        *rns.Identity
	router          *lxmf.LXMRouter
	deliveryDest    *rns.Destination
	controlDest     *rns.Destination
	storageDir      string
	messagesDir     string
	receivedMessage chan *lxmf.LXMessage
}

func setupLXMDTestGlobals(t *testing.T) {
	t.Helper()

	oldTransport := rns.Transport
	oldTransportIdentity := rns.TransportIdentity
	oldOwner := rns.Owner
	oldDestinations := append([]*rns.Destination(nil), rns.Destinations...)
	oldReceipts := append([]*rns.PacketReceipt(nil), rns.Receipts...)
	oldLocalClients := append([]*rns.Interface(nil), rns.LocalClientInterfaces...)
	oldInterfaces := append([]*rns.Interface(nil), rns.Interfaces...)

	oldConfigPath := configPath
	oldIgnoredPath := ignoredPath
	oldAllowedPath := allowedPath
	oldIdentityPath := identityPath
	oldStorageDir := storageDir
	oldMessagesDir := messagesDir
	oldIdentity := identity
	oldRouter := messageRouter
	oldDest := lxmfDestination
	oldActiveConfig := activeConfig

	t.Cleanup(func() {
		rns.Transport = oldTransport
		rns.TransportIdentity = oldTransportIdentity
		rns.Owner = oldOwner
		rns.Destinations = oldDestinations
		rns.Receipts = oldReceipts
		rns.LocalClientInterfaces = oldLocalClients
		rns.Interfaces = oldInterfaces

		configPath = oldConfigPath
		ignoredPath = oldIgnoredPath
		allowedPath = oldAllowedPath
		identityPath = oldIdentityPath
		storageDir = oldStorageDir
		messagesDir = oldMessagesDir
		identity = oldIdentity
		messageRouter = oldRouter
		lxmfDestination = oldDest
		activeConfig = oldActiveConfig
	})

	transportIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("transport identity: %v", err)
	}
	rns.TransportIdentity = transportIdentity
	rns.Owner = &rns.Reticulum{StoragePath: t.TempDir()}
	rns.Transport = &lxmdLoopbackTransport{
		ifc: &rns.Interface{Name: "cmd-e2e0", IN: true, OUT: true, Online: true},
	}
	rns.Destinations = nil
	rns.Receipts = nil
	rns.LocalClientInterfaces = nil
	rns.Interfaces = []*rns.Interface{rns.Transport.(*lxmdLoopbackTransport).ifc}
}

func newLXMDTestNode(t *testing.T, displayName string, propagation bool, controlAllowed [][]byte, deliveryCB func(*lxmf.LXMessage)) *lxmdTestNode {
	t.Helper()

	id, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	rns.IdentityRemember(nil, id.Hash, id.GetPublicKey(), nil)
	storage, err := os.MkdirTemp("", "lxmd-node-*")
	if err != nil {
		t.Fatalf("temp storage dir: %v", err)
	}
	t.Cleanup(func() {
		time.Sleep(50 * time.Millisecond)
		_ = os.RemoveAll(storage)
	})
	router, err := lxmf.NewLXMRouter(id, storage)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	router.Name = displayName
	if deliveryCB != nil {
		router.RegisterDeliveryCallback(deliveryCB)
	}

	deliveryDest := router.RegisterDeliveryIdentity(id, &displayName, nil)
	if deliveryDest == nil {
		t.Fatalf("register delivery identity returned nil")
	}
	rns.IdentityRemember(nil, deliveryDest.Hash, id.GetPublicKey(), nil)

	if propagation {
		if err := router.EnablePropagation(); err != nil {
			t.Fatalf("enable propagation: %v", err)
		}
		for _, allowed := range controlAllowed {
			if err := router.AllowControl(allowed); err != nil {
				t.Fatalf("allow control: %v", err)
			}
		}
	}

	node := &lxmdTestNode{
		identity:        id,
		router:          router,
		deliveryDest:    deliveryDest,
		controlDest:     router.ControlDestination,
		storageDir:      storage,
		messagesDir:     filepath.Join(storage, "messages"),
		receivedMessage: make(chan *lxmf.LXMessage, 1),
	}
	if err := os.MkdirAll(node.messagesDir, 0o755); err != nil {
		t.Fatalf("mkdir messages dir: %v", err)
	}
	if node.controlDest != nil {
		rns.IdentityRemember(nil, node.controlDest.Hash, id.GetPublicKey(), nil)
	}
	return node
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stdout: %v", err)
	}
	os.Stdout = w
	defer func() {
		os.Stdout = old
	}()

	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()

	fn()
	_ = w.Close()
	out := <-done
	_ = r.Close()
	return out
}

func waitForInboundFile(t *testing.T, dir string) string {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read dir: %v", err)
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				return filepath.Join(dir, entry.Name())
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for inbound file in %s", dir)
	return ""
}

func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool, description string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func injectRemoteAnnounceForTest(t *testing.T, dest *rns.Destination, appData []byte) {
	t.Helper()
	if dest == nil {
		t.Fatalf("destination is required")
	}
	packet := dest.Announce(appData, false, nil, nil, false)
	if packet == nil {
		t.Fatalf("expected announce packet")
	}
	if err := packet.Pack(); err != nil {
		t.Fatalf("pack announce: %v", err)
	}

	var removed *rns.Destination
	for i, existing := range rns.Destinations {
		if existing == dest {
			removed = existing
			rns.Destinations = append(rns.Destinations[:i], rns.Destinations[i+1:]...)
			break
		}
	}
	rns.Inbound(append([]byte(nil), packet.Raw...), nil)
	if removed != nil {
		rns.Destinations = append(rns.Destinations, removed)
	}
}

func relaxControlAccessForTest(t *testing.T, node *lxmdTestNode) {
	t.Helper()
	if node == nil || node.controlDest == nil {
		t.Fatalf("node control destination is required")
	}
	if err := node.controlDest.RegisterRequestHandler(lxmf.StatsGetPath, node.router.StatsGetRequest, rns.DestinationALLOW_ALL, nil); err != nil {
		t.Fatalf("register stats handler: %v", err)
	}
	if err := node.controlDest.RegisterRequestHandler(lxmf.SyncRequestPath, node.router.PeerSyncRequest, rns.DestinationALLOW_ALL, nil); err != nil {
		t.Fatalf("register sync handler: %v", err)
	}
	if err := node.controlDest.RegisterRequestHandler(lxmf.UnpeerRequestPath, node.router.PeerUnpeerRequest, rns.DestinationALLOW_ALL, nil); err != nil {
		t.Fatalf("register unpeer handler: %v", err)
	}
}

func TestLXMDControlRequestStatsBetweenTwoNodes(t *testing.T) {
	setupLXMDTestGlobals(t)

	requester := newLXMDTestNode(t, "requester", false, nil, nil)
	remote := newLXMDTestNode(t, "remote-node", true, [][]byte{requester.identity.Hash}, nil)
	if remote.controlDest == nil {
		t.Fatalf("expected propagation node control destination")
	}
	relaxControlAccessForTest(t, remote)

	identity = requester.identity
	out := captureStdout(t, func() {
		if err := printStatusResponse(hex.EncodeToString(remote.identity.Hash), true, true, 3); err != nil {
			t.Fatalf("printStatusResponse stats: %v", err)
		}
	})
	if !strings.Contains(out, "LXMF Propagation Node running on") {
		t.Fatalf("expected stats output, got:\n%s", out)
	}
	if !strings.Contains(out, "uptime is") {
		t.Fatalf("expected uptime in stats output, got:\n%s", out)
	}
}

func TestLXMDRemoteSyncAndUnpeerBetweenTwoNodes(t *testing.T) {
	setupLXMDTestGlobals(t)

	requester := newLXMDTestNode(t, "requester", false, nil, nil)
	remote := newLXMDTestNode(t, "remote-node", true, [][]byte{requester.identity.Hash}, nil)
	if remote.controlDest == nil {
		t.Fatalf("expected propagation node control destination")
	}
	relaxControlAccessForTest(t, remote)

	peerHash := []byte("peer-hash-sync12")
	peer := lxmf.NewLXMPeer(remote.router, peerHash, lxmf.PeerDefaultSyncStrategy)
	peer.PeeringCost = 10
	peer.PropagationStampCost = 8
	peer.PropagationStampCostFlexibility = 0
	remote.router.Peers[string(peerHash)] = peer

	identity = requester.identity
	remoteHex := hex.EncodeToString(remote.controlDest.Hash)
	targetHex := hex.EncodeToString(peerHash)

	if err := requestSyncPeer(targetHex, remoteHex, 3); err != nil {
		t.Fatalf("requestSyncPeer: %v", err)
	}
	if peer.LastSyncAttempt == 0 {
		t.Fatalf("expected peer sync to update LastSyncAttempt")
	}

	if err := requestUnpeerPeer(targetHex, remoteHex, 3); err != nil {
		t.Fatalf("requestUnpeerPeer: %v", err)
	}
	if remote.router.Peers[string(peerHash)] != nil {
		t.Fatalf("expected peer to be removed after unpeer request")
	}
}

func TestLXMDDaemonToDaemonInboundMessageDelivery(t *testing.T) {
	setupLXMDTestGlobals(t)

	receiver := newLXMDTestNode(t, "receiver", false, nil, nil)
	requester := newLXMDTestNode(t, "sender", false, nil, nil)

	receiver.router.RegisterDeliveryCallback(func(msg *lxmf.LXMessage) {
		select {
		case receiver.receivedMessage <- msg:
		default:
		}

		oldMessagesDir := messagesDir
		oldActiveConfig := activeConfig
		messagesDir = receiver.messagesDir
		activeConfig = activeConfiguration{}
		defer func() {
			messagesDir = oldMessagesDir
			activeConfig = oldActiveConfig
		}()
		lxmfDelivery(msg)
	})

	msg, err := lxmf.NewLXMessage(receiver.deliveryDest, requester.deliveryDest, "hello from sender", "cmd-test", nil, lxmf.MethodOpportunistic, nil, nil, nil, false)
	if err != nil {
		t.Fatalf("new message: %v", err)
	}
	msg.DeferStamp = true
	if err := msg.Pack(false); err != nil {
		t.Fatalf("pack: %v", err)
	}
	msg.Send()

	select {
	case delivered := <-receiver.receivedMessage:
		if delivered.ContentAsString() != "hello from sender" {
			t.Fatalf("unexpected delivered content: %q", delivered.ContentAsString())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for daemon-to-daemon delivery callback")
	}

	inboundPath := waitForInboundFile(t, receiver.messagesDir)
	data, err := os.ReadFile(inboundPath)
	if err != nil {
		t.Fatalf("read inbound file: %v", err)
	}
	if !strings.Contains(string(data), "hello from sender") {
		t.Fatalf("expected inbound file to contain message payload, got %q", string(data))
	}
}

func TestLXMDDaemonPropagationNodeMessageDownloadBetweenTwoNodes(t *testing.T) {
	setupLXMDTestGlobals(t)

	client := newLXMDTestNode(t, "client", false, nil, nil)
	pn := newLXMDTestNode(t, "propagation-node", true, nil, nil)
	if pn.router.PropagationDestination == nil {
		t.Fatalf("expected propagation destination")
	}

	client.router.RegisterDeliveryCallback(func(msg *lxmf.LXMessage) {
		select {
		case client.receivedMessage <- msg:
		default:
		}

		oldMessagesDir := messagesDir
		oldActiveConfig := activeConfig
		messagesDir = client.messagesDir
		activeConfig = activeConfiguration{}
		defer func() {
			messagesDir = oldMessagesDir
			activeConfig = oldActiveConfig
		}()
		lxmfDelivery(msg)
	})

	injectRemoteAnnounceForTest(t, pn.router.PropagationDestination, pn.router.GetPropagationNodeAppData())
	if err := client.router.SetOutboundPropagationNode(pn.router.PropagationDestination.Hash); err != nil {
		t.Fatalf("set outbound propagation node: %v", err)
	}
	rns.IdentityRemember(nil, pn.router.PropagationDestination.Hash, pn.identity.GetPublicKey(), pn.router.GetPropagationNodeAppData())
	waitForCondition(t, 2*time.Second, func() bool {
		return rns.HasPath(pn.router.PropagationDestination.Hash)
	}, "path to propagation node")

	msg, err := lxmf.NewLXMessage(client.deliveryDest, pn.deliveryDest, "hello via propagation", "pn-test", nil, lxmf.MethodPropagated, nil, nil, nil, false)
	if err != nil {
		t.Fatalf("new propagated message: %v", err)
	}
	msg.DeferStamp = true
	if err := msg.Pack(false); err != nil {
		t.Fatalf("pack propagated message: %v", err)
	}
	lxmfData := append(append([]byte{}, msg.Packed[:lxmf.DestinationLength]...), msg.PNEncryptedData...)
	if _, dup := pn.router.LXMPropagation(lxmfData, nil, 0, make([]byte, lxmf.StampSize), nil, nil, false, false); dup {
		t.Fatalf("expected propagation node to store propagated message, dup=%v", dup)
	}

	client.router.RequestMessagesFromPropagationNode(client.identity, nil)

	select {
	case delivered := <-client.receivedMessage:
		if delivered.ContentAsString() != "hello via propagation" {
			t.Fatalf("unexpected delivered content: %q", delivered.ContentAsString())
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for propagation download delivery")
	}

	inboundPath := waitForInboundFile(t, client.messagesDir)
	data, err := os.ReadFile(inboundPath)
	if err != nil {
		t.Fatalf("read inbound file: %v", err)
	}
	if !strings.Contains(string(data), "hello via propagation") {
		t.Fatalf("expected inbound file to contain propagated payload, got %q", string(data))
	}
}

func TestLXMDDaemonPropagationUploadToNodeBetweenTwoNodes(t *testing.T) {
	setupLXMDTestGlobals(t)

	sender := newLXMDTestNode(t, "sender", false, nil, nil)
	pn := newLXMDTestNode(t, "propagation-node", true, nil, nil)
	if pn.router.PropagationDestination == nil {
		t.Fatalf("expected propagation destination")
	}

	// Make propagation uploads deterministic in test by allowing zero-cost stamps.
	pn.router.PropagationStampCost = 0
	pn.router.PropagationStampCostFlexibility = 0

	injectRemoteAnnounceForTest(t, pn.router.PropagationDestination, pn.router.GetPropagationNodeAppData())
	if err := sender.router.SetOutboundPropagationNode(pn.router.PropagationDestination.Hash); err != nil {
		t.Fatalf("set sender outbound propagation node: %v", err)
	}
	rns.IdentityRemember(nil, pn.router.PropagationDestination.Hash, pn.identity.GetPublicKey(), pn.router.GetPropagationNodeAppData())
	waitForCondition(t, 2*time.Second, func() bool {
		return rns.HasPath(pn.router.PropagationDestination.Hash)
	}, "path to propagation node")
	sender.router.RequestMessagesFromPropagationNode(sender.identity, nil)
	waitForCondition(t, 3*time.Second, func() bool {
		return sender.router.OutboundPropagationLink != nil && sender.router.OutboundPropagationLink.Status == rns.LinkActive
	}, "sender propagation link establishment")

	receiverIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("receiver identity: %v", err)
	}
	receiverDest, err := rns.NewDestination(receiverIdentity, rns.DestinationIN, rns.DestinationSINGLE, lxmf.AppName, "delivery")
	if err != nil {
		t.Fatalf("receiver destination: %v", err)
	}
	rns.IdentityRemember(nil, receiverDest.Hash, receiverIdentity.GetPublicKey(), nil)

	msg, err := lxmf.NewLXMessage(receiverDest, sender.deliveryDest, "hello full propagation round-trip", "pn-roundtrip", nil, lxmf.MethodPropagated, nil, nil, nil, false)
	if err != nil {
		t.Fatalf("new propagated message: %v", err)
	}
	msg.DeferStamp = false
	msg.PropagationStamp = msg.GetPropagationStamp(0, nil)
	if len(msg.PropagationStamp) != lxmf.StampSize {
		t.Fatalf("expected generated propagation stamp of size %d, got %d", lxmf.StampSize, len(msg.PropagationStamp))
	}
	msg.DeferPropagationStamp = false
	msg.DeliveryAttempts = 0

	sender.router.HandleOutbound(msg)

	waitForCondition(t, 3*time.Second, func() bool {
		return len(pn.router.PropagationEntries) == 1
	}, "propagation upload from sender to PN")

	if msg.State == lxmf.MessageFailed || msg.State == lxmf.MessageRejected || msg.State == lxmf.MessageCancelled {
		t.Fatalf("expected propagated message upload to remain successful, got state %d", msg.State)
	}
	if len(pn.router.PropagationEntries) != 1 {
		t.Fatalf("expected PN to store one propagated message, got %d", len(pn.router.PropagationEntries))
	}
	storedPath := waitForInboundFile(t, pn.router.MessagePath)
	stored, err := os.ReadFile(storedPath)
	if err != nil {
		t.Fatalf("read stored propagation payload: %v", err)
	}
	if len(stored) <= lxmf.StampSize {
		t.Fatalf("expected stored propagation payload to include LXMF bytes and stamp, got %d bytes", len(stored))
	}
}

func TestLXMDControlRequestAccessDeniedBetweenTwoNodes(t *testing.T) {
	setupLXMDTestGlobals(t)

	requester := newLXMDTestNode(t, "requester", false, nil, nil)
	remote := newLXMDTestNode(t, "remote-node", true, nil, nil)
	if remote.controlDest == nil {
		t.Fatalf("expected propagation node control destination")
	}

	identity = requester.identity
	err := printStatusResponse(hex.EncodeToString(remote.identity.Hash), true, true, 3)
	if err == nil {
		t.Fatalf("expected printStatusResponse to fail when requester is not allow-listed")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLXMDRequestSyncPeerNotFoundBetweenTwoNodes(t *testing.T) {
	setupLXMDTestGlobals(t)

	requester := newLXMDTestNode(t, "requester", false, nil, nil)
	remote := newLXMDTestNode(t, "remote-node", true, [][]byte{requester.identity.Hash}, nil)
	if remote.controlDest == nil {
		t.Fatalf("expected propagation node control destination")
	}
	relaxControlAccessForTest(t, remote)

	identity = requester.identity
	remoteHex := hex.EncodeToString(remote.controlDest.Hash)
	targetHex := hex.EncodeToString([]byte("peer-missing-123"))

	err := requestSyncPeer(targetHex, remoteHex, 3)
	if err == nil {
		t.Fatalf("expected requestSyncPeer to fail when peer is missing")
	}
	if !strings.Contains(err.Error(), "peer not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLXMDRequestUnpeerPeerNotFoundBetweenTwoNodes(t *testing.T) {
	setupLXMDTestGlobals(t)

	requester := newLXMDTestNode(t, "requester", false, nil, nil)
	remote := newLXMDTestNode(t, "remote-node", true, [][]byte{requester.identity.Hash}, nil)
	if remote.controlDest == nil {
		t.Fatalf("expected propagation node control destination")
	}
	relaxControlAccessForTest(t, remote)

	identity = requester.identity
	remoteHex := hex.EncodeToString(remote.controlDest.Hash)
	targetHex := hex.EncodeToString([]byte("peer-missing-123"))

	err := requestUnpeerPeer(targetHex, remoteHex, 3)
	if err == nil {
		t.Fatalf("expected requestUnpeerPeer to fail when peer is missing")
	}
	if !strings.Contains(err.Error(), "peer not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}
