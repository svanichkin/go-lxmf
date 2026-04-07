package lxmf

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/svanichkin/go-reticulum/rns"
)

type e2eLoopbackTransport struct {
	ifc *rns.Interface
	mu  sync.Mutex
}

func (t *e2eLoopbackTransport) Outbound(p *rns.Packet) bool {
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

	raw := append([]byte(nil), p.Raw...)
	ifc := t.ifc
	go rns.Inbound(raw, ifc)
	return true
}

func (*e2eLoopbackTransport) HopsTo([]byte) int { return 1 }

func (*e2eLoopbackTransport) GetFirstHopTimeout([]byte) time.Duration {
	return 10 * time.Millisecond
}

func (*e2eLoopbackTransport) GetPacketRSSI([]byte) *float64 { return nil }
func (*e2eLoopbackTransport) GetPacketSNR([]byte) *float64  { return nil }
func (*e2eLoopbackTransport) GetPacketQ([]byte) *float64    { return nil }

func linkPacketCallbackForTest(t *testing.T, link *rns.Link) func([]byte, *rns.Packet) {
	t.Helper()

	linkValue := reflect.ValueOf(link).Elem()
	callbacksField := linkValue.FieldByName("callbacks")
	if !callbacksField.IsValid() {
		t.Fatalf("link callbacks field not found")
	}
	callbacksValue := reflect.NewAt(callbacksField.Type(), unsafe.Pointer(callbacksField.UnsafeAddr())).Elem()
	packetField := callbacksValue.FieldByName("Packet")
	if !packetField.IsValid() {
		t.Fatalf("link packet callback field not found")
	}
	if packetField.IsNil() {
		t.Fatalf("link packet callback is nil")
	}
	cb, ok := packetField.Interface().(func([]byte, *rns.Packet))
	if !ok {
		t.Fatalf("unexpected link packet callback type: %T", packetField.Interface())
	}
	return cb
}

func TestRouterEnablePropagationIndexesStore(t *testing.T) {
	storage := t.TempDir()
	identity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	router, err := NewLXMRouter(identity, storage)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	messageDir := filepath.Join(storage, "lxmf", "messagestore")
	if err := os.MkdirAll(messageDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	transientID := make([]byte, rns.HashLengthBytes)
	for i := range transientID {
		transientID[i] = byte(i)
	}
	received := time.Now().Unix()
	stampValue := 3
	filename := hex.EncodeToString(transientID) + "_" + strconv.FormatInt(received, 10) + "_" + strconv.Itoa(stampValue)
	path := filepath.Join(messageDir, filename)

	payload := make([]byte, DestinationLength)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write message: %v", err)
	}

	if err := router.EnablePropagation(); err != nil {
		t.Fatalf("enable propagation: %v", err)
	}

	if len(router.PropagationEntries) != 1 {
		t.Fatalf("expected 1 propagation entry, got %d", len(router.PropagationEntries))
	}
}

func TestRouterLXMDeliveryStampEnforcementOverrides(t *testing.T) {
	destID, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("dest identity: %v", err)
	}
	srcID, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("src identity: %v", err)
	}
	dest, err := rns.NewDestination(destID, rns.DestinationIN, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		t.Fatalf("dest destination: %v", err)
	}
	src, err := rns.NewDestination(srcID, rns.DestinationIN, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		t.Fatalf("src destination: %v", err)
	}

	cost := 2
	msg, err := NewLXMessage(dest, src, "content", "title", nil, MethodOpportunistic, nil, nil, &cost, false)
	if err != nil {
		t.Fatalf("new message: %v", err)
	}
	msg.DeferStamp = true
	if err := msg.Pack(false); err != nil {
		t.Fatalf("pack: %v", err)
	}

	router := &LXMRouter{
		DeliveryConfigs: map[string]*deliveryConfig{
			string(dest.Hash()): {StampCost: &cost},
		},
		enforceStamps: true,
		AvailableTickets: newAvailableTickets(),
		LocallyDelivered: map[string]int64{},
	}

	if ok := router.LXMDelivery(msg.Packed, rns.DestinationSINGLE, nil, nil, MethodOpportunistic, true, false); !ok {
		t.Fatalf("expected delivery to succeed with no_stamp_enforcement")
	}
	if ok := router.LXMDelivery(msg.Packed, rns.DestinationSINGLE, nil, nil, MethodOpportunistic, false, false); ok {
		t.Fatalf("expected delivery to fail with stamp enforcement")
	}
}

func TestRouterLXMDeliveryBetweenTwoIdentities(t *testing.T) {
	receiverIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("receiver identity: %v", err)
	}
	senderIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("sender identity: %v", err)
	}

	receiverStorage := t.TempDir()
	receiverRouter, err := NewLXMRouter(receiverIdentity, receiverStorage)
	if err != nil {
		t.Fatalf("new receiver router: %v", err)
	}

	receiverDest := receiverRouter.RegisterDeliveryIdentity(receiverIdentity, "receiver", nil)
	if receiverDest == nil {
		t.Fatalf("register receiver delivery identity: nil destination")
	}

	senderDest, err := rns.NewDestination(senderIdentity, rns.DestinationIN, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		t.Fatalf("sender destination: %v", err)
	}

	if err := rns.IdentityRemember(nil, receiverDest.Hash(), receiverIdentity.GetPublicKey(), nil); err != nil {
		t.Fatalf("remember receiver identity: %v", err)
	}
	if err := rns.IdentityRemember(nil, senderDest.Hash(), senderIdentity.GetPublicKey(), nil); err != nil {
		t.Fatalf("remember sender identity: %v", err)
	}

	fields := map[any]any{FieldDebug: "e2e"}
	outbound, err := NewLXMessage(receiverDest, senderDest, "hello receiver", "greeting", fields, MethodOpportunistic, nil, nil, nil, false)
	if err != nil {
		t.Fatalf("new outbound message: %v", err)
	}
	outbound.DeferStamp = true
	if err := outbound.Pack(false); err != nil {
		t.Fatalf("pack outbound: %v", err)
	}

	var delivered *LXMessage
	receiverRouter.RegisterDeliveryCallback(func(msg *LXMessage) {
		delivered = msg
	})

	if ok := receiverRouter.LXMDelivery(outbound.Packed, rns.DestinationSINGLE, nil, nil, MethodOpportunistic, true, false); !ok {
		t.Fatalf("expected delivery to succeed")
	}
	if delivered == nil {
		t.Fatalf("expected delivery callback to receive message")
	}
	if delivered.ContentAsString() != "hello receiver" {
		t.Fatalf("unexpected delivered content: %q", delivered.ContentAsString())
	}
	if delivered.TitleAsString() != "greeting" {
		t.Fatalf("unexpected delivered title: %q", delivered.TitleAsString())
	}
	if len(delivered.Fields) != 1 {
		t.Fatalf("expected one delivered field, got %#v", delivered.Fields)
	}
	fieldPreserved := false
	for _, v := range delivered.Fields {
		if v == "e2e" {
			fieldPreserved = true
			break
		}
	}
	if !fieldPreserved {
		t.Fatalf("expected delivered fields to preserve payload, got %#v", delivered.Fields)
	}
	if string(delivered.DestinationHash) != string(receiverDest.Hash()) {
		t.Fatalf("unexpected destination hash on delivered message")
	}
	if string(delivered.SourceHash) != string(senderDest.Hash()) {
		t.Fatalf("unexpected source hash on delivered message")
	}
	if !delivered.Incoming {
		t.Fatalf("expected delivered message to be marked incoming")
	}
	if !delivered.SignatureValidated {
		t.Fatalf("expected delivered message signature to validate")
	}
	if !delivered.TransportEncrypted || delivered.TransportEncryption != EncryptionDescriptionEC {
		t.Fatalf("expected single-destination delivery to be transport-encrypted with %s", EncryptionDescriptionEC)
	}
	if receiverRouter.HasMessage(delivered.Hash) == false {
		t.Fatalf("expected delivered message hash to be tracked as locally delivered")
	}
}

func TestRouterDeliveryPacketBetweenTwoIdentities(t *testing.T) {
	receiverIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("receiver identity: %v", err)
	}
	senderIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("sender identity: %v", err)
	}

	receiverRouter, err := NewLXMRouter(receiverIdentity, t.TempDir())
	if err != nil {
		t.Fatalf("new receiver router: %v", err)
	}

	receiverDest := receiverRouter.RegisterDeliveryIdentity(receiverIdentity, "receiver", nil)
	if receiverDest == nil {
		t.Fatalf("register receiver delivery identity: nil destination")
	}

	senderDest, err := rns.NewDestination(senderIdentity, rns.DestinationIN, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		t.Fatalf("sender destination: %v", err)
	}

	if err := rns.IdentityRemember(nil, receiverDest.Hash(), receiverIdentity.GetPublicKey(), nil); err != nil {
		t.Fatalf("remember receiver identity: %v", err)
	}
	if err := rns.IdentityRemember(nil, senderDest.Hash(), senderIdentity.GetPublicKey(), nil); err != nil {
		t.Fatalf("remember sender identity: %v", err)
	}

	outbound, err := NewLXMessage(receiverDest, senderDest, "hello via packet", "packet", nil, MethodOpportunistic, nil, nil, nil, false)
	if err != nil {
		t.Fatalf("new outbound message: %v", err)
	}
	outbound.DeferStamp = true
	if err := outbound.Pack(false); err != nil {
		t.Fatalf("pack outbound: %v", err)
	}

	var delivered *LXMessage
	receiverRouter.RegisterDeliveryCallback(func(msg *LXMessage) {
		delivered = msg
	})

	packetPayload := append([]byte{}, outbound.Packed[DestinationLength:]...)
	packet := rns.NewPacket(receiverDest, packetPayload)
	if packet == nil {
		t.Fatalf("new packet: nil")
	}
	packet.DestinationType = byte(rns.DestinationSINGLE)
	rssi := -73.0
	snr := 11.5
	q := 0.87
	packet.RSSI = &rssi
	packet.SNR = &snr
	packet.Q = &q

	receiverRouter.DeliveryPacket(packetPayload, packet)

	if delivered == nil {
		t.Fatalf("expected delivery callback to receive message")
	}
	if delivered.ContentAsString() != "hello via packet" {
		t.Fatalf("unexpected delivered content: %q", delivered.ContentAsString())
	}
	if delivered.TitleAsString() != "packet" {
		t.Fatalf("unexpected delivered title: %q", delivered.TitleAsString())
	}
	if delivered.RSSI == nil || *delivered.RSSI != rssi {
		t.Fatalf("expected delivered RSSI %.1f, got %#v", rssi, delivered.RSSI)
	}
	if delivered.SNR == nil || *delivered.SNR != snr {
		t.Fatalf("expected delivered SNR %.1f, got %#v", snr, delivered.SNR)
	}
	if delivered.Q == nil || *delivered.Q != q {
		t.Fatalf("expected delivered Q %.2f, got %#v", q, delivered.Q)
	}
	if string(delivered.DestinationHash) != string(receiverDest.Hash()) {
		t.Fatalf("unexpected destination hash on delivered message")
	}
	if string(delivered.SourceHash) != string(senderDest.Hash()) {
		t.Fatalf("unexpected source hash on delivered message")
	}
	if !delivered.SignatureValidated {
		t.Fatalf("expected delivered message signature to validate")
	}
}

func TestRouterDeliveryLinkEstablishedDirectCallbackBetweenTwoIdentities(t *testing.T) {
	receiverIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("receiver identity: %v", err)
	}
	senderIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("sender identity: %v", err)
	}

	receiverRouter, err := NewLXMRouter(receiverIdentity, t.TempDir())
	if err != nil {
		t.Fatalf("new receiver router: %v", err)
	}

	receiverDest := receiverRouter.RegisterDeliveryIdentity(receiverIdentity, "receiver", nil)
	if receiverDest == nil {
		t.Fatalf("register receiver delivery identity: nil destination")
	}

	senderDest, err := rns.NewDestination(senderIdentity, rns.DestinationIN, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		t.Fatalf("sender destination: %v", err)
	}

	if err := rns.IdentityRemember(nil, receiverDest.Hash(), receiverIdentity.GetPublicKey(), nil); err != nil {
		t.Fatalf("remember receiver identity: %v", err)
	}
	if err := rns.IdentityRemember(nil, senderDest.Hash(), senderIdentity.GetPublicKey(), nil); err != nil {
		t.Fatalf("remember sender identity: %v", err)
	}

	outbound, err := NewLXMessage(receiverDest, senderDest, "hello direct", "direct", nil, MethodDirect, nil, nil, nil, false)
	if err != nil {
		t.Fatalf("new outbound message: %v", err)
	}
	outbound.DeferStamp = true
	if err := outbound.Pack(false); err != nil {
		t.Fatalf("pack outbound: %v", err)
	}

	link, err := rns.NewLink(nil, receiverDest, rns.LinkModeAES256CBC, nil, nil)
	if err != nil {
		t.Fatalf("new incoming link: %v", err)
	}
	receiverRouter.DeliveryLinkEstablished(link)
	packetCallback := linkPacketCallbackForTest(t, link)

	var delivered *LXMessage
	receiverRouter.RegisterDeliveryCallback(func(msg *LXMessage) {
		delivered = msg
	})

	packet := &rns.Packet{
		Destination:     receiverDest,
		DestinationType: byte(rns.DestinationLINK),
		PacketType:      rns.PacketTypeData,
		Context:         rns.PacketCtxNone,
	}
	rssi := -61.0
	snr := 8.0
	q := 0.42
	packet.RSSI = &rssi
	packet.SNR = &snr
	packet.Q = &q

	packetCallback(outbound.Packed, packet)

	if delivered == nil {
		t.Fatalf("expected delivery callback to receive message")
	}
	if delivered.Method != MethodDirect {
		t.Fatalf("expected delivered method %d, got %d", MethodDirect, delivered.Method)
	}
	if delivered.ContentAsString() != "hello direct" {
		t.Fatalf("unexpected delivered content: %q", delivered.ContentAsString())
	}
	if delivered.TitleAsString() != "direct" {
		t.Fatalf("unexpected delivered title: %q", delivered.TitleAsString())
	}
	if delivered.RSSI == nil || *delivered.RSSI != rssi {
		t.Fatalf("expected delivered RSSI %.1f, got %#v", rssi, delivered.RSSI)
	}
	if delivered.SNR == nil || *delivered.SNR != snr {
		t.Fatalf("expected delivered SNR %.1f, got %#v", snr, delivered.SNR)
	}
	if delivered.Q == nil || *delivered.Q != q {
		t.Fatalf("expected delivered Q %.2f, got %#v", q, delivered.Q)
	}
	if delivered.TransportEncryption != EncryptionDescriptionEC {
		t.Fatalf("expected direct delivery encryption %q, got %q", EncryptionDescriptionEC, delivered.TransportEncryption)
	}
	if !delivered.SignatureValidated {
		t.Fatalf("expected delivered message signature to validate")
	}
}

func TestRouterAnnounceDrivenMessageDeliveryBetweenTwoIdentities(t *testing.T) {
	oldTransport := rns.Transport
	oldTransportIdentity := rns.TransportIdentity
	oldOwner := rns.Owner
	t.Cleanup(func() {
		rns.Transport = oldTransport
		rns.TransportIdentity = oldTransportIdentity
		rns.Owner = oldOwner
	})

	transportIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("transport identity: %v", err)
	}
	rns.TransportIdentity = transportIdentity
	rns.Owner = &rns.Reticulum{StoragePath: t.TempDir()}
	rns.Transport = &e2eLoopbackTransport{
		ifc: &rns.Interface{Name: "e2e0", IN: true, OUT: true, Online: true},
	}

	receiverIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("receiver identity: %v", err)
	}
	senderIdentity, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("sender identity: %v", err)
	}

	receiverRouter, err := NewLXMRouter(receiverIdentity, t.TempDir())
	if err != nil {
		t.Fatalf("new receiver router: %v", err)
	}
	senderRouter, err := NewLXMRouter(senderIdentity, t.TempDir())
	if err != nil {
		t.Fatalf("new sender router: %v", err)
	}

	senderDest, err := rns.NewDestination(senderIdentity, rns.DestinationIN, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		t.Fatalf("sender destination: %v", err)
	}

	deliveredCh := make(chan *LXMessage, 1)
	receiverRouter.RegisterDeliveryCallback(func(msg *LXMessage) {
		select {
		case deliveredCh <- msg:
		default:
		}
	})

	stampCost := 7
	announceDest, err := rns.NewDestination(receiverIdentity, rns.DestinationIN, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		t.Fatalf("announce destination: %v", err)
	}
	receiverRouter.DeliveryConfigs[string(announceDest.Hash())] = &deliveryConfig{
		DisplayName: "receiver",
		StampCost:   &stampCost,
	}
	announcePacket := announceDest.Announce(receiverRouter.GetAnnounceAppData(announceDest.Hash()), false, nil, nil, false)
	if announcePacket == nil {
		t.Fatalf("expected announce packet")
	}
	if err := announcePacket.Pack(); err != nil {
		t.Fatalf("pack announce: %v", err)
	}
	for i, d := range rns.Destinations {
		if d == announceDest {
			rns.Destinations = append(rns.Destinations[:i], rns.Destinations[i+1:]...)
			break
		}
	}
	rns.Inbound(append([]byte(nil), announcePacket.Raw...), nil)

	deadline := time.Now().Add(2 * time.Second)
	for !rns.TransportHasPath(announceDest.Hash()) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !rns.TransportHasPath(announceDest.Hash()) {
		t.Fatalf("expected receiver announce to create a transport path")
	}
	if entry, ok := senderRouter.OutboundStampCosts[string(announceDest.Hash())]; !ok {
		t.Fatalf("expected sender router to learn receiver delivery announce metadata")
	} else if entry.Cost != stampCost {
		t.Fatalf("expected learned stamp cost %d, got %d", stampCost, entry.Cost)
	}
	if recalled := rns.IdentityRecall(announceDest.Hash()); recalled == nil {
		t.Fatalf("expected receiver identity to be remembered after announce")
	}

	receiverDest, err := rns.NewDestination(receiverIdentity, rns.DestinationIN, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		t.Fatalf("receiver destination: %v", err)
	}
	receiverDest.SetPacketCallback(receiverRouter.DeliveryPacket)
	receiverDest.SetLinkEstablishedCallback(receiverRouter.DeliveryLinkEstablished)
	receiverRouter.DeliveryDestinations[string(receiverDest.Hash())] = receiverDest
	receiverRouter.DeliveryConfigs[string(receiverDest.Hash())] = &deliveryConfig{
		DisplayName: "receiver",
		StampCost:   nil,
	}

	outbound, err := NewLXMessage(receiverDest, senderDest, "hello after announce", "announced", nil, MethodOpportunistic, nil, nil, nil, false)
	if err != nil {
		t.Fatalf("new outbound message: %v", err)
	}
	outbound.DeferStamp = true
	if err := outbound.Pack(false); err != nil {
		t.Fatalf("pack outbound: %v", err)
	}
	outbound.Send()

	select {
	case delivered := <-deliveredCh:
		if delivered.Method != MethodOpportunistic {
			t.Fatalf("expected delivered method %d, got %d", MethodOpportunistic, delivered.Method)
		}
		if delivered.ContentAsString() != "hello after announce" {
			t.Fatalf("unexpected delivered content: %q", delivered.ContentAsString())
		}
		if delivered.TitleAsString() != "announced" {
			t.Fatalf("unexpected delivered title: %q", delivered.TitleAsString())
		}
		if string(delivered.SourceHash) != string(senderDest.Hash()) {
			t.Fatalf("unexpected source hash on delivered message")
		}
		if string(delivered.DestinationHash) != string(receiverDest.Hash()) {
			t.Fatalf("unexpected destination hash on delivered message")
		}
		if !delivered.SignatureValidated {
			t.Fatalf("expected delivered message signature to validate")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for announce-driven delivery")
	}
}
