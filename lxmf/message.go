package lxmf

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/svanichkin/go-reticulum/rns"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

const (
	MessageGenerating = 0x00
	MessageOutbound   = 0x01
	MessageSending    = 0x02
	MessageSent       = 0x04
	MessageDelivered  = 0x08
	MessageRejected   = 0xFD
	MessageCancelled  = 0xFE
	MessageFailed     = 0xFF
)

const (
	RepresentationUnknown  = 0x00
	RepresentationPacket   = 0x01
	RepresentationResource = 0x02
)

const (
	MethodUnknown       = 0x00
	MethodOpportunistic = 0x01
	MethodDirect        = 0x02
	MethodPropagated    = 0x03
	MethodPaper         = 0x05
)

const (
	UnverifiedSourceUnknown    = 0x01
	UnverifiedSignatureInvalid = 0x02
)

const (
	DestinationLength = rns.ReticulumTruncatedHashLength / 8
	SignatureLength   = rns.SigLengthBytes
	TicketLength      = rns.ReticulumTruncatedHashLength / 8

	TicketExpiry   = 21 * 24 * 60 * 60
	TicketGrace    = 5 * 24 * 60 * 60
	TicketRenew    = 14 * 24 * 60 * 60
	TicketInterval = 1 * 24 * 60 * 60
	CostTicket     = 0x100

	TimestampSize  = 8
	StructOverhead = 8
	LXMFOverhead   = 2*DestinationLength + SignatureLength + TimestampSize + StructOverhead

	EncryptionDescriptionAES         = "AES-128"
	EncryptionDescriptionEC          = "Curve25519"
	EncryptionDescriptionUnencrypted = "Unencrypted"

	URISchema         = "lxm"
	QRErrorCorrection = "ERROR_CORRECT_L"
	QRBorder          = 1
	QRMaxStorage      = 2953
	PaperMDU          = ((QRMaxStorage - (len(URISchema) + len("://"))) * 6) / 8
)

var (
	EncryptedPacketMDU        = rns.PacketEncryptedMDU + TimestampSize
	EncryptedPacketMaxContent = EncryptedPacketMDU - LXMFOverhead + DestinationLength
	LinkPacketMDU             = rns.MDU
	LinkPacketMaxContent      = LinkPacketMDU - LXMFOverhead
	PlainPacketMDU            = rns.PacketPlainMDU
	PlainPacketMaxContent     = PlainPacketMDU - LXMFOverhead + DestinationLength
)

type QRGenerator func(data string, errorCorrection string, border int) (any, error)

var qrGenerator QRGenerator

func RegisterQRGenerator(generator QRGenerator) {
	qrGenerator = generator
}

type LXMessage struct {
	destination *rns.Destination
	source      *rns.Destination

	DestinationHash []byte
	SourceHash      []byte
	Title           []byte
	Content         []byte
	Fields          map[any]any

	Payload     []any
	Timestamp   float64
	Signature   []byte
	Hash        []byte
	MessageID   []byte
	TransientID []byte
	Packed      []byte
	PackedSize  int

	State  byte
	Method byte

	Progress float64
	RSSI     *float64
	SNR      *float64
	Q        *float64

	Stamp                   []byte
	StampCost               *int
	StampValue              *int
	StampValid              bool
	StampChecked            bool
	PropagationStamp        []byte
	PropagationStampValue   *int
	PropagationStampValid   bool
	PropagationTargetCost   *int
	DeferStamp              bool
	DeferPropagationStamp   bool
	OutboundTicket          []byte
	IncludeTicket           bool
	PropagationPacked       []byte
	PaperPacked             []byte
	Incoming                bool
	SignatureValidated      bool
	UnverifiedReason        byte
	RatchetID               []byte
	Representation          byte
	DesiredMethod           byte
	DeliveryAttempts        int
	NextDeliveryAttempt     int64
	TransportEncrypted      bool
	TransportEncryption     string
	PacketRepresentation    *rns.Packet
	ResourceRepresentation  *rns.Resource
	DeliveryDestination     any
	DeliveryCallback        func(*LXMessage)
	FailedCallback          func(*LXMessage)
	PNEncryptedData         []byte
	DeferredStampGenerating bool
}

func NewLXMessage(destination, source *rns.Destination, content, title string, fields map[any]any, desiredMethod byte, destinationHash, sourceHash []byte, stampCost *int, includeTicket bool) (*LXMessage, error) {
	msg := &LXMessage{
		StampCost:             stampCost,
		IncludeTicket:         includeTicket,
		State:                 MessageGenerating,
		Method:                MethodUnknown,
		Representation:        RepresentationUnknown,
		DesiredMethod:         desiredMethod,
		DeferStamp:            true,
		DeferPropagationStamp: true,
	}

	if destination != nil {
		msg.destination = destination
		msg.DestinationHash = destination.Hash()
	} else if destinationHash != nil {
		msg.DestinationHash = copyBytes(destinationHash)
	} else {
		return nil, errors.New("LXMessage initialised with invalid destination")
	}

	if source != nil {
		msg.source = source
		msg.SourceHash = source.Hash()
	} else if sourceHash != nil {
		msg.SourceHash = copyBytes(sourceHash)
	} else {
		return nil, errors.New("LXMessage initialised with invalid source")
	}

	if title != "" {
		msg.SetTitleFromString(title)
	} else {
		msg.SetTitleFromString("")
	}
	if content != "" {
		msg.SetContentFromString(content)
	} else {
		msg.SetContentFromString("")
	}

	msg.SetFields(fields)
	return msg, nil
}

func (m *LXMessage) String() string {
	if len(m.Hash) > 0 {
		return fmt.Sprintf("<LXMessage %s>", rns.HexRep(m.Hash, false))
	}
	return "<LXMessage>"
}

func (m *LXMessage) SetTitleFromString(title string) {
	m.Title = []byte(title)
}

func (m *LXMessage) SetTitleFromBytes(title []byte) {
	m.Title = copyBytes(title)
}

func (m *LXMessage) TitleAsString() string {
	return string(m.Title)
}

func (m *LXMessage) SetContentFromString(content string) {
	m.Content = []byte(content)
}

func (m *LXMessage) SetContentFromBytes(content []byte) {
	m.Content = copyBytes(content)
}

func (m *LXMessage) ContentAsString() string {
	return string(m.Content)
}

func (m *LXMessage) SetFields(fields map[any]any) {
	if fields == nil {
		m.Fields = map[any]any{}
		return
	}
	m.Fields = fields
}

func (m *LXMessage) Destination() *rns.Destination {
	return m.destination
}

func (m *LXMessage) Source() *rns.Destination {
	return m.source
}

func (m *LXMessage) SetDestination(destination *rns.Destination) error {
	if m.destination != nil {
		return errors.New("cannot reassign destination on LXMessage")
	}
	if destination == nil {
		return errors.New("invalid destination set on LXMessage")
	}
	m.destination = destination
	m.DestinationHash = destination.Hash()
	return nil
}

func (m *LXMessage) SetSource(source *rns.Destination) error {
	if m.source != nil {
		return errors.New("cannot reassign source on LXMessage")
	}
	if source == nil {
		return errors.New("invalid source set on LXMessage")
	}
	m.source = source
	m.SourceHash = source.Hash()
	return nil
}

func (m *LXMessage) SetDeliveryDestination(dest any) {
	m.DeliveryDestination = dest
}

func (m *LXMessage) RegisterDeliveryCallback(cb func(*LXMessage)) {
	m.DeliveryCallback = cb
}

func (m *LXMessage) RegisterFailedCallback(cb func(*LXMessage)) {
	m.FailedCallback = cb
}

func (m *LXMessage) ValidateStamp(targetCost int, tickets [][]byte) bool {
	if tickets != nil {
		for _, ticket := range tickets {
			if len(ticket) == 0 {
				continue
			}
			try := rns.TruncatedHash(append(append([]byte{}, ticket...), m.MessageID...))
			if m.Stamp != nil && bytes.Equal(m.Stamp, try) {
				tv := CostTicket
				m.StampValue = &tv
				return true
			}
		}
	}

	if m.Stamp == nil {
		return false
	}
	workblock := StampWorkblock(m.MessageID, WorkblockExpandRounds)
	if StampValid(m.Stamp, targetCost, workblock) {
		value := StampValue(workblock, m.Stamp)
		m.StampValue = &value
		return true
	}
	return false
}

func (m *LXMessage) GetStamp(timeout *time.Duration) []byte {
	if len(m.OutboundTicket) == TicketLength {
		generated := rns.TruncatedHash(append(append([]byte{}, m.OutboundTicket...), m.MessageID...))
		v := CostTicket
		m.StampValue = &v
		return generated
	}

	if m.StampCost == nil {
		m.StampValue = nil
		return nil
	}

	if m.Stamp != nil {
		return m.Stamp
	}

	generated, value := generateStampWithTimeout(m.MessageID, *m.StampCost, WorkblockExpandRounds, timeout)
	if generated != nil {
		m.StampValue = &value
		m.StampValid = true
		return generated
	}
	return nil
}

func (m *LXMessage) GetPropagationStamp(targetCost int, timeout *time.Duration) []byte {
	if m.PropagationStamp != nil {
		return m.PropagationStamp
	}

	m.PropagationTargetCost = &targetCost
	if m.PropagationTargetCost == nil {
		panic("Cannot generate propagation stamp without configured target propagation cost")
	}

	if len(m.TransientID) == 0 {
		_ = m.Pack(false)
	}
	generated, value := generateStampWithTimeout(m.TransientID, targetCost, WorkblockExpandRoundsPN, timeout)
	if generated != nil {
		m.PropagationStamp = generated
		m.PropagationStampValue = &value
		m.PropagationStampValid = true
		return generated
	}
	return nil
}

func (m *LXMessage) Pack(payloadUpdated bool) error {
	if len(m.Packed) > 0 {
		return fmt.Errorf("attempt to re-pack LXMessage %s that was already packed", m.String())
	}
	if m.Timestamp == 0 {
		m.Timestamp = nowSeconds()
	}

	m.PropagationPacked = nil
	m.PaperPacked = nil

	m.Payload = []any{m.Timestamp, m.Title, m.Content, m.Fields}

	hashedPart := make([]byte, 0, len(m.DestinationHash)+len(m.SourceHash))
	hashedPart = append(hashedPart, m.DestinationHash...)
	hashedPart = append(hashedPart, m.SourceHash...)
	packedPayload, err := umsgpack.Packb(m.Payload)
	if err != nil {
		return err
	}
	hashedPart = append(hashedPart, packedPayload...)
	m.Hash = rns.FullHash(hashedPart)
	m.MessageID = copyBytes(m.Hash)

	if !m.DeferStamp {
		m.Stamp = m.GetStamp(nil)
		if m.Stamp != nil {
			m.Payload = append(m.Payload, m.Stamp)
			packedPayload, err = umsgpack.Packb(m.Payload)
			if err != nil {
				return err
			}
		}
	}

	signedPart := append(append([]byte{}, hashedPart...), m.Hash...)
	m.Signature = m.source.Sign(signedPart)
	m.SignatureValidated = true

	packedPayload, err = umsgpack.Packb(m.Payload)
	if err != nil {
		return err
	}

	m.Packed = make([]byte, 0, len(m.DestinationHash)+len(m.SourceHash)+len(m.Signature)+len(packedPayload))
	m.Packed = append(m.Packed, m.DestinationHash...)
	m.Packed = append(m.Packed, m.SourceHash...)
	m.Packed = append(m.Packed, m.Signature...)
	m.Packed = append(m.Packed, packedPayload...)
	m.PackedSize = len(m.Packed)
	contentSize := len(packedPayload) - TimestampSize - StructOverhead

	if m.DesiredMethod == 0 {
		m.DesiredMethod = MethodDirect
	}

	if m.DesiredMethod == MethodOpportunistic {
		if m.destination != nil && m.destination.Type == rns.DestinationSINGLE {
			if contentSize > EncryptedPacketMaxContent {
				rns.Log(fmt.Sprintf("Opportunistic delivery was requested for %s, but content of length %d exceeds packet size limit. Falling back to link-based delivery.", m.String(), contentSize), rns.LOG_DEBUG)
				m.DesiredMethod = MethodDirect
			}
		}
	}

	switch m.DesiredMethod {
	case MethodOpportunistic:
		var singlePacketContentLimit int
		switch {
		case m.destination != nil && m.destination.Type == rns.DestinationSINGLE:
			singlePacketContentLimit = EncryptedPacketMaxContent
		case m.destination != nil && m.destination.Type == rns.DestinationPLAIN:
			singlePacketContentLimit = PlainPacketMaxContent
		default:
			singlePacketContentLimit = EncryptedPacketMaxContent
		}
		if contentSize > singlePacketContentLimit {
			return fmt.Errorf("LXMessage desired opportunistic delivery method, but content of length %d exceeds single-packet content limit of %d", contentSize, singlePacketContentLimit)
		}
		m.Method = MethodOpportunistic
		m.Representation = RepresentationPacket
		m.DeliveryDestination = m.destination
	case MethodDirect:
		singlePacketContentLimit := LinkPacketMaxContent
		if contentSize <= singlePacketContentLimit {
			m.Method = m.DesiredMethod
			m.Representation = RepresentationPacket
		} else {
			m.Method = m.DesiredMethod
			m.Representation = RepresentationResource
		}
	case MethodPropagated:
		singlePacketContentLimit := LinkPacketMaxContent
		if m.PNEncryptedData == nil || payloadUpdated {
			m.PNEncryptedData = m.destination.Encrypt(m.Packed[DestinationLength:])
			m.setRatchetFromDestination()
		}
		lxmfData := append(append([]byte{}, m.Packed[:DestinationLength]...), m.PNEncryptedData...)
		m.TransientID = rns.FullHash(lxmfData)
		if m.PropagationStamp != nil {
			lxmfData = append(lxmfData, m.PropagationStamp...)
		}
		packed, err := umsgpack.Packb([]any{nowSeconds(), []any{lxmfData}})
		if err != nil {
			return err
		}
		m.PropagationPacked = packed
		contentSize = len(m.PropagationPacked)
		if contentSize <= singlePacketContentLimit {
			m.Method = m.DesiredMethod
			m.Representation = RepresentationPacket
		} else {
			m.Method = m.DesiredMethod
			m.Representation = RepresentationResource
		}
	case MethodPaper:
		paperContentLimit := PaperMDU
		encrypted := m.destination.Encrypt(m.Packed[DestinationLength:])
		m.setRatchetFromDestination()
		m.PaperPacked = append(append([]byte{}, m.Packed[:DestinationLength]...), encrypted...)
		contentSize = len(m.PaperPacked)
		if contentSize <= paperContentLimit {
			m.Method = m.DesiredMethod
			m.Representation = MethodPaper
		} else {
			return errors.New("LXMessage desired paper delivery method, but content exceeds paper message maximum size")
		}
	default:
		return errors.New("invalid delivery method")
	}

	return nil
}

func (m *LXMessage) Send() {
	m.DetermineTransportEncryption()

	switch m.Method {
	case MethodOpportunistic:
		pkt := m.asPacket()
		receipt := pkt.Send()
		m.setRatchetFromPacket(pkt)
		if receipt != nil {
			receipt.SetDeliveryCallback(func(_ *rns.PacketReceipt) { m.markDelivered(nil) })
		}
		m.Progress = 0.50
		m.State = MessageSent
	case MethodDirect:
		m.State = MessageSending
		switch m.Representation {
		case RepresentationPacket:
			pkt := m.asPacket()
			receipt := pkt.Send()
			m.setRatchetFromLink()
			if receipt != nil {
				receipt.SetDeliveryCallback(func(_ *rns.PacketReceipt) { m.markDelivered(nil) })
				receipt.SetTimeoutCallback(m.linkPacketTimedOut)
				m.Progress = 0.50
			} else {
				m.teardownDeliveryDestination()
			}
		case RepresentationResource:
			m.ResourceRepresentation = m.asResource()
			m.setRatchetFromLink()
			m.Progress = 0.10
		}
	case MethodPropagated:
		m.State = MessageSending
		switch m.Representation {
		case RepresentationPacket:
			pkt := m.asPacket()
			receipt := pkt.Send()
			if receipt != nil {
				receipt.SetDeliveryCallback(func(_ *rns.PacketReceipt) { m.markPropagated(nil) })
				receipt.SetTimeoutCallback(m.linkPacketTimedOut)
				m.Progress = 0.50
			} else {
				m.teardownDeliveryDestination()
			}
		case RepresentationResource:
			m.ResourceRepresentation = m.asResource()
			m.Progress = 0.10
		}
	}
}

func (m *LXMessage) DetermineTransportEncryption() {
	switch m.Method {
	case MethodOpportunistic:
		m.setEncryptionForDestination()
	case MethodDirect:
		m.TransportEncrypted = true
		m.TransportEncryption = EncryptionDescriptionEC
	case MethodPropagated:
		m.setEncryptionForDestination()
	case MethodPaper:
		m.setEncryptionForDestination()
	default:
		m.TransportEncrypted = false
		m.TransportEncryption = EncryptionDescriptionUnencrypted
	}
}

func (m *LXMessage) setEncryptionForDestination() {
	if m.destination == nil {
		m.TransportEncrypted = false
		m.TransportEncryption = EncryptionDescriptionUnencrypted
		return
	}
	switch m.destination.Type {
	case rns.DestinationSINGLE:
		m.TransportEncrypted = true
		m.TransportEncryption = EncryptionDescriptionEC
	case rns.DestinationGROUP:
		m.TransportEncrypted = true
		m.TransportEncryption = EncryptionDescriptionAES
	default:
		m.TransportEncrypted = false
		m.TransportEncryption = EncryptionDescriptionUnencrypted
	}
}

func (m *LXMessage) markDelivered(_ *rns.PacketReceipt) {
	rns.Log("Received delivery notification for "+m.String(), rns.LOG_DEBUG)
	m.State = MessageDelivered
	m.Progress = 1.0
	m.invokeDeliveryCallback()
}

func (m *LXMessage) markPropagated(_ *rns.PacketReceipt) {
	rns.Log("Received propagation success notification for "+m.String(), rns.LOG_DEBUG)
	m.State = MessageSent
	m.Progress = 1.0
	m.invokeDeliveryCallback()
}

func (m *LXMessage) markPaperGenerated(_ *rns.PacketReceipt) {
	rns.Log("Paper message generation succeeded for "+m.String(), rns.LOG_DEBUG)
	m.State = MethodPaper
	m.Progress = 1.0
	m.invokeDeliveryCallback()
}

func (m *LXMessage) resourceConcluded(res *rns.Resource) {
	if res == nil {
		return
	}
	if res.Status() == rns.ResourceComplete {
		m.markDelivered(nil)
		return
	}
	if res.Status() == rns.ResourceRejected {
		m.State = MessageRejected
		return
	}
	if m.State != MessageCancelled && res.Link() != nil {
		res.Link().Teardown()
		m.State = MessageOutbound
	}
}

func (m *LXMessage) propagationResourceConcluded(res *rns.Resource) {
	if res == nil {
		return
	}
	if res.Status() == rns.ResourceComplete {
		m.markPropagated(nil)
		return
	}
	if m.State != MessageCancelled && res.Link() != nil {
		res.Link().Teardown()
		m.State = MessageOutbound
	}
}

func (m *LXMessage) linkPacketTimedOut(receipt *rns.PacketReceipt) {
	if m.State == MessageCancelled {
		return
	}
	if receipt != nil {
		if receipt.Link != nil {
			receipt.Link.Teardown()
		}
	}
	m.State = MessageOutbound
}

func (m *LXMessage) updateTransferProgress(res *rns.Resource) {
	if res == nil {
		return
	}
	m.Progress = 0.10 + (res.GetProgress() * 0.90)
}

func (m *LXMessage) asPacket() *rns.Packet {
	if len(m.Packed) == 0 {
		_ = m.Pack(false)
	}
	if m.DeliveryDestination == nil {
		panic("can't synthesize packet for LXMF message before delivery destination is known")
	}
	switch m.Method {
	case MethodOpportunistic:
		return rns.NewPacket(m.DeliveryDestination, m.Packed[DestinationLength:])
	case MethodDirect:
		return rns.NewPacket(m.DeliveryDestination, m.Packed)
	case MethodPropagated:
		return rns.NewPacket(m.DeliveryDestination, m.PropagationPacked)
	default:
		return nil
	}
}

func (m *LXMessage) asResource() *rns.Resource {
	if len(m.Packed) == 0 {
		_ = m.Pack(false)
	}
	link, ok := m.DeliveryDestination.(*rns.Link)
	if !ok || link == nil {
		panic("can't synthesize resource for LXMF message before delivery destination is known")
	}
	if link.Status != rns.LinkActive {
		panic("tried to synthesize resource for LXMF message on a link that was not active")
	}

	callback := m.resourceConcluded
	if m.Method == MethodPropagated {
		callback = m.propagationResourceConcluded
	}

	res, err := rns.NewResource(
		m.resourcePayload(),
		nil,
		link,
		nil,
		true,
		false,
		callback,
		m.updateTransferProgress,
		nil,
		0,
		nil,
		nil,
		false,
		0,
	)
	if err != nil {
		return nil
	}
	return res
}

func (m *LXMessage) resourcePayload() []byte {
	if m.Method == MethodPropagated {
		return m.PropagationPacked
	}
	return m.Packed
}

func (m *LXMessage) PackedContainer() ([]byte, error) {
	if len(m.Packed) == 0 {
		if err := m.Pack(false); err != nil {
			return nil, err
		}
	}

	container := map[string]any{
		"state":                m.State,
		"lxmf_bytes":           m.Packed,
		"transport_encrypted":  m.TransportEncrypted,
		"transport_encryption": m.TransportEncryption,
		"method":               m.Method,
	}
	return umsgpack.Packb(container)
}

func (m *LXMessage) WriteToDirectory(directoryPath string) (string, error) {
	fileName := rns.HexRep(m.Hash, false)
	filePath := directoryPath + "/" + fileName
	container, err := m.PackedContainer()
	if err != nil {
		rns.Log("Error while writing LXMF message to file \""+filePath+"\". The contained exception was: "+err.Error(), rns.LOG_ERROR)
		return "", err
	}
	if err := os.WriteFile(filePath, container, 0o600); err != nil {
		rns.Log("Error while writing LXMF message to file \""+filePath+"\". The contained exception was: "+err.Error(), rns.LOG_ERROR)
		return "", err
	}
	return filePath, nil
}

func (m *LXMessage) AsURI(finalise bool) (string, error) {
	if len(m.Packed) == 0 {
		if err := m.Pack(false); err != nil {
			return "", err
		}
	}

	if m.DesiredMethod == MethodPaper && len(m.PaperPacked) > 0 {
		encoded := base64.URLEncoding.EncodeToString(m.PaperPacked)
		encoded = trimBase64Padding(encoded)
		lxmURI := URISchema + "://" + encoded
		if finalise {
			m.DetermineTransportEncryption()
			m.markPaperGenerated(nil)
		}
		return lxmURI, nil
	}

	return "", errors.New("attempt to represent LXM with non-paper delivery method as URI")
}

func (m *LXMessage) AsQR() (any, error) {
	if len(m.Packed) == 0 {
		if err := m.Pack(false); err != nil {
			return nil, err
		}
	}

	if m.DesiredMethod == MethodPaper && len(m.PaperPacked) > 0 {
		if qrGenerator == nil {
			rns.Log("Generating QR-code representations of LXMs requires a QR generator to be registered.", rns.LOG_CRITICAL)
			return nil, errors.New("QR output is not available; register a QR generator")
		}
		uri, err := m.AsURI(false)
		if err != nil {
			return nil, err
		}
		qr, err := qrGenerator(uri, QRErrorCorrection, QRBorder)
		if err != nil {
			return nil, err
		}
		m.DetermineTransportEncryption()
		m.markPaperGenerated(nil)
		return qr, nil
	}

	return nil, errors.New("attempt to represent LXM with non-paper delivery method as QR-code")
}

func UnpackFromBytes(lxmfBytes []byte, originalMethod byte) (*LXMessage, error) {
	destinationHash := lxmfBytes[:DestinationLength]
	sourceHash := lxmfBytes[DestinationLength : 2*DestinationLength]
	signature := lxmfBytes[2*DestinationLength : 2*DestinationLength+SignatureLength]
	packedPayload := lxmfBytes[2*DestinationLength+SignatureLength:]

	var unpacked []any
	if err := umsgpack.Unpackb(packedPayload, &unpacked); err != nil {
		return nil, err
	}

	var stamp []byte
	if len(unpacked) > 4 {
		if b, ok := unpacked[4].([]byte); ok {
			stamp = b
		}
		unpacked = unpacked[:4]
		packedPayload, _ = umsgpack.Packb(unpacked)
	}

	hashedPart := append(append([]byte{}, destinationHash...), sourceHash...)
	hashedPart = append(hashedPart, packedPayload...)
	messageHash := rns.FullHash(hashedPart)
	signedPart := append(append([]byte{}, hashedPart...), messageHash...)

	timestamp := floatFromAny(unpacked[0])
	titleBytes := toBytes(unpacked[1])
	contentBytes := toBytes(unpacked[2])
	fields := coerceFields(unpacked[3])

	var destination *rns.Destination
	if destID := rns.IdentityRecall(destinationHash); destID != nil {
		destination, _ = rns.NewDestination(destID, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "delivery")
	}
	var source *rns.Destination
	if sourceID := rns.IdentityRecall(sourceHash); sourceID != nil {
		source, _ = rns.NewDestination(sourceID, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "delivery")
	}

	msg, err := NewLXMessage(destination, source, "", "", fields, originalMethod, destinationHash, sourceHash, nil, false)
	if err != nil {
		return nil, err
	}
	msg.Hash = messageHash
	msg.MessageID = copyBytes(msg.Hash)
	msg.Signature = signature
	msg.Stamp = stamp
	msg.Incoming = true
	msg.Timestamp = timestamp
	msg.Packed = copyBytes(lxmfBytes)
	msg.PackedSize = len(lxmfBytes)
	msg.SetTitleFromBytes(titleBytes)
	msg.SetContentFromBytes(contentBytes)

	if source != nil && source.Identity() != nil {
		if source.Identity().Validate(signature, signedPart) {
			msg.SignatureValidated = true
		} else {
			msg.SignatureValidated = false
			msg.UnverifiedReason = UnverifiedSignatureInvalid
		}
	} else {
		msg.SignatureValidated = false
		msg.UnverifiedReason = UnverifiedSourceUnknown
		rns.Log("Unpacked LXMF message signature could not be validated, since source identity is unknown", rns.LOG_DEBUG)
	}

	return msg, nil
}

func UnpackFromFile(f *os.File) (*LXMessage, error) {
	container := map[string]any{}
	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	if err := umsgpack.Unpackb(buf, &container); err != nil {
		return nil, err
	}

	lxmBytes, ok := container["lxmf_bytes"].([]byte)
	if !ok {
		return nil, errors.New("invalid LXMF container: missing lxmf_bytes")
	}
	msg, err := UnpackFromBytes(lxmBytes, 0)
	if err != nil {
		return nil, err
	}

	if v, ok := container["state"].(int); ok {
		msg.State = byte(v)
	}
	if v, ok := container["transport_encrypted"].(bool); ok {
		msg.TransportEncrypted = v
	}
	if v, ok := container["transport_encryption"].(string); ok {
		msg.TransportEncryption = v
	}
	if v, ok := container["method"].(int); ok {
		msg.Method = byte(v)
	}

	return msg, nil
}

func (m *LXMessage) teardownDeliveryDestination() {
	if m.DeliveryDestination == nil {
		return
	}
	if link, ok := m.DeliveryDestination.(*rns.Link); ok && link != nil {
		link.Teardown()
	}
}

func nowSeconds() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func copyBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func trimBase64Padding(s string) string {
	for len(s) > 0 && s[len(s)-1] == '=' {
		s = s[:len(s)-1]
	}
	return s
}

func (m *LXMessage) invokeDeliveryCallback() {
	if m.DeliveryCallback == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			rns.Log("An error occurred in the external delivery callback for "+m.String()+": "+fmt.Sprint(r), rns.LOG_ERROR)
		}
	}()
	m.DeliveryCallback(m)
}

func (m *LXMessage) setRatchetFromPacket(pkt *rns.Packet) {
	if pkt == nil || len(pkt.RatchetID) == 0 {
		return
	}
	m.RatchetID = copyBytes(pkt.RatchetID)
}

func (m *LXMessage) setRatchetFromLink() {
	link, ok := m.DeliveryDestination.(*rns.Link)
	if !ok || link == nil || len(link.LinkID) == 0 {
		return
	}
	m.RatchetID = copyBytes(link.LinkID)
}

func (m *LXMessage) setRatchetFromDestination() {
	if len(m.DestinationHash) == 0 {
		return
	}
	if ratchetID := rns.IdentityCurrentRatchetID(m.DestinationHash); len(ratchetID) > 0 {
		m.RatchetID = copyBytes(ratchetID)
	}
}

func toBytes(v any) []byte {
	switch t := v.(type) {
	case []byte:
		return copyBytes(t)
	case string:
		return []byte(t)
	default:
		return nil
	}
}

type stampResult struct {
	stamp []byte
	value int
}

func generateStampWithTimeout(messageID []byte, cost int, expandRounds int, timeout *time.Duration) ([]byte, int) {
	if timeout == nil || *timeout <= 0 {
		return GenerateStamp(messageID, cost, expandRounds)
	}
	resultCh := make(chan stampResult, 1)
	go func() {
		stamp, value := GenerateStamp(messageID, cost, expandRounds)
		resultCh <- stampResult{stamp: stamp, value: value}
	}()

	timer := time.NewTimer(*timeout)
	defer timer.Stop()

	select {
	case result := <-resultCh:
		return result.stamp, result.value
	case <-timer.C:
		CancelWork(messageID)
		return nil, 0
	}
}

func floatFromAny(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case uint64:
		return float64(t)
	default:
		return 0
	}
}

func coerceFields(v any) map[any]any {
	switch t := v.(type) {
	case map[any]any:
		return t
	case map[string]any:
		out := make(map[any]any, len(t))
		for k, v := range t {
			out[k] = v
		}
		return out
	default:
		return map[any]any{}
	}
}
