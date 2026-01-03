package lxmf

import (
	crypto_rand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"os/signal"
	"time"
	"syscall"

	"github.com/svanichkin/go-reticulum/rns"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

const (
	MaxDeliveryAttempts = 5
	ProcessingInterval  = 4
	DeliveryRetryWait   = 10
	PathRequestWait     = 7
	MaxPathlessTries    = 1
	LinkMaxInactivity   = 10 * 60
	PLinkMaxInactivity  = 3 * 60

	MessageExpiry   = 30 * 24 * 60 * 60
	StampCostExpiry = 45 * 24 * 60 * 60

	NodeAnnounceDelay = 20

	MaxPeers            = 20
	AutoPeerDefault     = true
	AutoPeerMaxDepth    = 4
	FastestNRandomPool  = 2
	RotationHeadroomPct = 10
	RotationARMax       = 0.5

	PeeringCostDefault         = 18
	MaxPeeringCostDefault      = 26
	PropagationCostMin         = 13
	PropagationCostFlexDefault = 3
	PropagationCostDefault     = 16
	PropagationLimitDefault    = 256
	SyncLimitDefault           = PropagationLimitDefault * 40
	DeliveryLimitDefault       = 1000

	PRPathTimeout   = 10
	PNStampThrottle = 180
)

const (
	PRIdle             = 0x00
	PRPathRequested    = 0x01
	PRLinkEstablishing = 0x02
	PRLinkEstablished  = 0x03
	PRRequestSent      = 0x04
	PRReceiving        = 0x05
	PRResponseReceived = 0x06
	PRComplete         = 0x07
	PRNoPath           = 0xf0
	PRLinkFailed       = 0xf1
	PRTransferFailed   = 0xf2
	PRNoIdentityRcvd   = 0xf3
	PRNoAccess         = 0xf4
	PRFailed           = 0xfe
)

const (
	PRAllMessages = 0x00
)

const (
	DuplicateSignal   = "lxmf_duplicate"
	StatsGetPath      = "/pn/get/stats"
	SyncRequestPath   = "/pn/peer/sync"
	UnpeerRequestPath = "/pn/peer/unpeer"
)

const (
	JobOutboundInterval  = 1
	JobStampsInterval    = 1
	JobLinksInterval     = 1
	JobTransientInterval = 60
	JobStoreInterval     = 120
	JobPeerSyncInterval  = 6
	JobPeerIngestInterval = JobPeerSyncInterval
	JobRotateInterval     = 56 * JobPeerIngestInterval
)

type stampCostEntry struct {
	Cost    int
	Updated int64
}

type deliveryConfig struct {
	DisplayName string
	StampCost   *int
}

type propagationEntry struct {
	DestinationHash []byte
	FilePath        string
	Received        float64
	Size            int64
	HandledPeers    []string
	UnhandledPeers  []string
	StampValue      int
}

type peerDistributionEntry struct {
	TransientID []byte
	FromPeer    []byte
}

type LXMRouter struct {
	PendingInbound       []*LXMessage
	PendingOutbound      []*LXMessage
	FailedOutbound       []*LXMessage
	DeliveryDestinations map[string]*rns.Destination
	DeliveryConfigs      map[string]*deliveryConfig
	DeliveryCallback     func(*LXMessage)

	PrioritisedList    []string
	IgnoredList        []string
	AllowedList        []string
	ControlAllowedList []string
	AuthRequired       bool

	DefaultSyncStrategy int
	ProcessingInbound   bool
	ProcessingCount     int
	Name                string

	PropagationNode                     bool
	PropagationNodeStartTime            int64
	MessagePath                         string
	PropagationEntries                  map[string]*propagationEntry
	PropagationTransferState            int
	PropagationTransferProgress         float64
	PropagationTransferLastResult       *int
	PropagationTransferLastDuplicates   *int
	PropagationTransferMaxMessages      int
	WantsDownloadOnPathAvailableFrom    []byte
	WantsDownloadOnPathAvailableTo      *rns.Identity
	WantsDownloadOnPathAvailableTimeout time.Time
	RetainSyncedOnNode                  bool
	ThrottledPeers                      map[string]int64
	ActivePropagationLinks              []*rns.Link
	PeerDistributionQueue               []peerDistributionEntry
	ValidatedPeerLinks                  map[string]bool
	PrioritiseRotatingUnreachablePeers  bool

	StoragePath string
	RatchetPath string

	MessageStorageLimit             *int
	InformationStorageLimit         *int
	PropagationPerTransferLimit     int
	PropagationPerSyncLimit         int
	DeliveryPerTransferLimit        int
	PropagationStampCost            int
	PropagationStampCostFlexibility int
	PeeringCost                     int
	MaxPeeringCost                  int
	EnforceRatchets                 bool
	enforceStamps                   bool

	AutoPeer         bool
	AutoPeerMaxDepth int
	MaxPeers         int
	FromStaticOnly   bool
	StaticPeers      [][]byte

	Peers            map[string]*LXMPeer
	DirectLinks      map[string]*rns.Link
	BackchannelLinks map[string]*rns.Link
	backchannelIdentified map[*rns.Link]bool

	OutboundStampCosts    map[string]stampCostEntry
	AvailableTickets      *availableTickets
	LocallyDelivered      map[string]int64
	LocallyProcessed      map[string]int64
	PendingDeferredStamps map[string]*LXMessage

	outboundProcessingMu sync.Mutex
	processingOutbound   bool
	costFileMu           sync.Mutex
	ticketFileMu         sync.Mutex
	stampGenMu           sync.Mutex

	Identity                          *rns.Identity
	PropagationDestination            *rns.Destination
	OutboundPropagationNode           []byte
	OutboundPropagationLink           *rns.Link
	OutboundPropagationForMessage     *LXMessage
	ControlDestination                *rns.Destination
	ClientPropagationMessagesReceived int
	ClientPropagationMessagesServed   int
	UnpeeredPropagationIncoming       int
	UnpeeredPropagationRxBytes        int

	exitHandlerRunning bool
}

func NewLXMRouter(identity *rns.Identity, storagePath string) (*LXMRouter, error) {
	if storagePath == "" {
		return nil, errors.New("LXMF cannot be initialised without a storage path")
	}

	var seedBytes [8]byte
	if _, err := crypto_rand.Read(seedBytes[:]); err == nil {
		rand.Seed(int64(binary.LittleEndian.Uint64(seedBytes[:])))
	} else {
		rand.Seed(time.Now().UnixNano())
	}

	if identity == nil {
		id, err := rns.NewIdentity()
		if err != nil {
			return nil, err
		}
		identity = id
	}

	router := &LXMRouter{
		PendingInbound:       []*LXMessage{},
		PendingOutbound:      []*LXMessage{},
		FailedOutbound:       []*LXMessage{},
		DeliveryDestinations: map[string]*rns.Destination{},
		DeliveryConfigs:      map[string]*deliveryConfig{},

		PrioritisedList:    []string{},
		IgnoredList:        []string{},
		AllowedList:        []string{},
		ControlAllowedList: []string{},
		AuthRequired:       false,

		DefaultSyncStrategy: 0,
		ProcessingInbound:   false,
		ProcessingCount:     0,

		PropagationNode:                false,
		PropagationEntries:             map[string]*propagationEntry{},
		PropagationTransferState:       PRIdle,
		PropagationTransferProgress:    0,
		PropagationTransferMaxMessages: PRAllMessages,
		RetainSyncedOnNode:             false,
		ThrottledPeers:                 map[string]int64{},
		ActivePropagationLinks:         []*rns.Link{},
		PeerDistributionQueue:          []peerDistributionEntry{},
		ValidatedPeerLinks:             map[string]bool{},

		StoragePath: storagePath + "/lxmf",
		RatchetPath: storagePath + "/lxmf/ratchets",

		PropagationPerTransferLimit:     PropagationLimitDefault,
		PropagationPerSyncLimit:         SyncLimitDefault,
		DeliveryPerTransferLimit:        DeliveryLimitDefault,
		PropagationStampCost:            PropagationCostDefault,
		PropagationStampCostFlexibility: PropagationCostFlexDefault,
		PeeringCost:                     PeeringCostDefault,
		MaxPeeringCost:                  MaxPeeringCostDefault,
		EnforceRatchets:                 false,
		enforceStamps:                   false,

		AutoPeer:         AutoPeerDefault,
		AutoPeerMaxDepth: AutoPeerMaxDepth,
		MaxPeers:         MaxPeers,
		FromStaticOnly:   false,
		StaticPeers:      [][]byte{},

		Peers:                 map[string]*LXMPeer{},
		DirectLinks:           map[string]*rns.Link{},
		BackchannelLinks:      map[string]*rns.Link{},
		backchannelIdentified: map[*rns.Link]bool{},
		OutboundStampCosts:    map[string]stampCostEntry{},
		AvailableTickets:      newAvailableTickets(),
		LocallyDelivered:      map[string]int64{},
		LocallyProcessed:      map[string]int64{},
		PendingDeferredStamps: map[string]*LXMessage{},

		Identity: identity,
	}

	dest, err := rns.NewDestination(identity, rns.DestinationIN, rns.DestinationSINGLE, AppName, "propagation")
	if err != nil {
		return nil, err
	}
	router.PropagationDestination = dest
	router.PropagationDestination.SetDefaultAppData(router.GetPropagationNodeAppData)

	rns.RegisterAnnounceHandler(NewDeliveryAnnounceHandler(router))
	rns.RegisterAnnounceHandler(NewPropagationAnnounceHandler(router))

	router.loadLocallyDelivered()
	router.loadLocallyProcessed()
	router.loadOutboundStampCosts()
	router.loadAvailableTickets()

	router.InstallExitHandlers()
	go router.JobLoop()

	return router, nil
}

func (r *LXMRouter) Announce(destinationHash []byte, attachedInterface *rns.Interface) {
	if dest, ok := r.DeliveryDestinations[string(destinationHash)]; ok && dest != nil {
		appData := r.GetAnnounceAppData(destinationHash)
		dest.Announce(appData, false, attachedInterface, nil, true)
	}
}

func (r *LXMRouter) GetPropagationNodeAnnounceMetadata() map[any]any {
	meta := map[any]any{}
	if r.Name != "" {
		meta[PNMetaName] = []byte(r.Name)
	}
	return meta
}

func (r *LXMRouter) GetPropagationNodeAppData() []byte {
	metadata := r.GetPropagationNodeAnnounceMetadata()
	nodeState := r.PropagationNode && !r.FromStaticOnly
	stampCost := []any{r.PropagationStampCost, r.PropagationStampCostFlexibility, r.PeeringCost}
	announceData := []any{
		false,
		int(time.Now().Unix()),
		nodeState,
		r.PropagationPerTransferLimit,
		r.PropagationPerSyncLimit,
		stampCost,
		metadata,
	}
	data, err := umsgpack.Packb(announceData)
	if err != nil {
		return nil
	}
	return data
}

func (r *LXMRouter) AnnouncePropagationNode() {
	go func() {
		time.Sleep(NodeAnnounceDelay * time.Second)
		if r.PropagationDestination != nil {
			r.PropagationDestination.Announce(r.GetPropagationNodeAppData(), false, nil, nil, true)
		}
	}()
}

func (r *LXMRouter) RegisterDeliveryIdentity(identity *rns.Identity, displayName string, stampCost *int) *rns.Destination {
	if len(r.DeliveryDestinations) != 0 {
		rns.Log("Currently only one delivery identity is supported per LXMF router instance", rns.LOG_ERROR)
		return nil
	}

	if err := os.MkdirAll(r.RatchetPath, 0o700); err != nil {
		return nil
	}

	dest, err := rns.NewDestination(identity, rns.DestinationIN, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		return nil
	}
	ratchetFile := r.RatchetPath + "/" + rns.HexRep(dest.Hash(), false) + ".ratchets"
	_, _ = dest.EnableRatchets(ratchetFile)
	dest.SetPacketCallback(r.DeliveryPacket)
	dest.SetLinkEstablishedCallback(r.DeliveryLinkEstablished)
	if r.EnforceRatchets {
		dest.EnforceRatchets()
	}

	if displayName != "" {
		dest.SetDefaultAppData(func() []byte {
			return r.GetAnnounceAppData(dest.Hash())
		})
	}

	r.DeliveryDestinations[string(dest.Hash())] = dest
	r.DeliveryConfigs[string(dest.Hash())] = &deliveryConfig{
		DisplayName: displayName,
		StampCost:   stampCost,
	}

	r.SetInboundStampCost(dest.Hash(), stampCost)

	return dest
}

func (r *LXMRouter) RegisterDeliveryCallback(cb func(*LXMessage)) {
	r.DeliveryCallback = cb
}

func (r *LXMRouter) SetInboundStampCost(destinationHash []byte, stampCost *int) bool {
	if dest, ok := r.DeliveryDestinations[string(destinationHash)]; ok && dest != nil {
		cfg := r.DeliveryConfigs[string(destinationHash)]
		if cfg == nil {
			cfg = &deliveryConfig{}
			r.DeliveryConfigs[string(destinationHash)] = cfg
		}
		if stampCost == nil {
			cfg.StampCost = nil
			return true
		}
		if *stampCost < 1 {
			cfg.StampCost = nil
		} else if *stampCost < 255 {
			cfg.StampCost = stampCost
		} else {
			return false
		}
		return true
	}
	return false
}

func (r *LXMRouter) GetAnnounceAppData(destinationHash []byte) []byte {
	if cfg, ok := r.DeliveryConfigs[string(destinationHash)]; ok && cfg != nil {
		var displayName []byte
		if cfg.DisplayName != "" {
			displayName = []byte(cfg.DisplayName)
		}
		var stampCost any
		if cfg.StampCost != nil && *cfg.StampCost > 0 && *cfg.StampCost < 255 {
			stampCost = *cfg.StampCost
		}
		peerData := []any{displayName, stampCost}
		data, err := umsgpack.Packb(peerData)
		if err != nil {
			return nil
		}
		return data
	}
	return nil
}

func (r *LXMRouter) SetAuthentication(required bool) {
	r.AuthRequired = required
}

func (r *LXMRouter) RequiresAuthentication() bool {
	return r.AuthRequired
}

func (r *LXMRouter) Allow(identityHash []byte) error {
	if len(identityHash) != rns.ReticulumTruncatedHashLength/8 {
		return errors.New("allowed identity hash must be 16 bytes")
	}
	key := string(identityHash)
	for _, existing := range r.AllowedList {
		if existing == key {
			return nil
		}
	}
	r.AllowedList = append(r.AllowedList, key)
	return nil
}

func (r *LXMRouter) Disallow(identityHash []byte) error {
	if len(identityHash) != rns.ReticulumTruncatedHashLength/8 {
		return errors.New("disallowed identity hash must be 16 bytes")
	}
	key := string(identityHash)
	for i, existing := range r.AllowedList {
		if existing == key {
			r.AllowedList = append(r.AllowedList[:i], r.AllowedList[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *LXMRouter) AllowControl(identityHash []byte) error {
	if len(identityHash) != rns.ReticulumTruncatedHashLength/8 {
		return errors.New("allowed identity hash must be 16 bytes")
	}
	key := string(identityHash)
	for _, existing := range r.ControlAllowedList {
		if existing == key {
			return nil
		}
	}
	r.ControlAllowedList = append(r.ControlAllowedList, key)
	return nil
}

func (r *LXMRouter) DisallowControl(identityHash []byte) error {
	if len(identityHash) != rns.ReticulumTruncatedHashLength/8 {
		return errors.New("disallowed identity hash must be 16 bytes")
	}
	key := string(identityHash)
	for i, existing := range r.ControlAllowedList {
		if existing == key {
			r.ControlAllowedList = append(r.ControlAllowedList[:i], r.ControlAllowedList[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *LXMRouter) Prioritise(destinationHash []byte) error {
	if len(destinationHash) != rns.ReticulumTruncatedHashLength/8 {
		return errors.New("prioritised destination hash must be 16 bytes")
	}
	key := string(destinationHash)
	for _, existing := range r.PrioritisedList {
		if existing == key {
			return nil
		}
	}
	r.PrioritisedList = append(r.PrioritisedList, key)
	return nil
}

func (r *LXMRouter) Unprioritise(destinationHash []byte) error {
	if len(destinationHash) != rns.ReticulumTruncatedHashLength/8 {
		return errors.New("prioritised destination hash must be 16 bytes")
	}
	key := string(destinationHash)
	for i, existing := range r.PrioritisedList {
		if existing == key {
			r.PrioritisedList = append(r.PrioritisedList[:i], r.PrioritisedList[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *LXMRouter) IgnoreDestination(destinationHash []byte) {
	key := string(destinationHash)
	for _, existing := range r.IgnoredList {
		if existing == key {
			return
		}
	}
	r.IgnoredList = append(r.IgnoredList, key)
}

func (r *LXMRouter) UnignoreDestination(destinationHash []byte) {
	key := string(destinationHash)
	for i, existing := range r.IgnoredList {
		if existing == key {
			r.IgnoredList = append(r.IgnoredList[:i], r.IgnoredList[i+1:]...)
			return
		}
	}
}

func (r *LXMRouter) MessageStorageSize() int64 {
	if !r.PropagationNode {
		return 0
	}
	var total int64
	for _, entry := range r.PropagationEntries {
		if entry != nil {
			total += entry.Size
		}
	}
	return total
}

func (r *LXMRouter) InformationStorageSize() int64 {
	if r.StoragePath == "" {
		return 0
	}
	var total int64
	_ = filepath.WalkDir(r.StoragePath, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

func (r *LXMRouter) SetMessageStorageLimit(kb, mb, gb int) error {
	limit := int64(0)
	if kb > 0 {
		limit += int64(kb) * 1000
	}
	if mb > 0 {
		limit += int64(mb) * 1000 * 1000
	}
	if gb > 0 {
		limit += int64(gb) * 1000 * 1000 * 1000
	}
	if limit == 0 {
		r.MessageStorageLimit = nil
		return nil
	}
	if limit < 0 {
		return errors.New("cannot set LXMF information storage limit")
	}
	limitInt := int(limit)
	r.MessageStorageLimit = &limitInt
	return nil
}

func (r *LXMRouter) SetActivePropagationNode(destinationHash []byte) error {
	return r.SetOutboundPropagationNode(destinationHash)
}

func (r *LXMRouter) SetOutboundPropagationNode(destinationHash []byte) error {
	if len(destinationHash) != rns.ReticulumTruncatedHashLength/8 {
		return errors.New("invalid destination hash for outbound propagation node")
	}
	if r.OutboundPropagationNode != nil && bytesEqual(r.OutboundPropagationNode, destinationHash) {
		return nil
	}
	r.OutboundPropagationNode = copyBytes(destinationHash)
	if r.OutboundPropagationLink != nil {
		r.OutboundPropagationLink.Teardown()
		r.OutboundPropagationLink = nil
	}
	return nil
}

func (r *LXMRouter) GetOutboundPropagationNode() []byte {
	return copyBytes(r.OutboundPropagationNode)
}

func (r *LXMRouter) GetOutboundPropagationCost() *int {
	pnHash := r.GetOutboundPropagationNode()
	if len(pnHash) == 0 {
		return nil
	}
	appData := rns.IdentityRecallAppData(pnHash)
	if PNAnnounceDataIsValid(appData) {
		var config []any
		if err := umsgpack.Unpackb(appData, &config); err == nil && len(config) > 5 {
			if costs, ok := config[5].([]any); ok && len(costs) > 0 {
				if cost, ok := asInt(costs[0]); ok {
					return &cost
				}
			}
		}
	}

	rns.TransportRequestPath(pnHash)
	timeout := time.Now().Add(time.Duration(PathRequestWait) * time.Second)
	for time.Now().Before(timeout) {
		time.Sleep(500 * time.Millisecond)
		appData = rns.IdentityRecallAppData(pnHash)
		if PNAnnounceDataIsValid(appData) {
			var config []any
			if err := umsgpack.Unpackb(appData, &config); err == nil && len(config) > 5 {
				if costs, ok := config[5].([]any); ok && len(costs) > 0 {
					if cost, ok := asInt(costs[0]); ok {
						return &cost
					}
				}
			}
		}
	}
	rns.Log("Propagation node stamp cost still unavailable after path request", rns.LOG_ERROR)
	return nil
}

func (r *LXMRouter) RequestMessagesFromPropagationNode(identity *rns.Identity, maxMessages int) {
	if maxMessages == 0 {
		maxMessages = PRAllMessages
	}

	r.PropagationTransferProgress = 0
	r.PropagationTransferMaxMessages = maxMessages

	if len(r.OutboundPropagationNode) == 0 {
		rns.Log("Cannot request LXMF propagation node sync, no default propagation node configured", rns.LOG_WARNING)
		return
	}

	if r.OutboundPropagationLink != nil && r.OutboundPropagationLink.Status == rns.LinkActive {
		r.PropagationTransferState = PRLinkEstablished
		rns.Log("Requesting message list from propagation node", rns.LOG_DEBUG)
		if identity != nil {
			r.OutboundPropagationLink.Identify(identity)
		}
		r.OutboundPropagationLink.Request(
			MessageGetPath,
			[]any{nil, nil},
			r.MessageListResponse,
			r.MessageGetFailed,
			nil,
			0,
		)
		r.PropagationTransferState = PRRequestSent
		return
	}

	if r.OutboundPropagationLink != nil {
		rns.Log("Waiting for propagation node link to become active", rns.LOG_EXTREME)
		return
	}

	if rns.TransportHasPath(r.OutboundPropagationNode) {
		r.WantsDownloadOnPathAvailableFrom = nil
		r.PropagationTransferState = PRLinkEstablishing
		rns.Log("Establishing link to "+rns.PrettyHexRep(r.OutboundPropagationNode)+" for message download", rns.LOG_DEBUG)
		peerID := rns.IdentityRecall(r.OutboundPropagationNode)
		if peerID == nil {
			rns.Log("Cannot establish propagation link, identity recall failed", rns.LOG_DEBUG)
			return
		}
		dest, err := rns.NewDestination(peerID, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "propagation")
		if err != nil {
			return
		}
		link, err := rns.NewOutgoingLink(dest, rns.LinkModeDefault, func(l *rns.Link) {
			r.OutboundPropagationLink = l
			r.RequestMessagesFromPropagationNode(identity, r.PropagationTransferMaxMessages)
		}, func(l *rns.Link) {
			if r.OutboundPropagationLink == l {
				r.OutboundPropagationLink = nil
			}
		})
		if err != nil {
			return
		}
		r.OutboundPropagationLink = link
		return
	}

	rns.Log("No path known for message download from propagation node "+rns.PrettyHexRep(r.OutboundPropagationNode)+". Requesting path...", rns.LOG_DEBUG)
	rns.TransportRequestPath(r.OutboundPropagationNode)
	r.WantsDownloadOnPathAvailableFrom = copyBytes(r.OutboundPropagationNode)
	r.WantsDownloadOnPathAvailableTo = identity
	r.WantsDownloadOnPathAvailableTimeout = time.Now().Add(time.Duration(PRPathTimeout) * time.Second)
	r.PropagationTransferState = PRPathRequested
	r.RequestMessagesPathJob()
}

func (r *LXMRouter) CancelPropagationNodeRequests() {
	if r.OutboundPropagationLink != nil {
		r.OutboundPropagationLink.Teardown()
		r.OutboundPropagationLink = nil
	}
	r.AcknowledgeSyncCompletion(true, nil)
}

func (r *LXMRouter) RequestMessagesPathJob() {
	go func() {
		pathTimeout := r.WantsDownloadOnPathAvailableTimeout
		for len(r.WantsDownloadOnPathAvailableFrom) > 0 && time.Now().Before(pathTimeout) {
			if rns.TransportHasPath(r.WantsDownloadOnPathAvailableFrom) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		if len(r.WantsDownloadOnPathAvailableFrom) == 0 {
			return
		}

		if rns.TransportHasPath(r.WantsDownloadOnPathAvailableFrom) {
			r.RequestMessagesFromPropagationNode(r.WantsDownloadOnPathAvailableTo, r.PropagationTransferMaxMessages)
			return
		}

		rns.Log("Propagation node path request timed out", rns.LOG_DEBUG)
		r.AcknowledgeSyncCompletion(false, intPtr(PRNoPath))
	}()
}

func (r *LXMRouter) MessageListResponse(receipt *rns.RequestReceipt) {
	if receipt == nil {
		return
	}

	if code, ok := intFromAny(receipt.Response()); ok {
		switch code {
		case PeerErrorNoIdentity:
			rns.Log("Propagation node indicated missing identification on list request, tearing down link.", rns.LOG_DEBUG)
			if r.OutboundPropagationLink != nil {
				r.OutboundPropagationLink.Teardown()
			}
			r.PropagationTransferState = PRNoIdentityRcvd
			return
		case PeerErrorNoAccess:
			rns.Log("Propagation node did not allow list request, tearing down link.", rns.LOG_DEBUG)
			if r.OutboundPropagationLink != nil {
				r.OutboundPropagationLink.Teardown()
			}
			r.PropagationTransferState = PRNoAccess
			return
		}
	}

	response := decodeIDList(receipt.Response())
	if response == nil {
		rns.Log("Invalid message list data received from propagation node", rns.LOG_DEBUG)
		if r.OutboundPropagationLink != nil {
			r.OutboundPropagationLink.Teardown()
		}
		return
	}

	haves := make([][]byte, 0)
	wants := make([][]byte, 0)
	if len(response) > 0 {
		for _, transientID := range response {
			if r.HasMessage(transientID) {
				if !r.RetainSyncedOnNode {
					haves = append(haves, transientID)
				}
			} else {
				if r.PropagationTransferMaxMessages == PRAllMessages || len(wants) < r.PropagationTransferMaxMessages {
					wants = append(wants, transientID)
				}
			}
		}

		msgSuffix := "s"
		if len(wants) == 1 {
			msgSuffix = ""
		}
		rns.Log(fmt.Sprintf("Requesting %d message%s from propagation node", len(wants), msgSuffix), rns.LOG_DEBUG)
		if r.OutboundPropagationLink != nil {
			r.OutboundPropagationLink.Request(
				MessageGetPath,
				[]any{wants, haves, r.DeliveryPerTransferLimit},
				r.MessageGetResponse,
				r.MessageGetFailed,
				r.MessageGetProgress,
				0,
			)
		}
		return
	}

	r.PropagationTransferState = PRComplete
	r.PropagationTransferProgress = 1.0
	result := 0
	r.PropagationTransferLastResult = &result
}

func (r *LXMRouter) MessageGetResponse(receipt *rns.RequestReceipt) {
	if receipt == nil {
		return
	}

	if code, ok := intFromAny(receipt.Response()); ok {
		switch code {
		case PeerErrorNoIdentity:
			rns.Log("Propagation node indicated missing identification on get request, tearing down link.", rns.LOG_DEBUG)
			if r.OutboundPropagationLink != nil {
				r.OutboundPropagationLink.Teardown()
			}
			r.PropagationTransferState = PRNoIdentityRcvd
			return
		case PeerErrorNoAccess:
			rns.Log("Propagation node did not allow get request, tearing down link.", rns.LOG_DEBUG)
			if r.OutboundPropagationLink != nil {
				r.OutboundPropagationLink.Teardown()
			}
			r.PropagationTransferState = PRNoAccess
			return
		}
	}

	response := decodeIDList(receipt.Response())
	duplicates := 0
	haves := make([][]byte, 0)

	for _, lxmfData := range response {
		if len(lxmfData) == 0 {
			continue
		}
		_, dup := r.LXMPropagation(lxmfData, nil, 0, nil, false, false)
		if dup {
			duplicates++
		}
		haves = append(haves, rns.FullHash(lxmfData))
	}

	if len(haves) > 0 && r.OutboundPropagationLink != nil {
		r.OutboundPropagationLink.Request(
			MessageGetPath,
			[]any{nil, haves},
			nil,
			r.MessageGetFailed,
			nil,
			0,
		)
	}

	r.PropagationTransferState = PRComplete
	r.PropagationTransferProgress = 1.0
	r.PropagationTransferLastDuplicates = intPtr(duplicates)
	result := len(response)
	r.PropagationTransferLastResult = &result
	r.saveLocallyDelivered()
}

func (r *LXMRouter) MessageGetProgress(receipt *rns.RequestReceipt) {
	if receipt == nil {
		return
	}
	r.PropagationTransferState = PRReceiving
	r.PropagationTransferProgress = receipt.Progress()
}

func (r *LXMRouter) MessageGetFailed(_ *rns.RequestReceipt) {
	rns.Log("Message list/get request failed", rns.LOG_DEBUG)
	if r.OutboundPropagationLink != nil {
		r.OutboundPropagationLink.Teardown()
	}
}

func (r *LXMRouter) AcknowledgeSyncCompletion(resetState bool, failureState *int) {
	r.PropagationTransferLastResult = nil
	if resetState || r.PropagationTransferState <= PRComplete {
		if failureState == nil {
			r.PropagationTransferState = PRIdle
		} else {
			r.PropagationTransferState = *failureState
		}
	}
	r.PropagationTransferProgress = 0
	r.WantsDownloadOnPathAvailableFrom = nil
	r.WantsDownloadOnPathAvailableTo = nil
}

func (r *LXMRouter) PropagationTransferSignallingPacket(data []byte, packet *rns.Packet) {
	if len(data) == 0 || packet == nil {
		return
	}
	var unpacked []any
	if err := umsgpack.Unpackb(data, &unpacked); err != nil || len(unpacked) == 0 {
		return
	}
	code, ok := intFromAny(unpacked[0])
	if !ok {
		return
	}
	if code != PeerErrorInvalidStamp {
		return
	}
	rns.Log("Message rejected by propagation node", rns.LOG_ERROR)
	if r.OutboundPropagationForMessage != nil {
		rns.Log("Invalid propagation stamp on "+r.OutboundPropagationForMessage.String(), rns.LOG_ERROR)
		r.CancelOutbound(r.OutboundPropagationForMessage.MessageID, MessageRejected)
		r.OutboundPropagationForMessage = nil
	}
}

func (r *LXMRouter) EnablePropagation() error {
	r.MessagePath = r.StoragePath + "/messagestore"
	if err := os.MkdirAll(r.StoragePath, 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(r.MessagePath, 0o700); err != nil {
		return err
	}

	r.PropagationEntries = map[string]*propagationEntry{}
	start := time.Now()
	rns.Log("Indexing messagestore...", rns.LOG_NOTICE)
	if err := r.indexMessageStore(); err != nil {
		return err
	}
	elapsed := time.Since(start).Seconds()
	mps := 0
	if elapsed > 0 {
		mps = int(math.Floor(float64(len(r.PropagationEntries)) / elapsed))
	}
	rns.Log(fmt.Sprintf("Indexed %d messages in %s, %d msgs/s", len(r.PropagationEntries), rns.PrettyTime(elapsed, false, false), mps), rns.LOG_NOTICE)

	rns.Log("Rebuilding peer synchronisation states...", rns.LOG_NOTICE)
	start = time.Now()
	if err := r.loadPeers(); err != nil {
		return err
	}

	if len(r.StaticPeers) > 0 {
		for _, staticPeer := range r.StaticPeers {
			if _, ok := r.Peers[string(staticPeer)]; ok {
				continue
			}
			rns.Log("Activating static peering with "+rns.PrettyHexRep(staticPeer), rns.LOG_NOTICE)
			peer := NewLXMPeer(r, staticPeer, r.DefaultSyncStrategy)
			r.Peers[string(staticPeer)] = peer
			if peer.LastHeard == 0 {
				rns.TransportRequestPath(staticPeer)
			}
		}
	}

	rns.Log(fmt.Sprintf("Rebuilt synchronisation state for %d peers in %s", len(r.Peers), rns.PrettyTime(time.Since(start).Seconds(), false, false)), rns.LOG_NOTICE)

	r.loadNodeStats()

	r.PropagationNode = true
	r.PropagationNodeStartTime = time.Now().Unix()
	if r.PropagationDestination != nil {
		r.PropagationDestination.SetLinkEstablishedCallback(r.PropagationLinkEstablished)
		r.PropagationDestination.SetPacketCallback(r.PropagationPacket)
		_ = r.PropagationDestination.RegisterRequestHandler(OfferRequestPath, r.OfferRequest, rns.DestinationAllowAll, nil)
		_ = r.PropagationDestination.RegisterRequestHandler(MessageGetPath, r.MessageGetRequest, rns.DestinationAllowAll, nil)
	}

	r.ControlAllowedList = []string{string(r.Identity.Hash)}
	controlDest, err := rns.NewDestination(r.Identity, rns.DestinationIN, rns.DestinationSINGLE, AppName, "propagation", "control")
	if err == nil {
		r.ControlDestination = controlDest
		allowed := r.controlAllowedBytes()
		_ = r.ControlDestination.RegisterRequestHandler(StatsGetPath, r.StatsGetRequest, rns.DestinationAllowList, allowed)
		_ = r.ControlDestination.RegisterRequestHandler(SyncRequestPath, r.PeerSyncRequest, rns.DestinationAllowList, allowed)
		_ = r.ControlDestination.RegisterRequestHandler(UnpeerRequestPath, r.PeerUnpeerRequest, rns.DestinationAllowList, allowed)
	}

	if r.MessageStorageLimit != nil {
		rns.Log("LXMF Propagation Node message store size is "+rns.PrettySize(float64(r.MessageStorageSize()))+", limit is "+rns.PrettySize(float64(*r.MessageStorageLimit)), rns.LOG_DEBUG)
	} else {
		rns.Log("LXMF Propagation Node message store size is "+rns.PrettySize(float64(r.MessageStorageSize())), rns.LOG_DEBUG)
	}

	r.AnnouncePropagationNode()
	return nil
}

func (r *LXMRouter) DisablePropagation() {
	r.PropagationNode = false
	r.AnnouncePropagationNode()
}

func (r *LXMRouter) EnforceStamps() {
	r.enforceStamps = true
}

func (r *LXMRouter) IgnoreStamps() {
	r.enforceStamps = false
}

func (r *LXMRouter) SetInformationStorageLimit(kb, mb, gb int) error {
	limit := int64(0)
	if kb > 0 {
		limit += int64(kb) * 1000
	}
	if mb > 0 {
		limit += int64(mb) * 1000 * 1000
	}
	if gb > 0 {
		limit += int64(gb) * 1000 * 1000 * 1000
	}
	if limit == 0 {
		r.InformationStorageLimit = nil
		return nil
	}
	if limit < 0 {
		return errors.New("cannot set LXMF information storage limit")
	}
	limitInt := int(limit)
	r.InformationStorageLimit = &limitInt
	return nil
}

func (r *LXMRouter) UpdateStampCost(destinationHash []byte, cost *int) {
	if cost == nil {
		return
	}
	r.OutboundStampCosts[string(destinationHash)] = stampCostEntry{
		Cost:    *cost,
		Updated: time.Now().Unix(),
	}
	go r.saveOutboundStampCosts()
}

func (r *LXMRouter) outboundProcessingLockLocked() bool {
	r.outboundProcessingMu.Lock()
	defer r.outboundProcessingMu.Unlock()
	return r.processingOutbound
}

func (r *LXMRouter) ProcessOutbound() {
	r.outboundProcessingMu.Lock()
	if r.processingOutbound {
		r.outboundProcessingMu.Unlock()
		return
	}
	r.processingOutbound = true
	r.outboundProcessingMu.Unlock()

	defer func() {
		r.outboundProcessingMu.Lock()
		r.processingOutbound = false
		r.outboundProcessingMu.Unlock()
	}()

	now := time.Now().Unix()
	filtered := r.PendingOutbound[:0]
	for _, msg := range r.PendingOutbound {
		if msg == nil {
			continue
		}
		switch msg.State {
		case MessageDelivered:
			if msg.IncludeTicket {
				r.AvailableTickets.LastDeliveries[string(msg.DestinationHash)] = now
				r.saveAvailableTickets()
			}
			if msg.Method == MethodDirect {
				if link, ok := r.DirectLinks[string(msg.DestinationHash)]; ok && link != nil {
					if link.Initiator && !r.backchannelIdentified[link] {
						if src, ok := r.DeliveryDestinations[string(msg.SourceHash)]; ok && src != nil {
							identity := src.Identity()
							link.Identify(identity)
							r.backchannelIdentified[link] = true
							r.DeliveryLinkEstablished(link)
							var backchannelHash []byte
							if identity != nil {
								backchannelHash = rns.HashFromNameAndIdentity("lxmf.delivery", identity.Hash)
							}
							rns.Log("Performed backchannel identification as "+rns.PrettyHexRep(backchannelHash)+" on "+link.String(), rns.LOG_DEBUG)
						}
					}
				}
			}
			continue
		case MessageSent:
			if msg.Method == MethodPropagated {
				continue
			}
		case MessageCancelled:
			if msg.FailedCallback != nil {
				msg.FailedCallback(msg)
			}
			continue
		case MessageRejected:
			if msg.FailedCallback != nil {
				msg.FailedCallback(msg)
			}
			continue
		}
		filtered = append(filtered, msg)
	}
	r.PendingOutbound = filtered

	now = time.Now().Unix()
	for _, msg := range r.PendingOutbound {
		if msg == nil {
			continue
		}
		if msg.Progress == 0 {
			msg.Progress = 0.01
		}
		if msg.NextDeliveryAttempt > now {
			continue
		}

		if len(msg.Packed) == 0 {
			_ = msg.Pack(false)
		}

		switch msg.Method {
		case MethodOpportunistic:
			if msg.DeliveryAttempts <= MaxDeliveryAttempts {
				if msg.DeliveryAttempts >= MaxPathlessTries && !rns.TransportHasPath(msg.DestinationHash) {
					rns.Log("Requesting path to "+rns.PrettyHexRep(msg.DestinationHash)+" after "+fmt.Sprintf("%d", msg.DeliveryAttempts)+" pathless tries for "+msg.String(), rns.LOG_DEBUG)
					msg.DeliveryAttempts++
					rns.TransportRequestPath(msg.DestinationHash)
					msg.NextDeliveryAttempt = time.Now().Unix() + PathRequestWait
					msg.Progress = 0.01
					continue
				} else if msg.DeliveryAttempts == MaxPathlessTries+1 && rns.TransportHasPath(msg.DestinationHash) {
					rns.Log("Opportunistic delivery for "+msg.String()+" still unsuccessful after "+fmt.Sprintf("%d", msg.DeliveryAttempts)+" attempts, trying to rediscover path to "+rns.PrettyHexRep(msg.DestinationHash), rns.LOG_DEBUG)
					msg.DeliveryAttempts++
					_ = rns.DropPath(msg.DestinationHash)
					go func() {
						time.Sleep(500 * time.Millisecond)
						rns.TransportRequestPath(msg.DestinationHash)
					}()
					msg.NextDeliveryAttempt = time.Now().Unix() + PathRequestWait
					msg.Progress = 0.01
					continue
				}

				if msg.NextDeliveryAttempt == 0 || time.Now().Unix() > msg.NextDeliveryAttempt {
					msg.DeliveryAttempts++
					msg.NextDeliveryAttempt = time.Now().Unix() + DeliveryRetryWait
					rns.Log("Opportunistic delivery attempt "+fmt.Sprintf("%d", msg.DeliveryAttempts)+" for "+msg.String()+" to "+rns.PrettyHexRep(msg.DestinationHash), rns.LOG_DEBUG)
					msg.SetDeliveryDestination(msg.Destination())
					msg.Send()
				}
			} else {
				rns.Log("Max delivery attempts reached for oppertunistic "+msg.String()+" to "+rns.PrettyHexRep(msg.DestinationHash), rns.LOG_DEBUG)
				r.failMessage(msg)
			}
		case MethodDirect:
			if msg.DeliveryAttempts <= MaxDeliveryAttempts {
				var link *rns.Link
				if existing, ok := r.DirectLinks[string(msg.DestinationHash)]; ok && existing != nil {
					link = existing
					rns.Log("Using available direct link "+link.String()+" to "+rns.PrettyHexRep(msg.DestinationHash), rns.LOG_DEBUG)
				} else if existing, ok := r.BackchannelLinks[string(msg.DestinationHash)]; ok && existing != nil {
					link = existing
					rns.Log("Using available backchannel link "+link.String()+" to "+rns.PrettyHexRep(msg.DestinationHash), rns.LOG_DEBUG)
				}

				if link != nil {
					if link.Status == rns.LinkActive {
						if msg.Progress < 0.05 {
							msg.Progress = 0.05
						}
						if msg.State != MessageSending {
							rns.Log("Starting transfer of "+msg.String()+" to "+rns.PrettyHexRep(msg.DestinationHash)+" on link "+link.String(), rns.LOG_DEBUG)
							msg.SetDeliveryDestination(link)
							msg.Send()
						} else {
							if msg.Representation == RepresentationResource {
								rns.Log("The transfer of "+msg.String()+" is in progress ("+fmt.Sprintf("%.1f", msg.Progress*100)+"%)", rns.LOG_DEBUG)
							} else {
								rns.Log("Waiting for proof for "+msg.String()+" sent as link packet", rns.LOG_DEBUG)
							}
						}
					} else if link.Status == rns.LinkClosed {
						rns.Log("The link to "+rns.PrettyHexRep(msg.DestinationHash)+" was closed", rns.LOG_DEBUG)
						rns.TransportRequestPath(msg.DestinationHash)
						msg.SetDeliveryDestination(nil)
						delete(r.DirectLinks, string(msg.DestinationHash))
						delete(r.BackchannelLinks, string(msg.DestinationHash))
						msg.NextDeliveryAttempt = time.Now().Unix() + DeliveryRetryWait
					} else {
						rns.Log("The link to "+rns.PrettyHexRep(msg.DestinationHash)+" is pending, waiting for link to become active", rns.LOG_DEBUG)
					}
				} else {
					if msg.NextDeliveryAttempt == 0 || time.Now().Unix() > msg.NextDeliveryAttempt {
						msg.DeliveryAttempts++
						msg.NextDeliveryAttempt = time.Now().Unix() + DeliveryRetryWait
						if msg.DeliveryAttempts < MaxDeliveryAttempts {
							if rns.TransportHasPath(msg.DestinationHash) {
								rns.Log("Establishing link to "+rns.PrettyHexRep(msg.DestinationHash)+" for delivery attempt "+fmt.Sprintf("%d", msg.DeliveryAttempts)+" to "+rns.PrettyHexRep(msg.DestinationHash), rns.LOG_DEBUG)
								r.getOrCreateDirectLink(msg.DestinationHash)
								msg.Progress = 0.03
							} else {
								rns.Log("No path known for delivery attempt "+fmt.Sprintf("%d", msg.DeliveryAttempts)+" to "+rns.PrettyHexRep(msg.DestinationHash)+". Requesting path...", rns.LOG_DEBUG)
								rns.TransportRequestPath(msg.DestinationHash)
								msg.NextDeliveryAttempt = time.Now().Unix() + PathRequestWait
								msg.Progress = 0.01
							}
						}
					}
				}
			} else {
				rns.Log("Max delivery attempts reached for direct "+msg.String()+" to "+rns.PrettyHexRep(msg.DestinationHash), rns.LOG_DEBUG)
				r.failMessage(msg)
			}
		case MethodPropagated:
			rns.Log("Attempting propagated delivery for "+msg.String()+" to "+rns.PrettyHexRep(msg.DestinationHash), rns.LOG_DEBUG)
			if len(r.OutboundPropagationNode) == 0 {
				rns.Log("No outbound propagation node specified for propagated "+msg.String()+" to "+rns.PrettyHexRep(msg.DestinationHash), rns.LOG_ERROR)
				r.failMessage(msg)
				continue
			}
			if msg.DeliveryAttempts <= MaxDeliveryAttempts {
				if r.OutboundPropagationLink != nil {
					if r.OutboundPropagationLink.Status == rns.LinkActive {
						if msg.State != MessageSending {
							r.OutboundPropagationForMessage = msg
							r.OutboundPropagationLink.SetPacketCallback(r.PropagationTransferSignallingPacket)
							rns.Log("Starting propagation transfer of "+msg.String()+" to "+rns.PrettyHexRep(msg.DestinationHash)+" via "+rns.PrettyHexRep(r.OutboundPropagationNode), rns.LOG_DEBUG)
							msg.SetDeliveryDestination(r.OutboundPropagationLink)
							msg.Send()
						} else {
							if msg.Representation == RepresentationResource {
								rns.Log("The transfer of "+msg.String()+" is in progress ("+fmt.Sprintf("%.1f", msg.Progress*100)+"%)", rns.LOG_DEBUG)
							} else {
								rns.Log("Waiting for proof for "+msg.String()+" sent as link packet", rns.LOG_DEBUG)
							}
						}
					} else if r.OutboundPropagationLink.Status == rns.LinkClosed {
						rns.Log("The link to "+rns.PrettyHexRep(r.OutboundPropagationNode)+" was closed", rns.LOG_DEBUG)
						r.OutboundPropagationLink = nil
						msg.NextDeliveryAttempt = time.Now().Unix() + DeliveryRetryWait
					} else {
						rns.Log("The propagation link to "+rns.PrettyHexRep(r.OutboundPropagationNode)+" is pending, waiting for link to become active", rns.LOG_DEBUG)
					}
				} else {
					if msg.NextDeliveryAttempt == 0 || time.Now().Unix() > msg.NextDeliveryAttempt {
						msg.DeliveryAttempts++
						msg.NextDeliveryAttempt = time.Now().Unix() + DeliveryRetryWait
						if msg.DeliveryAttempts < MaxDeliveryAttempts {
							if rns.TransportHasPath(r.OutboundPropagationNode) {
								rns.Log("Establishing link to "+rns.PrettyHexRep(r.OutboundPropagationNode)+" for propagation attempt "+fmt.Sprintf("%d", msg.DeliveryAttempts)+" to "+rns.PrettyHexRep(msg.DestinationHash), rns.LOG_DEBUG)
								r.getOrCreatePropagationLink()
							} else {
								rns.Log("No path known for propagation attempt "+fmt.Sprintf("%d", msg.DeliveryAttempts)+" to "+rns.PrettyHexRep(r.OutboundPropagationNode)+". Requesting path...", rns.LOG_DEBUG)
								rns.TransportRequestPath(r.OutboundPropagationNode)
								msg.NextDeliveryAttempt = time.Now().Unix() + PathRequestWait
							}
						}
					}
				}
			} else {
				rns.Log("Max delivery attempts reached for propagated "+msg.String()+" to "+rns.PrettyHexRep(msg.DestinationHash), rns.LOG_DEBUG)
				r.failMessage(msg)
			}
		default:
			msg.State = MessageFailed
		}
	}
}

func (r *LXMRouter) ProcessDeferredStamps() {
	r.stampGenMu.Lock()
	defer r.stampGenMu.Unlock()

	var selectedKey string
	var selectedMsg *LXMessage
	for key, msg := range r.PendingDeferredStamps {
		if msg != nil {
			selectedKey = key
			selectedMsg = msg
			break
		}
	}
	if selectedMsg == nil {
		return
	}

	if selectedMsg.State == MessageCancelled {
		rns.Log("Message cancelled during deferred stamp generation for "+selectedMsg.String(), rns.LOG_DEBUG)
		delete(r.PendingDeferredStamps, selectedKey)
		r.failMessage(selectedMsg)
		return
	}

	if selectedMsg.DeferStamp && selectedMsg.Stamp == nil {
		rns.Log("Starting stamp generation for "+selectedMsg.String()+"...", rns.LOG_DEBUG)
		generated := selectedMsg.GetStamp(nil)
		if generated == nil {
			if selectedMsg.State == MessageCancelled {
				rns.Log("Message cancelled during deferred stamp generation for "+selectedMsg.String(), rns.LOG_DEBUG)
			} else {
				rns.Log("Deferred stamp generation did not succeed. Failing "+selectedMsg.String(), rns.LOG_ERROR)
			}
			delete(r.PendingDeferredStamps, selectedKey)
			r.failMessage(selectedMsg)
			return
		}
		selectedMsg.Stamp = generated
		selectedMsg.DeferStamp = false
		selectedMsg.Packed = nil
		_ = selectedMsg.Pack(true)
		rns.Log("Stamp generation completed for "+selectedMsg.String(), rns.LOG_DEBUG)
	}

	if selectedMsg.DesiredMethod == MethodPropagated && selectedMsg.DeferPropagationStamp && selectedMsg.PropagationStamp == nil {
		rns.Log("Starting propagation stamp generation for "+selectedMsg.String()+"...", rns.LOG_DEBUG)
		targetCost := r.GetOutboundPropagationCost()
		if targetCost == nil {
			rns.Log("Failed to get propagation node stamp cost, cannot generate propagation stamp", rns.LOG_ERROR)
			delete(r.PendingDeferredStamps, selectedKey)
			r.failMessage(selectedMsg)
			return
		}
		generated := selectedMsg.GetPropagationStamp(*targetCost, nil)
		if generated == nil {
			if selectedMsg.State == MessageCancelled {
				rns.Log("Message cancelled during deferred propagation stamp generation for "+selectedMsg.String(), rns.LOG_DEBUG)
			} else {
				rns.Log("Deferred propagation stamp generation did not succeed. Failing "+selectedMsg.String(), rns.LOG_ERROR)
			}
			delete(r.PendingDeferredStamps, selectedKey)
			r.failMessage(selectedMsg)
			return
		}
		selectedMsg.PropagationStamp = generated
		selectedMsg.DeferPropagationStamp = false
		selectedMsg.Packed = nil
		_ = selectedMsg.Pack(false)
		rns.Log("Propagation stamp generation completed for "+selectedMsg.String(), rns.LOG_DEBUG)
	}

	delete(r.PendingDeferredStamps, selectedKey)
	r.PendingOutbound = append(r.PendingOutbound, selectedMsg)
	go r.ProcessOutbound()
}

func (r *LXMRouter) CleanTransientIDCaches() {
	now := time.Now().Unix()
	cutoff := int64(MessageExpiry * 6)
	for id, ts := range r.LocallyDelivered {
		if now > ts+cutoff {
			delete(r.LocallyDelivered, id)
		}
	}
	for id, ts := range r.LocallyProcessed {
		if now > ts+cutoff {
			delete(r.LocallyProcessed, id)
		}
	}
}

func (r *LXMRouter) CleanLinks() {
	for hash, link := range r.DirectLinks {
		if link == nil {
			delete(r.DirectLinks, hash)
			continue
		}
		if link.Status == rns.LinkClosed {
			if len(link.LinkID) > 0 {
				delete(r.ValidatedPeerLinks, string(link.LinkID))
			}
			delete(r.backchannelIdentified, link)
			delete(r.DirectLinks, hash)
		}
	}

	activeLinks := r.ActivePropagationLinks[:0]
	for _, link := range r.ActivePropagationLinks {
		if link == nil || link.Status == rns.LinkClosed {
			if link != nil {
				link.Teardown()
			}
			continue
		}
		activeLinks = append(activeLinks, link)
	}
	r.ActivePropagationLinks = activeLinks

	if r.OutboundPropagationLink != nil && r.OutboundPropagationLink.Status == rns.LinkClosed {
		r.OutboundPropagationLink = nil
		if r.PropagationTransferState == PRComplete {
			r.AcknowledgeSyncCompletion(false, nil)
		} else if r.PropagationTransferState < PRLinkEstablished {
			r.AcknowledgeSyncCompletion(false, intPtr(PRLinkFailed))
		} else if r.PropagationTransferState >= PRLinkEstablished && r.PropagationTransferState < PRComplete {
			r.AcknowledgeSyncCompletion(false, intPtr(PRTransferFailed))
		} else {
			r.AcknowledgeSyncCompletion(false, nil)
		}
		rns.Log("Cleaned outbound propagation link", rns.LOG_DEBUG)
	}
}

func (r *LXMRouter) Jobs() {
	if r.exitHandlerRunning {
		return
	}
	r.ProcessingCount++
	if r.ProcessingCount%JobOutboundInterval == 0 {
		r.ProcessOutbound()
	}
	if r.ProcessingCount%JobStampsInterval == 0 {
		go r.ProcessDeferredStamps()
	}
	if r.ProcessingCount%JobLinksInterval == 0 {
		r.CleanLinks()
	}
	if r.ProcessingCount%JobTransientInterval == 0 {
		r.CleanTransientIDCaches()
	}
	if r.ProcessingCount%JobStoreInterval == 0 {
		if r.PropagationNode {
			r.CleanMessageStore()
		}
	}
	if r.ProcessingCount%JobPeerIngestInterval == 0 {
		if r.PropagationNode {
			r.FlushQueues()
		}
	}
	if r.ProcessingCount%JobRotateInterval == 0 {
		if r.PropagationNode {
			r.RotatePeers()
		}
	}
	if r.ProcessingCount%JobPeerSyncInterval == 0 {
		if r.PropagationNode {
			r.SyncPeers()
		}
		r.CleanThrottledPeers()
	}
}

func (r *LXMRouter) JobLoop() {
	for {
		r.Jobs()
		time.Sleep(time.Duration(ProcessingInterval) * time.Second)
	}
}

func (r *LXMRouter) ExitHandler() {
	if r.exitHandlerRunning {
		return
	}
	r.exitHandlerRunning = true

	r.saveLocallyDelivered()
	r.saveLocallyProcessed()
	r.saveNodeStats()
	r.savePeers()
	r.exitHandlerRunning = false
}

func (r *LXMRouter) InstallExitHandlers() {
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		for range sigCh {
			if !r.exitHandlerRunning {
				rns.Log("Received shutdown signal, shutting down now!", rns.LOG_WARNING)
				r.ExitHandler()
				rns.Exit(0)
			} else {
				rns.Log("Received shutdown signal, but exit handler is running, keeping process alive until storage persist is complete", rns.LOG_WARNING)
			}
		}
	}()
}

func (r *LXMRouter) GetOutboundStampCost(destinationHash []byte) *int {
	if entry, ok := r.OutboundStampCosts[string(destinationHash)]; ok {
		cost := entry.Cost
		return &cost
	}
	return nil
}

func (r *LXMRouter) HandleOutbound(msg *LXMessage) {
	if msg == nil {
		return
	}

	destinationHash := msg.DestinationHash

	if msg.StampCost == nil {
		if cost := r.GetOutboundStampCost(destinationHash); cost != nil {
			msg.StampCost = cost
		}
	}

	msg.State = MessageOutbound

	msg.OutboundTicket = r.GetOutboundTicket(destinationHash)
	if msg.OutboundTicket != nil && msg.DeferStamp {
		msg.DeferStamp = false
	}

	if msg.IncludeTicket {
		ticket := r.GenerateTicket(destinationHash, TicketExpiry)
		if ticket != nil {
			if msg.Fields == nil {
				msg.Fields = map[any]any{}
			}
			msg.Fields[FieldTicket] = ticket
		}
	}

	if len(msg.Packed) == 0 {
		_ = msg.Pack(false)
	}

	unknownPathRequested := false
	if !rns.TransportHasPath(destinationHash) && msg.Method == MethodOpportunistic {
		rns.Log("Pre-emptively requesting unknown path for opportunistic "+msg.String(), rns.LOG_DEBUG)
		rns.TransportRequestPath(destinationHash)
		msg.NextDeliveryAttempt = time.Now().Unix() + PathRequestWait
		unknownPathRequested = true
	}

	msg.DetermineTransportEncryption()

	if msg.DeferStamp && msg.StampCost == nil {
		msg.DeferStamp = false
	}

	if !msg.DeferStamp && !(msg.DesiredMethod == MethodPropagated && msg.DeferPropagationStamp) {
		if !unknownPathRequested {
			r.PendingOutbound = append(r.PendingOutbound, msg)
			go r.ProcessOutbound()
		} else {
			r.PendingOutbound = append(r.PendingOutbound, msg)
		}
	} else {
		r.PendingDeferredStamps[string(msg.MessageID)] = msg
	}
}

func (r *LXMRouter) IsStaticPeer(destinationHash []byte) bool {
	for _, peer := range r.StaticPeers {
		if bytesEqual(peer, destinationHash) {
			return true
		}
	}
	return false
}

func (r *LXMRouter) PeerByHash(destinationHash []byte) *LXMPeer {
	return r.Peers[string(destinationHash)]
}

func (r *LXMRouter) HasPeer(destinationHash []byte) bool {
	_, ok := r.Peers[string(destinationHash)]
	return ok
}

func (r *LXMRouter) Peer(destinationHash []byte, nodeTimebase int, propagationTransferLimit int, propagationSyncLimit int, propagationStampCost int, propagationStampCostFlexibility int, peeringCost int, metadata map[any]any) {
	key := string(destinationHash)
	if peeringCost > r.MaxPeeringCost {
		if r.HasPeer(destinationHash) {
			rns.Log(fmt.Sprintf("Peer %s increased peering cost beyond local accepted maximum, breaking peering...", rns.PrettyHexRep(destinationHash)), rns.LOG_NOTICE)
			r.Unpeer(destinationHash, nodeTimebase)
		} else {
			rns.Log(fmt.Sprintf("Not peering with %s, since its peering cost of %d exceeds local maximum of %d", rns.PrettyHexRep(destinationHash), peeringCost, r.MaxPeeringCost), rns.LOG_NOTICE)
		}
		return
	}

	peer, ok := r.Peers[key]
	if ok {
		if float64(nodeTimebase) > peer.PeeringTimebase {
			peer.Alive = true
			peer.Metadata = metadata
			peer.SyncBackoff = 0
			peer.NextSyncAttempt = 0
			peer.PeeringTimebase = float64(nodeTimebase)
			peer.LastHeard = nowSeconds()
			peer.PropagationStampCost = propagationStampCost
			peer.PropagationStampCostFlexibility = propagationStampCostFlexibility
			peer.PeeringCost = peeringCost
			peer.PropagationTransferLimit = float64(propagationTransferLimit)
			if propagationSyncLimit != 0 {
				peer.PropagationSyncLimit = float64(propagationSyncLimit)
			} else {
				peer.PropagationSyncLimit = float64(propagationTransferLimit)
			}
			rns.Log(fmt.Sprintf("Peering config updated for %s", rns.PrettyHexRep(destinationHash)), rns.LOG_VERBOSE)
		}
		return
	}

	if len(r.Peers) >= r.MaxPeers {
		rns.Log(fmt.Sprintf("Max peers reached, not peering with %s", rns.PrettyHexRep(destinationHash)), rns.LOG_DEBUG)
		return
	}

	peer = NewLXMPeer(r, destinationHash, r.DefaultSyncStrategy)
	peer.Alive = true
	peer.Metadata = metadata
	peer.LastHeard = nowSeconds()
	peer.PropagationStampCost = propagationStampCost
	peer.PropagationStampCostFlexibility = propagationStampCostFlexibility
	peer.PeeringCost = peeringCost
	peer.PropagationTransferLimit = float64(propagationTransferLimit)
	if propagationSyncLimit != 0 {
		peer.PropagationSyncLimit = float64(propagationSyncLimit)
	} else {
		peer.PropagationSyncLimit = float64(propagationTransferLimit)
	}
	r.Peers[key] = peer
	rns.Log(fmt.Sprintf("Peered with %s", rns.PrettyHexRep(destinationHash)), rns.LOG_NOTICE)
}

func (r *LXMRouter) Unpeer(destinationHash []byte, nodeTimebase int) {
	timestamp := float64(nodeTimebase)
	if timestamp == 0 {
		timestamp = nowSeconds()
	}
	if peer, ok := r.Peers[string(destinationHash)]; ok {
		if timestamp >= peer.PeeringTimebase {
			delete(r.Peers, string(destinationHash))
			rns.Log("Broke peering with "+peer.String(), rns.LOG_INFO)
		}
	}
}

func (r *LXMRouter) GetSize(transientID []byte) int64 {
	if r == nil || len(transientID) == 0 {
		return 0
	}
	if entry, ok := r.PropagationEntries[string(transientID)]; ok && entry != nil {
		return entry.Size
	}
	return 0
}

func (r *LXMRouter) GetWeight(transientID []byte) float64 {
	if r == nil || len(transientID) == 0 {
		return 0
	}
	entry, ok := r.PropagationEntries[string(transientID)]
	if !ok || entry == nil {
		return 0
	}

	ageWeight := math.Max(1, (nowSeconds()-entry.Received)/(60*60*24*4))
	priorityWeight := 1.0
	if r.isPrioritised(entry.DestinationHash) {
		priorityWeight = 0.1
	}
	return priorityWeight * ageWeight * float64(entry.Size)
}

func (r *LXMRouter) GetStampValue(transientID []byte) int {
	if r == nil || len(transientID) == 0 {
		return 0
	}
	if entry, ok := r.PropagationEntries[string(transientID)]; ok && entry != nil {
		return entry.StampValue
	}
	return 0
}

func (r *LXMRouter) isPrioritised(destinationHash []byte) bool {
	if r == nil || len(destinationHash) == 0 {
		return false
	}
	key := string(destinationHash)
	for _, entry := range r.PrioritisedList {
		if entry == key {
			return true
		}
	}
	return false
}

func (r *LXMRouter) CancelOutbound(messageID []byte, cancelState byte) {
	if messageID == nil {
		return
	}
	key := string(messageID)
	if msg, ok := r.PendingDeferredStamps[key]; ok {
		msg.State = cancelState
		CancelWork(messageID)
		delete(r.PendingDeferredStamps, key)
	}

	var target *LXMessage
	for _, msg := range r.PendingOutbound {
		if msg != nil && bytesEqual(msg.MessageID, messageID) {
			target = msg
			break
		}
	}

	if target != nil {
		target.State = cancelState
		if target.Representation == RepresentationResource && target.ResourceRepresentation != nil {
			target.ResourceRepresentation.Cancel()
		}
		r.ProcessOutbound()
	}
}

func (r *LXMRouter) failMessage(msg *LXMessage) {
	if msg == nil {
		return
	}
	msg.Progress = 0
	if msg.State != MessageRejected {
		msg.State = MessageFailed
	}
	r.FailedOutbound = append(r.FailedOutbound, msg)
	if msg.FailedCallback != nil {
		defer func() {
			if rec := recover(); rec != nil {
				rns.Log("Error while invoking failed callback for "+msg.String()+": "+fmt.Sprint(rec), rns.LOG_ERROR)
			}
		}()
		msg.FailedCallback(msg)
	}
}

func (r *LXMRouter) GetOutboundProgress(lxmHash []byte) *float64 {
	for _, msg := range r.PendingOutbound {
		if msg != nil && bytesEqual(msg.Hash, lxmHash) {
			val := msg.Progress
			return &val
		}
	}
	for _, msg := range r.PendingDeferredStamps {
		if msg != nil && bytesEqual(msg.Hash, lxmHash) {
			val := msg.Progress
			return &val
		}
	}
	return nil
}

func (r *LXMRouter) GetOutboundLXMStampCost(lxmHash []byte) *int {
	for _, msg := range r.PendingOutbound {
		if msg != nil && bytesEqual(msg.Hash, lxmHash) {
			if msg.OutboundTicket != nil {
				return nil
			}
			return msg.StampCost
		}
	}
	for _, msg := range r.PendingDeferredStamps {
		if msg != nil && bytesEqual(msg.Hash, lxmHash) {
			if msg.OutboundTicket != nil {
				return nil
			}
			return msg.StampCost
		}
	}
	return nil
}

func (r *LXMRouter) GetOutboundLXMPropagationStampCost(lxmHash []byte) *int {
	for _, msg := range r.PendingOutbound {
		if msg != nil && bytesEqual(msg.Hash, lxmHash) {
			if msg.PropagationTargetCost == nil {
				return nil
			}
			cost := *msg.PropagationTargetCost
			return &cost
		}
	}
	for _, msg := range r.PendingDeferredStamps {
		if msg != nil && bytesEqual(msg.Hash, lxmHash) {
			if msg.PropagationTargetCost == nil {
				return nil
			}
			cost := *msg.PropagationTargetCost
			return &cost
		}
	}
	return nil
}

func (r *LXMRouter) HasMessage(transientID []byte) bool {
	if transientID == nil {
		return false
	}
	_, ok := r.LocallyDelivered[string(transientID)]
	return ok
}

func (r *LXMRouter) LXMDelivery(lxmfData []byte, destinationType int, phyStats map[string]float64, ratchetID []byte, method byte, noStampEnforcement bool, allowDuplicate bool) bool {
	msg, err := UnpackFromBytes(lxmfData, method)
	if err != nil || msg == nil {
		rns.Log("Could not assemble LXMF message from received data", rns.LOG_NOTICE)
		return false
	}

	if len(ratchetID) > 0 && len(msg.RatchetID) == 0 {
		msg.RatchetID = ratchetID
	}
	if method != 0 {
		msg.Method = method
	}

	if msg.SignatureValidated {
		if entry, ok := msg.Fields[FieldTicket]; ok {
			if ticketEntry, ok := entry.([]any); ok && len(ticketEntry) > 1 {
				expires := int64(floatFromAny(ticketEntry[0]))
				ticket := toBytes(ticketEntry[1])
				if time.Now().Unix() < expires && len(ticket) == TicketLength {
					r.RememberTicket(msg.SourceHash, ticketEntry)
					go r.saveAvailableTickets()
				}
			}
		}
	}

	cfg := r.DeliveryConfigs[string(msg.DestinationHash)]
	if cfg != nil && cfg.StampCost != nil {
		requiredCost := *cfg.StampCost
		destinationTickets := r.GetInboundTickets(msg.SourceHash)
		if msg.ValidateStamp(requiredCost, destinationTickets) {
			msg.StampValid = true
			msg.StampChecked = true
			rns.Log("Received "+msg.String()+" with valid stamp", rns.LOG_DEBUG)
		} else {
			msg.StampValid = false
			msg.StampChecked = true
			if noStampEnforcement {
				rns.Log("Received "+msg.String()+" with invalid stamp, but allowing anyway, since stamp enforcement was temporarily disabled", rns.LOG_NOTICE)
			} else if r.enforceStamps {
				rns.Log("Received "+msg.String()+" with invalid stamp, rejecting", rns.LOG_NOTICE)
				return false
			} else {
				rns.Log("Received "+msg.String()+" with invalid stamp, but allowing anyway, since stamp enforcement is disabled", rns.LOG_NOTICE)
			}
		}
	}

	if phyStats != nil {
		if v, ok := phyStats["rssi"]; ok {
			msg.RSSI = &v
		}
		if v, ok := phyStats["snr"]; ok {
			msg.SNR = &v
		}
		if v, ok := phyStats["q"]; ok {
			msg.Q = &v
		}
	}

	switch destinationType {
	case rns.DestinationSINGLE:
		msg.TransportEncrypted = true
		msg.TransportEncryption = EncryptionDescriptionEC
	case rns.DestinationGROUP:
		msg.TransportEncrypted = true
		msg.TransportEncryption = EncryptionDescriptionAES
	case rns.DestinationLINK:
		msg.TransportEncrypted = true
		msg.TransportEncryption = EncryptionDescriptionEC
	default:
		msg.TransportEncrypted = false
		msg.TransportEncryption = ""
	}

	sourceKey := string(msg.SourceHash)
	for _, ignored := range r.IgnoredList {
		if ignored == sourceKey {
			rns.Log("ignored message from "+rns.PrettyHexRep(msg.SourceHash), rns.LOG_DEBUG)
			return false
		}
	}

	if !allowDuplicate && r.HasMessage(msg.Hash) {
		rns.Log("ignored already received message from "+rns.PrettyHexRep(msg.SourceHash), rns.LOG_DEBUG)
		return false
	}
	r.LocallyDelivered[string(msg.Hash)] = time.Now().Unix()

	if r.DeliveryCallback != nil {
		r.DeliveryCallback(msg)
	}
	return true
}

func (r *LXMRouter) DeliveryPacket(data []byte, packet *rns.Packet) {
	if packet == nil {
		return
	}
	packet.Prove(nil)

	var (
		method   byte
		lxmfData []byte
	)
	if packet.DestinationType != rns.DestinationLINK {
		method = MethodOpportunistic
		lxmfData = append(append([]byte{}, packet.Destination.Hash()...), data...)
	} else {
		method = MethodDirect
		lxmfData = append([]byte{}, data...)
	}

	phyStats := map[string]float64{}
	r.fillPacketStats(packet, phyStats)

	r.LXMDelivery(lxmfData, int(packet.DestinationType), phyStats, packet.RatchetID, method, false, false)
}

func (r *LXMRouter) fillPacketStats(packet *rns.Packet, phyStats map[string]float64) {
	if packet == nil || phyStats == nil {
		return
	}
	if v := packet.GetRSSI(); v != nil {
		phyStats["rssi"] = *v
	} else if rns.Transport != nil {
		if v := rns.Transport.GetPacketRSSI(packet.GetHash()); v != nil {
			phyStats["rssi"] = *v
		}
	}
	if v := packet.GetSNR(); v != nil {
		phyStats["snr"] = *v
	} else if rns.Transport != nil {
		if v := rns.Transport.GetPacketSNR(packet.GetHash()); v != nil {
			phyStats["snr"] = *v
		}
	}
	if v := packet.GetQ(); v != nil {
		phyStats["q"] = *v
	} else if rns.Transport != nil {
		if v := rns.Transport.GetPacketQ(packet.GetHash()); v != nil {
			phyStats["q"] = *v
		}
	}
}

func (r *LXMRouter) CompileStats() map[any]any {
	if !r.PropagationNode {
		return nil
	}

	peerStats := map[any]any{}
	for peerID, peer := range r.Peers {
		if peer == nil {
			continue
		}
		peerType := "discovered"
		if r.IsStaticPeer([]byte(peerID)) {
			peerType = "static"
		}
		peerStats[[]byte(peerID)] = map[string]any{
			"type":                   peerType,
			"state":                  peer.State,
			"alive":                  peer.Alive,
			"name":                   peer.Name(),
			"last_heard":             int(peer.LastHeard),
			"next_sync_attempt":      peer.NextSyncAttempt,
			"last_sync_attempt":      peer.LastSyncAttempt,
			"sync_backoff":           peer.SyncBackoff,
			"peering_timebase":       peer.PeeringTimebase,
			"ler":                    int(peer.LinkEstablishmentRate),
			"str":                    int(peer.SyncTransferRate),
			"transfer_limit":         peer.PropagationTransferLimit,
			"sync_limit":             peer.PropagationSyncLimit,
			"target_stamp_cost":      peer.PropagationStampCost,
			"stamp_cost_flexibility": peer.PropagationStampCostFlexibility,
			"peering_cost":           peer.PeeringCost,
			"peering_key":            peer.PeeringKeyValue(),
			"network_distance":       rns.TransportHopsTo([]byte(peerID)),
			"rx_bytes":               peer.RxBytes,
			"tx_bytes":               peer.TxBytes,
			"acceptance_rate":        peer.AcceptanceRate(),
			"messages": map[string]any{
				"offered":   peer.Offered,
				"outgoing":  peer.Outgoing,
				"incoming":  peer.Incoming,
				"unhandled": peer.UnhandledMessageCount(),
			},
		}
	}

	nodeStats := map[any]any{
		"identity_hash":          r.Identity.Hash,
		"destination_hash":       r.PropagationDestination.Hash(),
		"uptime":                 nowSeconds() - float64(r.PropagationNodeStartTime),
		"delivery_limit":         r.DeliveryPerTransferLimit,
		"propagation_limit":      r.PropagationPerTransferLimit,
		"sync_limit":             r.PropagationPerSyncLimit,
		"target_stamp_cost":      r.PropagationStampCost,
		"stamp_cost_flexibility": r.PropagationStampCostFlexibility,
		"peering_cost":           r.PeeringCost,
		"max_peering_cost":       r.MaxPeeringCost,
		"autopeer_maxdepth":      r.AutoPeerMaxDepth,
		"from_static_only":       r.FromStaticOnly,
		"messagestore": map[string]any{
			"count": len(r.PropagationEntries),
			"bytes": r.MessageStorageSize(),
			"limit": r.MessageStorageLimit,
		},
		"clients": map[string]any{
			"client_propagation_messages_received": r.ClientPropagationMessagesReceived,
			"client_propagation_messages_served":   r.ClientPropagationMessagesServed,
		},
		"unpeered_propagation_incoming": r.UnpeeredPropagationIncoming,
		"unpeered_propagation_rx_bytes": r.UnpeeredPropagationRxBytes,
		"static_peers":                  len(r.StaticPeers),
		"discovered_peers":              len(r.Peers) - len(r.StaticPeers),
		"total_peers":                   len(r.Peers),
		"max_peers":                     r.MaxPeers,
		"peers":                         peerStats,
	}

	return nodeStats
}

func (r *LXMRouter) StatsGetRequest(_ string, _ any, _ []byte, _ []byte, remoteIdentity *rns.Identity, _ time.Time) any {
	if remoteIdentity == nil {
		return PeerErrorNoIdentity
	}
	if !r.controlAllowed(remoteIdentity) {
		return PeerErrorNoAccess
	}
	return r.CompileStats()
}

func (r *LXMRouter) PeerSyncRequest(_ string, data any, _ []byte, _ []byte, remoteIdentity *rns.Identity, _ time.Time) any {
	if remoteIdentity == nil {
		return PeerErrorNoIdentity
	}
	if !r.controlAllowed(remoteIdentity) {
		return PeerErrorNoAccess
	}
	hash := toBytes(data)
	if len(hash) != rns.ReticulumTruncatedHashLength/8 {
		return PeerErrorInvalidData
	}
	peer := r.Peers[string(hash)]
	if peer == nil {
		return PeerErrorNotFound
	}
	peer.Sync()
	return true
}

func (r *LXMRouter) PeerUnpeerRequest(_ string, data any, _ []byte, _ []byte, remoteIdentity *rns.Identity, _ time.Time) any {
	if remoteIdentity == nil {
		return PeerErrorNoIdentity
	}
	if !r.controlAllowed(remoteIdentity) {
		return PeerErrorNoAccess
	}
	hash := toBytes(data)
	if len(hash) != rns.ReticulumTruncatedHashLength/8 {
		return PeerErrorInvalidData
	}
	if _, ok := r.Peers[string(hash)]; !ok {
		return PeerErrorNotFound
	}
	r.Unpeer(hash, 0)
	return true
}

func (r *LXMRouter) DeliveryLinkEstablished(link *rns.Link) {
	if link == nil {
		return
	}
	link.TrackPhyStats(true)
	link.SetPacketCallback(r.DeliveryPacket)
	_ = link.SetResourceStrategy(rns.LinkAcceptApp)
	link.SetResourceCallback(r.DeliveryResourceAdvertised)
	link.SetResourceStartedCallback(r.ResourceTransferBegan)
	link.SetResourceConcludedCallback(r.DeliveryResourceConcluded)
	link.SetRemoteIdentifiedCallback(r.DeliveryRemoteIdentified)
}

func (r *LXMRouter) DeliveryResourceAdvertised(res *rns.ResourceAdvertisement) bool {
	if res == nil {
		return false
	}
	size := res.GetDataSize()
	limit := r.DeliveryPerTransferLimit * 1000
	if r.DeliveryPerTransferLimit > 0 && size > limit {
		rns.Log("Rejecting incoming LXMF delivery resource, since it exceeds the limit", rns.LOG_DEBUG)
		return false
	}
	return true
}

func (r *LXMRouter) ResourceTransferBegan(res *rns.Resource) {
	if res == nil {
		return
	}
	rns.Log("Transfer began for LXMF delivery resource "+res.String(), rns.LOG_DEBUG)
}

func (r *LXMRouter) DeliveryResourceConcluded(res *rns.Resource) {
	if res == nil {
		return
	}
	if res.Status() == rns.ResourceComplete {
		link := res.Link()
		var ratchetID []byte
		if link != nil {
			ratchetID = link.LinkID
		}
		phyStats := map[string]float64{}
		if link != nil {
			if v := link.GetRSSI(); v != nil {
				phyStats["rssi"] = *v
			}
			if v := link.GetSNR(); v != nil {
				phyStats["snr"] = *v
			}
			if v := link.GetQ(); v != nil {
				phyStats["q"] = *v
			}
		}
		data, err := os.ReadFile(res.DataFile())
		if err == nil {
			r.LXMDelivery(data, rns.DestinationLINK, phyStats, ratchetID, MethodDirect, false, false)
		}
	}
}

func (r *LXMRouter) DeliveryRemoteIdentified(link *rns.Link, identity *rns.Identity) {
	if identity == nil {
		return
	}
	hash := rns.HashFromNameAndIdentity("lxmf.delivery", identity.Hash)
	if hash != nil {
		r.BackchannelLinks[string(hash)] = link
	}
}

func (r *LXMRouter) PropagationLinkEstablished(link *rns.Link) {
	if link == nil {
		return
	}
	link.SetPacketCallback(r.PropagationPacket)
	_ = link.SetResourceStrategy(rns.LinkAcceptApp)
	link.SetResourceCallback(r.PropagationResourceAdvertised)
	link.SetResourceStartedCallback(r.ResourceTransferBegan)
	link.SetResourceConcludedCallback(r.PropagationResourceConcluded)
	r.ActivePropagationLinks = append(r.ActivePropagationLinks, link)
}

func (r *LXMRouter) PropagationResourceAdvertised(res *rns.ResourceAdvertisement) bool {
	if res == nil {
		return false
	}
	if r.FromStaticOnly {
		link := res.Link
		if link == nil {
			rns.Log("Rejecting propagation resource from unidentified peer", rns.LOG_DEBUG)
			return false
		}
		remoteIdentity := link.RemoteIdentity()
		if remoteIdentity == nil {
			rns.Log("Rejecting propagation resource from unidentified peer", rns.LOG_DEBUG)
			return false
		}
		dest, err := rns.NewDestination(remoteIdentity, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "propagation")
		if err != nil {
			return false
		}
		remoteHash := dest.Hash()
		if !r.IsStaticPeer(remoteHash) {
			rns.Log("Rejecting propagation resource from "+rns.PrettyHexRep(remoteHash)+" not in static peers list", rns.LOG_DEBUG)
			return false
		}
	}

	size := res.GetDataSize()
	limit := r.PropagationPerSyncLimit * 1000
	if r.PropagationPerSyncLimit > 0 && size > limit {
		rns.Log("Rejecting "+rns.PrettySize(float64(size))+" incoming propagation resource, since it exceeds the limit of "+rns.PrettySize(float64(limit)), rns.LOG_DEBUG)
		return false
	}
	return true
}

func (r *LXMRouter) PropagationPacket(data []byte, packet *rns.Packet) {
	if packet == nil {
		return
	}
	if packet.DestinationType != rns.DestinationLINK {
		return
	}

	var unpacked []any
	if err := umsgpack.Unpackb(data, &unpacked); err != nil || len(unpacked) < 2 {
		return
	}
	messages := decodeIDList(unpacked[1])
	if len(messages) == 0 {
		return
	}

	minAcceptedCost := r.PropagationStampCost - r.PropagationStampCostFlexibility
	if minAcceptedCost < 0 {
		minAcceptedCost = 0
	}
	validated := ValidatePNStamps(messages, minAcceptedCost)
	for _, entry := range validated {
		if len(entry) < 4 {
			continue
		}
		lxmfData := toBytes(entry[1])
		stampValue := int(floatFromAny(entry[2]))
		stampData := toBytes(entry[3])
		r.LXMPropagation(lxmfData, nil, stampValue, stampData, false, false)
		r.ClientPropagationMessagesReceived++
	}

	if len(validated) == len(messages) {
		ms := "s"
		if len(messages) == 1 {
			ms = ""
		}
		rns.Log(fmt.Sprintf("Received %d propagation message%s from client with valid stamp%s", len(messages), ms, ms), rns.LOG_DEBUG)
		packet.Prove(nil)
		return
	}

	rns.Log("Propagation transfer from client contained messages with invalid stamps", rns.LOG_NOTICE)
	rejectData, err := umsgpack.Packb([]any{PeerErrorInvalidStamp})
	if err != nil {
		return
	}
	if packet.Link != nil {
		rns.NewPacket(packet.Link, rejectData).Send()
		packet.Link.Teardown()
	}
}

func (r *LXMRouter) PropagationResourceConcluded(res *rns.Resource) {
	if res == nil || res.Status() != rns.ResourceComplete {
		return
	}

	data, err := os.ReadFile(res.DataFile())
	if err != nil {
		rns.Log("Error while unpacking received propagation resource", rns.LOG_DEBUG)
		return
	}

	var unpacked []any
	if err := umsgpack.Unpackb(data, &unpacked); err != nil || len(unpacked) < 2 {
		rns.Log("Invalid data structure received at propagation destination, ignoring", rns.LOG_DEBUG)
		return
	}
	messages := decodeIDList(unpacked[1])
	if len(messages) == 0 {
		return
	}

	link := res.Link()
	var (
		remoteHash []byte
		remoteStr  = "unknown client"
		peer       *LXMPeer
	)

	if link != nil {
		remoteIdentity := link.RemoteIdentity()
		if remoteIdentity != nil {
			remoteDest, err := rns.NewDestination(remoteIdentity, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "propagation")
			if err == nil {
				remoteHash = remoteDest.Hash()
				remoteStr = rns.PrettyHexRep(remoteHash)
				if p, ok := r.Peers[string(remoteHash)]; ok {
					peer = p
					remoteStr = "peer " + remoteStr
				} else {
					appData := rns.IdentityRecallAppData(remoteHash)
					if PNAnnounceDataIsValid(appData) && r.AutoPeer && rns.TransportHopsTo(remoteHash) <= r.AutoPeerMaxDepth {
						var config []any
						if err := umsgpack.Unpackb(appData, &config); err == nil && len(config) > 6 {
							nodeTimebase := int(floatFromAny(config[1]))
							propagationLimit := int(floatFromAny(config[3]))
							syncLimit := int(floatFromAny(config[4]))
							stampCosts, _ := config[5].([]any)
							propagationCost := int(floatFromAny(stampCosts[0]))
							propagationFlex := int(floatFromAny(stampCosts[1]))
							peeringCost := int(floatFromAny(stampCosts[2]))
							metadata, _ := config[6].(map[any]any)

							rns.Log("Auto-peering with "+remoteStr+" discovered via incoming sync", rns.LOG_DEBUG)
							r.Peer(remoteHash, nodeTimebase, propagationLimit, syncLimit, propagationCost, propagationFlex, peeringCost, metadata)
							peer = r.Peers[string(remoteHash)]
						}
					}
				}
			}
		}
	}

	peeringKeyValid := false
	if link != nil && len(link.LinkID) > 0 {
		if ok, exists := r.ValidatedPeerLinks[string(link.LinkID)]; exists && ok {
			peeringKeyValid = true
		}
	}

	if !peeringKeyValid && len(messages) > 1 {
		if link != nil {
			link.Teardown()
		}
		rns.Log("Received multiple propagation messages from "+remoteStr+" without valid peering key presentation. This is not supposed to happen, ignoring.", rns.LOG_WARNING)
		rns.Log("Clients and peers without a valid peering key can only deliver 1 message per transfer.", rns.LOG_WARNING)
		return
	}

	ms := "s"
	if len(messages) == 1 {
		ms = ""
	}
	rns.Log("Received "+fmt.Sprintf("%d", len(messages))+" message"+ms+" from "+remoteStr+", validating stamps...", rns.LOG_VERBOSE)

	minAcceptedCost := r.PropagationStampCost - r.PropagationStampCostFlexibility
	if minAcceptedCost < 0 {
		minAcceptedCost = 0
	}
	validated := ValidatePNStamps(messages, minAcceptedCost)
	invalidStamps := len(messages) - len(validated)
	if invalidStamps == 0 {
		rns.Log("All message stamps validated from "+remoteStr, rns.LOG_VERBOSE)
	} else {
		ms = "s"
		if invalidStamps == 1 {
			ms = ""
		}
		rns.Log("Transfer from "+remoteStr+" contained "+fmt.Sprintf("%d", invalidStamps)+" invalid stamp"+ms, rns.LOG_WARNING)
	}

	for _, entry := range validated {
		if len(entry) < 4 {
			continue
		}
		transientID := toBytes(entry[0])
		lxmfData := toBytes(entry[1])
		stampValue := int(floatFromAny(entry[2]))
		stampData := toBytes(entry[3])

		if peer != nil {
			peer.Incoming++
			peer.RxBytes += len(lxmfData)
		} else if remoteHash != nil {
			r.UnpeeredPropagationIncoming++
			r.UnpeeredPropagationRxBytes += len(lxmfData)
		} else {
			r.ClientPropagationMessagesReceived++
		}

		r.LXMPropagation(lxmfData, peer, stampValue, stampData, false, false)
		if peer != nil {
			peer.QueueHandledMessage(transientID)
		}
	}

	if invalidStamps > 0 && link != nil {
		link.Teardown()
		if remoteHash != nil {
			throttleTime := int64(PNStampThrottle)
			r.ThrottledPeers[string(remoteHash)] = time.Now().Unix() + throttleTime
			ms := "s"
			if invalidStamps == 1 {
				ms = ""
			}
			rns.Log("Propagation transfer from "+remoteStr+" contained "+fmt.Sprintf("%d", invalidStamps)+" message"+ms+" with invalid stamps, throttled for "+rns.PrettyTime(float64(throttleTime), false, false), rns.LOG_NOTICE)
		}
	}
}

func (r *LXMRouter) OfferRequest(_ string, data any, _ []byte, linkID []byte, remoteIdentity *rns.Identity, _ time.Time) any {
	if remoteIdentity == nil {
		return PeerErrorNoIdentity
	}

	remoteDest, err := rns.NewDestination(remoteIdentity, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "propagation")
	if err != nil {
		return PeerErrorInvalidData
	}
	remoteHash := remoteDest.Hash()
	remoteStr := rns.PrettyHexRep(remoteHash)

	if until, ok := r.ThrottledPeers[string(remoteHash)]; ok {
		if time.Now().Unix() < until {
			remaining := until - time.Now().Unix()
			rns.Log("Propagation offer from node "+remoteStr+" rejected, throttled for "+rns.PrettyTime(float64(remaining), false, false)+" more", rns.LOG_NOTICE)
			return PeerErrorThrottled
		}
		delete(r.ThrottledPeers, string(remoteHash))
	}

	if r.FromStaticOnly && !r.IsStaticPeer(remoteHash) {
		rns.Log("Rejecting propagation request from "+remoteStr+" not in static peers list", rns.LOG_DEBUG)
		return PeerErrorNoAccess
	}

	list, ok := data.([]any)
	if !ok || len(list) < 2 {
		return PeerErrorInvalidData
	}

	peeringID := append(copyBytes(r.Identity.Hash), remoteIdentity.Hash...)
	targetCost := r.PeeringCost
	peeringKey := toBytes(list[0])
	transientIDs := decodeIDList(list[1])

	if len(peeringKey) == 0 {
		return PeerErrorInvalidKey
	}
	start := time.Now()
	peeringKeyValid := ValidatePeeringKey(peeringID, peeringKey, targetCost)
	duration := time.Since(start).Seconds()

	if !peeringKeyValid {
		rns.Log("Invalid peering key for incoming sync offer", rns.LOG_DEBUG)
		return PeerErrorInvalidKey
	}

	rns.Log("Peering key validated for incoming offer in "+rns.PrettyTime(duration, false, false), rns.LOG_DEBUG)
	if len(linkID) > 0 {
		r.ValidatedPeerLinks[string(linkID)] = true
	}

	wanted := make([][]byte, 0)
	for _, transientID := range transientIDs {
		if _, ok := r.PropagationEntries[string(transientID)]; !ok {
			wanted = append(wanted, transientID)
		}
	}

	if len(wanted) == 0 {
		return false
	}
	if len(wanted) == len(transientIDs) {
		return true
	}
	return wanted
}

func (r *LXMRouter) MessageGetRequest(_ string, data any, _ []byte, _ []byte, remoteIdentity *rns.Identity, _ time.Time) any {
	if remoteIdentity == nil {
		return PeerErrorNoIdentity
	}
	if !r.identityAllowed(remoteIdentity) {
		return PeerErrorNoAccess
	}

	list, ok := data.([]any)
	if !ok || len(list) < 2 {
		return PeerErrorInvalidData
	}

	remoteDest, err := rns.NewDestination(remoteIdentity, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		return PeerErrorInvalidData
	}

	wantField := list[0]
	haveField := list[1]

	if wantField == nil && haveField == nil {
		type availableEntry struct {
			id   []byte
			size int64
		}
		available := make([]availableEntry, 0)
		for transientKey, entry := range r.PropagationEntries {
			if entry == nil {
				continue
			}
			if !bytesEqual(entry.DestinationHash, remoteDest.Hash()) {
				continue
			}
			available = append(available, availableEntry{
				id:   []byte(transientKey),
				size: entry.Size,
			})
		}
		sort.Slice(available, func(i, j int) bool { return available[i].size < available[j].size })
		transientIDs := make([][]byte, 0, len(available))
		for _, entry := range available {
			transientIDs = append(transientIDs, entry.id)
		}
		return transientIDs
	}

	if haveField != nil && !r.RetainSyncedOnNode {
		for _, transientID := range decodeIDList(haveField) {
			entry, ok := r.PropagationEntries[string(transientID)]
			if !ok || entry == nil {
				continue
			}
			if !bytesEqual(entry.DestinationHash, remoteDest.Hash()) {
				continue
			}
			filePath := entry.FilePath
			delete(r.PropagationEntries, string(transientID))
			if err := os.Remove(filePath); err != nil {
				rns.Log("Error while processing message purge request from "+rns.PrettyHexRep(remoteDest.Hash())+". The contained exception was: "+err.Error(), rns.LOG_ERROR)
			}
		}
	}

	responseMessages := make([][]byte, 0)
	if wantField != nil {
		var clientTransferLimit *float64
		if len(list) >= 3 {
			limit := floatFromAny(list[2]) * 1000
			if limit > 0 {
				clientTransferLimit = &limit
				rns.Log("Client indicates transfer limit of "+rns.PrettySize(limit), rns.LOG_DEBUG)
			}
		}

		perMessageOverhead := int64(16)
		cumulativeSize := int64(24)
		for _, transientID := range decodeIDList(wantField) {
			entry, ok := r.PropagationEntries[string(transientID)]
			if !ok || entry == nil {
				continue
			}
			if !bytesEqual(entry.DestinationHash, remoteDest.Hash()) {
				continue
			}
			data, err := os.ReadFile(entry.FilePath)
			if err != nil {
				rns.Log("Error while processing message download request from "+rns.PrettyHexRep(remoteDest.Hash())+". The contained exception was: "+err.Error(), rns.LOG_ERROR)
				continue
			}

			lxmSize := int64(len(data))
			nextSize := cumulativeSize + (lxmSize + perMessageOverhead)
			if clientTransferLimit != nil && float64(nextSize) > *clientTransferLimit {
				continue
			}

			if len(data) > StampSize {
				responseMessages = append(responseMessages, data[:len(data)-StampSize])
				cumulativeSize += (lxmSize + perMessageOverhead)
			}
		}
	}

	r.ClientPropagationMessagesServed += len(responseMessages)
	return responseMessages
}

func (r *LXMRouter) EnqueuePeerDistribution(transientID []byte, fromPeer *LXMPeer) {
	if len(transientID) == 0 {
		return
	}
	var fromHash []byte
	if fromPeer != nil {
		fromHash = copyBytes(fromPeer.DestinationHash)
	}
	r.PeerDistributionQueue = append(r.PeerDistributionQueue, peerDistributionEntry{
		TransientID: copyBytes(transientID),
		FromPeer:    fromHash,
	})
}

func (r *LXMRouter) FlushPeerDistributionQueue() {
	if len(r.PeerDistributionQueue) == 0 {
		return
	}
	entries := r.PeerDistributionQueue
	r.PeerDistributionQueue = nil

	for _, peer := range r.Peers {
		if peer == nil {
			continue
		}
		for _, entry := range entries {
			if len(entry.FromPeer) > 0 && bytesEqual(peer.DestinationHash, entry.FromPeer) {
				continue
			}
			peer.QueueUnhandledMessage(entry.TransientID)
		}
	}
}

func (r *LXMRouter) LXMPropagation(lxmfData []byte, fromPeer *LXMPeer, stampValue int, stampData []byte, allowDuplicate bool, isPaperMessage bool) (bool, bool) {
	if len(lxmfData) < LXMFOverhead {
		return false, false
	}

	transientID := rns.FullHash(lxmfData)
	if !allowDuplicate {
		if _, ok := r.PropagationEntries[string(transientID)]; ok {
			return false, true
		}
		if _, ok := r.LocallyProcessed[string(transientID)]; ok {
			return false, true
		}
	}

	received := nowSeconds()
	r.LocallyProcessed[string(transientID)] = int64(received)

	destinationHash := lxmfData[:DestinationLength]
	if dest, ok := r.DeliveryDestinations[string(destinationHash)]; ok && dest != nil {
		encrypted := lxmfData[DestinationLength:]
		decrypted := dest.Decrypt(encrypted)
		if decrypted != nil {
			deliveryData := append(append([]byte{}, destinationHash...), decrypted...)
			noStampEnforcement := isPaperMessage
			ratchetID := rns.IdentityCurrentRatchetID(dest.Hash())
			r.LXMDelivery(deliveryData, dest.Type, nil, ratchetID, MethodPropagated, noStampEnforcement, allowDuplicate)
			r.LocallyDelivered[string(transientID)] = time.Now().Unix()
		}
		return true, false
	}

	if !r.PropagationNode {
		rns.Log("Received propagated LXMF message "+rns.PrettyHexRep(transientID)+", but this instance is not hosting a propagation node, discarding message.", rns.LOG_DEBUG)
		return true, false
	}

	stampedData := append(append([]byte{}, lxmfData...), stampData...)
	fileName := fmt.Sprintf("%s_%s_%d", rns.HexRep(transientID, false), strconv.FormatFloat(received, 'f', -1, 64), stampValue)
	filePath := filepath.Join(r.MessagePath, fileName)
	if err := os.WriteFile(filePath, stampedData, 0o600); err != nil {
		rns.Log("Could not store propagated LXMF message "+rns.PrettyHexRep(transientID)+": "+err.Error(), rns.LOG_ERROR)
		return false, false
	}

	rns.Log("Received propagated LXMF message "+rns.PrettyHexRep(transientID)+" with stamp value "+fmt.Sprintf("%d", stampValue)+", adding to peer distribution queues...", rns.LOG_EXTREME)
	r.PropagationEntries[string(transientID)] = &propagationEntry{
		DestinationHash: copyBytes(destinationHash),
		FilePath:        filePath,
		Received:        received,
		Size:            int64(len(stampedData)),
		HandledPeers:    []string{},
		UnhandledPeers:  []string{},
		StampValue:      stampValue,
	}
	r.EnqueuePeerDistribution(transientID, fromPeer)

	return true, false
}

func (r *LXMRouter) CleanMessageStore() {
	rns.Log("Cleaning message store", rns.LOG_VERBOSE)
	now := nowSeconds()
	removedEntries := map[string]string{}

	for transientKey, entry := range r.PropagationEntries {
		if entry == nil {
			continue
		}
		filePath := entry.FilePath
		stampValue := entry.StampValue
		filename := filepath.Base(filePath)
		components := strings.Split(filename, "_")
		if len(components) == 3 {
			ts, err := strconv.ParseFloat(components[1], 64)
			if err == nil && ts > 0 && len(components[0]) == rns.HashLengthBytes*2 {
				sv, err := strconv.Atoi(components[2])
				if err == nil && sv == stampValue {
					if now > ts+MessageExpiry {
						rns.Log("Purging message "+rns.PrettyHexRep([]byte(transientKey))+" due to expiry", rns.LOG_EXTREME)
						removedEntries[transientKey] = filePath
					}
					continue
				}
			}
		}
		rns.Log("Purging message "+rns.PrettyHexRep([]byte(transientKey))+" due to invalid file path", rns.LOG_WARNING)
		removedEntries[transientKey] = filePath
	}

	removedCount := 0
	for transientKey, filePath := range removedEntries {
		delete(r.PropagationEntries, transientKey)
		if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			rns.Log("Could not remove "+rns.PrettyHexRep([]byte(transientKey))+" from message store. The contained exception was: "+err.Error(), rns.LOG_ERROR)
		}
		removedCount++
	}

	if removedCount > 0 {
		rns.Log("Cleaned "+fmt.Sprintf("%d", removedCount)+" entries from the message store", rns.LOG_VERBOSE)
	}

	var bytesNeeded int64
	if r.MessageStorageLimit != nil {
		messageStorageSize := r.MessageStorageSize()
		if messageStorageSize > int64(*r.MessageStorageLimit) {
			bytesNeeded = messageStorageSize - int64(*r.MessageStorageLimit)
		}
	}
	if r.InformationStorageLimit != nil {
		infoSize := r.InformationStorageSize()
		if infoSize > int64(*r.InformationStorageLimit) {
			infoNeeded := infoSize - int64(*r.InformationStorageLimit)
			if infoNeeded > bytesNeeded {
				bytesNeeded = infoNeeded
			}
		}
	}
	if bytesNeeded == 0 {
		return
	}

	bytesCleaned := int64(0)

	type weightedEntry struct {
		entry *propagationEntry
		weight float64
		id    []byte
	}
	weightedEntries := make([]weightedEntry, 0, len(r.PropagationEntries))
	for transientKey := range r.PropagationEntries {
		id := []byte(transientKey)
		weightedEntries = append(weightedEntries, weightedEntry{
			entry:  r.PropagationEntries[transientKey],
			weight: r.GetWeight(id),
			id:     id,
		})
	}
	sort.Slice(weightedEntries, func(i, j int) bool {
		return weightedEntries[i].weight > weightedEntries[j].weight
	})

	for _, entry := range weightedEntries {
		if bytesCleaned >= bytesNeeded {
			break
		}
		if entry.entry == nil {
			continue
		}
		if err := os.Remove(entry.entry.FilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			rns.Log("Error while cleaning LXMF message from message store. The contained exception was: "+err.Error(), rns.LOG_ERROR)
			continue
		}
		delete(r.PropagationEntries, string(entry.id))
		bytesCleaned += entry.entry.Size
		rns.Log("Removed "+rns.PrettyHexRep(entry.id)+" with weight "+fmt.Sprintf("%f", entry.weight)+" to clear up "+rns.PrettySize(float64(entry.entry.Size))+", now cleaned "+rns.PrettySize(float64(bytesCleaned))+" out of "+rns.PrettySize(float64(bytesNeeded))+" needed", rns.LOG_EXTREME)
	}

	rns.Log("LXMF message store size is now "+rns.PrettySize(float64(r.MessageStorageSize()))+" for "+fmt.Sprintf("%d", len(r.PropagationEntries))+" items", rns.LOG_EXTREME)
}

func (r *LXMRouter) SyncPeers() {
	culledPeers := make([][]byte, 0)
	waitingPeers := make([]*LXMPeer, 0)
	unresponsivePeers := make([]*LXMPeer, 0)

	now := nowSeconds()
	peers := make(map[string]*LXMPeer, len(r.Peers))
	for k, v := range r.Peers {
		peers[k] = v
	}

	for peerID, peer := range peers {
		if peer == nil {
			continue
		}
		if now > peer.LastHeard+PeerMaxUnreachable {
			if !r.IsStaticPeer([]byte(peerID)) {
				culledPeers = append(culledPeers, []byte(peerID))
			}
			continue
		}

		if peer.State == PeerIdle && peer.UnhandledMessageCount() > 0 {
			if peer.Alive {
				waitingPeers = append(waitingPeers, peer)
			} else if now > peer.NextSyncAttempt {
				unresponsivePeers = append(unresponsivePeers, peer)
			}
		}
	}

	peerPool := make([]*LXMPeer, 0)
	if len(waitingPeers) > 0 {
		sort.Slice(waitingPeers, func(i, j int) bool {
			return waitingPeers[i].SyncTransferRate > waitingPeers[j].SyncTransferRate
		})
		fastestCount := minInt(FastestNRandomPool, len(waitingPeers))
		peerPool = append(peerPool, waitingPeers[:fastestCount]...)

		unknownSpeed := make([]*LXMPeer, 0)
		for _, peer := range waitingPeers {
			if peer.SyncTransferRate == 0 {
				unknownSpeed = append(unknownSpeed, peer)
			}
		}
		if len(unknownSpeed) > 0 {
			unknownCount := minInt(len(unknownSpeed), fastestCount)
			peerPool = append(peerPool, unknownSpeed[:unknownCount]...)
		}
		rns.Log("Selecting peer to sync from "+fmt.Sprintf("%d", len(waitingPeers))+" waiting peers.", rns.LOG_DEBUG)
	} else if len(unresponsivePeers) > 0 {
		rns.Log("No active peers available, randomly selecting peer to sync from "+fmt.Sprintf("%d", len(unresponsivePeers))+" unresponsive peers.", rns.LOG_DEBUG)
		peerPool = append(peerPool, unresponsivePeers...)
	}

	if len(peerPool) > 0 {
		selectedIndex := rand.Intn(len(peerPool))
		selectedPeer := peerPool[selectedIndex]
		rns.Log("Selected waiting peer "+fmt.Sprintf("%d", selectedIndex)+": "+rns.PrettyHexRep(selectedPeer.DestinationHash), rns.LOG_DEBUG)
		selectedPeer.Sync()
	}

	for _, peerID := range culledPeers {
		rns.Log("Removing peer "+rns.PrettyHexRep(peerID)+" due to excessive unreachability", rns.LOG_WARNING)
		delete(r.Peers, string(peerID))
	}
}

func (r *LXMRouter) CleanThrottledPeers() {
	now := time.Now().Unix()
	for peerHash, until := range r.ThrottledPeers {
		if now > until {
			delete(r.ThrottledPeers, peerHash)
		}
	}
}

func (r *LXMRouter) FlushQueues() {
	if len(r.Peers) == 0 {
		return
	}
	r.FlushPeerDistributionQueue()
	rns.Log("Calculating peer distribution queue mappings...", rns.LOG_DEBUG)
	start := time.Now()
	for _, peer := range r.Peers {
		if peer == nil {
			continue
		}
		if peer.QueuedItems() {
			peer.ProcessQueues()
		}
	}
	rns.Log("Distribution queue mapping completed in "+rns.PrettyTime(time.Since(start).Seconds(), false, false), rns.LOG_DEBUG)
}

func (r *LXMRouter) RotatePeers() {
	rotationHeadroom := int(math.Floor(float64(r.MaxPeers) * (RotationHeadroomPct / 100.0)))
	if rotationHeadroom < 1 {
		rotationHeadroom = 1
	}
	requiredDrops := len(r.Peers) - (r.MaxPeers - rotationHeadroom)
	if requiredDrops <= 0 || len(r.Peers)-requiredDrops <= 1 {
		return
	}

	untestedPeers := make([]*LXMPeer, 0)
	for _, peer := range r.Peers {
		if peer != nil && peer.LastSyncAttempt == 0 {
			untestedPeers = append(untestedPeers, peer)
		}
	}
	if len(untestedPeers) >= rotationHeadroom {
		rns.Log("Newly added peer threshold reached, postponing peer rotation", rns.LOG_DEBUG)
		return
	}

	rotationPool := make(map[string]*LXMPeer, len(r.Peers))
	for key, peer := range r.Peers {
		rotationPool[key] = peer
	}

	fullySynced := make(map[string]*LXMPeer)
	for peerID, peer := range rotationPool {
		if peer == nil {
			continue
		}
		if peer.UnhandledMessageCount() == 0 {
			fullySynced[peerID] = peer
		}
	}
	if len(fullySynced) > 0 {
		rotationPool = fullySynced
		ms := "s"
		if len(fullySynced) == 1 {
			ms = ""
		}
		rns.Log("Found "+fmt.Sprintf("%d", len(fullySynced))+" fully synced peer"+ms+", using as peer rotation pool basis", rns.LOG_DEBUG)
	}

	waitingPeers := make([]*LXMPeer, 0)
	unresponsivePeers := make([]*LXMPeer, 0)
	for peerID, peer := range rotationPool {
		if peer == nil {
			continue
		}
		if r.IsStaticPeer([]byte(peerID)) || peer.State != PeerIdle {
			continue
		}
		if peer.Alive {
			if peer.Offered != 0 {
				waitingPeers = append(waitingPeers, peer)
			}
		} else {
			unresponsivePeers = append(unresponsivePeers, peer)
		}
	}

	dropPool := make([]*LXMPeer, 0)
	if len(unresponsivePeers) > 0 {
		dropPool = append(dropPool, unresponsivePeers...)
		if !r.PrioritiseRotatingUnreachablePeers {
			dropPool = append(dropPool, waitingPeers...)
		}
	} else {
		dropPool = append(dropPool, waitingPeers...)
	}

	if len(dropPool) == 0 {
		return
	}

	dropCount := minInt(requiredDrops, len(dropPool))
	sort.Slice(dropPool, func(i, j int) bool {
		return dropPool[i].AcceptanceRate() < dropPool[j].AcceptanceRate()
	})

	droppedPeers := 0
	for _, peer := range dropPool[:dropCount] {
		ar := peer.AcceptanceRate() * 100
		if ar < RotationARMax*100 {
			reachableStr := "reachable"
			if !peer.Alive {
				reachableStr = "unreachable"
			}
			rns.Log("Acceptance rate for "+reachableStr+" peer "+rns.PrettyHexRep(peer.DestinationHash)+" was: "+fmt.Sprintf("%.2f", ar)+"% ("+fmt.Sprintf("%d", peer.Outgoing)+"/"+fmt.Sprintf("%d", peer.Offered)+", "+fmt.Sprintf("%d", peer.UnhandledMessageCount())+" unhandled messages)", rns.LOG_DEBUG)
			r.Unpeer(peer.DestinationHash, 0)
			droppedPeers++
		}
	}

	ms := "s"
	if droppedPeers == 1 {
		ms = ""
	}
	rns.Log("Dropped "+fmt.Sprintf("%d", droppedPeers)+" low acceptance rate peer"+ms+" to increase peering headroom", rns.LOG_DEBUG)
}

func (r *LXMRouter) getOrCreateDirectLink(destinationHash []byte) *rns.Link {
	key := string(destinationHash)
	if link, ok := r.DirectLinks[key]; ok && link != nil {
		if link.Status != rns.LinkClosed {
			return link
		}
		delete(r.DirectLinks, key)
	}

	if !rns.TransportHasPath(destinationHash) {
		rns.TransportRequestPath(destinationHash)
		return nil
	}

	peerID := rns.IdentityRecall(destinationHash)
	if peerID == nil {
		return nil
	}
	dest, err := rns.NewDestination(peerID, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "delivery")
	if err != nil {
		return nil
	}

	link, err := rns.NewOutgoingLink(dest, rns.LinkModeDefault, func(l *rns.Link) {
		r.DirectLinks[key] = l
		r.DeliveryLinkEstablished(l)
		r.ProcessOutbound()
	}, func(l *rns.Link) {
		delete(r.DirectLinks, key)
	})
	if err != nil {
		return nil
	}
	r.DirectLinks[key] = link
	return link
}

func (r *LXMRouter) getOrCreatePropagationLink() *rns.Link {
	if len(r.OutboundPropagationNode) == 0 {
		return nil
	}
	if r.OutboundPropagationLink != nil && r.OutboundPropagationLink.Status != rns.LinkClosed {
		return r.OutboundPropagationLink
	}
	r.OutboundPropagationLink = nil

	if !rns.TransportHasPath(r.OutboundPropagationNode) {
		rns.TransportRequestPath(r.OutboundPropagationNode)
		return nil
	}

	peerID := rns.IdentityRecall(r.OutboundPropagationNode)
	if peerID == nil {
		return nil
	}
	dest, err := rns.NewDestination(peerID, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "propagation")
	if err != nil {
		return nil
	}

	link, err := rns.NewOutgoingLink(dest, rns.LinkModeDefault, func(l *rns.Link) {
		r.OutboundPropagationLink = l
		l.SetPacketCallback(r.PropagationTransferSignallingPacket)
		r.ProcessOutbound()
	}, func(l *rns.Link) {
		if r.OutboundPropagationLink == l {
			r.OutboundPropagationLink = nil
		}
	})
	if err != nil {
		return nil
	}
	r.OutboundPropagationLink = link
	return link
}

func (r *LXMRouter) GenerateTicket(destinationHash []byte, expiry int) []any {
	now := time.Now().Unix()
	if expiry <= 0 {
		expiry = TicketExpiry
	}

	if last, ok := r.AvailableTickets.LastDeliveries[string(destinationHash)]; ok {
		elapsed := now - last
		if elapsed < TicketInterval {
			rns.Log("A ticket for "+rns.PrettyHexRep(destinationHash)+" was already delivered recently, not including another ticket yet", rns.LOG_DEBUG)
			return nil
		}
	}

	if inbound, ok := r.AvailableTickets.Inbound[string(destinationHash)]; ok {
		for key, entry := range inbound {
			validityLeft := entry.Expires - now
			if validityLeft > TicketRenew {
				rns.Log("Found generated ticket for "+rns.PrettyHexRep(destinationHash)+" with enough validity left, re-using this one", rns.LOG_DEBUG)
				return []any{entry.Expires, []byte(key)}
			}
		}
	} else {
		r.AvailableTickets.Inbound[string(destinationHash)] = map[string]ticketEntry{}
	}

	expires := now + int64(expiry)
	ticket := make([]byte, TicketLength)
	_, _ = crypto_rand.Read(ticket)
	r.AvailableTickets.Inbound[string(destinationHash)][string(ticket)] = ticketEntry{Expires: expires}
	r.saveAvailableTickets()
	return []any{expires, ticket}
}

func (r *LXMRouter) RememberTicket(destinationHash []byte, entry []any) {
	if len(entry) < 2 {
		return
	}
	expires := int64(floatFromAny(entry[0]))
	ticket := toBytes(entry[1])
	if ticket == nil {
		return
	}
	r.AvailableTickets.Outbound[string(destinationHash)] = ticketEntry{
		Expires: expires,
		Ticket:  ticket,
	}
}

func (r *LXMRouter) GetOutboundTicket(destinationHash []byte) []byte {
	if entry, ok := r.AvailableTickets.Outbound[string(destinationHash)]; ok {
		if entry.Expires > time.Now().Unix() {
			return entry.Ticket
		}
	}
	return nil
}

func (r *LXMRouter) GetOutboundTicketExpiry(destinationHash []byte) *int64 {
	if entry, ok := r.AvailableTickets.Outbound[string(destinationHash)]; ok {
		if entry.Expires > time.Now().Unix() {
			expires := entry.Expires
			return &expires
		}
	}
	return nil
}

func (r *LXMRouter) GetInboundTickets(destinationHash []byte) [][]byte {
	now := time.Now().Unix()
	tickets := [][]byte{}
	if inbound, ok := r.AvailableTickets.Inbound[string(destinationHash)]; ok {
		for key, entry := range inbound {
			if now < entry.Expires {
				tickets = append(tickets, []byte(key))
			}
		}
	}
	if len(tickets) == 0 {
		return nil
	}
	return tickets
}

func (r *LXMRouter) loadLocallyDelivered() {
	path := r.StoragePath + "/local_deliveries"
	if _, err := os.Stat(path); err != nil {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		rns.Log("Could not load locally delivered message ID cache from storage. The contained exception was: "+err.Error(), rns.LOG_ERROR)
		return
	}
	var raw map[any]any
	if err := umsgpack.Unpackb(data, &raw); err != nil {
		rns.Log("Invalid data format for loaded locally delivered transient IDs, recreating...", rns.LOG_ERROR)
		r.LocallyDelivered = map[string]int64{}
		return
	}
	r.LocallyDelivered = coerceStringInt64Map(raw)
}

func (r *LXMRouter) loadLocallyProcessed() {
	path := r.StoragePath + "/locally_processed"
	if _, err := os.Stat(path); err != nil {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		rns.Log("Could not load locally processed message ID cache from storage. The contained exception was: "+err.Error(), rns.LOG_ERROR)
		return
	}
	var raw map[any]any
	if err := umsgpack.Unpackb(data, &raw); err != nil {
		rns.Log("Invalid data format for loaded locally processed transient IDs, recreating...", rns.LOG_ERROR)
		r.LocallyProcessed = map[string]int64{}
		return
	}
	r.LocallyProcessed = coerceStringInt64Map(raw)
}

func (r *LXMRouter) saveLocallyDelivered() {
	if len(r.LocallyDelivered) == 0 {
		return
	}
	path := r.StoragePath + "/local_deliveries"
	data, err := umsgpack.Packb(r.LocallyDelivered)
	if err != nil {
		return
	}
	_ = os.WriteFile(path, data, 0o600)
}

func (r *LXMRouter) saveLocallyProcessed() {
	if len(r.LocallyProcessed) == 0 {
		return
	}
	path := r.StoragePath + "/locally_processed"
	data, err := umsgpack.Packb(r.LocallyProcessed)
	if err != nil {
		return
	}
	_ = os.WriteFile(path, data, 0o600)
}

func (r *LXMRouter) loadOutboundStampCosts() {
	path := r.StoragePath + "/outbound_stamp_costs"
	if _, err := os.Stat(path); err != nil {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		rns.Log("Could not load outbound stamp costs from storage. The contained exception was: "+err.Error(), rns.LOG_ERROR)
		return
	}
	var raw map[any]any
	if err := umsgpack.Unpackb(data, &raw); err != nil {
		rns.Log("Invalid data format for loaded outbound stamp costs, recreating...", rns.LOG_ERROR)
		r.OutboundStampCosts = map[string]stampCostEntry{}
		return
	}
	r.OutboundStampCosts = coerceStampCosts(raw)
	r.cleanOutboundStampCosts()
	r.saveOutboundStampCosts()
}

func (r *LXMRouter) cleanOutboundStampCosts() {
	now := time.Now().Unix()
	for k, entry := range r.OutboundStampCosts {
		if now-entry.Updated > StampCostExpiry {
			delete(r.OutboundStampCosts, k)
		}
	}
}

func (r *LXMRouter) saveOutboundStampCosts() {
	r.costFileMu.Lock()
	defer r.costFileMu.Unlock()
	path := r.StoragePath + "/outbound_stamp_costs"
	data, err := umsgpack.Packb(r.OutboundStampCosts)
	if err != nil {
		return
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		rns.Log("Could not save outbound stamp costs to storage. The contained exception was: "+err.Error(), rns.LOG_ERROR)
	}
}

func (r *LXMRouter) loadAvailableTickets() {
	path := r.StoragePath + "/available_tickets"
	if _, err := os.Stat(path); err != nil {
		return
	}
	r.ticketFileMu.Lock()
	defer r.ticketFileMu.Unlock()
	data, err := os.ReadFile(path)
	if err != nil {
		rns.Log("Could not load outbound stamp costs from storage. The contained exception was: "+err.Error(), rns.LOG_ERROR)
		return
	}
	var raw map[any]any
	if err := umsgpack.Unpackb(data, &raw); err != nil {
		rns.Log("Invalid data format for loaded available tickets, recreating...", rns.LOG_ERROR)
		r.AvailableTickets = newAvailableTickets()
		return
	}
	r.AvailableTickets = coerceAvailableTickets(raw)
	r.cleanAvailableTickets()
	r.saveAvailableTickets()
}

func (r *LXMRouter) cleanAvailableTickets() {
	now := time.Now().Unix()

	expiredOutbound := []string{}
	for dest, entry := range r.AvailableTickets.Outbound {
		if now > entry.Expires {
			expiredOutbound = append(expiredOutbound, dest)
		}
	}
	for _, dest := range expiredOutbound {
		delete(r.AvailableTickets.Outbound, dest)
	}

	for dest, tickets := range r.AvailableTickets.Inbound {
		expiredInbound := []string{}
		for ticket, entry := range tickets {
			if now > entry.Expires+TicketGrace {
				expiredInbound = append(expiredInbound, ticket)
			}
		}
		for _, ticket := range expiredInbound {
			delete(tickets, ticket)
		}
		r.AvailableTickets.Inbound[dest] = tickets
	}
}

func (r *LXMRouter) saveAvailableTickets() {
	r.ticketFileMu.Lock()
	defer r.ticketFileMu.Unlock()
	path := r.StoragePath + "/available_tickets"
	data, err := umsgpack.Packb(r.AvailableTickets.toMap())
	if err != nil {
		return
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		rns.Log("Could not save available tickets to storage. The contained exception was: "+err.Error(), rns.LOG_ERROR)
	}
}

func (r *LXMRouter) loadPeers() error {
	peersStoragePath := r.StoragePath + "/peers"
	data, err := os.ReadFile(peersStoragePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	if len(data) == 0 {
		return nil
	}

	var serialisedPeers []any
	if err := umsgpack.Unpackb(data, &serialisedPeers); err != nil {
		rns.Log("Could not load propagation node peering data from storage. The contained exception was: "+err.Error(), rns.LOG_ERROR)
		rns.Log("The peering data file located at "+peersStoragePath+" is likely corrupt.", rns.LOG_ERROR)
		rns.Log("You can delete this file and attempt starting the propagation node again, but peer synchronization states will be lost.", rns.LOG_ERROR)
		return err
	}

	for len(serialisedPeers) > 0 {
		idx := len(serialisedPeers) - 1
		entry := serialisedPeers[idx]
		serialisedPeers = serialisedPeers[:idx]

		rawPeer, ok := entry.([]byte)
		if !ok {
			continue
		}
		peer, err := LXMPeerFromBytes(rawPeer, r)
		if err != nil || peer == nil {
			continue
		}
		if r.IsStaticPeer(peer.DestinationHash) && peer.LastHeard == 0 {
			rns.TransportRequestPath(peer.DestinationHash)
		}
		if peer.Identity != nil {
			r.Peers[string(peer.DestinationHash)] = peer
			limStr := ", no transfer limit"
			if peer.PropagationTransferLimit > 0 {
				limStr = ", " + rns.PrettySize(peer.PropagationTransferLimit*1000) + " transfer limit"
			}
			rns.Log("Rebuilt peer "+rns.PrettyHexRep(peer.DestinationHash)+" with "+fmt.Sprintf("%d", peer.UnhandledMessageCount())+" unhandled messages"+limStr, rns.LOG_DEBUG)
		} else {
			rns.Log("Peer "+rns.PrettyHexRep(peer.DestinationHash)+" could not be loaded, because its identity could not be recalled. Dropping peer.", rns.LOG_DEBUG)
		}
	}
	return nil
}

func (r *LXMRouter) loadNodeStats() {
	path := r.StoragePath + "/node_stats"
	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			rns.Log("Could not load local node stats. The contained exception was: "+err.Error(), rns.LOG_ERROR)
		}
		return
	}

	var nodeStats map[any]any
	if err := umsgpack.Unpackb(data, &nodeStats); err != nil {
		rns.Log("Could not load local node stats. The contained exception was: "+err.Error(), rns.LOG_ERROR)
		return
	}
	if nodeStats == nil {
		rns.Log("Invalid data format for loaded local node stats, node stats will be reset", rns.LOG_ERROR)
		return
	}

	r.ClientPropagationMessagesReceived = int(floatFromAny(nodeStats["client_propagation_messages_received"]))
	r.ClientPropagationMessagesServed = int(floatFromAny(nodeStats["client_propagation_messages_served"]))
	r.UnpeeredPropagationIncoming = int(floatFromAny(nodeStats["unpeered_propagation_incoming"]))
	r.UnpeeredPropagationRxBytes = int(floatFromAny(nodeStats["unpeered_propagation_rx_bytes"]))
}

func (r *LXMRouter) saveNodeStats() {
	nodeStats := map[any]any{
		"client_propagation_messages_received": r.ClientPropagationMessagesReceived,
		"client_propagation_messages_served":   r.ClientPropagationMessagesServed,
		"unpeered_propagation_incoming":        r.UnpeeredPropagationIncoming,
		"unpeered_propagation_rx_bytes":        r.UnpeeredPropagationRxBytes,
	}
	data, err := umsgpack.Packb(nodeStats)
	if err != nil {
		return
	}
	path := r.StoragePath + "/node_stats"
	if err := os.WriteFile(path, data, 0o600); err != nil {
		rns.Log("Could not save local node stats. The contained exception was: "+err.Error(), rns.LOG_ERROR)
	}
}

func (r *LXMRouter) savePeers() {
	if len(r.Peers) == 0 {
		return
	}
	serialisedPeers := make([]any, 0, len(r.Peers))
	for _, peer := range r.Peers {
		if peer == nil {
			continue
		}
		raw, err := peer.ToBytes()
		if err != nil {
			continue
		}
		serialisedPeers = append(serialisedPeers, raw)
	}
	data, err := umsgpack.Packb(serialisedPeers)
	if err != nil {
		return
	}
	path := r.StoragePath + "/peers"
	if err := os.WriteFile(path, data, 0o600); err != nil {
		rns.Log("Could not save propagation node peers to storage. The contained exception was: "+err.Error(), rns.LOG_ERROR)
	}
}

func (r *LXMRouter) controlAllowed(identity *rns.Identity) bool {
	if identity == nil {
		return false
	}
	key := string(identity.Hash)
	for _, entry := range r.ControlAllowedList {
		if entry == key {
			return true
		}
	}
	return false
}

func (r *LXMRouter) identityAllowed(identity *rns.Identity) bool {
	if !r.AuthRequired {
		return true
	}
	if identity == nil {
		return false
	}
	key := string(identity.Hash)
	for _, entry := range r.AllowedList {
		if entry == key {
			return true
		}
	}
	return false
}

func (r *LXMRouter) controlAllowedBytes() [][]byte {
	out := make([][]byte, 0, len(r.ControlAllowedList))
	for _, entry := range r.ControlAllowedList {
		if entry == "" {
			continue
		}
		out = append(out, []byte(entry))
	}
	return out
}

func (r *LXMRouter) indexMessageStore() error {
	entries, err := os.ReadDir(r.MessagePath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		filename := entry.Name()
		components := strings.Split(filename, "_")
		if len(components) < 3 {
			continue
		}
		received, err := strconv.ParseFloat(components[1], 64)
		if err != nil || received <= 0 {
			continue
		}
		if len(components[0]) != rns.HashLengthBytes*2 {
			continue
		}
		stampValue, err := strconv.Atoi(components[2])
		if err != nil {
			continue
		}
		transientID, err := hex.DecodeString(components[0])
		if err != nil || len(transientID) == 0 {
			continue
		}
		filePath := filepath.Join(r.MessagePath, filename)
		info, err := entry.Info()
		if err != nil {
			continue
		}
		msgSize := info.Size()
		file, err := os.Open(filePath)
		if err != nil {
			rns.Log("Could not read LXM from message store. The contained exception was: "+err.Error(), rns.LOG_ERROR)
			continue
		}
		destHash := make([]byte, DestinationLength)
		if _, err := io.ReadFull(file, destHash); err != nil {
			_ = file.Close()
			rns.Log("Could not read LXM from message store. The contained exception was: "+err.Error(), rns.LOG_ERROR)
			continue
		}
		_ = file.Close()

		r.PropagationEntries[string(transientID)] = &propagationEntry{
			DestinationHash: copyBytes(destHash),
			FilePath:        filePath,
			Received:        received,
			Size:            msgSize,
			HandledPeers:    []string{},
			UnhandledPeers:  []string{},
			StampValue:      stampValue,
		}
	}
	return nil
}

func coerceStringInt64Map(raw map[any]any) map[string]int64 {
	out := map[string]int64{}
	for k, v := range raw {
		ks := fmt.Sprintf("%s", k)
		out[ks] = int64(floatFromAny(v))
	}
	return out
}

func coerceStampCosts(raw map[any]any) map[string]stampCostEntry {
	out := map[string]stampCostEntry{}
	for k, v := range raw {
		ks := fmt.Sprintf("%s", k)
		list, ok := v.([]any)
		if !ok || len(list) < 2 {
			continue
		}
		ts := int64(floatFromAny(list[0]))
		cost := int(floatFromAny(list[1]))
		out[ks] = stampCostEntry{Updated: ts, Cost: cost}
	}
	return out
}

type ticketEntry struct {
	Expires int64
	Ticket  []byte
}

type availableTickets struct {
	Outbound       map[string]ticketEntry
	Inbound        map[string]map[string]ticketEntry
	LastDeliveries map[string]int64
}

func newAvailableTickets() *availableTickets {
	return &availableTickets{
		Outbound:       map[string]ticketEntry{},
		Inbound:        map[string]map[string]ticketEntry{},
		LastDeliveries: map[string]int64{},
	}
}

func (a *availableTickets) toMap() map[string]any {
	outbound := map[any]any{}
	for dest, entry := range a.Outbound {
		outbound[[]byte(dest)] = []any{entry.Expires, entry.Ticket}
	}
	inbound := map[any]any{}
	for dest, tickets := range a.Inbound {
		inboundTickets := map[any]any{}
		for key, entry := range tickets {
			inboundTickets[[]byte(key)] = []any{entry.Expires}
		}
		inbound[[]byte(dest)] = inboundTickets
	}
	lastDeliveries := map[any]any{}
	for dest, ts := range a.LastDeliveries {
		lastDeliveries[[]byte(dest)] = ts
	}
	return map[string]any{
		"outbound":        outbound,
		"inbound":         inbound,
		"last_deliveries": lastDeliveries,
	}
}

func coerceAvailableTickets(raw map[any]any) *availableTickets {
	at := newAvailableTickets()

	if v, ok := raw["outbound"]; ok {
		if outboundRaw, ok := v.(map[any]any); ok {
			for k, entryRaw := range outboundRaw {
				ks := string(toBytes(k))
				if entry, ok := entryRaw.([]any); ok && len(entry) >= 2 {
					at.Outbound[ks] = ticketEntry{
						Expires: int64(floatFromAny(entry[0])),
						Ticket:  toBytes(entry[1]),
					}
				}
			}
		}
	}

	if v, ok := raw["inbound"]; ok {
		if inboundRaw, ok := v.(map[any]any); ok {
			for k, entryRaw := range inboundRaw {
				dest := string(toBytes(k))
				tickets := map[string]ticketEntry{}
				if entryMap, ok := entryRaw.(map[any]any); ok {
					for tk, te := range entryMap {
						ticketKey := string(toBytes(tk))
						if entry, ok := te.([]any); ok && len(entry) >= 1 {
							tickets[ticketKey] = ticketEntry{
								Expires: int64(floatFromAny(entry[0])),
							}
						}
					}
				}
				at.Inbound[dest] = tickets
			}
		}
	}

	if v, ok := raw["last_deliveries"]; ok {
		if lastRaw, ok := v.(map[any]any); ok {
			for k, val := range lastRaw {
				key := string(toBytes(k))
				at.LastDeliveries[key] = int64(floatFromAny(val))
			}
		}
	}

	return at
}

func intPtr(v int) *int {
	return &v
}
