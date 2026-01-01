package lxmf

import (
	"errors"
	"sync"
	"time"

	"github.com/svanichkin/go-reticulum/rns"
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

type stampCostEntry struct {
	Cost    int
	Updated int64
}

type LXMRouter struct {
	PendingInbound  []*LXMessage
	PendingOutbound []*LXMessage
	FailedOutbound  []*LXMessage

	PrioritisedList    []string
	IgnoredList        []string
	AllowedList        []string
	ControlAllowedList []string
	AuthRequired       bool

	DefaultSyncStrategy int
	ProcessingInbound   bool
	ProcessingCount     int
	Name                string

	PropagationNode          bool
	PropagationNodeStartTime int64

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
	EnforceStamps                   bool

	AutoPeer         bool
	AutoPeerMaxDepth int
	MaxPeers         int
	FromStaticOnly   bool
	StaticPeers      [][]byte

	Peers map[string]*LXMPeer

	OutboundStampCosts map[string]stampCostEntry

	outboundProcessingMu sync.Mutex
	processingOutbound   bool

	Identity               *rns.Identity
	PropagationDestination *rns.Destination
}

func NewLXMRouter(identity *rns.Identity, storagePath string) (*LXMRouter, error) {
	if storagePath == "" {
		return nil, errors.New("LXMF cannot be initialised without a storage path")
	}

	if identity == nil {
		id, err := rns.NewIdentity()
		if err != nil {
			return nil, err
		}
		identity = id
	}

	router := &LXMRouter{
		PendingInbound:  []*LXMessage{},
		PendingOutbound: []*LXMessage{},
		FailedOutbound:  []*LXMessage{},

		PrioritisedList:    []string{},
		IgnoredList:        []string{},
		AllowedList:        []string{},
		ControlAllowedList: []string{},
		AuthRequired:       false,

		DefaultSyncStrategy: 0,
		ProcessingInbound:   false,
		ProcessingCount:     0,

		PropagationNode: false,

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
		EnforceStamps:                   false,

		AutoPeer:         AutoPeerDefault,
		AutoPeerMaxDepth: AutoPeerMaxDepth,
		MaxPeers:         MaxPeers,
		FromStaticOnly:   false,
		StaticPeers:      [][]byte{},

		Peers:              map[string]*LXMPeer{},
		OutboundStampCosts: map[string]stampCostEntry{},

		Identity: identity,
	}

	dest, err := rns.NewDestination(identity, rns.DestinationIN, rns.DestinationSINGLE, AppName, "propagation")
	if err != nil {
		return nil, err
	}
	router.PropagationDestination = dest

	rns.RegisterAnnounceHandler(NewDeliveryAnnounceHandler(router))
	rns.RegisterAnnounceHandler(NewPropagationAnnounceHandler(router))

	return router, nil
}

func (r *LXMRouter) UpdateStampCost(destinationHash []byte, cost *int) {
	if cost == nil {
		return
	}
	r.OutboundStampCosts[string(destinationHash)] = stampCostEntry{
		Cost:    *cost,
		Updated: time.Now().Unix(),
	}
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

	// TODO: Port outbound processing from python/LXMF/LXMRouter.py.
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
	peer, ok := r.Peers[key]
	if !ok {
		peer = &LXMPeer{DestinationHash: copyBytes(destinationHash)}
		r.Peers[key] = peer
	}
	peer.LastHeard = int64(nodeTimebase)
	peer.PropagationTransferLimit = propagationTransferLimit
	peer.PropagationSyncLimit = propagationSyncLimit
	peer.PropagationStampCost = propagationStampCost
	peer.PropagationStampCostFlexibility = propagationStampCostFlexibility
	peer.PeeringCost = peeringCost
	peer.Metadata = metadata
}

func (r *LXMRouter) Unpeer(destinationHash []byte, nodeTimebase int) {
	delete(r.Peers, string(destinationHash))
}
