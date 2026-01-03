package lxmf

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/svanichkin/go-reticulum/rns"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

const (
	OfferRequestPath = "/offer"
	MessageGetPath   = "/get"

	PeerIdle                 = 0x00
	PeerLinkEstablishing     = 0x01
	PeerLinkReady            = 0x02
	PeerRequestSent          = 0x03
	PeerResponseReceived     = 0x04
	PeerResourceTransferring = 0x05

	PeerErrorNoIdentity   = 0xf0
	PeerErrorNoAccess     = 0xf1
	PeerErrorInvalidKey   = 0xf3
	PeerErrorInvalidData  = 0xf4
	PeerErrorInvalidStamp = 0xf5
	PeerErrorThrottled    = 0xf6
	PeerErrorNotFound     = 0xfd
	PeerErrorTimeout      = 0xfe

	PeerStrategyLazy        = 0x01
	PeerStrategyPersistent  = 0x02
	PeerDefaultSyncStrategy = PeerStrategyPersistent

	PeerMaxUnreachable   = 14 * 24 * 60 * 60
	PeerSyncBackoffStep  = 12 * 60
	PeerPathRequestGrace = 7.5
)

type LXMPeer struct {
	Router *LXMRouter

	Alive        bool
	LastHeard    float64
	SyncStrategy int
	PeeringKey   []any
	PeeringCost  int
	Metadata     map[any]any

	NextSyncAttempt       float64
	LastSyncAttempt       float64
	SyncBackoff           float64
	PeeringTimebase       float64
	LinkEstablishmentRate float64
	SyncTransferRate      float64

	PropagationTransferLimit        float64
	PropagationSyncLimit            float64
	PropagationStampCost            int
	PropagationStampCostFlexibility int

	CurrentlyTransferringMessages [][]byte
	CurrentSyncTransferStarted    float64
	HandledMessagesQueue          [][]byte
	UnhandledMessagesQueue        [][]byte

	Offered  int
	Outgoing int
	Incoming int
	RxBytes  int
	TxBytes  int

	hmCount        int
	umCount        int
	hmCountsSynced bool
	umCountsSynced bool

	peeringKeyMu sync.Mutex
	Link         *rns.Link
	State        int

	LastOffer [][]byte

	DestinationHash []byte
	Identity        *rns.Identity
	Destination     *rns.Destination
}

func NewLXMPeer(router *LXMRouter, destinationHash []byte, syncStrategy int) *LXMPeer {
	peer := &LXMPeer{
		Router:                          router,
		Alive:                           false,
		LastHeard:                       0,
		SyncStrategy:                    syncStrategy,
		PeeringCost:                     0,
		Metadata:                        nil,
		NextSyncAttempt:                 0,
		LastSyncAttempt:                 0,
		SyncBackoff:                     0,
		PeeringTimebase:                 0,
		LinkEstablishmentRate:           0,
		SyncTransferRate:                0,
		PropagationTransferLimit:        0,
		PropagationSyncLimit:            0,
		PropagationStampCost:            0,
		PropagationStampCostFlexibility: 0,
		Offered:                         0,
		Outgoing:                        0,
		Incoming:                        0,
		RxBytes:                         0,
		TxBytes:                         0,
		hmCount:                         0,
		umCount:                         0,
		hmCountsSynced:                  false,
		umCountsSynced:                  false,
		DestinationHash:                 copyBytes(destinationHash),
		State:                           PeerIdle,
	}

	peer.Identity = rns.IdentityRecall(destinationHash)
	if peer.Identity != nil {
		dest, _ := rns.NewDestination(peer.Identity, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "propagation")
		peer.Destination = dest
	} else {
		rns.Log(fmt.Sprintf("Could not recall identity for LXMF propagation peer %s, will retry identity resolution on next sync", rns.PrettyHexRep(peer.DestinationHash)), rns.LOG_WARNING)
	}

	return peer
}

func LXMPeerFromBytes(peerBytes []byte, router *LXMRouter) (*LXMPeer, error) {
	if len(peerBytes) == 0 {
		return nil, errors.New("empty peer data")
	}
	var dict map[any]any
	if err := umsgpack.Unpackb(peerBytes, &dict); err != nil {
		return nil, err
	}
	rawHash := toBytes(dict["destination_hash"])
	if len(rawHash) == 0 {
		return nil, errors.New("missing destination hash")
	}
	peer := NewLXMPeer(router, rawHash, PeerDefaultSyncStrategy)

	peer.PeeringTimebase = floatFromAny(dict["peering_timebase"])
	peer.Alive = boolFromAny(dict["alive"])
	peer.LastHeard = floatFromAny(dict["last_heard"])
	peer.LinkEstablishmentRate = floatFromAny(dict["link_establishment_rate"])
	peer.SyncTransferRate = floatFromAny(dict["sync_transfer_rate"])
	if v, ok := dict["propagation_transfer_limit"]; ok {
		peer.PropagationTransferLimit = floatFromAny(v)
	}
	if v, ok := dict["propagation_sync_limit"]; ok {
		peer.PropagationSyncLimit = floatFromAny(v)
	} else {
		peer.PropagationSyncLimit = peer.PropagationTransferLimit
	}
	if v, ok := dict["propagation_stamp_cost"]; ok {
		peer.PropagationStampCost = int(floatFromAny(v))
	}
	if v, ok := dict["propagation_stamp_cost_flexibility"]; ok {
		peer.PropagationStampCostFlexibility = int(floatFromAny(v))
	}
	if v, ok := dict["peering_cost"]; ok {
		peer.PeeringCost = int(floatFromAny(v))
	}
	if v, ok := dict["sync_strategy"]; ok {
		peer.SyncStrategy = int(floatFromAny(v))
	}
	if v, ok := dict["offered"]; ok {
		peer.Offered = int(floatFromAny(v))
	}
	if v, ok := dict["outgoing"]; ok {
		peer.Outgoing = int(floatFromAny(v))
	}
	if v, ok := dict["incoming"]; ok {
		peer.Incoming = int(floatFromAny(v))
	}
	if v, ok := dict["rx_bytes"]; ok {
		peer.RxBytes = int(floatFromAny(v))
	}
	if v, ok := dict["tx_bytes"]; ok {
		peer.TxBytes = int(floatFromAny(v))
	}
	if v, ok := dict["last_sync_attempt"]; ok {
		peer.LastSyncAttempt = floatFromAny(v)
	}
	if v, ok := dict["peering_key"]; ok {
		if key, ok := v.([]any); ok {
			peer.PeeringKey = key
		}
	}
	if meta, ok := dict["metadata"].(map[any]any); ok {
		peer.Metadata = meta
	}

	hmCount := 0
	for _, id := range decodeIDList(dict["handled_ids"]) {
		if router != nil {
			if _, ok := router.PropagationEntries[string(id)]; ok {
				peer.AddHandledMessage(id)
				hmCount++
			}
		}
	}

	umCount := 0
	for _, id := range decodeIDList(dict["unhandled_ids"]) {
		if router != nil {
			if _, ok := router.PropagationEntries[string(id)]; ok {
				peer.AddUnhandledMessage(id)
				umCount++
			}
		}
	}

	peer.hmCount = hmCount
	peer.umCount = umCount
	peer.hmCountsSynced = true
	peer.umCountsSynced = true

	return peer, nil
}

func (p *LXMPeer) UnhandledMessageCount() int {
	if p == nil {
		return 0
	}
	if !p.umCountsSynced {
		p.updateCounts()
	}
	return p.umCount
}

func boolFromAny(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case int:
		return t != 0
	case int64:
		return t != 0
	case float64:
		return t != 0
	default:
		return false
	}
}

func (p *LXMPeer) ToBytes() ([]byte, error) {
	if p == nil {
		return nil, errors.New("nil peer")
	}
	dictionary := map[any]any{
		"peering_timebase":                   p.PeeringTimebase,
		"alive":                              p.Alive,
		"metadata":                           p.Metadata,
		"last_heard":                         p.LastHeard,
		"sync_strategy":                      p.SyncStrategy,
		"peering_key":                        p.PeeringKey,
		"destination_hash":                   p.DestinationHash,
		"link_establishment_rate":            p.LinkEstablishmentRate,
		"sync_transfer_rate":                 p.SyncTransferRate,
		"propagation_transfer_limit":         p.PropagationTransferLimit,
		"propagation_sync_limit":             p.PropagationSyncLimit,
		"propagation_stamp_cost":             p.PropagationStampCost,
		"propagation_stamp_cost_flexibility": p.PropagationStampCostFlexibility,
		"peering_cost":                       p.PeeringCost,
		"last_sync_attempt":                  p.LastSyncAttempt,
		"offered":                            p.Offered,
		"outgoing":                           p.Outgoing,
		"incoming":                           p.Incoming,
		"rx_bytes":                           p.RxBytes,
		"tx_bytes":                           p.TxBytes,
		"handled_ids":                        p.HandledMessages(),
		"unhandled_ids":                      p.UnhandledMessages(),
	}

	peerBytes, err := umsgpack.Packb(dictionary)
	if err != nil {
		return nil, err
	}
	return peerBytes, nil
}

func (p *LXMPeer) PeeringKeyReady() bool {
	if p.PeeringCost <= 0 {
		return false
	}
	if len(p.PeeringKey) == 2 {
		value, ok := intFromAny(p.PeeringKey[1])
		if ok && value >= p.PeeringCost {
			return true
		}
		rns.Log(fmt.Sprintf("Peering key value mismatch for %s. Current value is %d, but peer requires %d. Scheduling regeneration...", p, value, p.PeeringCost), rns.LOG_WARNING)
		p.PeeringKey = nil
	}
	return false
}

func (p *LXMPeer) PeeringKeyValue() *int {
	if len(p.PeeringKey) == 2 {
		if value, ok := intFromAny(p.PeeringKey[1]); ok {
			return &value
		}
	}
	return nil
}

func (p *LXMPeer) GeneratePeeringKey() bool {
	if p.PeeringCost <= 0 {
		return false
	}
	p.peeringKeyMu.Lock()
	defer p.peeringKeyMu.Unlock()

	if p.PeeringKey != nil {
		return true
	}
	rns.Log(fmt.Sprintf("Generating peering key for %s", p), rns.LOG_NOTICE)
	if p.Router == nil || p.Router.Identity == nil {
		rns.Log(fmt.Sprintf("Could not update peering key for %s since the local LXMF router identity is not configured", p), rns.LOG_ERROR)
		return false
	}

	if p.Identity == nil {
		p.Identity = rns.IdentityRecall(p.DestinationHash)
		if p.Identity == nil {
			rns.Log(fmt.Sprintf("Could not update peering key for %s since its identity could not be recalled", p), rns.LOG_ERROR)
			return false
		}
	}

	keyMaterial := append(copyBytes(p.Identity.Hash), p.Router.Identity.Hash...)
	peeringKey, value := GenerateStamp(keyMaterial, p.PeeringCost, WorkblockExpandRoundsPeering)
	if value >= p.PeeringCost {
		p.PeeringKey = []any{peeringKey, value}
		rns.Log(fmt.Sprintf("Peering key successfully generated for %s", p), rns.LOG_NOTICE)
		return true
	}
	return false
}

func (p *LXMPeer) Sync() {
	if p == nil || p.Router == nil {
		return
	}
	rns.Log("Initiating LXMF Propagation Node sync with peer "+rns.PrettyHexRep(p.DestinationHash), rns.LOG_DEBUG)
	p.LastSyncAttempt = nowSeconds()

	syncTimeReached := nowSeconds() > p.NextSyncAttempt
	stampCostsKnown := p.PropagationStampCost != 0 && p.PropagationStampCostFlexibility != 0 && p.PeeringCost != 0
	peeringKeyReady := p.PeeringKeyReady()
	syncChecks := syncTimeReached && stampCostsKnown && peeringKeyReady

	if !syncChecks {
		if !syncTimeReached {
			if p.LastSyncAttempt > p.LastHeard {
				p.Alive = false
			}
			delay := p.NextSyncAttempt - nowSeconds()
			postponeDelay := ""
			if delay > 0 {
				postponeDelay = " for " + rns.PrettyTime(delay, false, false)
			}
			rns.Log("Postponing sync with peer "+rns.PrettyHexRep(p.DestinationHash)+postponeDelay+" due to previous failures", rns.LOG_DEBUG)
		} else if !stampCostsKnown {
			rns.Log("Postponing sync with peer "+rns.PrettyHexRep(p.DestinationHash)+" since its required stamp costs are not yet known", rns.LOG_DEBUG)
		} else if !peeringKeyReady {
			rns.Log("Postponing sync with peer "+rns.PrettyHexRep(p.DestinationHash)+" since a peering key has not been generated yet", rns.LOG_DEBUG)
			go func() {
				_ = p.GeneratePeeringKey()
			}()
		}
		return
	}

	if !rns.TransportHasPath(p.DestinationHash) {
		rns.Log("No path to peer "+rns.PrettyHexRep(p.DestinationHash)+" exists, requesting...", rns.LOG_DEBUG)
		rns.TransportRequestPath(p.DestinationHash)
		time.Sleep(time.Duration(PeerPathRequestGrace * float64(time.Second)))
	}

	if !rns.TransportHasPath(p.DestinationHash) {
		rns.Log("Path request was not answered, retrying sync with peer "+rns.PrettyHexRep(p.DestinationHash)+" later", rns.LOG_DEBUG)
		return
	}

	if p.Identity == nil {
		p.Identity = rns.IdentityRecall(p.DestinationHash)
		if p.Identity != nil {
			dest, _ := rns.NewDestination(p.Identity, rns.DestinationOUT, rns.DestinationSINGLE, AppName, "propagation")
			p.Destination = dest
		}
	}

	if p.Destination == nil {
		rns.Log("Could not request sync to peer "+rns.PrettyHexRep(p.DestinationHash)+" since its identity could not be recalled.", rns.LOG_ERROR)
		return
	}

	unhandled := p.UnhandledMessages()
	if len(unhandled) == 0 {
		rns.Log(fmt.Sprintf("Sync requested for %s, but no unhandled messages exist for peer. Sync complete.", p), rns.LOG_DEBUG)
		return
	}

	if p.CurrentlyTransferringMessages != nil {
		rns.Log(fmt.Sprintf("Sync requested for %s, but current message transfer index was not clear. Aborting.", p), rns.LOG_ERROR)
		return
	}

	if p.State == PeerIdle {
		rns.Log("Establishing link for sync to peer "+rns.PrettyHexRep(p.DestinationHash)+"...", rns.LOG_DEBUG)
		p.SyncBackoff += PeerSyncBackoffStep
		p.NextSyncAttempt = nowSeconds() + p.SyncBackoff
		link, err := rns.NewOutgoingLink(p.Destination, rns.LinkModeDefault, p.LinkEstablished, p.LinkClosed)
		if err != nil {
			rns.Log("Could not establish sync link for "+rns.PrettyHexRep(p.DestinationHash)+": "+err.Error(), rns.LOG_ERROR)
			return
		}
		p.Link = link
		p.State = PeerLinkEstablishing
		return
	}

	if p.State != PeerLinkReady {
		return
	}

	p.Alive = true
	p.LastHeard = nowSeconds()
	p.SyncBackoff = 0
	minAcceptedCost := minInt(0, p.PropagationStampCost-p.PropagationStampCostFlexibility)

	rns.Log("Synchronisation link to peer "+rns.PrettyHexRep(p.DestinationHash)+" established, preparing sync offer...", rns.LOG_DEBUG)
	type unhandledEntry struct {
		id     []byte
		weight float64
		size   int64
	}
	unhandledEntries := make([]unhandledEntry, 0)
	unhandledIDs := make([][]byte, 0)
	purgedIDs := make([][]byte, 0)
	lowValueIDs := make([][]byte, 0)

	for _, transientID := range unhandled {
		entry := p.Router.PropagationEntries[string(transientID)]
		if entry != nil {
			if p.Router.GetStampValue(transientID) < minAcceptedCost {
				lowValueIDs = append(lowValueIDs, transientID)
			} else {
				unhandledEntries = append(unhandledEntries, unhandledEntry{
					id:     transientID,
					weight: p.Router.GetWeight(transientID),
					size:   int64(p.Router.GetSize(transientID)),
				})
			}
		} else {
			purgedIDs = append(purgedIDs, transientID)
		}
	}

	for _, transientID := range purgedIDs {
		rns.Log("Dropping unhandled message "+rns.PrettyHexRep(transientID)+" for peer "+rns.PrettyHexRep(p.DestinationHash)+" since it no longer exists in the message store.", rns.LOG_DEBUG)
		p.RemoveUnhandledMessage(transientID)
	}

	for _, transientID := range lowValueIDs {
		rns.Log("Dropping unhandled message "+rns.PrettyHexRep(transientID)+" for peer "+rns.PrettyHexRep(p.DestinationHash)+" since its stamp value is lower than peer requirement of "+fmt.Sprintf("%d", minAcceptedCost)+".", rns.LOG_DEBUG)
		p.RemoveUnhandledMessage(transientID)
	}

	sort.Slice(unhandledEntries, func(i, j int) bool {
		return unhandledEntries[i].weight < unhandledEntries[j].weight
	})

	perMessageOverhead := int64(16)
	cumulativeSize := int64(24)
	if p.PropagationTransferLimit > 0 || p.PropagationSyncLimit > 0 {
		rns.Log(fmt.Sprintf("Syncing to peer with per-message limit %s and sync limit %s",
			rns.PrettySize(p.PropagationTransferLimit*1000),
			rns.PrettySize(p.PropagationSyncLimit*1000)), rns.LOG_DEBUG)
	}

	for _, entry := range unhandledEntries {
		lxmTransferSize := entry.size + perMessageOverhead
		nextSize := cumulativeSize + lxmTransferSize

		if p.PropagationTransferLimit > 0 && float64(lxmTransferSize) > p.PropagationTransferLimit*1000 {
			p.RemoveUnhandledMessage(entry.id)
			p.AddHandledMessage(entry.id)
			continue
		}

		if p.PropagationSyncLimit > 0 && float64(nextSize) >= p.PropagationSyncLimit*1000 {
			continue
		}

		cumulativeSize += lxmTransferSize
		unhandledIDs = append(unhandledIDs, entry.id)
	}

	offer := []any{p.PeeringKey[0], unhandledIDs}

	rns.Log(fmt.Sprintf("Offering %d messages to peer %s (%s)", len(unhandledIDs), rns.PrettyHexRep(p.Destination.Hash()), rns.PrettySize(float64(lenMustPack(unhandledIDs)))), rns.LOG_VERBOSE)
	p.LastOffer = copyIDList(unhandledIDs)
	if p.Link != nil {
		p.Link.Request(OfferRequestPath, offer, p.OfferResponse, p.RequestFailed, nil, 0)
		p.State = PeerRequestSent
	}
}

func (p *LXMPeer) RequestFailed(_ *rns.RequestReceipt) {
	rns.Log(fmt.Sprintf("Sync request to peer %v failed", p.Destination), rns.LOG_DEBUG)
	if p.Link != nil {
		p.Link.Teardown()
	}
	p.State = PeerIdle
}

func (p *LXMPeer) OfferResponse(receipt *rns.RequestReceipt) {
	defer func() {
		if r := recover(); r != nil {
			rns.Log("Error while handling offer response from peer "+fmt.Sprintf("%v", p.Destination), rns.LOG_ERROR)
			if p.Link != nil {
				p.Link.Teardown()
			}
			p.Link = nil
			p.State = PeerIdle
		}
	}()

	p.State = PeerResponseReceived
	if receipt == nil {
		return
	}
	response := receipt.Response()

	if code, ok := intFromAny(response); ok {
		switch code {
		case PeerErrorNoIdentity:
			if p.Link != nil {
				rns.Log("Remote peer indicated that no identification was received, retrying...", rns.LOG_VERBOSE)
				if p.Router != nil && p.Router.Identity != nil {
					p.Link.Identify(p.Router.Identity)
				}
				p.State = PeerLinkReady
				p.Sync()
				return
			}
		case PeerErrorNoAccess:
			rns.Log("Remote indicated that access was denied, breaking peering", rns.LOG_VERBOSE)
			if p.Router != nil {
				p.Router.Unpeer(p.DestinationHash, 0)
			}
			return
		case PeerErrorThrottled:
			throttleTime := float64(PNStampThrottle)
			rns.Log(fmt.Sprintf("Remote indicated that we're throttled, postponing sync for %s", rns.PrettyTime(throttleTime, false, false)), rns.LOG_VERBOSE)
			p.NextSyncAttempt = nowSeconds() + throttleTime
			return
		}
	}

	wantedMessages := make([]*propagationEntry, 0)
	wantedMessageIDs := make([][]byte, 0)
	unhandledMap := make(map[string]bool)
	for _, id := range p.UnhandledMessages() {
		unhandledMap[string(id)] = true
	}

	if v, ok := response.(bool); ok {
		if !v {
			for _, transientID := range p.LastOffer {
				if unhandledMap[string(transientID)] {
					p.AddHandledMessage(transientID)
					p.RemoveUnhandledMessage(transientID)
				}
			}
		} else {
			for _, transientID := range p.LastOffer {
				if entry, ok := p.Router.PropagationEntries[string(transientID)]; ok {
					wantedMessages = append(wantedMessages, entry)
					wantedMessageIDs = append(wantedMessageIDs, transientID)
				}
			}
		}
	} else {
		responseIDs := decodeIDList(response)
		responseSet := make(map[string]bool, len(responseIDs))
		for _, id := range responseIDs {
			responseSet[string(id)] = true
		}

		for _, transientID := range p.LastOffer {
			if !responseSet[string(transientID)] {
				p.AddHandledMessage(transientID)
				p.RemoveUnhandledMessage(transientID)
			}
		}

		for _, transientID := range responseIDs {
			if entry, ok := p.Router.PropagationEntries[string(transientID)]; ok {
				wantedMessages = append(wantedMessages, entry)
				wantedMessageIDs = append(wantedMessageIDs, transientID)
			}
		}
	}

	if len(wantedMessages) > 0 {
		rns.Log(fmt.Sprintf("Peer %s wanted %d of the available messages", rns.PrettyHexRep(p.DestinationHash), len(wantedMessages)), rns.LOG_VERBOSE)
		lxmList := make([][]byte, 0, len(wantedMessages))
		for _, entry := range wantedMessages {
			if entry == nil {
				continue
			}
			if _, err := os.Stat(entry.FilePath); err != nil {
				continue
			}
			data, err := os.ReadFile(entry.FilePath)
			if err != nil {
				continue
			}
			lxmList = append(lxmList, data)
		}

		data, err := umsgpack.Packb([]any{nowSeconds(), lxmList})
		if err != nil {
			rns.Log("Could not pack sync data for peer "+rns.PrettyHexRep(p.DestinationHash)+": "+err.Error(), rns.LOG_ERROR)
			if p.Link != nil {
				p.Link.Teardown()
			}
			p.Link = nil
			p.State = PeerIdle
			return
		}

		rns.Log(fmt.Sprintf("Total transfer size for this sync is %s", rns.PrettySize(float64(len(data)))), rns.LOG_VERBOSE)
		_, err = rns.NewResource(
			data,
			nil,
			p.Link,
			nil,
			true,
			false,
			p.ResourceConcluded,
			nil,
			nil,
			0,
			nil,
			nil,
			false,
			0,
		)
		if err != nil {
			rns.Log("Could not start sync resource transfer for peer "+rns.PrettyHexRep(p.DestinationHash)+": "+err.Error(), rns.LOG_ERROR)
			if p.Link != nil {
				p.Link.Teardown()
			}
			p.Link = nil
			p.State = PeerIdle
			return
		}

		p.CurrentlyTransferringMessages = copyIDList(wantedMessageIDs)
		p.CurrentSyncTransferStarted = nowSeconds()
		p.State = PeerResourceTransferring
		return
	}

	rns.Log(fmt.Sprintf("Peer %s did not request any of the available messages, sync completed", rns.PrettyHexRep(p.DestinationHash)), rns.LOG_VERBOSE)
	p.Offered += len(p.LastOffer)
	if p.Link != nil {
		p.Link.Teardown()
	}
	p.Link = nil
	p.State = PeerIdle
}

func (p *LXMPeer) ResourceConcluded(resource *rns.Resource) {
	if resource == nil {
		return
	}
	if resource.Status() == rns.ResourceComplete {
		if p.CurrentlyTransferringMessages == nil {
			rns.Log(fmt.Sprintf("Sync transfer completed on %s, but transferred message index was unavailable. Aborting.", p), rns.LOG_ERROR)
			if p.Link != nil {
				p.Link.Teardown()
			}
			p.Link = nil
			p.State = PeerIdle
			return
		}

		for _, transientID := range p.CurrentlyTransferringMessages {
			p.AddHandledMessage(transientID)
			p.RemoveUnhandledMessage(transientID)
		}

		if p.Link != nil {
			p.Link.Teardown()
		}
		p.Link = nil
		p.State = PeerIdle

		rateStr := ""
		if p.CurrentSyncTransferStarted > 0 {
			duration := nowSeconds() - p.CurrentSyncTransferStarted
			if duration > 0 {
				p.SyncTransferRate = float64(resource.GetTransferSize()*8) / duration
				rateStr = " at " + rns.PrettySpeed(p.SyncTransferRate)
			}
		}

		rns.Log(fmt.Sprintf("Syncing %d messages to peer %s completed%s", len(p.CurrentlyTransferringMessages), rns.PrettyHexRep(p.DestinationHash), rateStr), rns.LOG_VERBOSE)
		p.Alive = true
		p.LastHeard = nowSeconds()
		p.Offered += len(p.LastOffer)
		p.Outgoing += len(p.CurrentlyTransferringMessages)
		p.TxBytes += resource.GetDataSize()

		p.CurrentlyTransferringMessages = nil
		p.CurrentSyncTransferStarted = 0

		if p.SyncStrategy == PeerStrategyPersistent {
			if p.UnhandledMessageCount() > 0 {
				p.Sync()
			}
		}
		return
	}

	rns.Log("Resource transfer for LXMF peer sync failed to "+fmt.Sprintf("%v", p.Destination), rns.LOG_VERBOSE)
	if p.Link != nil {
		p.Link.Teardown()
	}
	p.Link = nil
	p.State = PeerIdle
	p.CurrentlyTransferringMessages = nil
	p.CurrentSyncTransferStarted = 0
}

func (p *LXMPeer) LinkEstablished(link *rns.Link) {
	if p.Router != nil && p.Router.Identity != nil {
		link.Identify(p.Router.Identity)
	}
	if link.EstablishmentRate > 0 {
		p.LinkEstablishmentRate = link.EstablishmentRate
	}

	p.State = PeerLinkReady
	p.NextSyncAttempt = 0
	p.Sync()
}

func (p *LXMPeer) LinkClosed(_ *rns.Link) {
	p.Link = nil
	p.State = PeerIdle
}

func (p *LXMPeer) QueuedItems() bool {
	return len(p.HandledMessagesQueue) > 0 || len(p.UnhandledMessagesQueue) > 0
}

func (p *LXMPeer) QueueUnhandledMessage(transientID []byte) {
	if len(transientID) == 0 {
		return
	}
	p.UnhandledMessagesQueue = append(p.UnhandledMessagesQueue, copyBytes(transientID))
}

func (p *LXMPeer) QueueHandledMessage(transientID []byte) {
	if len(transientID) == 0 {
		return
	}
	p.HandledMessagesQueue = append(p.HandledMessagesQueue, copyBytes(transientID))
}

func (p *LXMPeer) ProcessQueues() {
	if len(p.UnhandledMessagesQueue) == 0 && len(p.HandledMessagesQueue) == 0 {
		return
	}
	handledMessages := p.HandledMessages()
	unhandledMessages := p.UnhandledMessages()

	for len(p.HandledMessagesQueue) > 0 {
		idx := len(p.HandledMessagesQueue) - 1
		transientID := p.HandledMessagesQueue[idx]
		p.HandledMessagesQueue = p.HandledMessagesQueue[:idx]

		if !containsBytes(handledMessages, transientID) {
			p.AddHandledMessage(transientID)
		}
		if containsBytes(unhandledMessages, transientID) {
			p.RemoveUnhandledMessage(transientID)
		}
	}

	for len(p.UnhandledMessagesQueue) > 0 {
		idx := len(p.UnhandledMessagesQueue) - 1
		transientID := p.UnhandledMessagesQueue[idx]
		p.UnhandledMessagesQueue = p.UnhandledMessagesQueue[:idx]

		if !containsBytes(handledMessages, transientID) && !containsBytes(unhandledMessages, transientID) {
			p.AddUnhandledMessage(transientID)
		}
	}
}

func (p *LXMPeer) HandledMessages() [][]byte {
	if p == nil || p.Router == nil {
		return nil
	}
	destKey := string(p.DestinationHash)
	hm := make([][]byte, 0)
	for transientID, entry := range p.Router.PropagationEntries {
		if entry == nil {
			continue
		}
		if containsString(entry.HandledPeers, destKey) {
			hm = append(hm, []byte(transientID))
		}
	}
	p.hmCount = len(hm)
	p.hmCountsSynced = true
	return hm
}

func (p *LXMPeer) UnhandledMessages() [][]byte {
	if p == nil || p.Router == nil {
		return nil
	}
	destKey := string(p.DestinationHash)
	um := make([][]byte, 0)
	for transientID, entry := range p.Router.PropagationEntries {
		if entry == nil {
			continue
		}
		if containsString(entry.UnhandledPeers, destKey) {
			um = append(um, []byte(transientID))
		}
	}
	p.umCount = len(um)
	p.umCountsSynced = true
	return um
}

func (p *LXMPeer) HandledMessageCount() int {
	if p == nil {
		return 0
	}
	if !p.hmCountsSynced {
		p.updateCounts()
	}
	return p.hmCount
}

func (p *LXMPeer) AcceptanceRate() float64 {
	if p == nil || p.Offered == 0 {
		return 0
	}
	return float64(p.Outgoing) / float64(p.Offered)
}

func (p *LXMPeer) updateCounts() {
	if !p.hmCountsSynced {
		_ = p.HandledMessages()
	}
	if !p.umCountsSynced {
		_ = p.UnhandledMessages()
	}
}

func (p *LXMPeer) AddHandledMessage(transientID []byte) {
	if p == nil || p.Router == nil || len(transientID) == 0 {
		return
	}
	if entry, ok := p.Router.PropagationEntries[string(transientID)]; ok && entry != nil {
		destKey := string(p.DestinationHash)
		if !containsString(entry.HandledPeers, destKey) {
			entry.HandledPeers = append(entry.HandledPeers, destKey)
			p.hmCountsSynced = false
		}
	}
}

func (p *LXMPeer) AddUnhandledMessage(transientID []byte) {
	if p == nil || p.Router == nil || len(transientID) == 0 {
		return
	}
	if entry, ok := p.Router.PropagationEntries[string(transientID)]; ok && entry != nil {
		destKey := string(p.DestinationHash)
		if !containsString(entry.UnhandledPeers, destKey) {
			entry.UnhandledPeers = append(entry.UnhandledPeers, destKey)
			p.umCount++
		}
	}
}

func (p *LXMPeer) RemoveHandledMessage(transientID []byte) {
	if p == nil || p.Router == nil || len(transientID) == 0 {
		return
	}
	if entry, ok := p.Router.PropagationEntries[string(transientID)]; ok && entry != nil {
		destKey := string(p.DestinationHash)
		if containsString(entry.HandledPeers, destKey) {
			entry.HandledPeers = removeString(entry.HandledPeers, destKey)
			p.hmCountsSynced = false
		}
	}
}

func (p *LXMPeer) RemoveUnhandledMessage(transientID []byte) {
	if p == nil || p.Router == nil || len(transientID) == 0 {
		return
	}
	if entry, ok := p.Router.PropagationEntries[string(transientID)]; ok && entry != nil {
		destKey := string(p.DestinationHash)
		if containsString(entry.UnhandledPeers, destKey) {
			entry.UnhandledPeers = removeString(entry.UnhandledPeers, destKey)
			p.umCountsSynced = false
		}
	}
}

func (p *LXMPeer) Name() string {
	if p == nil || p.Metadata == nil {
		return ""
	}
	var name any
	for key, value := range p.Metadata {
		if keyInt, ok := intFromAny(key); ok && keyInt == PNMetaName {
			name = value
			break
		}
	}
	if name == nil {
		return ""
	}
	if b, ok := name.([]byte); ok {
		return string(b)
	}
	if s, ok := name.(string); ok {
		return s
	}
	return ""
}

func (p *LXMPeer) String() string {
	if len(p.DestinationHash) > 0 {
		return rns.PrettyHexRep(p.DestinationHash)
	}
	return "<Unknown>"
}

func decodeIDList(v any) [][]byte {
	if v == nil {
		return nil
	}
	switch ids := v.(type) {
	case [][]byte:
		out := make([][]byte, 0, len(ids))
		for _, id := range ids {
			if len(id) > 0 {
				out = append(out, copyBytes(id))
			}
		}
		return out
	case []any:
		out := make([][]byte, 0, len(ids))
		for _, entry := range ids {
			id := toBytes(entry)
			if len(id) > 0 {
				out = append(out, id)
			}
		}
		return out
	case []string:
		out := make([][]byte, 0, len(ids))
		for _, entry := range ids {
			if entry != "" {
				out = append(out, []byte(entry))
			}
		}
		return out
	default:
		return nil
	}
}

func copyIDList(ids [][]byte) [][]byte {
	if len(ids) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		out = append(out, copyBytes(id))
	}
	return out
}

func containsBytes(list [][]byte, id []byte) bool {
	for _, entry := range list {
		if bytesEqual(entry, id) {
			return true
		}
	}
	return false
}

func containsString(list []string, value string) bool {
	for _, entry := range list {
		if entry == value {
			return true
		}
	}
	return false
}

func removeString(list []string, value string) []string {
	for i, entry := range list {
		if entry == value {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func intFromAny(v any) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, true
	case int8:
		return int(t), true
	case int16:
		return int(t), true
	case int32:
		return int(t), true
	case int64:
		return int(t), true
	case uint:
		return int(t), true
	case uint8:
		return int(t), true
	case uint16:
		return int(t), true
	case uint32:
		return int(t), true
	case uint64:
		return int(t), true
	case float32:
		return int(t), true
	case float64:
		return int(t), true
	default:
		return 0, false
	}
}

func lenMustPack(ids [][]byte) int {
	packed, err := umsgpack.Packb(ids)
	if err != nil {
		return 0
	}
	return len(packed)
}
