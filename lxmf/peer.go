package lxmf

import (
	"bytes"
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
		DestinationHash:                 append([]byte(nil), destinationHash...),
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

func (p *LXMPeer) GeneratePeeringKey() bool {
	if p.PeeringCost <= 0 {
		return false
	}
	p.peeringKeyMu.Lock()
	defer p.peeringKeyMu.Unlock()

	if p.PeeringKey != nil {
		return true
	}
	rns.Log("Generating peering key for "+rns.PrettyHexRep(p.DestinationHash), rns.LOG_NOTICE)
	if p.Router == nil || p.Router.Identity == nil {
		rns.Log("Could not update peering key for "+rns.PrettyHexRep(p.DestinationHash)+" since the local LXMF router identity is not configured", rns.LOG_ERROR)
		return false
	}

	if p.Identity == nil {
		p.Identity = rns.IdentityRecall(p.DestinationHash)
		if p.Identity == nil {
			rns.Log("Could not update peering key for "+rns.PrettyHexRep(p.DestinationHash)+" since its identity could not be recalled", rns.LOG_ERROR)
			return false
		}
	}

	keyMaterial := append(append([]byte(nil), p.Identity.Hash...), p.Router.Identity.Hash...)
	peeringKey, value := GenerateStamp(keyMaterial, p.PeeringCost, WorkblockExpandRoundsPeering)
	if value >= p.PeeringCost {
		p.PeeringKey = []any{peeringKey, value}
		rns.Log("Peering key successfully generated for "+rns.PrettyHexRep(p.DestinationHash), rns.LOG_NOTICE)
		return true
	}
	return false
}

func (p *LXMPeer) Sync() {
	if p == nil || p.Router == nil {
		return
	}
	rns.Log("Initiating LXMF Propagation Node sync with peer "+rns.PrettyHexRep(p.DestinationHash), rns.LOG_DEBUG)
	p.LastSyncAttempt = float64(time.Now().UnixNano()) / 1e9

	syncTimeReached := float64(time.Now().UnixNano())/1e9 > p.NextSyncAttempt
	stampCostsKnown := p.PeeringCost > 0 && p.PropagationStampCost >= 0 && p.PropagationStampCostFlexibility >= 0
	peeringKeyReady := false
	if p.PeeringCost > 0 && len(p.PeeringKey) == 2 {
		value := 0
		switch v := p.PeeringKey[1].(type) {
		case int:
			value = v
		case int8:
			value = int(v)
		case int16:
			value = int(v)
		case int32:
			value = int(v)
		case int64:
			value = int(v)
		case uint:
			value = int(v)
		case uint8:
			value = int(v)
		case uint16:
			value = int(v)
		case uint32:
			value = int(v)
		case uint64:
			value = int(v)
		case float32:
			value = int(v)
		case float64:
			value = int(v)
		}
		if value >= p.PeeringCost {
			peeringKeyReady = true
		} else {
			rns.Log("Peering key value mismatch for "+rns.PrettyHexRep(p.DestinationHash)+". Current value is "+fmt.Sprintf("%d", value)+", but peer requires "+fmt.Sprintf("%d", p.PeeringCost)+". Scheduling regeneration...", rns.LOG_WARNING)
			p.PeeringKey = nil
		}
	}
	syncChecks := syncTimeReached && stampCostsKnown && peeringKeyReady

	if !syncChecks {
		if !syncTimeReached {
			if p.LastSyncAttempt > p.LastHeard {
				p.Alive = false
			}
			delay := p.NextSyncAttempt - float64(time.Now().UnixNano())/1e9
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

	if !rns.HasPath(p.DestinationHash) {
		rns.Log("No path to peer "+rns.PrettyHexRep(p.DestinationHash)+" exists, requesting...", rns.LOG_DEBUG)
		rns.RequestPath(p.DestinationHash, nil, nil, false)
		time.Sleep(time.Duration(PeerPathRequestGrace * float64(time.Second)))
	}

	if !rns.HasPath(p.DestinationHash) {
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
		rns.Log("Sync requested for "+rns.PrettyHexRep(p.DestinationHash)+", but no unhandled messages exist for peer. Sync complete.", rns.LOG_DEBUG)
		return
	}

	if p.CurrentlyTransferringMessages != nil {
		rns.Log("Sync requested for "+rns.PrettyHexRep(p.DestinationHash)+", but current message transfer index was not clear. Aborting.", rns.LOG_ERROR)
		return
	}

	if p.State == PeerIdle {
		rns.Log("Establishing link for sync to peer "+rns.PrettyHexRep(p.DestinationHash)+"...", rns.LOG_DEBUG)
		p.SyncBackoff += PeerSyncBackoffStep
		p.NextSyncAttempt = float64(time.Now().UnixNano())/1e9 + p.SyncBackoff
		link, err := rns.NewLink(p.Destination, nil, rns.LinkModeDefault, p.LinkEstablished, p.LinkClosed)
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
	p.LastHeard = float64(time.Now().UnixNano()) / 1e9
	p.SyncBackoff = 0
	minAcceptedCost := p.PropagationStampCost - p.PropagationStampCostFlexibility
	if minAcceptedCost < 0 {
		minAcceptedCost = 0
	}

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
			if stampValue := p.Router.GetStampValue(transientID); stampValue == nil || *stampValue < minAcceptedCost {
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

	sort.SliceStable(unhandledEntries, func(i, j int) bool {
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

	packedOffer, err := umsgpack.Packb(unhandledIDs)
	if err != nil {
		packedOffer = nil
	}
	rns.Log(fmt.Sprintf("Offering %d messages to peer %s (%s)", len(unhandledIDs), rns.PrettyHexRep(p.Destination.Hash), rns.PrettySize(float64(len(packedOffer)))), rns.LOG_VERBOSE)
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
	response := receipt.GetResponse()

	code := 0
	codeSet := false
	switch v := response.(type) {
	case int:
		code = v
		codeSet = true
	case int8:
		code = int(v)
		codeSet = true
	case int16:
		code = int(v)
		codeSet = true
	case int32:
		code = int(v)
		codeSet = true
	case int64:
		code = int(v)
		codeSet = true
	case uint:
		code = int(v)
		codeSet = true
	case uint8:
		code = int(v)
		codeSet = true
	case uint16:
		code = int(v)
		codeSet = true
	case uint32:
		code = int(v)
		codeSet = true
	case uint64:
		code = int(v)
		codeSet = true
	case float32:
		code = int(v)
		codeSet = true
	case float64:
		code = int(v)
		codeSet = true
	}
	if codeSet {
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
				p.Router.Unpeer(p.DestinationHash, nil)
			}
			return
		case PeerErrorThrottled:
			throttleTime := float64(PNStampThrottle)
			rns.Log(fmt.Sprintf("Remote indicated that we're throttled, postponing sync for %s", rns.PrettyTime(throttleTime, false, false)), rns.LOG_VERBOSE)
			p.NextSyncAttempt = float64(time.Now().UnixNano())/1e9 + throttleTime
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
		responseIDs := func(v any) [][]byte {
			if v == nil {
				return nil
			}
			switch ids := v.(type) {
			case [][]byte:
				out := make([][]byte, 0, len(ids))
				for _, id := range ids {
					if len(id) > 0 {
						out = append(out, append([]byte(nil), id...))
					}
				}
				return out
			case []any:
				out := make([][]byte, 0, len(ids))
				for _, entry := range ids {
					switch id := entry.(type) {
					case []byte:
						if len(id) > 0 {
							out = append(out, append([]byte(nil), id...))
						}
					case string:
						if id != "" {
							out = append(out, []byte(id))
						}
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
		}(response)
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

		data, err := umsgpack.Packb([]any{float64(time.Now().UnixNano()) / 1e9, lxmList})
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
		p.CurrentSyncTransferStarted = float64(time.Now().UnixNano()) / 1e9
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
	if resource.Status == rns.ResourceComplete {
		if p.CurrentlyTransferringMessages == nil {
			rns.Log("Sync transfer completed on "+rns.PrettyHexRep(p.DestinationHash)+", but transferred message index was unavailable. Aborting.", rns.LOG_ERROR)
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
			duration := float64(time.Now().UnixNano())/1e9 - p.CurrentSyncTransferStarted
			if duration > 0 {
				p.SyncTransferRate = float64(resource.GetTransferSize()*8) / duration
				rateStr = " at " + rns.PrettySpeed(p.SyncTransferRate)
			}
		}

		rns.Log(fmt.Sprintf("Syncing %d messages to peer %s completed%s", len(p.CurrentlyTransferringMessages), rns.PrettyHexRep(p.DestinationHash), rateStr), rns.LOG_VERBOSE)
		p.Alive = true
		p.LastHeard = float64(time.Now().UnixNano()) / 1e9
		p.Offered += len(p.LastOffer)
		p.Outgoing += len(p.CurrentlyTransferringMessages)
		p.TxBytes += resource.GetDataSize()

		p.CurrentlyTransferringMessages = nil
		p.CurrentSyncTransferStarted = 0

		if p.SyncStrategy == PeerStrategyPersistent {
			if len(p.UnhandledMessages()) > 0 {
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
	p.UnhandledMessagesQueue = append(p.UnhandledMessagesQueue, append([]byte(nil), transientID...))
}

func (p *LXMPeer) QueueHandledMessage(transientID []byte) {
	p.HandledMessagesQueue = append(p.HandledMessagesQueue, append([]byte(nil), transientID...))
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

		if !func() bool {
			for _, entry := range handledMessages {
				if bytes.Equal(entry, transientID) {
					return true
				}
			}
			return false
		}() {
			p.AddHandledMessage(transientID)
		}
		if func() bool {
			for _, entry := range unhandledMessages {
				if bytes.Equal(entry, transientID) {
					return true
				}
			}
			return false
		}() {
			p.RemoveUnhandledMessage(transientID)
		}
	}

	for len(p.UnhandledMessagesQueue) > 0 {
		idx := len(p.UnhandledMessagesQueue) - 1
		transientID := p.UnhandledMessagesQueue[idx]
		p.UnhandledMessagesQueue = p.UnhandledMessagesQueue[:idx]

		if !func() bool {
			for _, entry := range handledMessages {
				if bytes.Equal(entry, transientID) {
					return true
				}
			}
			return false
		}() && !func() bool {
			for _, entry := range unhandledMessages {
				if bytes.Equal(entry, transientID) {
					return true
				}
			}
			return false
		}() {
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
	for _, transientKey := range p.Router.PropagationOrder {
		entry := p.Router.PropagationEntries[transientKey]
		if entry == nil {
			continue
		}
		found := false
		for _, peer := range entry.HandledPeers {
			if peer == destKey {
				found = true
				break
			}
		}
		if found {
			hm = append(hm, []byte(transientKey))
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
	for _, transientKey := range p.Router.PropagationOrder {
		entry := p.Router.PropagationEntries[transientKey]
		if entry == nil {
			continue
		}
		found := false
		for _, peer := range entry.UnhandledPeers {
			if peer == destKey {
				found = true
				break
			}
		}
		if found {
			um = append(um, []byte(transientKey))
		}
	}
	p.umCount = len(um)
	p.umCountsSynced = true
	return um
}

func (p *LXMPeer) AcceptanceRate() float64 {
	if p == nil || p.Offered == 0 {
		return 0
	}
	return float64(p.Outgoing) / float64(p.Offered)
}

func (p *LXMPeer) AddHandledMessage(transientID []byte) {
	if p == nil || p.Router == nil {
		return
	}
	if entry, ok := p.Router.PropagationEntries[string(transientID)]; ok && entry != nil {
		destKey := string(p.DestinationHash)
		found := false
		for _, peer := range entry.HandledPeers {
			if peer == destKey {
				found = true
				break
			}
		}
		if !found {
			entry.HandledPeers = append(entry.HandledPeers, destKey)
			p.hmCountsSynced = false
		}
	}
}

func (p *LXMPeer) AddUnhandledMessage(transientID []byte) {
	if p == nil || p.Router == nil {
		return
	}
	if entry, ok := p.Router.PropagationEntries[string(transientID)]; ok && entry != nil {
		destKey := string(p.DestinationHash)
		found := false
		for _, peer := range entry.UnhandledPeers {
			if peer == destKey {
				found = true
				break
			}
		}
		if !found {
			entry.UnhandledPeers = append(entry.UnhandledPeers, destKey)
			p.umCount++
		}
	}
}

func (p *LXMPeer) RemoveHandledMessage(transientID []byte) {
	if p == nil || p.Router == nil {
		return
	}
	if entry, ok := p.Router.PropagationEntries[string(transientID)]; ok && entry != nil {
		destKey := string(p.DestinationHash)
		found := false
		for _, peer := range entry.HandledPeers {
			if peer == destKey {
				found = true
				break
			}
		}
		if found {
			for i, peer := range entry.HandledPeers {
				if peer == destKey {
					entry.HandledPeers = append(entry.HandledPeers[:i], entry.HandledPeers[i+1:]...)
					break
				}
			}
			p.hmCountsSynced = false
		}
	}
}

func (p *LXMPeer) RemoveUnhandledMessage(transientID []byte) {
	if p == nil || p.Router == nil {
		return
	}
	if entry, ok := p.Router.PropagationEntries[string(transientID)]; ok && entry != nil {
		destKey := string(p.DestinationHash)
		found := false
		for _, peer := range entry.UnhandledPeers {
			if peer == destKey {
				found = true
				break
			}
		}
		if found {
			for i, peer := range entry.UnhandledPeers {
				if peer == destKey {
					entry.UnhandledPeers = append(entry.UnhandledPeers[:i], entry.UnhandledPeers[i+1:]...)
					break
				}
			}
			p.umCountsSynced = false
		}
	}
}

func copyIDList(ids [][]byte) [][]byte {
	if len(ids) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		out = append(out, append([]byte(nil), id...))
	}
	return out
}
