package lxmf

import (
	"time"

	"github.com/svanichkin/go-reticulum/rns"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

type DeliveryAnnounceHandler struct {
	AspectFilter         string
	ReceivePathResponses bool
	Router               *LXMRouter
}

func NewDeliveryAnnounceHandler(router *LXMRouter) *DeliveryAnnounceHandler {
	return &DeliveryAnnounceHandler{
		AspectFilter:         AppName + ".delivery",
		ReceivePathResponses: true,
		Router:               router,
	}
}

func (h *DeliveryAnnounceHandler) ReceivedAnnounce(destinationHash []byte, announcedIdentity *rns.Identity, appData []byte) {
	if h.Router == nil {
		return
	}
	if stampCost, ok := StampCostFromAppData(appData); ok {
		h.Router.UpdateStampCost(destinationHash, &stampCost)
	}

	for _, msg := range h.Router.PendingOutbound {
		if msg == nil {
			continue
		}
		if bytesEqual(destinationHash, msg.DestinationHash) {
			if msg.Method == MethodDirect || msg.Method == MethodOpportunistic {
				msg.NextDeliveryAttempt = time.Now().Unix()
				go func() {
					for h.Router.outboundProcessingLockLocked() {
						time.Sleep(100 * time.Millisecond)
					}
					h.Router.ProcessOutbound()
				}()
			}
		}
	}
}

type PropagationAnnounceHandler struct {
	AspectFilter         string
	ReceivePathResponses bool
	Router               *LXMRouter
}

func NewPropagationAnnounceHandler(router *LXMRouter) *PropagationAnnounceHandler {
	return &PropagationAnnounceHandler{
		AspectFilter:         AppName + ".propagation",
		ReceivePathResponses: true,
		Router:               router,
	}
}

func (h *PropagationAnnounceHandler) ReceivedAnnounce(destinationHash []byte, announcedIdentity *rns.Identity, appData []byte, announcePacketHash []byte, isPathResponse bool) {
	if h.Router == nil || len(appData) == 0 {
		return
	}
	if !PNAnnounceDataIsValid(appData) {
		return
	}
	var data []any
	if err := umsgpack.Unpackb(appData, &data); err != nil || len(data) < 7 {
		rns.Log("Error while evaluating propagation node announce, ignoring announce.", rns.LOG_DEBUG)
		if err != nil {
			rns.Log("The contained exception was: "+err.Error(), rns.LOG_DEBUG)
		}
		return
	}

	nodeTimebase, _ := asInt(data[1])
	propagationEnabled, _ := data[2].(bool)
	propagationTransferLimit, _ := asInt(data[3])
	propagationSyncLimit, _ := asInt(data[4])
	costs, _ := data[5].([]any)
	metadata, _ := data[6].(map[any]any)
	if len(costs) < 3 {
		return
	}
	propagationStampCost, _ := asInt(costs[0])
	propagationStampCostFlexibility, _ := asInt(costs[1])
	peeringCost, _ := asInt(costs[2])

	if h.Router.IsStaticPeer(destinationHash) {
		staticPeer := h.Router.PeerByHash(destinationHash)
		if staticPeer != nil {
			if !isPathResponse || staticPeer.LastHeard == 0 {
				h.Router.Peer(destinationHash, nodeTimebase, propagationTransferLimit, propagationSyncLimit, propagationStampCost, propagationStampCostFlexibility, peeringCost, metadata)
			}
		}
		return
	}

	if h.Router.AutoPeer && !isPathResponse {
		if propagationEnabled {
			if rns.TransportHopsTo(destinationHash) <= h.Router.AutoPeerMaxDepth {
				h.Router.Peer(destinationHash, nodeTimebase, propagationTransferLimit, propagationSyncLimit, propagationStampCost, propagationStampCostFlexibility, peeringCost, metadata)
			} else if h.Router.HasPeer(destinationHash) {
				rns.Log("Peer moved outside auto-peering range, breaking peering...", rns.LOG_INFO)
				h.Router.Unpeer(destinationHash, nodeTimebase)
			}
		} else {
			h.Router.Unpeer(destinationHash, nodeTimebase)
		}
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
