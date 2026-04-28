package lxmf

import (
	"bytes"
	"time"

	"github.com/svanichkin/go-reticulum/rns"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

type DeliveryAnnounceHandler struct {
	aspectFilter         string
	receivePathResponses bool
	Router               *LXMRouter
}

func NewDeliveryAnnounceHandler(router *LXMRouter) *DeliveryAnnounceHandler {
	return &DeliveryAnnounceHandler{
		aspectFilter:         AppName + ".delivery",
		receivePathResponses: true,
		Router:               router,
	}
}

func (h *DeliveryAnnounceHandler) AspectFilter() string {
	return h.aspectFilter
}

func (h *DeliveryAnnounceHandler) ReceivePathResponses() bool {
	return h != nil && h.receivePathResponses
}

func (h *DeliveryAnnounceHandler) ReceivedAnnounce(destinationHash []byte, announcedIdentity *rns.Identity, appData []byte) {
	if h.Router == nil {
		return
	}
	if stampCost := StampCostFromAppData(appData); stampCost != nil {
		h.Router.UpdateStampCost(destinationHash, stampCost)
	}

	for _, msg := range h.Router.PendingOutbound {
		if msg == nil {
			continue
		}
		if bytes.Equal(destinationHash, msg.DestinationHash) {
			if msg.Method == MethodDirect || msg.Method == MethodOpportunistic {
				msg.NextDeliveryAttempt = float64(time.Now().UnixNano()) / 1e9
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
	aspectFilter         string
	receivePathResponses bool
	Router               *LXMRouter
}

func NewPropagationAnnounceHandler(router *LXMRouter) *PropagationAnnounceHandler {
	return &PropagationAnnounceHandler{
		aspectFilter:         AppName + ".propagation",
		receivePathResponses: true,
		Router:               router,
	}
}

func (h *PropagationAnnounceHandler) AspectFilter() string {
	return h.aspectFilter
}

func (h *PropagationAnnounceHandler) ReceivePathResponses() bool {
	return h != nil && h.receivePathResponses
}

func (h *PropagationAnnounceHandler) ReceivedAnnounce(destinationHash []byte, announcedIdentity *rns.Identity, appData []byte) {
	h.ReceivedAnnounceWithPacketInfo(destinationHash, announcedIdentity, appData, nil, false)
}

func (h *PropagationAnnounceHandler) ReceivedAnnounceWithPacketInfo(destinationHash []byte, announcedIdentity *rns.Identity, appData []byte, _ []byte, isPathResponse bool) {
	if h.Router == nil || !h.Router.PropagationNode || len(appData) == 0 {
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

	nodeTimebase, _ := func() (int, bool) {
		switch t := data[1].(type) {
		case int:
			return t, true
		case *int:
			if t == nil {
				return 0, false
			}
			return *t, true
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
		case float64:
			return int(t), true
		case bool:
			if t {
				return 1, true
			}
			return 0, true
		default:
			return 0, false
		}
	}()
	propagationEnabled, _ := data[2].(bool)
	propagationTransferLimit, _ := func() (int, bool) {
		switch t := data[3].(type) {
		case int:
			return t, true
		case *int:
			if t == nil {
				return 0, false
			}
			return *t, true
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
		case float64:
			return int(t), true
		case bool:
			if t {
				return 1, true
			}
			return 0, true
		default:
			return 0, false
		}
	}()
	propagationSyncLimit := data[4]
	costs, _ := data[5].([]any)
	metadata, _ := data[6].(map[any]any)
	if len(costs) < 3 {
		return
	}
	propagationStampCost, _ := func() (int, bool) {
		switch t := costs[0].(type) {
		case int:
			return t, true
		case *int:
			if t == nil {
				return 0, false
			}
			return *t, true
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
		case float64:
			return int(t), true
		case bool:
			if t {
				return 1, true
			}
			return 0, true
		default:
			return 0, false
		}
	}()
	propagationStampCostFlexibility, _ := func() (int, bool) {
		switch t := costs[1].(type) {
		case int:
			return t, true
		case *int:
			if t == nil {
				return 0, false
			}
			return *t, true
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
		case float64:
			return int(t), true
		case bool:
			if t {
				return 1, true
			}
			return 0, true
		default:
			return 0, false
		}
	}()
	peeringCost, _ := func() (int, bool) {
		switch t := costs[2].(type) {
		case int:
			return t, true
		case *int:
			if t == nil {
				return 0, false
			}
			return *t, true
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
		case float64:
			return int(t), true
		case bool:
			if t {
				return 1, true
			}
			return 0, true
		default:
			return 0, false
		}
	}()

	if h.Router.StaticPeer(destinationHash) {
		staticPeer := h.Router.Peers[string(destinationHash)]
		if !isPathResponse || staticPeer.LastHeard == 0 {
			h.Router.Peer(destinationHash, nodeTimebase, propagationTransferLimit, propagationSyncLimit, propagationStampCost, propagationStampCostFlexibility, peeringCost, metadata)
		}
		return
	}

	if h.Router.AutoPeer && !isPathResponse {
		if propagationEnabled {
			if rns.HopsTo(destinationHash) <= h.Router.AutoPeerMaxDepth {
				h.Router.Peer(destinationHash, nodeTimebase, propagationTransferLimit, propagationSyncLimit, propagationStampCost, propagationStampCostFlexibility, peeringCost, metadata)
			} else if _, ok := h.Router.Peers[string(destinationHash)]; ok {
				rns.Log("Peer moved outside auto-peering range, breaking peering...", rns.LOG_INFO)
				h.Router.Unpeer(destinationHash, nodeTimebase)
			}
		} else {
			h.Router.Unpeer(destinationHash, nodeTimebase)
		}
	}
}
