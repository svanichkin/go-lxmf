package lxmf

// TODO: Port from python/LXMF/LXMPeer.py.

type LXMPeer struct {
	DestinationHash                 []byte
	LastHeard                       int64
	PropagationTransferLimit        int
	PropagationSyncLimit            int
	PropagationStampCost            int
	PropagationStampCostFlexibility int
	PeeringCost                     int
	Metadata                        map[any]any
}
