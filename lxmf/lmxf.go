package lxmf

import (
	"fmt"
	"unicode/utf8"

	"github.com/svanichkin/go-reticulum/rns"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

const AppName = "lxmf"

// Core LXMF field identifiers.
const (
	FieldEmbeddedLXMs    = 0x01
	FieldTelemetry       = 0x02
	FieldTelemetryStream = 0x03
	FieldIconAppearance  = 0x04
	FieldFileAttachments = 0x05
	FieldImage           = 0x06
	FieldAudio           = 0x07
	FieldThread          = 0x08
	FieldCommands        = 0x09
	FieldResults         = 0x0A
	FieldGroup           = 0x0B
	FieldTicket          = 0x0C
	FieldEvent           = 0x0D
	FieldRnrRefs         = 0x0E
	FieldRenderer        = 0x0F

	FieldCustomType = 0xFB
	FieldCustomData = 0xFC
	FieldCustomMeta = 0xFD

	FieldNonSpecific = 0xFE
	FieldDebug       = 0xFF
)

// Audio modes for FieldAudio.
const (
	AMCodec2450PWB = 0x01
	AMCodec2450    = 0x02
	AMCodec2700C   = 0x03
	AMCodec21200   = 0x04
	AMCodec21300   = 0x05
	AMCodec21400   = 0x06
	AMCodec21600   = 0x07
	AMCodec22400   = 0x08
	AMCodec23200   = 0x09

	AMOpusOgg       = 0x10
	AMOpusLBW       = 0x11
	AMOpusMBW       = 0x12
	AMOpusPTT       = 0x13
	AMOpusRTHDX     = 0x14
	AMOpusRTFDX     = 0x15
	AMOpusStandard  = 0x16
	AMOpusHQ        = 0x17
	AMOpusBroadcast = 0x18
	AMOpusLossless  = 0x19

	AMCustom = 0xFF
)

// Renderer specifications for FieldRenderer.
const (
	RendererPlain    = 0x00
	RendererMicron   = 0x01
	RendererMarkdown = 0x02
	RendererBBCode   = 0x03
)

// Propagation node metadata fields.
const (
	PNMetaVersion      = 0x00
	PNMetaName         = 0x01
	PNMetaSyncStratum  = 0x02
	PNMetaSyncThrottle = 0x03
	PNMetaAuthBand     = 0x04
	PNMetaUtilPressure = 0x05
	PNMetaCustom       = 0xFF
)

func DisplayNameFromAppData(appData []byte) string {
	if len(appData) == 0 {
		return ""
	}

	if appData[0] >= 0x90 && appData[0] <= 0x9f || appData[0] == 0xdc {
		var peerData []any
		if err := umsgpack.Unpackb(appData, &peerData); err != nil {
			rns.Log(fmt.Sprintf("Could not decode display name in included announce data. The contained exception was: %v", err), rns.LOG_ERROR)
			return ""
		}
		if len(peerData) < 1 || peerData[0] == nil {
			return ""
		}
		switch name := peerData[0].(type) {
		case []byte:
			if !utf8.Valid(name) {
				rns.Log("Could not decode display name in included announce data: invalid UTF-8", rns.LOG_ERROR)
				return ""
			}
			return string(name)
		case string:
			if !utf8.ValidString(name) {
				rns.Log("Could not decode display name in included announce data: invalid UTF-8", rns.LOG_ERROR)
				return ""
			}
			return name
		default:
			rns.Log("Could not decode display name in included announce data: invalid type", rns.LOG_ERROR)
			return ""
		}
	}

	return string(appData)
}

func StampCostFromAppData(appData []byte) (int, bool) {
	if len(appData) == 0 {
		return 0, false
	}

	if appData[0] >= 0x90 && appData[0] <= 0x9f || appData[0] == 0xdc {
		var peerData []any
		if err := umsgpack.Unpackb(appData, &peerData); err != nil {
			return 0, false
		}
		if len(peerData) < 2 {
			return 0, false
		}
		switch v := peerData[1].(type) {
		case int:
			return v, true
		case int64:
			return int(v), true
		case int32:
			return int(v), true
		case uint:
			return int(v), true
		case uint64:
			return int(v), true
		case uint32:
			return int(v), true
		case float64:
			return int(v), true
		default:
			return 0, false
		}
	}

	return 0, false
}

func PNNameFromAppData(appData []byte) string {
	if len(appData) == 0 {
		return ""
	}
	if !PNAnnounceDataIsValid(appData) {
		return ""
	}

	var data []any
	if err := umsgpack.Unpackb(appData, &data); err != nil || len(data) < 7 {
		return ""
	}
	meta, ok := data[6].(map[any]any)
	var name any
	if ok {
		if v, ok := meta[PNMetaName]; ok {
			name = v
		} else {
			for k, v := range meta {
				if keyInt, ok := asInt(k); ok && keyInt == PNMetaName {
					name = v
					break
				}
				switch key := k.(type) {
				case []byte:
					if len(key) == 1 && int(key[0]) == PNMetaName {
						name = v
						break
					}
				case string:
					if len(key) == 1 && int(key[0]) == PNMetaName {
						name = v
						break
					}
				}
			}
		}
	} else if metaStr, ok := data[6].(map[string]any); ok {
		if v, ok := metaStr[fmt.Sprintf("%d", PNMetaName)]; ok {
			name = v
		} else if v, ok := metaStr[string([]byte{byte(PNMetaName)})]; ok {
			name = v
		}
	}
	if name == nil {
		return ""
	}
	switch v := name.(type) {
	case []byte:
		if !utf8.Valid(v) {
			return ""
		}
		return string(v)
	case string:
		if !utf8.ValidString(v) {
			return ""
		}
		return v
	default:
		return ""
	}
}

func PNStampCostFromAppData(appData []byte) (int, bool) {
	if len(appData) == 0 {
		return 0, false
	}
	if !PNAnnounceDataIsValid(appData) {
		return 0, false
	}

	var data []any
	if err := umsgpack.Unpackb(appData, &data); err != nil || len(data) < 6 {
		return 0, false
	}
	costs, ok := data[5].([]any)
	if !ok || len(costs) < 1 {
		return 0, false
	}
	switch v := costs[0].(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case int32:
		return int(v), true
	case uint:
		return int(v), true
	case uint64:
		return int(v), true
	case uint32:
		return int(v), true
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

func PNAnnounceDataIsValid(data []byte) bool {
	if len(data) == 0 {
		return false
	}

	var decoded []any
	if err := umsgpack.Unpackb(data, &decoded); err != nil {
		rns.Log(fmt.Sprintf("Could not validate propagation node announce data: %v", err), rns.LOG_DEBUG)
		return false
	}
	if len(decoded) < 7 {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Insufficient peer data, likely from deprecated LXMF version", rns.LOG_DEBUG)
		return false
	}

	if _, ok := asInt(decoded[1]); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode timebase", rns.LOG_DEBUG)
		return false
	}
	if _, ok := decoded[2].(bool); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Indeterminate propagation node status", rns.LOG_DEBUG)
		return false
	}
	if _, ok := asInt(decoded[3]); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode propagation transfer limit", rns.LOG_DEBUG)
		return false
	}
	if _, ok := asInt(decoded[4]); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode propagation sync limit", rns.LOG_DEBUG)
		return false
	}
	costs, ok := decoded[5].([]any)
	if !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode stamp costs", rns.LOG_DEBUG)
		return false
	}
	if len(costs) < 3 {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode stamp costs", rns.LOG_DEBUG)
		return false
	}
	if _, ok := asInt(costs[0]); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode target stamp cost", rns.LOG_DEBUG)
		return false
	}
	if _, ok := asInt(costs[1]); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode stamp cost flexibility", rns.LOG_DEBUG)
		return false
	}
	if _, ok := asInt(costs[2]); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode peering cost", rns.LOG_DEBUG)
		return false
	}
	if _, ok := decoded[6].(map[any]any); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode metadata", rns.LOG_DEBUG)
		return false
	}

	return true
}

func asInt(v any) (int, bool) {
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
	case float64:
		return int(t), true
	default:
		return 0, false
	}
}
