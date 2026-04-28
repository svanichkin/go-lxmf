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
		var peerData any
		if err := umsgpack.Unpackb(appData, &peerData); err != nil {
			panic(err)
		}
		list, ok := peerData.([]any)
		if !ok || len(list) < 1 || list[0] == nil {
			return ""
		}
		switch name := list[0].(type) {
		case []byte:
			if !utf8.Valid(name) {
				rns.Log("Could not decode display name in included announce data. The contained exception was: invalid UTF-8", rns.LOG_ERROR)
				return ""
			}
			return string(name)
		default:
			rns.Log("Could not decode display name in included announce data. The contained exception was: invalid type", rns.LOG_ERROR)
			return ""
		}
	}

	if !utf8.Valid(appData) {
		panic("invalid UTF-8")
	}
	return string(appData)
}

func StampCostFromAppData(appData []byte) any {
	if len(appData) == 0 {
		return nil
	}

	if appData[0] >= 0x90 && appData[0] <= 0x9f || appData[0] == 0xdc {
		var peerData any
		if err := umsgpack.Unpackb(appData, &peerData); err != nil {
			return nil
		}
		list, ok := peerData.([]any)
		if !ok || len(list) < 2 {
			return nil
		}
		return list[1]
	}

	return nil
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
	if !ok {
		return ""
	}
	for key, value := range meta {
		match := false
		switch k := key.(type) {
		case int:
			match = k == PNMetaName
		case int8:
			match = int(k) == PNMetaName
		case int16:
			match = int(k) == PNMetaName
		case int32:
			match = int(k) == PNMetaName
		case int64:
			match = int(k) == PNMetaName
		case uint:
			match = int(k) == PNMetaName
		case uint8:
			match = int(k) == PNMetaName
		case uint16:
			match = int(k) == PNMetaName
		case uint32:
			match = int(k) == PNMetaName
		case uint64:
			match = int(k) == PNMetaName
		}
		if !match {
			continue
		}
		if v, ok := value.([]byte); ok {
			if !utf8.Valid(v) {
				return ""
			}
			return string(v)
		}
	}
	return ""
}

func PNStampCostFromAppData(appData []byte) any {
	if len(appData) == 0 {
		return nil
	}
	if !PNAnnounceDataIsValid(appData) {
		return nil
	}

	var data []any
	if err := umsgpack.Unpackb(appData, &data); err != nil || len(data) < 6 {
		return nil
	}
	costs, ok := data[5].([]any)
	if !ok || len(costs) < 1 {
		return nil
	}
	return costs[0]
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

	if _, ok := func() (int, bool) {
		switch t := decoded[1].(type) {
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
	}(); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode timebase", rns.LOG_DEBUG)
		return false
	}
	statusValid := false
	switch v := decoded[2].(type) {
	case bool:
		statusValid = true
	case int:
		statusValid = v == 0 || v == 1
	case int8:
		statusValid = v == 0 || v == 1
	case int16:
		statusValid = v == 0 || v == 1
	case int32:
		statusValid = v == 0 || v == 1
	case int64:
		statusValid = v == 0 || v == 1
	case uint:
		statusValid = v == 0 || v == 1
	case uint8:
		statusValid = v == 0 || v == 1
	case uint16:
		statusValid = v == 0 || v == 1
	case uint32:
		statusValid = v == 0 || v == 1
	case uint64:
		statusValid = v == 0 || v == 1
	case float64:
		statusValid = v == 0 || v == 1
	}
	if !statusValid {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Indeterminate propagation node status", rns.LOG_DEBUG)
		return false
	}
	if _, ok := func() (int, bool) {
		switch t := decoded[3].(type) {
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
	}(); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode propagation transfer limit", rns.LOG_DEBUG)
		return false
	}
	if _, ok := func() (int, bool) {
		switch t := decoded[4].(type) {
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
	}(); !ok {
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
	if _, ok := func() (int, bool) {
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
	}(); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode target stamp cost", rns.LOG_DEBUG)
		return false
	}
	if _, ok := func() (int, bool) {
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
	}(); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode stamp cost flexibility", rns.LOG_DEBUG)
		return false
	}
	if _, ok := func() (int, bool) {
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
	}(); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode peering cost", rns.LOG_DEBUG)
		return false
	}
	if _, ok := decoded[6].(map[any]any); !ok {
		rns.Log("Could not validate propagation node announce data: Invalid announce data: Could not decode metadata", rns.LOG_DEBUG)
		return false
	}

	return true
}
