package uidriver

import (
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestDecodeRequestStrictOperationSchemas(t *testing.T) {
	for _, tt := range []struct {
		name, input string
		code        ports.UIErrorCode
	}{
		{"capture", `{"version":1,"id":1,"op":"capture","attachment":"a"}`, ""},
		{"keys", `{"version":1,"id":1,"op":"keys","attachment":"a","generation":2,"keys":["Alt+Space"],"timeout_ms":30}`, ""},
		{"text", `{"version":1,"id":1,"op":"text","attachment":"a","generation":2,"text":"hello"}`, ""},
		{"wait", `{"version":1,"id":1,"op":"wait","attachment":"a","after_action":3,"expect":{"text_contains":"hello","status":"attached","focus":{"tab_id":"t","pane_id":"p"},"session":{"lifecycle_id":"01000000000000000000000000000000","session_name":"work"}}}`, ""},
		{"unknown", `{"version":1,"id":1,"op":"capture","attachment":"a","extra":0}`, ports.UIErrInvalidRequest},
		{"wrong case", `{"Version":1,"id":1,"op":"capture","attachment":"a"}`, ports.UIErrInvalidRequest},
		{"duplicate", `{"version":1,"id":1,"id":2,"op":"capture","attachment":"a"}`, ports.UIErrInvalidRequest},
		{"escaped duplicate", `{"version":1,"id":1,"\u0069d":2,"op":"capture","attachment":"a"}`, ports.UIErrInvalidRequest},
		{"nested duplicate", `{"version":1,"id":1,"op":"wait","attachment":"a","expect":{"status":"attached","status":"detached"}}`, ports.UIErrInvalidRequest},
		{"nested unknown", `{"version":1,"id":1,"op":"wait","attachment":"a","expect":{"focus":{"tab_id":"t","pane_id":"p","extra":"x"}}}`, ports.UIErrInvalidRequest},
		{"unknown predicate", `{"version":1,"id":1,"op":"wait","attachment":"a","expect":{"regex":".*"}}`, ports.UIErrInvalidRequest},
		{"empty predicate", `{"version":1,"id":1,"op":"wait","attachment":"a","expect":{}}`, ports.UIErrInvalidRequest},
		{"null option", `{"version":1,"id":1,"op":"capture","attachment":"a","format":null}`, ports.UIErrInvalidRequest},
		{"zero id", `{"version":1,"id":0,"op":"capture","attachment":"a"}`, ports.UIErrInvalidRequest},
		{"negative id", `{"version":1,"id":-1,"op":"capture","attachment":"a"}`, ports.UIErrInvalidRequest},
		{"wrong version", `{"version":2,"id":1,"op":"capture","attachment":"a"}`, ports.UIErrUnsupportedVersion},
		{"mixed operation", `{"version":1,"id":1,"op":"keys","attachment":"a","generation":2,"keys":["x"],"text":"x"}`, ports.UIErrInvalidRequest},
		{"capture generation", `{"version":1,"id":1,"op":"capture","attachment":"a","generation":2}`, ports.UIErrInvalidRequest},
		{"zero generation", `{"version":1,"id":1,"op":"text","attachment":"a","generation":0,"text":"x"}`, ports.UIErrInvalidRequest},
		{"timeout bound", `{"version":1,"id":1,"op":"text","attachment":"a","generation":2,"text":"x","timeout_ms":30001}`, ports.UIErrInvalidRequest},
		{"truncated", `{"version":1`, ports.UIErrInvalidRequest},
		{"trailing", `{"version":1,"id":1,"op":"capture","attachment":"a"}{}`, ports.UIErrInvalidRequest},
		{"oversized", strings.Repeat(" ", maxRequestBytes+1), ports.UIErrInvalidRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := decodeRequest([]byte(tt.input))
			if tt.code == "" {
				require.NoError(t, err)
				require.Equal(t, uint64(1), decoded.ID)
			} else {
				var uiErr *ports.UIError
				require.ErrorAs(t, err, &uiErr)
				require.Equal(t, tt.code, uiErr.Code)
			}
		})
	}
}

func TestDecodeRequestDefaultsAndTypedValues(t *testing.T) {
	capture, err := decodeRequest([]byte(`{"version":1,"id":1,"op":"capture","attachment":"a"}`))
	require.NoError(t, err)
	require.Equal(t, formatText, capture.Format)
	keys, err := decodeRequest([]byte(`{"version":1,"id":2,"op":"keys","attachment":"a","generation":4,"keys":["Escape"],"timeout_ms":25}`))
	require.NoError(t, err)
	require.Equal(t, 25*time.Millisecond, keys.Action.Timeout)
	require.Equal(t, uint64(4), keys.Action.Generation)
}
