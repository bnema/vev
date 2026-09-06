// Package uidriver exposes attachment UI primitives through bounded JSONL.
package uidriver

import (
	"bytes"
	"encoding/json"
	"io"
	"time"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

const (
	apiVersion         = 1
	maxRequestBytes    = 64 << 10
	maxResponseBytes   = 32 << 20
	maxSerializedBytes = 128 << 20
	maxConnections     = 4
	maxColumns         = 512
	maxRows            = 256
	writeTimeout       = 5 * time.Second
)

type operation string

const (
	opCapture operation = "capture"
	opKeys    operation = "keys"
	opText    operation = "text"
	opWait    operation = "wait"
)

type captureFormat string

const (
	formatText  captureFormat = "text"
	formatCells captureFormat = "cells"
	formatBoth  captureFormat = "both"
)

type request struct {
	ID         uint64
	Op         operation
	Attachment string
	Format     captureFormat
	Action     ports.UIActionRequest
	Wait       ports.UIWaitRequest
}

func invalidRequest() error { return &ports.UIError{Code: ports.UIErrInvalidRequest} }

// decodeRequest validates the exact operation schema before touching a service.
// The standard decoder's case-insensitive struct matching is deliberately not
// used for member names; null is not a substitute for an omitted option.
func decodeRequest(data []byte) (request, error) {
	var result request
	if len(data) > maxRequestBytes || !utf8.Valid(data) || !uniqueJSON(data) {
		return result, invalidRequest()
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return result, invalidRequest()
	}
	field := func(name string, out any, required bool) bool {
		raw, exists := fields[name]
		if !exists {
			return !required
		}
		return !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) && json.Unmarshal(raw, out) == nil
	}
	var version uint16
	if !field("id", &result.ID, true) || result.ID == 0 || !field("version", &version, true) {
		return result, invalidRequest()
	}
	if version != apiVersion {
		return result, &ports.UIError{Code: ports.UIErrUnsupportedVersion}
	}
	if !field("op", &result.Op, true) || !field("attachment", &result.Attachment, true) || result.Attachment == "" {
		return result, invalidRequest()
	}
	allowed := map[string]bool{"version": true, "id": true, "op": true, "attachment": true}
	allow := func(names ...string) {
		for _, name := range names {
			allowed[name] = true
		}
	}
	var timeoutMS int64
	switch result.Op {
	case opCapture:
		allow("format")
		result.Format = formatText
		if !field("format", &result.Format, false) || result.Format != formatText && result.Format != formatCells && result.Format != formatBoth {
			return result, invalidRequest()
		}
	case opKeys, opText:
		allow("generation", "timeout_ms")
		result.Action.Attachment = result.Attachment
		if !field("generation", &result.Action.Generation, true) || result.Action.Generation == 0 {
			return result, invalidRequest()
		}
		if result.Op == opKeys {
			allow("keys")
			if !field("keys", &result.Action.Keys, true) || len(result.Action.Keys) == 0 || len(result.Action.Keys) > 256 {
				return result, invalidRequest()
			}
		} else {
			allow("text")
			if !field("text", &result.Action.Text, true) || result.Action.Text == "" {
				return result, invalidRequest()
			}
		}
	case opWait:
		allow("after_action", "timeout_ms", "expect")
		result.Wait.Attachment = result.Attachment
		if !field("after_action", &result.Wait.AfterAction, false) {
			return result, invalidRequest()
		}
		if _, exists := fields["after_action"]; exists && result.Wait.AfterAction == 0 {
			return result, invalidRequest()
		}
		expect, err := decodeExpect(fields["expect"])
		if err != nil {
			return result, err
		}
		result.Wait.Expect = expect
	default:
		return result, invalidRequest()
	}
	for name := range fields {
		if !allowed[name] {
			return result, invalidRequest()
		}
	}
	if raw, exists := fields["timeout_ms"]; exists {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &timeoutMS) != nil || timeoutMS < 1 || timeoutMS > 30000 {
			return result, invalidRequest()
		}
	}
	result.Action.Timeout = time.Duration(timeoutMS) * time.Millisecond
	result.Wait.Timeout = result.Action.Timeout
	return result, nil
}

func decodeExpect(data []byte) (ports.UIExpect, error) {
	var result ports.UIExpect
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || len(fields) == 0 {
		return result, invalidRequest()
	}
	for name, raw := range fields {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return result, invalidRequest()
		}
		switch name {
		case "text_contains":
			var text string
			if json.Unmarshal(raw, &text) != nil || len(text) > 4096 {
				return result, invalidRequest()
			}
			result.TextContains = &text
		case "status":
			var status ports.UIPresentationStatus
			if json.Unmarshal(raw, &status) != nil {
				return result, invalidRequest()
			}
			switch status {
			case ports.UIStatusAttached, ports.UIStatusTransitioning, ports.UIStatusReconnecting, ports.UIStatusDetached:
			default:
				return result, invalidRequest()
			}
			result.Status = &status
		case "session":
			object, ok := exactStringObject(raw, "lifecycle_id", "session_name")
			if !ok {
				return result, invalidRequest()
			}
			target := protocol.ExactSessionTarget{SessionName: object["session_name"]}
			if target.LifecycleID.UnmarshalText([]byte(object["lifecycle_id"])) != nil || target.Validate() != nil {
				return result, invalidRequest()
			}
			result.Session = &target
		case "focus":
			object, ok := exactStringObject(raw, "tab_id", "pane_id")
			if !ok {
				return result, invalidRequest()
			}
			focus := &ports.UIFocus{TabID: domain.TabStableID(object["tab_id"]), PaneID: domain.PaneStableID(object["pane_id"])}
			if domain.ValidateTabStableID(focus.TabID) != nil || domain.ValidatePaneStableID(focus.PaneID) != nil {
				return result, invalidRequest()
			}
			result.Focus = focus
		default:
			return result, invalidRequest()
		}
	}
	return result, nil
}

func exactStringObject(data []byte, names ...string) (map[string]string, bool) {
	var object map[string]string
	if json.Unmarshal(data, &object) != nil || len(object) != len(names) {
		return nil, false
	}
	for _, name := range names {
		if object[name] == "" {
			return nil, false
		}
	}
	return object, true
}

// uniqueJSON rejects duplicate members at every depth, including escaped aliases.
// Input is already byte-bounded. A small depth cap exceeds every API schema.
func uniqueJSON(data []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value func(int) bool
	value = func(depth int) bool {
		if depth > 8 {
			return false
		}
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return true
		}
		switch delimiter {
		case '{':
			seen := make(map[string]bool)
			for decoder.More() {
				token, err := decoder.Token()
				if err != nil {
					return false
				}
				name, ok := token.(string)
				if !ok || seen[name] {
					return false
				}
				seen[name] = true
				if !value(depth + 1) {
					return false
				}
			}
		case '[':
			for decoder.More() {
				if !value(depth + 1) {
					return false
				}
			}
		default:
			return false
		}
		_, err = decoder.Token()
		return err == nil
	}
	if !value(0) {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}
