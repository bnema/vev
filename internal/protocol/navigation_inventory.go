package protocol

import (
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
)

// Navigation inventory bounds. These are export limits for UI listings, not
// limits on stored or running sessions.
const (
	NavigationInventoryMaxSourceGroups = 65
	NavigationInventoryMaxLocalEntries = 4096
	NavigationInventoryMaxDisplayBytes = 256
	NavigationInventoryMaxKeyBytes     = 128
)

// NavigationInventoryOperation selects the control-source query.
type NavigationInventoryOperation uint8

const (
	NavigationInventorySnapshot NavigationInventoryOperation = 1
	NavigationInventoryResolve  NavigationInventoryOperation = 2
)

// NavigationInventoryStatus is the overall source-control outcome.
type NavigationInventoryStatus uint8

const (
	NavigationInventoryOK              NavigationInventoryStatus = 1
	NavigationInventoryUnavailable     NavigationInventoryStatus = 2
	NavigationInventoryInvalid         NavigationInventoryStatus = 3
	NavigationInventoryVersionMismatch NavigationInventoryStatus = 4
)

// NavigationInventorySourceStatus is the per-source outcome. Healthy sources
// survive another source's size or error failure.
type NavigationInventorySourceStatus uint8

const (
	NavigationInventorySourceOK          NavigationInventorySourceStatus = 1
	NavigationInventorySourceUnavailable NavigationInventorySourceStatus = 2
	NavigationInventorySourceTooLarge    NavigationInventorySourceStatus = 3
)

// NavigationInventoryFailureCode classifies resolve/selection failures.
type NavigationInventoryFailureCode uint8

const (
	NavigationInventoryStaleIdentity    NavigationInventoryFailureCode = 1
	NavigationInventorySourceGone       NavigationInventoryFailureCode = 2
	NavigationInventoryIncompatible     NavigationInventoryFailureCode = 3
	NavigationInventoryInvalidTarget    NavigationInventoryFailureCode = 4
	NavigationInventoryNavigationFailed NavigationInventoryFailureCode = 5
	NavigationInventoryRestoreFailed    NavigationInventoryFailureCode = 6
)

// NavigationInventoryEntry is one sanitized public row. It carries display
// facts only, never exact endpoints, connection parameters, CWD,
// environment, preview, or pane content.
type NavigationInventoryEntry struct {
	SourceKey     string
	EntryKey      string
	Name          string
	DisplayOrigin string
	State         string
	Reason        string
}

// NavigationInventorySourceGroup is one packed source with its own status.
type NavigationInventorySourceGroup struct {
	SourceKey string
	Status    NavigationInventorySourceStatus
	Entries   []NavigationInventoryEntry
}

// NavigationInventoryRequest queries a source control connection without
// creating an attachment or Hello session.
type NavigationInventoryRequest struct {
	Version      uint16
	RequestID    uint64
	Operation    NavigationInventoryOperation
	SourceKey    string
	EntryKey     string
	Registration domain.RemoteRegistration
}

// NavigationInventoryResponse answers a control query. Snapshot carries
// source groups; resolve carries a non-mutating AttachTarget; never both.
// Version-mismatch and malformed responses never carry a target.
type NavigationInventoryResponse struct {
	RequestID uint64
	Operation NavigationInventoryOperation
	Status    NavigationInventoryStatus
	Groups    []NavigationInventorySourceGroup
	Resolved  *AttachTarget
}

// NavigationInventoryDemand is serving daemon → client.
type NavigationInventoryDemand struct {
	InteractionGeneration uint64
	Open                  bool
}

// NavigationInventoryPublication is client → serving daemon.
type NavigationInventoryPublication struct {
	InteractionGeneration uint64
	PublicationGeneration uint64
	Groups                []NavigationInventorySourceGroup
}

// NavigationInventorySelection is serving daemon → client. No endpoint or
// name is supplied as authority; the key resolves through the source.
type NavigationInventorySelection struct {
	CauseActionID         uint64
	InteractionGeneration uint64
	PublicationGeneration uint64
	SourceKey             string
	EntryKey              string
}

// NavigationInventoryFailure is client → serving daemon.
type NavigationInventoryFailure struct {
	CauseActionID         uint64
	InteractionGeneration uint64
	SourceKey             string
	EntryKey              string
	Code                  NavigationInventoryFailureCode
}

func validNavigationInventoryOperation(op NavigationInventoryOperation) bool {
	return op == NavigationInventorySnapshot || op == NavigationInventoryResolve
}

func validNavigationInventoryStatus(status NavigationInventoryStatus) bool {
	switch status {
	case NavigationInventoryOK,
		NavigationInventoryUnavailable,
		NavigationInventoryInvalid,
		NavigationInventoryVersionMismatch:
		return true
	default:
		return false
	}
}

func validNavigationInventorySourceStatus(status NavigationInventorySourceStatus) bool {
	switch status {
	case NavigationInventorySourceOK,
		NavigationInventorySourceUnavailable,
		NavigationInventorySourceTooLarge:
		return true
	default:
		return false
	}
}

func validNavigationInventoryFailureCode(code NavigationInventoryFailureCode) bool {
	switch code {
	case NavigationInventoryStaleIdentity,
		NavigationInventorySourceGone,
		NavigationInventoryIncompatible,
		NavigationInventoryInvalidTarget,
		NavigationInventoryNavigationFailed,
		NavigationInventoryRestoreFailed:
		return true
	default:
		return false
	}
}

func validInventoryKey(value string) bool {
	if value == "" || len(value) > NavigationInventoryMaxKeyBytes {
		return false
	}
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f) {
			return false
		}
	}
	return true
}

func validInventoryDisplay(value string) bool {
	if len(value) > NavigationInventoryMaxDisplayBytes {
		return false
	}
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f) {
			return false
		}
	}
	return true
}

// ValidateNavigationInventoryRequest enforces nonzero IDs, operation-dependent
// unions, key shape, registration identity for resolve, and display safety.
func ValidateNavigationInventoryRequest(request NavigationInventoryRequest) error {
	if request.RequestID == 0 || !validNavigationInventoryOperation(request.Operation) {
		return ErrInvalidNavigation
	}
	if request.Operation == NavigationInventorySnapshot {
		if request.SourceKey != "" || request.EntryKey != "" || !request.Registration.IsZero() {
			return ErrInvalidNavigation
		}
		return nil
	}
	if !validInventoryKey(request.SourceKey) || !validInventoryKey(request.EntryKey) {
		return ErrInvalidNavigation
	}
	if request.Registration.Validate() != nil {
		return ErrInvalidNavigation
	}
	return nil
}

// ValidateNavigationInventoryResponse enforces ID/operation echo, status
// codes, union shape, unique keys, and display safety. Targets are rejected
// on version-mismatch or malformed outcomes.
func ValidateNavigationInventoryResponse(response NavigationInventoryResponse) error {
	if response.RequestID == 0 || !validNavigationInventoryOperation(response.Operation) {
		return ErrInvalidNavigation
	}
	if !validNavigationInventoryStatus(response.Status) {
		return ErrInvalidNavigation
	}
	if response.Status == NavigationInventoryVersionMismatch {
		if response.Resolved != nil || len(response.Groups) != 0 {
			return ErrInvalidNavigation
		}
		return nil
	}
	if response.Operation == NavigationInventorySnapshot {
		if response.Resolved != nil {
			return ErrInvalidNavigation
		}
		return validateNavigationInventoryGroups(response.Groups, true)
	}
	if len(response.Groups) != 0 {
		return ErrInvalidNavigation
	}
	if response.Status != NavigationInventoryOK {
		if response.Resolved != nil {
			return ErrInvalidNavigation
		}
		return nil
	}
	if response.Resolved == nil {
		return ErrInvalidNavigation
	}
	return ValidateAttachTarget(*response.Resolved)
}

func validateNavigationInventoryGroups(groups []NavigationInventorySourceGroup, snapshot bool) error {
	if len(groups) > NavigationInventoryMaxSourceGroups {
		return ErrInvalidNavigation
	}
	sources := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		if !validInventoryKey(group.SourceKey) || !validNavigationInventorySourceStatus(group.Status) {
			return ErrInvalidNavigation
		}
		if _, dup := sources[group.SourceKey]; dup {
			return ErrInvalidNavigation
		}
		sources[group.SourceKey] = struct{}{}
		if group.Status != NavigationInventorySourceOK && len(group.Entries) != 0 {
			return ErrInvalidNavigation
		}
		entries := make(map[string]struct{}, len(group.Entries))
		for _, entry := range group.Entries {
			if entry.SourceKey != group.SourceKey || !validInventoryKey(entry.EntryKey) {
				return ErrInvalidNavigation
			}
			if _, dup := entries[entry.EntryKey]; dup {
				return ErrInvalidNavigation
			}
			entries[entry.EntryKey] = struct{}{}
			if !validInventoryDisplay(entry.Name) || !validInventoryDisplay(entry.DisplayOrigin) ||
				!validInventoryDisplay(entry.State) || !validInventoryDisplay(entry.Reason) {
				return ErrInvalidNavigation
			}
		}
		if !snapshot && len(group.Entries) == 0 {
			return ErrInvalidNavigation
		}
	}
	return nil
}

// ValidateNavigationInventoryDemand requires a nonzero interaction.
func ValidateNavigationInventoryDemand(demand NavigationInventoryDemand) error {
	if demand.InteractionGeneration == 0 {
		return ErrInvalidNavigation
	}
	return nil
}

// ValidateNavigationInventoryPublication requires nonzero generations and
// sanitized groups.
func ValidateNavigationInventoryPublication(publication NavigationInventoryPublication) error {
	if publication.InteractionGeneration == 0 || publication.PublicationGeneration == 0 {
		return ErrInvalidNavigation
	}
	return validateNavigationInventoryGroups(publication.Groups, true)
}

// ValidateNavigationInventorySelection rejects unknown keys only at resolve
// time; wire validation enforces shape, nonzero IDs, and key safety.
func ValidateNavigationInventorySelection(selection NavigationInventorySelection) error {
	if selection.CauseActionID == 0 || selection.InteractionGeneration == 0 ||
		selection.PublicationGeneration == 0 {
		return ErrInvalidNavigation
	}
	if !validInventoryKey(selection.SourceKey) || !validInventoryKey(selection.EntryKey) {
		return ErrInvalidNavigation
	}
	return nil
}

// ValidateNavigationInventoryFailure enforces matching IDs and bounded codes.
func ValidateNavigationInventoryFailure(failure NavigationInventoryFailure) error {
	if failure.CauseActionID == 0 || failure.InteractionGeneration == 0 {
		return ErrInvalidNavigation
	}
	if !validInventoryKey(failure.SourceKey) || !validInventoryKey(failure.EntryKey) {
		return ErrInvalidNavigation
	}
	if !validNavigationInventoryFailureCode(failure.Code) {
		return ErrInvalidNavigation
	}
	return nil
}
