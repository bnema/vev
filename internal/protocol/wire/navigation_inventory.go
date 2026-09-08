package wire

import (
	"encoding/binary"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// Navigation inventory wire bounds. Complete encoded response/publication
// stays below MaxFrameLen; 4 MiB is the documented export budget.
const navigationInventoryMaxEncodedBytes = 4 << 20

// PeekNavigationInventoryVersion returns the leading protocol version from a
// NavigationInventoryRequest payload, mirroring the Hello/Command peekers.
func PeekNavigationInventoryVersion(b []byte) (uint16, bool) {
	if len(b) < 2 {
		return 0, false
	}
	return binary.BigEndian.Uint16(b), true
}

// PeekNavigationInventoryRequestID returns the request ID following the
// leading version in a NavigationInventoryRequest payload.
func PeekNavigationInventoryRequestID(b []byte) (uint64, bool) {
	if len(b) < 10 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b[2:10]), true
}

func marshalInventoryRegistration(w *payloadWriter, registration domain.RemoteRegistration) {
	w.putString(registration.Endpoint)
	w.putBytes(registration.Incarnation[:])
	w.putUint64(uint64(registration.Generation))
}

func unmarshalInventoryRegistration(r *payloadReader) (domain.RemoteRegistration, error) {
	var registration domain.RemoteRegistration
	var err error
	if registration.Endpoint, err = r.getString(); err != nil {
		return domain.RemoteRegistration{}, err
	}
	incarnation, err := r.getBytes(16)
	if err != nil {
		return domain.RemoteRegistration{}, err
	}
	copy(registration.Incarnation[:], incarnation)
	generation, err := r.getUint64()
	if err != nil {
		return domain.RemoteRegistration{}, err
	}
	registration.Generation = domain.RemoteGeneration(generation)
	// Zero registrations decode structurally: local resolve carries no
	// registration, and the semantic union validator enforces the
	// operation/source coupling afterward.
	if !registration.IsZero() {
		if err := registration.Validate(); err != nil {
			return domain.RemoteRegistration{}, err
		}
	}
	return registration, nil
}

func marshalInventoryGroups(w *payloadWriter, groups []protocol.NavigationInventorySourceGroup) bool {
	if len(groups) > protocol.NavigationInventoryMaxSourceGroups {
		return false
	}
	w.putUint16(uint16(len(groups)))
	for _, group := range groups {
		w.putString(group.SourceKey)
		w.putUint8(uint8(group.Status))
		if group.Status != protocol.NavigationInventorySourceOK {
			w.putUint32(0)
			continue
		}
		if len(group.Entries) > inventoryGroupEntryLimit(group.SourceKey) {
			return false
		}
		w.putUint32(uint32(len(group.Entries)))
		for _, entry := range group.Entries {
			w.putString(entry.EntryKey)
			w.putString(entry.Name)
			w.putString(entry.DisplayOrigin)
			w.putString(entry.State)
			w.putString(entry.Reason)
		}
	}
	return true
}

// inventoryGroupEntryLimit mirrors semantic validation: the local source
// streams live state, remote sources mirror bounded directory caches.
func inventoryGroupEntryLimit(sourceKey string) int {
	if sourceKey == protocol.NavigationInventoryLocalSourceKey {
		return protocol.NavigationInventoryMaxLocalEntries
	}
	return protocol.NavigationInventoryMaxRemoteEntriesPerSource
}

func unmarshalInventoryGroups(r *payloadReader) ([]protocol.NavigationInventorySourceGroup, error) {
	count, err := r.getUint16()
	if err != nil {
		return nil, err
	}
	if int(count) > protocol.NavigationInventoryMaxSourceGroups {
		return nil, protocol.ErrInvalidNavigation
	}
	groups := make([]protocol.NavigationInventorySourceGroup, 0, int(count))
	for range int(count) {
		var group protocol.NavigationInventorySourceGroup
		if group.SourceKey, err = r.getString(); err != nil {
			return nil, err
		}
		status, err := r.getUint8()
		if err != nil {
			return nil, err
		}
		group.Status = protocol.NavigationInventorySourceStatus(status)
		entryCount, err := r.getUint32()
		if err != nil {
			return nil, err
		}
		if entryCount > uint32(inventoryGroupEntryLimit(group.SourceKey)) {
			return nil, protocol.ErrInvalidNavigation
		}
		if group.Status != protocol.NavigationInventorySourceOK && entryCount != 0 {
			return nil, protocol.ErrInvalidNavigation
		}
		if entryCount > 0 {
			group.Entries = make([]protocol.NavigationInventoryEntry, 0, int(entryCount))
			for range int(entryCount) {
				var entry protocol.NavigationInventoryEntry
				entry.SourceKey = group.SourceKey
				if entry.EntryKey, err = r.getString(); err != nil {
					return nil, err
				}
				if entry.Name, err = r.getString(); err != nil {
					return nil, err
				}
				if entry.DisplayOrigin, err = r.getString(); err != nil {
					return nil, err
				}
				if entry.State, err = r.getString(); err != nil {
					return nil, err
				}
				if entry.Reason, err = r.getString(); err != nil {
					return nil, err
				}
				group.Entries = append(group.Entries, entry)
			}
		}
		groups = append(groups, group)
	}
	return groups, nil
}

// MarshalNavigationInventoryRequest encodes a control-source query. Version
// stays first so the version peeker works.
func MarshalNavigationInventoryRequest(request protocol.NavigationInventoryRequest) []byte {
	if protocol.ValidateNavigationInventoryRequest(request) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint16(request.Version)
	w.putUint64(request.RequestID)
	w.putUint8(uint8(request.Operation))
	if request.Operation == protocol.NavigationInventoryResolve {
		w.putString(request.SourceKey)
		w.putString(request.EntryKey)
		marshalInventoryRegistration(&w, request.Registration)
	}
	if len(w.b) > navigationInventoryMaxEncodedBytes {
		return nil
	}
	return w.b
}

// UnmarshalNavigationInventoryRequest decodes a strict control-source query.
func UnmarshalNavigationInventoryRequest(data []byte) (protocol.NavigationInventoryRequest, error) {
	r := payloadReader{b: data}
	var request protocol.NavigationInventoryRequest
	var err error
	if request.Version, err = r.getUint16(); err != nil {
		return protocol.NavigationInventoryRequest{}, err
	}
	if request.RequestID, err = r.getUint64(); err != nil {
		return protocol.NavigationInventoryRequest{}, err
	}
	operation, err := r.getUint8()
	if err != nil {
		return protocol.NavigationInventoryRequest{}, err
	}
	request.Operation = protocol.NavigationInventoryOperation(operation)
	if request.Operation == protocol.NavigationInventoryResolve {
		if request.SourceKey, err = r.getString(); err != nil {
			return protocol.NavigationInventoryRequest{}, err
		}
		if request.EntryKey, err = r.getString(); err != nil {
			return protocol.NavigationInventoryRequest{}, err
		}
		if request.Registration, err = unmarshalInventoryRegistration(&r); err != nil {
			return protocol.NavigationInventoryRequest{}, protocol.ErrInvalidNavigation
		}
	}
	if err := r.done(); err != nil {
		return protocol.NavigationInventoryRequest{}, err
	}
	if err := protocol.ValidateNavigationInventoryRequest(request); err != nil {
		return protocol.NavigationInventoryRequest{}, err
	}
	return request, nil
}

// MarshalNavigationInventoryResponse encodes a source answer. Snapshot
// carries groups; resolve carries an AttachTarget; never both.
func MarshalNavigationInventoryResponse(response protocol.NavigationInventoryResponse) []byte {
	if protocol.ValidateNavigationInventoryResponse(response) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(response.RequestID)
	w.putUint8(uint8(response.Operation))
	w.putUint8(uint8(response.Status))
	if response.Operation == protocol.NavigationInventorySnapshot && response.Status != protocol.NavigationInventoryVersionMismatch {
		if !marshalInventoryGroups(&w, response.Groups) {
			return nil
		}
	} else if response.Operation == protocol.NavigationInventoryResolve && response.Status == protocol.NavigationInventoryOK && response.Resolved != nil {
		inner := MarshalAttachTarget(*response.Resolved)
		if inner == nil {
			return nil
		}
		w.putLongBytes(inner)
	}
	if len(w.b) > navigationInventoryMaxEncodedBytes {
		return nil
	}
	return w.b
}

// UnmarshalNavigationInventoryResponse decodes a strict source answer.
func UnmarshalNavigationInventoryResponse(data []byte) (protocol.NavigationInventoryResponse, error) {
	if len(data) > navigationInventoryMaxEncodedBytes {
		return protocol.NavigationInventoryResponse{}, protocol.ErrInvalidNavigation
	}
	r := payloadReader{b: data}
	var response protocol.NavigationInventoryResponse
	var err error
	if response.RequestID, err = r.getUint64(); err != nil {
		return protocol.NavigationInventoryResponse{}, err
	}
	operation, err := r.getUint8()
	if err != nil {
		return protocol.NavigationInventoryResponse{}, err
	}
	response.Operation = protocol.NavigationInventoryOperation(operation)
	status, err := r.getUint8()
	if err != nil {
		return protocol.NavigationInventoryResponse{}, err
	}
	response.Status = protocol.NavigationInventoryStatus(status)
	if response.Status == protocol.NavigationInventoryVersionMismatch {
		if err := r.done(); err != nil {
			return protocol.NavigationInventoryResponse{}, err
		}
		if err := protocol.ValidateNavigationInventoryResponse(response); err != nil {
			return protocol.NavigationInventoryResponse{}, err
		}
		return response, nil
	}
	if response.Operation == protocol.NavigationInventorySnapshot {
		if response.Groups, err = unmarshalInventoryGroups(&r); err != nil {
			return protocol.NavigationInventoryResponse{}, err
		}
	} else if response.Status == protocol.NavigationInventoryOK {
		inner, err := r.getLongBytes()
		if err != nil {
			return protocol.NavigationInventoryResponse{}, err
		}
		target, err := UnmarshalAttachTarget(inner)
		if err != nil {
			return protocol.NavigationInventoryResponse{}, protocol.ErrInvalidNavigation
		}
		response.Resolved = &target
	}
	if err := r.done(); err != nil {
		return protocol.NavigationInventoryResponse{}, err
	}
	if err := protocol.ValidateNavigationInventoryResponse(response); err != nil {
		return protocol.NavigationInventoryResponse{}, err
	}
	return response, nil
}

// MarshalNavigationInventoryDemand encodes a serving-daemon open/close flag.
func MarshalNavigationInventoryDemand(demand protocol.NavigationInventoryDemand) []byte {
	if protocol.ValidateNavigationInventoryDemand(demand) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(demand.InteractionGeneration)
	w.putBool(demand.Open)
	return w.b
}

// UnmarshalNavigationInventoryDemand decodes a strict demand message.
func UnmarshalNavigationInventoryDemand(data []byte) (protocol.NavigationInventoryDemand, error) {
	r := payloadReader{b: data}
	var demand protocol.NavigationInventoryDemand
	var err error
	if demand.InteractionGeneration, err = r.getUint64(); err != nil {
		return protocol.NavigationInventoryDemand{}, err
	}
	if demand.Open, err = r.getBool(); err != nil {
		return protocol.NavigationInventoryDemand{}, err
	}
	if err := r.done(); err != nil {
		return protocol.NavigationInventoryDemand{}, err
	}
	if err := protocol.ValidateNavigationInventoryDemand(demand); err != nil {
		return protocol.NavigationInventoryDemand{}, err
	}
	return demand, nil
}

// MarshalNavigationInventoryPublication encodes sanitized source groups.
func MarshalNavigationInventoryPublication(publication protocol.NavigationInventoryPublication) []byte {
	if protocol.ValidateNavigationInventoryPublication(publication) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(publication.InteractionGeneration)
	w.putUint64(publication.PublicationGeneration)
	if !marshalInventoryGroups(&w, publication.Groups) {
		return nil
	}
	if len(w.b) > navigationInventoryMaxEncodedBytes {
		return nil
	}
	return w.b
}

// UnmarshalNavigationInventoryPublication decodes a strict publication.
func UnmarshalNavigationInventoryPublication(data []byte) (protocol.NavigationInventoryPublication, error) {
	if len(data) > navigationInventoryMaxEncodedBytes {
		return protocol.NavigationInventoryPublication{}, protocol.ErrInvalidNavigation
	}
	r := payloadReader{b: data}
	var publication protocol.NavigationInventoryPublication
	var err error
	if publication.InteractionGeneration, err = r.getUint64(); err != nil {
		return protocol.NavigationInventoryPublication{}, err
	}
	if publication.PublicationGeneration, err = r.getUint64(); err != nil {
		return protocol.NavigationInventoryPublication{}, err
	}
	if publication.Groups, err = unmarshalInventoryGroups(&r); err != nil {
		return protocol.NavigationInventoryPublication{}, err
	}
	if err := r.done(); err != nil {
		return protocol.NavigationInventoryPublication{}, err
	}
	if err := protocol.ValidateNavigationInventoryPublication(publication); err != nil {
		return protocol.NavigationInventoryPublication{}, err
	}
	return publication, nil
}

// MarshalNavigationInventorySelection encodes a key-only selection.
func MarshalNavigationInventorySelection(selection protocol.NavigationInventorySelection) []byte {
	if protocol.ValidateNavigationInventorySelection(selection) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(selection.CauseActionID)
	w.putUint64(selection.InteractionGeneration)
	w.putUint64(selection.PublicationGeneration)
	w.putString(selection.SourceKey)
	w.putString(selection.EntryKey)
	return w.b
}

// UnmarshalNavigationInventorySelection decodes a strict selection.
func UnmarshalNavigationInventorySelection(data []byte) (protocol.NavigationInventorySelection, error) {
	r := payloadReader{b: data}
	var selection protocol.NavigationInventorySelection
	var err error
	if selection.CauseActionID, err = r.getUint64(); err != nil {
		return protocol.NavigationInventorySelection{}, err
	}
	if selection.InteractionGeneration, err = r.getUint64(); err != nil {
		return protocol.NavigationInventorySelection{}, err
	}
	if selection.PublicationGeneration, err = r.getUint64(); err != nil {
		return protocol.NavigationInventorySelection{}, err
	}
	if selection.SourceKey, err = r.getString(); err != nil {
		return protocol.NavigationInventorySelection{}, err
	}
	if selection.EntryKey, err = r.getString(); err != nil {
		return protocol.NavigationInventorySelection{}, err
	}
	if err := r.done(); err != nil {
		return protocol.NavigationInventorySelection{}, err
	}
	if err := protocol.ValidateNavigationInventorySelection(selection); err != nil {
		return protocol.NavigationInventorySelection{}, err
	}
	return selection, nil
}

// MarshalNavigationInventoryFailure encodes a bounded failure code.
func MarshalNavigationInventoryFailure(failure protocol.NavigationInventoryFailure) []byte {
	if protocol.ValidateNavigationInventoryFailure(failure) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(failure.CauseActionID)
	w.putUint64(failure.InteractionGeneration)
	w.putString(failure.SourceKey)
	w.putString(failure.EntryKey)
	w.putUint8(uint8(failure.Code))
	return w.b
}

// UnmarshalNavigationInventoryFailure decodes a strict failure message.
func UnmarshalNavigationInventoryFailure(data []byte) (protocol.NavigationInventoryFailure, error) {
	r := payloadReader{b: data}
	var failure protocol.NavigationInventoryFailure
	var err error
	if failure.CauseActionID, err = r.getUint64(); err != nil {
		return protocol.NavigationInventoryFailure{}, err
	}
	if failure.InteractionGeneration, err = r.getUint64(); err != nil {
		return protocol.NavigationInventoryFailure{}, err
	}
	if failure.SourceKey, err = r.getString(); err != nil {
		return protocol.NavigationInventoryFailure{}, err
	}
	if failure.EntryKey, err = r.getString(); err != nil {
		return protocol.NavigationInventoryFailure{}, err
	}
	code, err := r.getUint8()
	if err != nil {
		return protocol.NavigationInventoryFailure{}, err
	}
	failure.Code = protocol.NavigationInventoryFailureCode(code)
	if err := r.done(); err != nil {
		return protocol.NavigationInventoryFailure{}, err
	}
	if err := protocol.ValidateNavigationInventoryFailure(failure); err != nil {
		return protocol.NavigationInventoryFailure{}, err
	}
	return failure, nil
}
