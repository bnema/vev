package wire

import (
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func validateRouteString(value, field string, allowEmpty bool) error {
	if err := protocol.ValidateRouteLabel(value, allowEmpty); err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	return nil
}

func marshalExactSessionTarget(w *payloadWriter, target protocol.ExactSessionTarget) {
	w.putBytes(target.LifecycleID[:])
	w.putString(target.SessionName)
}

func marshalExactTargetSection(w *payloadWriter, target *protocol.ExactSessionTarget) {
	w.putBool(target != nil)
	if target != nil {
		marshalExactSessionTarget(w, *target)
	}
}

func unmarshalExactTargetSection(r *payloadReader) (*protocol.ExactSessionTarget, error) {
	present, err := r.getBool()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	target, err := unmarshalExactSessionTarget(r)
	if err != nil {
		return nil, err
	}
	return &target, nil
}

func skipExactTargetSection(r *payloadReader) error {
	present, err := r.getBool()
	if err != nil {
		return err
	}
	if !present {
		return nil
	}
	if _, err := unmarshalExactSessionTarget(r); err != nil {
		return err
	}
	return nil
}

func unmarshalExactSessionTarget(r *payloadReader) (protocol.ExactSessionTarget, error) {
	var target protocol.ExactSessionTarget
	lifecycle, err := r.getBytes(len(target.LifecycleID))
	if err != nil {
		return protocol.ExactSessionTarget{}, err
	}
	copy(target.LifecycleID[:], lifecycle)
	if target.SessionName, err = r.getString(); err != nil {
		return protocol.ExactSessionTarget{}, err
	}
	if err := target.Validate(); err != nil {
		return protocol.ExactSessionTarget{}, fmt.Errorf("%w: exact target: %v", protocol.ErrInvalidRouteWire, err)
	}
	return target, nil
}

// MarshalExactSessionTarget encodes the exact lifecycle/name pair used by
// Hello and route-transition tests.
func MarshalExactSessionTarget(target protocol.ExactSessionTarget) ([]byte, error) {
	if err := target.Validate(); err != nil {
		return nil, fmt.Errorf("%w: exact target: %v", protocol.ErrInvalidRouteWire, err)
	}
	w := payloadWriter{}
	marshalExactSessionTarget(&w, target)
	return w.b, nil
}

// UnmarshalExactSessionTarget decodes one exact lifecycle/name pair and rejects
// truncation, invalid identity, and trailing bytes.
func UnmarshalExactSessionTarget(b []byte) (protocol.ExactSessionTarget, error) {
	r := payloadReader{b: b}
	target, err := unmarshalExactSessionTarget(&r)
	if err != nil {
		return protocol.ExactSessionTarget{}, err
	}
	if err := r.done(); err != nil {
		return protocol.ExactSessionTarget{}, err
	}
	return target, nil
}

// MarshalSamePeerSwitchRequest encodes the strict in-band session switch
// confirmation. The target is non-optional and carries no display origin.
func MarshalSamePeerSwitchRequest(request protocol.SamePeerSwitchRequest) ([]byte, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(request.RequestID)
	marshalExactSessionTarget(&w, request.Target)
	w.putString(string(request.PreferredTabID))
	return w.b, nil
}

// UnmarshalSamePeerSwitchRequest decodes one complete in-band switch request.
func UnmarshalSamePeerSwitchRequest(b []byte) (protocol.SamePeerSwitchRequest, error) {
	r := payloadReader{b: b}
	var request protocol.SamePeerSwitchRequest
	var err error
	if request.RequestID, err = r.getUint64(); err != nil {
		return protocol.SamePeerSwitchRequest{}, err
	}
	if request.Target, err = unmarshalExactSessionTarget(&r); err != nil {
		return protocol.SamePeerSwitchRequest{}, err
	}
	preferred, err := r.getString()
	if err != nil {
		return protocol.SamePeerSwitchRequest{}, err
	}
	request.PreferredTabID = domain.TabStableID(preferred)
	if err := r.done(); err != nil {
		return protocol.SamePeerSwitchRequest{}, err
	}
	if err := request.Validate(); err != nil {
		return protocol.SamePeerSwitchRequest{}, err
	}
	return request, nil
}

// MarshalSamePeerSwitchFailure encodes a bounded pre-commit rejection.
func MarshalSamePeerSwitchFailure(failure protocol.SamePeerSwitchFailure) ([]byte, error) {
	if err := failure.Validate(); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(failure.RequestID)
	w.putUint8(uint8(failure.Code))
	return w.b, nil
}

// UnmarshalSamePeerSwitchFailure decodes a complete bounded rejection.
func UnmarshalSamePeerSwitchFailure(b []byte) (protocol.SamePeerSwitchFailure, error) {
	r := payloadReader{b: b}
	var failure protocol.SamePeerSwitchFailure
	var err error
	if failure.RequestID, err = r.getUint64(); err != nil {
		return protocol.SamePeerSwitchFailure{}, err
	}
	code, err := r.getUint8()
	if err != nil {
		return protocol.SamePeerSwitchFailure{}, err
	}
	failure.Code = protocol.SamePeerSwitchFailureCode(code)
	if err := r.done(); err != nil {
		return protocol.SamePeerSwitchFailure{}, err
	}
	if err := failure.Validate(); err != nil {
		return protocol.SamePeerSwitchFailure{}, err
	}
	return failure, nil
}

func validateCommittedRouteIdentity(identity protocol.CommittedRouteIdentity) error {
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("%w: committed identity: %v", protocol.ErrInvalidRouteWire, err)
	}
	return nil
}

// MarshalCommittedRouteIdentity encodes the daemon's committed exact session
// identity without exposing transport or client-ledger details.
func MarshalCommittedRouteIdentity(identity protocol.CommittedRouteIdentity) ([]byte, error) {
	if err := validateCommittedRouteIdentity(identity); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	marshalCommittedRouteIdentityFields(&w, identity)
	return w.b, nil
}

func marshalCommittedRouteIdentityFields(w *payloadWriter, identity protocol.CommittedRouteIdentity) {
	marshalExactSessionTarget(w, identity.Target)
	w.putBool(identity.Ephemeral)
}

// marshalCommittedIdentitySection shares the standalone identity body and
// validates embedded identities before Welcome serialization.
func marshalCommittedIdentitySection(w *payloadWriter, identity *protocol.CommittedRouteIdentity) bool {
	w.putBool(identity != nil)
	if identity == nil {
		return true
	}
	if err := validateCommittedRouteIdentity(*identity); err != nil {
		return false
	}
	marshalCommittedRouteIdentityFields(w, *identity)
	return true
}

func unmarshalCommittedIdentitySection(r *payloadReader) (*protocol.CommittedRouteIdentity, error) {
	present, err := r.getBool()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	target, err := unmarshalExactSessionTarget(r)
	if err != nil {
		return nil, err
	}
	ephemeral, err := r.getBool()
	if err != nil {
		return nil, err
	}
	identity := &protocol.CommittedRouteIdentity{Target: target, Ephemeral: ephemeral}
	if err := validateCommittedRouteIdentity(*identity); err != nil {
		return nil, err
	}
	return identity, nil
}

func UnmarshalCommittedRouteIdentity(b []byte) (protocol.CommittedRouteIdentity, error) {
	r := payloadReader{b: b}
	target, err := unmarshalExactSessionTarget(&r)
	if err != nil {
		return protocol.CommittedRouteIdentity{}, err
	}
	ephemeral, err := r.getBool()
	if err != nil {
		return protocol.CommittedRouteIdentity{}, err
	}
	if err := r.done(); err != nil {
		return protocol.CommittedRouteIdentity{}, err
	}
	identity := protocol.CommittedRouteIdentity{Target: target, Ephemeral: ephemeral}
	if err := validateCommittedRouteIdentity(identity); err != nil {
		return protocol.CommittedRouteIdentity{}, err
	}
	return identity, nil
}

// MarshalRoutePosition encodes one exact session and its attachment-local tab
// cursor. Route positions are mutable client memory, not attach authority.
func MarshalRoutePosition(position protocol.RoutePosition) ([]byte, error) {
	if err := position.Validate(); err != nil {
		return nil, fmt.Errorf("%w: route position: %v", protocol.ErrInvalidRouteWire, err)
	}
	w := payloadWriter{}
	marshalExactSessionTarget(&w, position.Target)
	w.putString(string(position.ActiveTabID))
	return w.b, nil
}

// UnmarshalRoutePosition decodes one strict route-position payload.
func UnmarshalRoutePosition(b []byte) (protocol.RoutePosition, error) {
	r := payloadReader{b: b}
	target, err := unmarshalExactSessionTarget(&r)
	if err != nil {
		return protocol.RoutePosition{}, err
	}
	activeTabID, err := r.getString()
	if err != nil {
		return protocol.RoutePosition{}, err
	}
	if err := r.done(); err != nil {
		return protocol.RoutePosition{}, err
	}
	position := protocol.RoutePosition{Target: target, ActiveTabID: domain.TabStableID(activeTabID)}
	if err := position.Validate(); err != nil {
		return protocol.RoutePosition{}, fmt.Errorf("%w: route position: %v", protocol.ErrInvalidRouteWire, err)
	}
	return position, nil
}

func validateRouteRef(ref protocol.RouteRef) error {
	if err := ref.Validate(); err != nil {
		return fmt.Errorf("%w: route reference: %v", protocol.ErrInvalidRouteWire, err)
	}
	return nil
}

func marshalRouteRef(w *payloadWriter, ref protocol.RouteRef) {
	w.putUint64(ref.Key)
	w.putUint64(ref.Generation)
}

func unmarshalRouteRef(r *payloadReader) (protocol.RouteRef, error) {
	key, err := r.getUint64()
	if err != nil {
		return protocol.RouteRef{}, err
	}
	generation, err := r.getUint64()
	if err != nil {
		return protocol.RouteRef{}, err
	}
	ref := protocol.RouteRef{Key: key, Generation: generation}
	if err := validateRouteRef(ref); err != nil {
		return protocol.RouteRef{}, err
	}
	return ref, nil
}

func validateRecentRouteEntry(entry protocol.RecentRouteEntry) error {
	if entry.Key == 0 || entry.Generation == 0 {
		return fmt.Errorf("%w: route entry identity is zero", protocol.ErrInvalidRouteWire)
	}
	if err := entry.Target.Validate(); err != nil {
		return fmt.Errorf("%w: invalid route lifecycle target: %v", protocol.ErrInvalidRouteWire, err)
	}
	if entry.Name != entry.Target.SessionName {
		return fmt.Errorf("%w: route name does not match lifecycle target", protocol.ErrInvalidRouteWire)
	}
	if err := validateRouteString(entry.Name, "route name", false); err != nil {
		return fmt.Errorf("%w: %v", protocol.ErrInvalidRouteWire, err)
	}
	if err := validateRouteString(entry.HostLabel, "route host label", true); err != nil {
		return fmt.Errorf("%w: %v", protocol.ErrInvalidRouteWire, err)
	}
	if entry.Kind.Validate() != nil {
		return fmt.Errorf("%w: invalid route kind", protocol.ErrInvalidRouteWire)
	}
	if entry.Reachability.Validate() != nil {
		return fmt.Errorf("%w: invalid route reachability", protocol.ErrInvalidRouteWire)
	}
	return nil
}

func validateRecentRouteSnapshot(snapshot protocol.RecentRouteSnapshot) error {
	if len(snapshot.Entries) > protocol.RouteSnapshotMaxEntries {
		return fmt.Errorf("%w: too many route entries", protocol.ErrInvalidRouteWire)
	}
	if snapshot.Generation == 0 && (len(snapshot.Entries) != 0 || snapshot.ActiveEntry != (protocol.RecentRouteEntry{})) {
		return fmt.Errorf("%w: non-empty snapshot has zero generation", protocol.ErrInvalidRouteWire)
	}
	if err := validateRouteRef(snapshot.Active); err != nil {
		return err
	}
	if err := validateRouteRef(snapshot.Previous); err != nil {
		return err
	}
	if err := validateRouteRef(snapshot.Home); err != nil {
		return err
	}
	if snapshot.Active.IsZero() {
		if snapshot.ActiveEntry != (protocol.RecentRouteEntry{}) {
			return fmt.Errorf("%w: active presentation has no active route", protocol.ErrInvalidRouteWire)
		}
	} else {
		if err := validateRecentRouteEntry(snapshot.ActiveEntry); err != nil {
			return err
		}
		if snapshot.Active != (protocol.RouteRef{Key: snapshot.ActiveEntry.Key, Generation: snapshot.ActiveEntry.Generation}) {
			return fmt.Errorf("%w: active presentation does not match active route", protocol.ErrInvalidRouteWire)
		}
	}
	if !snapshot.Active.IsZero() && snapshot.Active == snapshot.Previous {
		return fmt.Errorf("%w: active and previous routes are identical", protocol.ErrInvalidRouteWire)
	}
	refs := make(map[protocol.RouteRef]struct{}, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		if snapshot.Active == (protocol.RouteRef{Key: entry.Key, Generation: entry.Generation}) {
			return fmt.Errorf("%w: active route must be metadata-only", protocol.ErrInvalidRouteWire)
		}
		if err := validateRecentRouteEntry(entry); err != nil {
			return err
		}
		ref := protocol.RouteRef{Key: entry.Key, Generation: entry.Generation}
		if _, exists := refs[ref]; exists {
			return fmt.Errorf("%w: duplicate route entry", protocol.ErrInvalidRouteWire)
		}
		refs[ref] = struct{}{}
	}
	for _, item := range []struct {
		name string
		ref  protocol.RouteRef
	}{
		{name: "previous", ref: snapshot.Previous},
		{name: "home", ref: snapshot.Home},
	} {
		if item.ref.IsZero() || item.ref == snapshot.Active {
			// Active is metadata-only and intentionally excluded from the
			// recent display entries. Home may point at active as well.
			continue
		}
		if _, exists := refs[item.ref]; !exists {
			return fmt.Errorf("%w: %s route reference is absent from snapshot", protocol.ErrInvalidRouteWire, item.name)
		}
	}
	return nil
}

func marshalRecentRouteEntry(w *payloadWriter, entry protocol.RecentRouteEntry) {
	w.putUint64(entry.Key)
	w.putUint64(entry.Generation)
	marshalExactSessionTarget(w, entry.Target)
	w.putString(entry.Name)
	w.putString(entry.HostLabel)
	w.putUint8(uint8(entry.Kind))
	w.putBool(entry.Ephemeral)
	w.putBool(entry.Attention)
	w.putUint8(uint8(entry.Reachability))
}

func unmarshalRecentRouteEntry(r *payloadReader) (protocol.RecentRouteEntry, error) {
	var entry protocol.RecentRouteEntry
	var err error
	if entry.Key, err = r.getUint64(); err != nil {
		return protocol.RecentRouteEntry{}, err
	}
	if entry.Generation, err = r.getUint64(); err != nil {
		return protocol.RecentRouteEntry{}, err
	}
	if entry.Target, err = unmarshalExactSessionTarget(r); err != nil {
		return protocol.RecentRouteEntry{}, err
	}
	if entry.Name, err = r.getString(); err != nil {
		return protocol.RecentRouteEntry{}, err
	}
	if entry.HostLabel, err = r.getString(); err != nil {
		return protocol.RecentRouteEntry{}, err
	}
	kind, err := r.getUint8()
	if err != nil {
		return protocol.RecentRouteEntry{}, err
	}
	entry.Kind = protocol.RouteKind(kind)
	if entry.Ephemeral, err = r.getBool(); err != nil {
		return protocol.RecentRouteEntry{}, err
	}
	if entry.Attention, err = r.getBool(); err != nil {
		return protocol.RecentRouteEntry{}, err
	}
	reachability, err := r.getUint8()
	if err != nil {
		return protocol.RecentRouteEntry{}, err
	}
	entry.Reachability = protocol.RouteReachability(reachability)
	return entry, nil
}

// MarshalRecentRouteSnapshot encodes a complete bounded client route view.
func MarshalRecentRouteSnapshot(snapshot protocol.RecentRouteSnapshot) ([]byte, error) {
	if err := validateRecentRouteSnapshot(snapshot); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(snapshot.Generation)
	marshalRouteRef(&w, snapshot.Active)
	if !snapshot.Active.IsZero() {
		marshalRecentRouteEntry(&w, snapshot.ActiveEntry)
	}
	marshalRouteRef(&w, snapshot.Previous)
	marshalRouteRef(&w, snapshot.Home)
	w.putUint8(uint8(len(snapshot.Entries)))
	for _, entry := range snapshot.Entries {
		marshalRecentRouteEntry(&w, entry)
	}
	// Keep the frame bound defensive even though current entry/label bounds
	// make this unreachable; it protects the wire contract if those bounds
	// change independently in a future protocol revision.
	if len(w.b) > MaxFrameLen-1 {
		return nil, fmt.Errorf("%w: route snapshot is too large", protocol.ErrInvalidRouteWire)
	}
	return w.b, nil
}

func validateRouteAttentionSubscription(subscription protocol.RouteAttentionSubscription) error {
	if len(subscription.Targets) > protocol.RouteSnapshotMaxEntries {
		return fmt.Errorf("%w: too many attention targets", protocol.ErrInvalidRouteWire)
	}
	refs := make(map[protocol.RouteRef]struct{}, len(subscription.Targets))
	for _, target := range subscription.Targets {
		if err := validateRouteRef(target.Ref); err != nil || target.Ref.IsZero() {
			return fmt.Errorf("%w: invalid attention route reference", protocol.ErrInvalidRouteWire)
		}
		if err := target.Target.Validate(); err != nil {
			return fmt.Errorf("%w: invalid attention target: %v", protocol.ErrInvalidRouteWire, err)
		}
		if _, exists := refs[target.Ref]; exists {
			return fmt.Errorf("%w: duplicate attention route reference", protocol.ErrInvalidRouteWire)
		}
		refs[target.Ref] = struct{}{}
	}
	return nil
}

// MarshalRouteAttentionSubscription encodes the bounded client mapping used
// by the connected daemon to resolve live route-attention indicators.
func MarshalRouteAttentionSubscription(subscription protocol.RouteAttentionSubscription) ([]byte, error) {
	if err := validateRouteAttentionSubscription(subscription); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint8(uint8(len(subscription.Targets)))
	for _, target := range subscription.Targets {
		marshalRouteRef(&w, target.Ref)
		marshalExactSessionTarget(&w, target.Target)
	}
	return w.b, nil
}

// UnmarshalRouteAttentionSubscription decodes one strict bounded attention
// mapping and rejects truncated, malformed, and trailing payloads.
func UnmarshalRouteAttentionSubscription(b []byte) (protocol.RouteAttentionSubscription, error) {
	r := payloadReader{b: b}
	count, err := r.getUint8()
	if err != nil {
		return protocol.RouteAttentionSubscription{}, err
	}
	if int(count) > protocol.RouteSnapshotMaxEntries {
		return protocol.RouteAttentionSubscription{}, fmt.Errorf("%w: too many attention targets", protocol.ErrInvalidRouteWire)
	}
	subscription := protocol.RouteAttentionSubscription{Targets: make([]protocol.RouteAttentionTarget, 0, int(count))}
	for range int(count) {
		ref, err := unmarshalRouteRef(&r)
		if err != nil {
			return protocol.RouteAttentionSubscription{}, err
		}
		target, err := unmarshalExactSessionTarget(&r)
		if err != nil {
			return protocol.RouteAttentionSubscription{}, err
		}
		subscription.Targets = append(subscription.Targets, protocol.RouteAttentionTarget{Ref: ref, Target: target})
	}
	if err := r.done(); err != nil {
		return protocol.RouteAttentionSubscription{}, err
	}
	if err := validateRouteAttentionSubscription(subscription); err != nil {
		return protocol.RouteAttentionSubscription{}, err
	}
	return subscription, nil
}

// UnmarshalRecentRouteSnapshot decodes and strictly validates one complete
// immutable snapshot, including every referenced active/previous/home entry.
func UnmarshalRecentRouteSnapshot(b []byte) (protocol.RecentRouteSnapshot, error) {
	if len(b) > MaxFrameLen-1 {
		return protocol.RecentRouteSnapshot{}, protocol.ErrInvalidRouteWire
	}
	r := payloadReader{b: b}
	var snapshot protocol.RecentRouteSnapshot
	var err error
	if snapshot.Generation, err = r.getUint64(); err != nil {
		return protocol.RecentRouteSnapshot{}, err
	}
	if snapshot.Active, err = unmarshalRouteRef(&r); err != nil {
		return protocol.RecentRouteSnapshot{}, err
	}
	if !snapshot.Active.IsZero() {
		if snapshot.ActiveEntry, err = unmarshalRecentRouteEntry(&r); err != nil {
			return protocol.RecentRouteSnapshot{}, err
		}
	}
	if snapshot.Previous, err = unmarshalRouteRef(&r); err != nil {
		return protocol.RecentRouteSnapshot{}, err
	}
	if snapshot.Home, err = unmarshalRouteRef(&r); err != nil {
		return protocol.RecentRouteSnapshot{}, err
	}
	count, err := r.getUint8()
	if err != nil {
		return protocol.RecentRouteSnapshot{}, err
	}
	if int(count) > protocol.RouteSnapshotMaxEntries {
		return protocol.RecentRouteSnapshot{}, fmt.Errorf("%w: too many route entries", protocol.ErrInvalidRouteWire)
	}
	if count != 0 {
		snapshot.Entries = make([]protocol.RecentRouteEntry, 0, int(count))
	}
	for range int(count) {
		entry, err := unmarshalRecentRouteEntry(&r)
		if err != nil {
			return protocol.RecentRouteSnapshot{}, err
		}
		snapshot.Entries = append(snapshot.Entries, entry)
	}
	if err := r.done(); err != nil {
		return protocol.RecentRouteSnapshot{}, err
	}
	if err := validateRecentRouteSnapshot(snapshot); err != nil {
		return protocol.RecentRouteSnapshot{}, err
	}
	return snapshot, nil
}

func validateRouteNavigationAction(action protocol.RouteNavigationAction) error {
	if action.SnapshotGeneration == 0 || action.Key == 0 || action.Generation == 0 {
		return fmt.Errorf("%w: navigation action identity is zero", protocol.ErrInvalidRouteWire)
	}
	return nil
}

func MarshalRouteNavigationAction(action protocol.RouteNavigationAction) ([]byte, error) {
	if err := validateRouteNavigationAction(action); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(action.SnapshotGeneration)
	w.putUint64(action.Key)
	w.putUint64(action.Generation)
	return w.b, nil
}

func UnmarshalRouteNavigationAction(b []byte) (protocol.RouteNavigationAction, error) {
	r := payloadReader{b: b}
	var action protocol.RouteNavigationAction
	var err error
	if action.SnapshotGeneration, err = r.getUint64(); err != nil {
		return protocol.RouteNavigationAction{}, err
	}
	if action.Key, err = r.getUint64(); err != nil {
		return protocol.RouteNavigationAction{}, err
	}
	if action.Generation, err = r.getUint64(); err != nil {
		return protocol.RouteNavigationAction{}, err
	}
	if err := r.done(); err != nil {
		return protocol.RouteNavigationAction{}, err
	}
	if err := validateRouteNavigationAction(action); err != nil {
		return protocol.RouteNavigationAction{}, err
	}
	return action, nil
}

func MarshalRouteCreateSessionAction(action protocol.RouteCreateSessionAction) ([]byte, error) {
	if err := action.Validate(); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(action.RequestID)
	w.putUint64(action.SnapshotGeneration)
	w.putUint64(action.Key)
	w.putUint64(action.Generation)
	w.putString(action.SessionName)
	return w.b, nil
}

func UnmarshalRouteCreateSessionAction(b []byte) (protocol.RouteCreateSessionAction, error) {
	r := payloadReader{b: b}
	var action protocol.RouteCreateSessionAction
	var err error
	if action.RequestID, err = r.getUint64(); err != nil {
		return protocol.RouteCreateSessionAction{}, err
	}
	if action.SnapshotGeneration, err = r.getUint64(); err != nil {
		return protocol.RouteCreateSessionAction{}, err
	}
	if action.Key, err = r.getUint64(); err != nil {
		return protocol.RouteCreateSessionAction{}, err
	}
	if action.Generation, err = r.getUint64(); err != nil {
		return protocol.RouteCreateSessionAction{}, err
	}
	if action.SessionName, err = r.getString(); err != nil {
		return protocol.RouteCreateSessionAction{}, err
	}
	if err := r.done(); err != nil {
		return protocol.RouteCreateSessionAction{}, err
	}
	if err := action.Validate(); err != nil {
		return protocol.RouteCreateSessionAction{}, err
	}
	return action, nil
}

func validateRouteNavigationFailure(failure protocol.RouteNavigationFailure) error {
	if failure.Key == 0 || failure.Generation == 0 {
		return fmt.Errorf("%w: failure identity is zero", protocol.ErrInvalidRouteWire)
	}
	if failure.Code.Validate() != nil {
		return fmt.Errorf("%w: invalid failure code", protocol.ErrInvalidRouteWire)
	}
	return nil
}

func MarshalRouteNavigationFailure(failure protocol.RouteNavigationFailure) ([]byte, error) {
	if err := validateRouteNavigationFailure(failure); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(failure.Key)
	w.putUint64(failure.Generation)
	w.putUint8(uint8(failure.Code))
	return w.b, nil
}

func MarshalSessionCreationFailure(failure protocol.SessionCreationFailure) ([]byte, error) {
	if err := failure.Validate(); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(failure.RequestID)
	w.putUint8(uint8(failure.Code))
	return w.b, nil
}

func UnmarshalSessionCreationFailure(b []byte) (protocol.SessionCreationFailure, error) {
	r := payloadReader{b: b}
	var failure protocol.SessionCreationFailure
	var err error
	if failure.RequestID, err = r.getUint64(); err != nil {
		return protocol.SessionCreationFailure{}, err
	}
	code, err := r.getUint8()
	if err != nil {
		return protocol.SessionCreationFailure{}, err
	}
	failure.Code = protocol.RouteFailureCode(code)
	if err := r.done(); err != nil {
		return protocol.SessionCreationFailure{}, err
	}
	if err := failure.Validate(); err != nil {
		return protocol.SessionCreationFailure{}, err
	}
	return failure, nil
}

func UnmarshalRouteNavigationFailure(b []byte) (protocol.RouteNavigationFailure, error) {
	r := payloadReader{b: b}
	var failure protocol.RouteNavigationFailure
	var err error
	if failure.Key, err = r.getUint64(); err != nil {
		return protocol.RouteNavigationFailure{}, err
	}
	if failure.Generation, err = r.getUint64(); err != nil {
		return protocol.RouteNavigationFailure{}, err
	}
	code, err := r.getUint8()
	if err != nil {
		return protocol.RouteNavigationFailure{}, err
	}
	failure.Code = protocol.RouteFailureCode(code)
	if err := r.done(); err != nil {
		return protocol.RouteNavigationFailure{}, err
	}
	if err := validateRouteNavigationFailure(failure); err != nil {
		return protocol.RouteNavigationFailure{}, err
	}
	return failure, nil
}
