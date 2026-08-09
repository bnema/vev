package ports

import (
	"errors"
	"fmt"
)

var ErrInvalidRouteWire = errors.New("invalid route wire message")

func validateRouteString(value, field string, allowEmpty bool) error {
	if err := ValidateRouteLabel(value, allowEmpty); err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	return nil
}

func marshalExactSessionTarget(w *payloadWriter, target ExactSessionTarget) {
	w.putBytes(target.LifecycleID[:])
	w.putString(target.SessionName)
}

func marshalExactTargetSection(w *payloadWriter, target *ExactSessionTarget) {
	w.putBool(target != nil)
	if target != nil {
		marshalExactSessionTarget(w, *target)
	}
}

func unmarshalExactTargetSection(r *payloadReader) (*ExactSessionTarget, error) {
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

func unmarshalExactSessionTarget(r *payloadReader) (ExactSessionTarget, error) {
	var target ExactSessionTarget
	lifecycle, err := r.getBytes(len(target.LifecycleID))
	if err != nil {
		return ExactSessionTarget{}, err
	}
	copy(target.LifecycleID[:], lifecycle)
	if target.SessionName, err = r.getString(); err != nil {
		return ExactSessionTarget{}, err
	}
	if err := target.Validate(); err != nil {
		return ExactSessionTarget{}, fmt.Errorf("%w: exact target: %v", ErrInvalidRouteWire, err)
	}
	return target, nil
}

// MarshalExactSessionTarget encodes the exact lifecycle/name pair used by
// Hello and route-transition tests.
func MarshalExactSessionTarget(target ExactSessionTarget) ([]byte, error) {
	if err := target.Validate(); err != nil {
		return nil, fmt.Errorf("%w: exact target: %v", ErrInvalidRouteWire, err)
	}
	w := payloadWriter{}
	marshalExactSessionTarget(&w, target)
	return w.b, nil
}

// UnmarshalExactSessionTarget decodes one exact lifecycle/name pair and rejects
// truncation, invalid identity, and trailing bytes.
func UnmarshalExactSessionTarget(b []byte) (ExactSessionTarget, error) {
	r := payloadReader{b: b}
	target, err := unmarshalExactSessionTarget(&r)
	if err != nil {
		return ExactSessionTarget{}, err
	}
	if err := r.done(); err != nil {
		return ExactSessionTarget{}, err
	}
	return target, nil
}

func validateCommittedRouteIdentity(identity CommittedRouteIdentity) error {
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("%w: committed identity: %v", ErrInvalidRouteWire, err)
	}
	return nil
}

// MarshalCommittedRouteIdentity encodes the daemon's committed exact session
// identity without exposing transport or client-ledger details.
func MarshalCommittedRouteIdentity(identity CommittedRouteIdentity) ([]byte, error) {
	if err := validateCommittedRouteIdentity(identity); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	marshalCommittedRouteIdentityFields(&w, identity)
	return w.b, nil
}

func marshalCommittedRouteIdentityFields(w *payloadWriter, identity CommittedRouteIdentity) {
	marshalExactSessionTarget(w, identity.Target)
	w.putBool(identity.Ephemeral)
}

// marshalCommittedIdentitySection shares the standalone identity body and
// validates embedded identities before Welcome serialization.
func marshalCommittedIdentitySection(w *payloadWriter, identity *CommittedRouteIdentity) bool {
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

func unmarshalCommittedIdentitySection(r *payloadReader) (*CommittedRouteIdentity, error) {
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
	identity := &CommittedRouteIdentity{Target: target, Ephemeral: ephemeral}
	if err := validateCommittedRouteIdentity(*identity); err != nil {
		return nil, err
	}
	return identity, nil
}

func UnmarshalCommittedRouteIdentity(b []byte) (CommittedRouteIdentity, error) {
	r := payloadReader{b: b}
	target, err := unmarshalExactSessionTarget(&r)
	if err != nil {
		return CommittedRouteIdentity{}, err
	}
	ephemeral, err := r.getBool()
	if err != nil {
		return CommittedRouteIdentity{}, err
	}
	if err := r.done(); err != nil {
		return CommittedRouteIdentity{}, err
	}
	identity := CommittedRouteIdentity{Target: target, Ephemeral: ephemeral}
	if err := validateCommittedRouteIdentity(identity); err != nil {
		return CommittedRouteIdentity{}, err
	}
	return identity, nil
}

func validateRouteRef(ref RouteRef) error {
	if err := ref.Validate(); err != nil {
		return fmt.Errorf("%w: route reference: %v", ErrInvalidRouteWire, err)
	}
	return nil
}

func marshalRouteRef(w *payloadWriter, ref RouteRef) {
	w.putUint64(ref.Key)
	w.putUint64(ref.Generation)
}

func unmarshalRouteRef(r *payloadReader) (RouteRef, error) {
	key, err := r.getUint64()
	if err != nil {
		return RouteRef{}, err
	}
	generation, err := r.getUint64()
	if err != nil {
		return RouteRef{}, err
	}
	ref := RouteRef{Key: key, Generation: generation}
	if err := validateRouteRef(ref); err != nil {
		return RouteRef{}, err
	}
	return ref, nil
}

func validateRecentRouteEntry(entry RecentRouteEntry) error {
	if entry.Key == 0 || entry.Generation == 0 {
		return fmt.Errorf("%w: route entry identity is zero", ErrInvalidRouteWire)
	}
	if err := validateRouteString(entry.Name, "route name", false); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRouteWire, err)
	}
	if err := validateRouteString(entry.HostLabel, "route host label", true); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRouteWire, err)
	}
	if !entry.Kind.valid() {
		return fmt.Errorf("%w: invalid route kind", ErrInvalidRouteWire)
	}
	if !entry.Reachability.valid() {
		return fmt.Errorf("%w: invalid route reachability", ErrInvalidRouteWire)
	}
	return nil
}

func validateRecentRouteSnapshot(snapshot RecentRouteSnapshot) error {
	if len(snapshot.Entries) > RouteSnapshotMaxEntries {
		return fmt.Errorf("%w: too many route entries", ErrInvalidRouteWire)
	}
	if snapshot.Generation == 0 && len(snapshot.Entries) != 0 {
		return fmt.Errorf("%w: non-empty snapshot has zero generation", ErrInvalidRouteWire)
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
	if !snapshot.Active.empty() && snapshot.Active == snapshot.Previous {
		return fmt.Errorf("%w: active and previous routes are identical", ErrInvalidRouteWire)
	}
	refs := make(map[RouteRef]struct{}, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		if snapshot.Active == (RouteRef{Key: entry.Key, Generation: entry.Generation}) {
			return fmt.Errorf("%w: active route must be metadata-only", ErrInvalidRouteWire)
		}
		if err := validateRecentRouteEntry(entry); err != nil {
			return err
		}
		ref := RouteRef{Key: entry.Key, Generation: entry.Generation}
		if _, exists := refs[ref]; exists {
			return fmt.Errorf("%w: duplicate route entry", ErrInvalidRouteWire)
		}
		refs[ref] = struct{}{}
	}
	for _, item := range []struct {
		name string
		ref  RouteRef
	}{
		{name: "previous", ref: snapshot.Previous},
		{name: "home", ref: snapshot.Home},
	} {
		if item.ref.empty() || item.ref == snapshot.Active {
			// Active is metadata-only and intentionally excluded from the
			// recent display entries. Home may point at active as well.
			continue
		}
		if _, exists := refs[item.ref]; !exists {
			return fmt.Errorf("%w: %s route reference is absent from snapshot", ErrInvalidRouteWire, item.name)
		}
	}
	return nil
}

// MarshalRecentRouteSnapshot encodes a complete bounded client route view.
func MarshalRecentRouteSnapshot(snapshot RecentRouteSnapshot) ([]byte, error) {
	if err := validateRecentRouteSnapshot(snapshot); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(snapshot.Generation)
	marshalRouteRef(&w, snapshot.Active)
	marshalRouteRef(&w, snapshot.Previous)
	marshalRouteRef(&w, snapshot.Home)
	w.putUint8(uint8(len(snapshot.Entries)))
	for _, entry := range snapshot.Entries {
		w.putUint64(entry.Key)
		w.putUint64(entry.Generation)
		w.putString(entry.Name)
		w.putString(entry.HostLabel)
		w.putUint8(uint8(entry.Kind))
		w.putBool(entry.Ephemeral)
		w.putBool(entry.Attention)
		w.putUint8(uint8(entry.Reachability))
	}
	// Keep the frame bound defensive even though current entry/label bounds
	// make this unreachable; it protects the wire contract if those bounds
	// change independently in a future protocol revision.
	if len(w.b) > MaxFrameLen-1 {
		return nil, fmt.Errorf("%w: route snapshot is too large", ErrInvalidRouteWire)
	}
	return w.b, nil
}

// UnmarshalRecentRouteSnapshot decodes and strictly validates one complete
// immutable snapshot, including every referenced active/previous/home entry.
func UnmarshalRecentRouteSnapshot(b []byte) (RecentRouteSnapshot, error) {
	if len(b) > MaxFrameLen-1 {
		return RecentRouteSnapshot{}, ErrInvalidRouteWire
	}
	r := payloadReader{b: b}
	var snapshot RecentRouteSnapshot
	var err error
	if snapshot.Generation, err = r.getUint64(); err != nil {
		return RecentRouteSnapshot{}, err
	}
	if snapshot.Active, err = unmarshalRouteRef(&r); err != nil {
		return RecentRouteSnapshot{}, err
	}
	if snapshot.Previous, err = unmarshalRouteRef(&r); err != nil {
		return RecentRouteSnapshot{}, err
	}
	if snapshot.Home, err = unmarshalRouteRef(&r); err != nil {
		return RecentRouteSnapshot{}, err
	}
	count, err := r.getUint8()
	if err != nil {
		return RecentRouteSnapshot{}, err
	}
	if int(count) > RouteSnapshotMaxEntries {
		return RecentRouteSnapshot{}, fmt.Errorf("%w: too many route entries", ErrInvalidRouteWire)
	}
	if count != 0 {
		snapshot.Entries = make([]RecentRouteEntry, 0, int(count))
	}
	for range int(count) {
		var entry RecentRouteEntry
		if entry.Key, err = r.getUint64(); err != nil {
			return RecentRouteSnapshot{}, err
		}
		if entry.Generation, err = r.getUint64(); err != nil {
			return RecentRouteSnapshot{}, err
		}
		if entry.Name, err = r.getString(); err != nil {
			return RecentRouteSnapshot{}, err
		}
		if entry.HostLabel, err = r.getString(); err != nil {
			return RecentRouteSnapshot{}, err
		}
		kind, err := r.getUint8()
		if err != nil {
			return RecentRouteSnapshot{}, err
		}
		entry.Kind = RouteKind(kind)
		if entry.Ephemeral, err = r.getBool(); err != nil {
			return RecentRouteSnapshot{}, err
		}
		if entry.Attention, err = r.getBool(); err != nil {
			return RecentRouteSnapshot{}, err
		}
		reachability, err := r.getUint8()
		if err != nil {
			return RecentRouteSnapshot{}, err
		}
		entry.Reachability = RouteReachability(reachability)
		snapshot.Entries = append(snapshot.Entries, entry)
	}
	if err := r.done(); err != nil {
		return RecentRouteSnapshot{}, err
	}
	if err := validateRecentRouteSnapshot(snapshot); err != nil {
		return RecentRouteSnapshot{}, err
	}
	return snapshot, nil
}

func validateRouteNavigationAction(action RouteNavigationAction) error {
	if action.SnapshotGeneration == 0 || action.Key == 0 || action.Generation == 0 {
		return fmt.Errorf("%w: navigation action identity is zero", ErrInvalidRouteWire)
	}
	return nil
}

func MarshalRouteNavigationAction(action RouteNavigationAction) ([]byte, error) {
	if err := validateRouteNavigationAction(action); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(action.SnapshotGeneration)
	w.putUint64(action.Key)
	w.putUint64(action.Generation)
	return w.b, nil
}

func UnmarshalRouteNavigationAction(b []byte) (RouteNavigationAction, error) {
	r := payloadReader{b: b}
	var action RouteNavigationAction
	var err error
	if action.SnapshotGeneration, err = r.getUint64(); err != nil {
		return RouteNavigationAction{}, err
	}
	if action.Key, err = r.getUint64(); err != nil {
		return RouteNavigationAction{}, err
	}
	if action.Generation, err = r.getUint64(); err != nil {
		return RouteNavigationAction{}, err
	}
	if err := r.done(); err != nil {
		return RouteNavigationAction{}, err
	}
	if err := validateRouteNavigationAction(action); err != nil {
		return RouteNavigationAction{}, err
	}
	return action, nil
}

func validateRouteNavigationFailure(failure RouteNavigationFailure) error {
	if failure.Key == 0 || failure.Generation == 0 {
		return fmt.Errorf("%w: failure identity is zero", ErrInvalidRouteWire)
	}
	if !failure.Code.valid() {
		return fmt.Errorf("%w: invalid failure code", ErrInvalidRouteWire)
	}
	return nil
}

func MarshalRouteNavigationFailure(failure RouteNavigationFailure) ([]byte, error) {
	if err := validateRouteNavigationFailure(failure); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(failure.Key)
	w.putUint64(failure.Generation)
	w.putUint8(uint8(failure.Code))
	return w.b, nil
}

func UnmarshalRouteNavigationFailure(b []byte) (RouteNavigationFailure, error) {
	r := payloadReader{b: b}
	var failure RouteNavigationFailure
	var err error
	if failure.Key, err = r.getUint64(); err != nil {
		return RouteNavigationFailure{}, err
	}
	if failure.Generation, err = r.getUint64(); err != nil {
		return RouteNavigationFailure{}, err
	}
	code, err := r.getUint8()
	if err != nil {
		return RouteNavigationFailure{}, err
	}
	failure.Code = RouteFailureCode(code)
	if err := r.done(); err != nil {
		return RouteNavigationFailure{}, err
	}
	if err := validateRouteNavigationFailure(failure); err != nil {
		return RouteNavigationFailure{}, err
	}
	return failure, nil
}
