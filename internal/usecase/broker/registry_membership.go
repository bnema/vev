package broker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// AddHost adds or pins endpoint under policy. Duplicate same-policy additions
// CAS-verify authority and preserve identity; a conflicting policy is refused.
func (r *Registry) LookupHost(ctx context.Context, endpoint string) (ports.BrokerHostRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ports.BrokerHostRecord{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, record := range r.authority.Hosts {
		if record.Registration.Endpoint == endpoint {
			record.Route = record.Route.Clone()
			return record, true, nil
		}
	}
	return ports.BrokerHostRecord{}, false, nil
}

func (r *Registry) AddHost(ctx context.Context, endpoint string, policy ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	if err := domain.ValidateRemoteHostTarget(endpoint); err != nil {
		return domain.RemoteRegistration{}, err
	}
	if err := policy.Validate(); err != nil {
		return domain.RemoteRegistration{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mutableLocked(ctx); err != nil {
		return domain.RemoteRegistration{}, err
	}
	records := append([]ports.BrokerHostRecord(nil), r.authority.Hosts...)
	for i := range records {
		if records[i].Registration.Endpoint != endpoint {
			continue
		}
		if records[i].Policy != policy {
			return domain.RemoteRegistration{}, ports.ErrBrokerHostConflict
		}
		records[i].Pinned = true
		if err := r.commitMembershipLocked(records); err != nil {
			return domain.RemoteRegistration{}, err
		}
		return records[i].Registration, nil
	}
	id, err := r.newIncarnation()
	if err != nil {
		return domain.RemoteRegistration{}, err
	}
	reg, err := domain.NewRemoteRegistration(endpoint, id)
	if err != nil {
		return domain.RemoteRegistration{}, err
	}
	if reg.IsZero() {
		return domain.RemoteRegistration{}, errors.New("broker: incarnation generator returned zero")
	}
	route, err := ports.BrokerRouteForTransport(policy.Transport, endpoint)
	if err != nil {
		return domain.RemoteRegistration{}, err
	}
	records = append(records, ports.BrokerHostRecord{Registration: reg, Pinned: true, Policy: policy, Route: route})
	if err := r.commitMembershipLocked(records); err != nil {
		return domain.RemoteRegistration{}, err
	}
	return reg, nil
}

// RemoveHost removes only the exact expected registration.
// BindAuthenticatedIdentity durably binds first-contact identity under the
// exact registration and policy fence. The same binding is idempotent; a
// different identity is refused and never overwrites authority.
func (r *Registry) BindAuthenticatedIdentity(ctx context.Context, request ports.BrokerIdentityBindingRequest) (ports.BrokerDaemonIdentity, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	if request.Fence.Local {
		return request.Identity, nil
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	records := append([]ports.BrokerHostRecord(nil), r.authority.Hosts...)
	for index := range records {
		record := &records[index]
		if record.Identity == request.Identity && record.Policy != request.Policy {
			return "", ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy, Cause: errors.New("broker: authenticated daemon identity is already bound under an incompatible policy")}
		}
	}
	for index := range records {
		record := &records[index]
		if !record.Registration.Equal(request.Fence.Registration) || record.Policy != request.Policy {
			continue
		}
		if record.Identity != "" {
			if record.Identity != request.Identity {
				return "", ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy, Cause: errors.New("broker: authenticated daemon identity conflicts with durable binding")}
			}
			return record.Identity, nil
		}
		record.Identity = request.Identity
		if err := r.commitMembershipLocked(records); err != nil {
			return "", err
		}
		return request.Identity, nil
	}
	return "", ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: errors.New("broker: stale identity binding fence")}
}

func (r *Registry) RemoveHost(ctx context.Context, expected domain.RemoteRegistration) (bool, error) {
	if err := expected.Validate(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mutableLocked(ctx); err != nil {
		return false, err
	}
	records := append([]ports.BrokerHostRecord(nil), r.authority.Hosts...)
	for i := range records {
		if records[i].Registration.Endpoint != expected.Endpoint {
			continue
		}
		if !records[i].Registration.Equal(expected) {
			return false, ports.ErrBrokerHostConflict
		}
		records = append(records[:i], records[i+1:]...)
		if err := r.commitMembershipLocked(records); err != nil {
			return false, err
		}
		return true, nil
	}
	// A no-op still verifies the loaded revision.
	return false, r.commitMembershipLocked(records)
}

// UpdateHostPolicy updates exact authority and advances its generation.
func (r *Registry) UpdateHostPolicy(ctx context.Context, expected domain.RemoteRegistration, policy ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	if err := expected.Validate(); err != nil {
		return domain.RemoteRegistration{}, err
	}
	if err := policy.Validate(); err != nil {
		return domain.RemoteRegistration{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mutableLocked(ctx); err != nil {
		return domain.RemoteRegistration{}, err
	}
	records := append([]ports.BrokerHostRecord(nil), r.authority.Hosts...)
	for i := range records {
		if records[i].Registration.Endpoint != expected.Endpoint {
			continue
		}
		if !records[i].Registration.Equal(expected) {
			return domain.RemoteRegistration{}, ports.ErrBrokerHostConflict
		}
		if records[i].Policy == policy {
			if err := r.commitMembershipLocked(records); err != nil {
				return domain.RemoteRegistration{}, err
			}
			return expected, nil
		}
		if expected.Generation == ^domain.RemoteGeneration(0) {
			return domain.RemoteRegistration{}, ports.ErrBrokerRevisionExhausted
		}
		route, err := ports.BrokerRouteForTransport(policy.Transport, expected.Endpoint)
		if err != nil {
			return domain.RemoteRegistration{}, err
		}
		records[i].Registration.Generation++
		records[i].Route = route
		records[i].Policy = policy
		if err := r.commitMembershipLocked(records); err != nil {
			return domain.RemoteRegistration{}, err
		}
		return records[i].Registration, nil
	}
	return domain.RemoteRegistration{}, ports.ErrBrokerHostConflict
}

func (r *Registry) mutableLocked(ctx context.Context) error {
	if r.membershipMode != MembershipMutable {
		return ports.ErrBrokerMembershipImmutable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.running.Load() == runStopped {
		return ports.ErrBrokerRegistryClosed
	}
	if r.revisionExhaustedLocked() {
		return ports.ErrBrokerRevisionExhausted
	}
	return nil
}

// commitMembershipLocked is the sole mutable-membership durable commit path.
// It installs projection only after CAS success.
func (r *Registry) commitMembershipLocked(records []ports.BrokerHostRecord) error {
	if err := ports.ValidateBrokerHostRecords(records); err != nil {
		return err
	}
	if len(records) > 0 && !r.observationDisabled && nilDependency(r.probe) {
		return errors.New("broker: remote hosts require a host probe")
	}
	next := make(map[string]ports.BrokerDaemonObservation, len(records))
	order := make([]string, 0, len(records))
	for rank, record := range records {
		observation := stampHostAuthority(ports.BrokerDaemonObservation{}, record, rank)
		// A membership the registry cannot publish is refused here, before the
		// store CAS, rather than persisted and left to freeze every later
		// publication: an endpoint whose derived display hint is empty after the
		// display rule drops its runes makes every snapshot carrying it invalid.
		if err := observation.Validate(); err != nil {
			return fmt.Errorf("broker: host record %q is not publishable: %w", record.Registration.Endpoint, err)
		}
		next[record.Registration.Endpoint] = observation
		order = append(order, record.Registration.Endpoint)
	}
	if err := r.hostStore.ReplaceHosts(r.authority.Revision, records); err != nil {
		return fmt.Errorf("broker: replace hosts: %w", err)
	}
	r.authority = ports.BrokerHosts{Revision: r.authority.Revision + 1, Hosts: ports.CloneBrokerHostRecords(records)}
	r.setHostsLocked(next, order)
	return nil
}

// ReplaceHosts durably replaces membership and explicit policy using the loaded
// authority revision. It delegates to commitMembershipLocked, the same
// validated commit path the runtime mutations use, so membership whose
// projection could never be published is refused before the store CAS instead
// of being persisted and silently freezing every later publication. Success
// means membership is committed, not that advisory observations have flushed.
// Store errors (including stale authority) are returned without changing the
// projection, probe attempts, or local token. A settled run reports
// ports.ErrBrokerRegistryClosed and an exhausted revision series reports
// ports.ErrBrokerRevisionExhausted; both refuse the mutation before the store
// CAS, so a refusal never changes durable authority either. Conflicts are not
// retried or refreshed implicitly: reconcile by reopening the owner. Callers
// must supply fresh incarnations when removing and re-adding. Even identical
// records perform CAS so stale authority cannot report success. The synchronous
// write holds mu; Snapshot remains lock-free, but scheduling waits for
// membership durability. Observation writes remain asynchronous.
func (r *Registry) ReplaceHosts(records []ports.BrokerHostRecord) error {
	var err error
	records, err = ports.UpgradeBrokerHostRoutes(records)
	if err != nil {
		return err
	}
	if err := ports.ValidateBrokerHostRecords(records); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running.Load() == runStopped {
		return fmt.Errorf("broker: membership replacement: %w", ports.ErrBrokerRegistryClosed)
	}
	if r.revisionExhaustedLocked() {
		return fmt.Errorf("broker: membership replacement: %w", ports.ErrBrokerRevisionExhausted)
	}
	return r.commitMembershipLocked(records)
}

// setHostsLocked applies only a projection; the public path commits authority
// first. The caller holds mu through both steps, fencing probe completions.
// order is the registration order the projection was built from; it becomes
// the publication order of remote daemons.
func (r *Registry) setHostsLocked(next map[string]ports.BrokerDaemonObservation, order []string) {
	if registrationsUnchanged(r.hosts, next, r.order, order) {
		return
	}
	for endpoint, current := range r.hosts {
		candidate, present := next[endpoint]
		if present && current.Registration.Equal(candidate.Registration) && current.Policy == candidate.Policy {
			// Unchanged authority: keep the live observation as is. Rank is the
			// one authority field that follows membership order rather than the
			// record, so it is re-stamped from the new projection.
			kept := current
			kept.Rank = candidate.Rank
			next[endpoint] = kept
			continue
		}
		// A retired registration owns no observation: cancelling its attempt
		// releases a ctx-honoring probe immediately, and dropping its record
		// means the retired completion can never clear or answer the
		// replacement, and the replacement is never blocked behind it. The
		// cancellation happens under the registry lock, so no replacement
		// attempt can start before the retired context is cancelled.
		r.cancelInflightLocked(endpoint)
		delete(r.pending, endpoint)
		if present {
			// The endpoint stays live under a new registration, so its
			// retirement is never published as a tombstone alongside a live
			// host; the attempt token fences the stale completion.
			delete(r.tombstones, endpoint)
			continue
		}
		r.retireLocked(current.Registration)
	}
	for endpoint := range next {
		// Re-adding an endpoint supersedes its tombstone: a live host and a
		// tombstone for the same endpoint are mutually exclusive.
		delete(r.tombstones, endpoint)
	}
	r.hosts = next
	r.order = order
	r.publishLocked(true)
	r.hint()
}

// registrationsUnchanged reports exact membership equality: the same endpoints
// in the same registration order, bound to the same registration identities and
// policy. Publication order and presentation rank are functions of durable
// membership order, so a reordering of identical records is a membership change
// that must republish; otherwise the same durable state yields different
// publications depending on process history. Projection fields such as
// availability, checking, or inventory never participate.
func registrationsUnchanged(current, next map[string]ports.BrokerDaemonObservation, currentOrder, nextOrder []string) bool {
	if len(current) != len(next) {
		return false
	}
	if !slices.Equal(currentOrder, nextOrder) {
		return false
	}
	for endpoint, candidate := range next {
		existing, ok := current[endpoint]
		if !ok || !existing.Registration.Equal(candidate.Registration) || existing.Policy != candidate.Policy {
			return false
		}
	}
	return true
}

// retireLocked records the tombstone for one removed registration at the
// revision of the publication that carries it. Mutation paths refuse an
// exhausted series before retiring, so the retirement revision can never wrap
// to zero or fall below the last published one.
func (r *Registry) retireLocked(registration domain.RemoteRegistration) {
	retired := r.revision + 1
	r.tombstones[registration.Endpoint] = ports.BrokerHostTombstone{
		Endpoint:        registration.Endpoint,
		Registration:    registration,
		RetiredRevision: retired,
	}
	r.pruneTombstonesLocked()
}

// pruneTombstonesLocked keeps only the newest BrokerMaxTombstones retirements
// so an endpoint that churns can never grow the published set without bound.
func (r *Registry) pruneTombstonesLocked() {
	for len(r.tombstones) > ports.BrokerMaxTombstones {
		oldest := ""
		var oldestRevision ports.BrokerRevision
		for endpoint, tombstone := range r.tombstones {
			if oldest == "" || tombstone.RetiredRevision < oldestRevision ||
				(tombstone.RetiredRevision == oldestRevision && endpoint < oldest) {
				oldest, oldestRevision = endpoint, tombstone.RetiredRevision
			}
		}
		delete(r.tombstones, oldest)
	}
}

// tombstonesLocked renders the bounded tombstone publication, newest first.
// Live endpoints are never published as tombstones.
func (r *Registry) tombstonesLocked() []ports.BrokerHostTombstone {
	if len(r.tombstones) == 0 {
		return nil
	}
	out := make([]ports.BrokerHostTombstone, 0, len(r.tombstones))
	for endpoint, tombstone := range r.tombstones {
		if _, live := r.hosts[endpoint]; live {
			continue
		}
		out = append(out, tombstone)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RetiredRevision != out[j].RetiredRevision {
			return out[i].RetiredRevision > out[j].RetiredRevision
		}
		return out[i].Endpoint < out[j].Endpoint
	})
	if len(out) > ports.BrokerMaxTombstones {
		out = out[:ports.BrokerMaxTombstones]
	}
	return out
}
