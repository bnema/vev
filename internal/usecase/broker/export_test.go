package broker

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/bnema/vev/internal/ports"
)

// NewRegistry restores a validated durable snapshot under a fresh broker
// epoch with the default RegistryConfig, delegating to the same
// NewRegistryWithConfig production composition uses. It is a test-only
// shorthand: production always selects an explicit membership/observation
// mode.
func NewRegistry(epoch ports.BrokerEpoch, store ports.BrokerHostStore, probe ports.BrokerHostProbe, clock ports.Clock, log *slog.Logger) (*Registry, error) {
	return NewRegistryWithConfig(epoch, store, probe, clock, log, RegistryConfig{})
}

// setHosts is projection-only; it is private so callers cannot bypass durable
// membership and policy. Observation tests use it without claiming durability.
// An unchanged membership is a no-op: it neither republishes nor restarts an
// in-flight observation. An exhausted revision series is refused with
// ports.ErrBrokerRevisionExhausted before the projection changes.
// Removed registrations retire bounded tombstones so a stale completion can
// never revive them; replaced or re-added endpoints are fenced by the
// registry attempt token instead.
func (r *Registry) setHosts(records []ports.BrokerHostRecord) error {
	if len(records) > ports.BrokerMaxHosts {
		return fmt.Errorf("broker: %d host registrations exceed the limit of %d", len(records), ports.BrokerMaxHosts)
	}
	if len(records) > 0 && !r.observationDisabled && nilDependency(r.probe) {
		return errors.New("broker: remote hosts require a host probe")
	}
	next := make(map[string]ports.BrokerDaemonObservation, len(records))
	order := make([]string, 0, len(records))
	for rank, record := range records {
		if err := record.Registration.Validate(); err != nil {
			return err
		}
		if err := record.Policy.Validate(); err != nil {
			return err
		}
		registration := record.Registration
		if _, duplicate := next[registration.Endpoint]; duplicate {
			return errors.New("broker: duplicate host registration")
		}
		observation := stampHostAuthority(ports.BrokerDaemonObservation{}, record, rank)
		// Membership the registry cannot publish is a caller error, refused here
		// rather than left to freeze every later publication: an endpoint whose
		// derived display hint is empty after the display rule drops its runes
		// makes every snapshot carrying it invalid.
		if err := observation.Validate(); err != nil {
			return fmt.Errorf("broker: host record %q is not publishable: %w", registration.Endpoint, err)
		}
		next[registration.Endpoint] = observation
		order = append(order, registration.Endpoint)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.revisionExhaustedLocked() {
		return fmt.Errorf("broker: set hosts: %w", ports.ErrBrokerRevisionExhausted)
	}
	r.setHostsLocked(next, order)
	return nil
}
