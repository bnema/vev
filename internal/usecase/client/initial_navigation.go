package client

import (
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Closed, one-shot initial navigation.
//
// InitialNavigation is the comparable, closed description of the single broker
// navigation a client performs after its first committed catalogue publication.
// It replaces the former single-variant enum outright: there is no alias, and
// the zero value is a safe no-navigation "picker" intent so a configuration
// that omits the intent opens nothing.
//
// The union carries no live authority of its own. Validate is total and
// performs no I/O; Resolve revalidates the intent against one broker snapshot
// and returns the exact ports.BrokerOpenStreamRequest gabarit, whose connection
// and stream identity the supervisor fills from its own connection before it
// opens the stream.
//
// Rules the closed union enforces:
//
//   - Picker carries nothing at all: a zero destination, a zero epoch, no name,
//     and a zero target.
//   - CreateEphemeral and CreateNamed carry a valid endpoint fence, a name
//     only for the named variant, and no target.
//   - AttachExact carries a valid endpoint fence, a nonzero epoch, no name, and
//     a validated protocol.ExactSessionTarget.
//   - A local creation may state epoch zero, meaning "bind to the first adopted
//     publication". Every remote intent, and every exact attach even locally,
//     requires the epoch of the snapshot that resolved it, and a nonzero epoch
//     must equal the current snapshot's epoch. Epochs never compare numerically
//     across broker processes.
type InitialNavigationKind uint8

const (
	// InitialNavigationPicker is the zero kind: present the picker and open
	// nothing. A configuration that omits the intent is safe.
	InitialNavigationPicker InitialNavigationKind = iota
	// InitialNavigationCreateEphemeral creates one ephemeral session.
	InitialNavigationCreateEphemeral
	// InitialNavigationCreateNamed creates one named session.
	InitialNavigationCreateNamed
	// InitialNavigationAttachExact attaches to one exact session lifecycle.
	InitialNavigationAttachExact
)

func (k InitialNavigationKind) String() string {
	switch k {
	case InitialNavigationPicker:
		return "picker"
	case InitialNavigationCreateEphemeral:
		return "create_ephemeral"
	case InitialNavigationCreateNamed:
		return "create_named"
	case InitialNavigationAttachExact:
		return "attach_exact"
	default:
		return fmt.Sprintf("invalid(%d)", uint8(k))
	}
}

// InitialNavigation is one closed, one-shot navigation intent. Destination is
// the existing ports.BrokerEndpointFence: a local destination is exactly
// Local=true with no registration, and a remote destination carries the
// complete registration observed in a broker snapshot, never a bare hostname
// and never an invented daemon identity.
type InitialNavigation struct {
	Kind        InitialNavigationKind
	Epoch       ports.BrokerEpoch
	Destination ports.BrokerEndpointFence
	Name        string
	Target      protocol.ExactSessionTarget
}

// InitialNavigationResolver translates one composition-owned CLI target (an
// attach name or a remote endpoint) into an exact navigation identity against
// the first committed broker snapshot. It is a translation of a target into
// identity, not an owner of navigation: it performs no I/O, it is consulted at
// most once, and the supervisor remains the only component that consumes the
// intent and opens the stream.
type InitialNavigationResolver func(ports.BrokerSnapshot) (InitialNavigation, error)

// Validate checks the closed union. It is total, pure, and never consults a
// snapshot: a caller can always reject a contradictory intent offline.
func (n InitialNavigation) Validate() error {
	switch n.Kind {
	case InitialNavigationPicker:
		if n.Destination != (ports.BrokerEndpointFence{}) {
			return errors.New("vev: picker navigation carries a destination")
		}
		if n.Epoch != 0 {
			return errors.New("vev: picker navigation carries an epoch")
		}
		if n.Name != "" {
			return errors.New("vev: picker navigation carries a session name")
		}
		if n.Target != (protocol.ExactSessionTarget{}) {
			return errors.New("vev: picker navigation carries an exact target")
		}
		return nil
	case InitialNavigationCreateEphemeral, InitialNavigationCreateNamed, InitialNavigationAttachExact:
	default:
		return fmt.Errorf("vev: unknown initial navigation kind %d", uint8(n.Kind))
	}
	if err := n.Destination.Validate(); err != nil {
		return fmt.Errorf("vev: initial navigation destination: %w", err)
	}
	if !n.Destination.Local && n.Epoch == 0 {
		// A remote destination cannot be decided before the connection: it
		// requires the epoch of the snapshot that resolved its registration.
		return errors.New("vev: remote initial navigation carries no epoch")
	}
	switch n.Kind {
	case InitialNavigationCreateEphemeral:
		if n.Name != "" {
			return errors.New("vev: ephemeral creation navigation carries a session name")
		}
		if n.Target != (protocol.ExactSessionTarget{}) {
			return errors.New("vev: ephemeral creation navigation carries an exact target")
		}
	case InitialNavigationCreateNamed:
		if err := domain.ValidateSessionName(n.Name); err != nil {
			return fmt.Errorf("vev: initial navigation session name: %w", err)
		}
		if n.Target != (protocol.ExactSessionTarget{}) {
			return errors.New("vev: named creation navigation carries an exact target")
		}
	case InitialNavigationAttachExact:
		if n.Epoch == 0 {
			return errors.New("vev: exact attach navigation carries no epoch")
		}
		if n.Name != "" {
			return errors.New("vev: exact attach navigation carries a session name")
		}
		if err := n.Target.Validate(); err != nil {
			return fmt.Errorf("vev: initial navigation target: %w", err)
		}
	}
	return nil
}

// Resolve revalidates the intent against one broker snapshot and returns the
// exact stream-request gabarit it names. The gabarit deliberately leaves
// Connection and Stream zero: the supervisor sets them from its adopted
// connection before it opens the stream. Policy is copied from the selected
// authority and the environment is never read from the broker process.
//
// Picker is a success without an operation for the supervisor, but resolving it
// directly is refused: a caller never receives a nil-but-usable request for a
// navigation that opens nothing.
func (n InitialNavigation) Resolve(snapshot ports.BrokerSnapshot) (ports.BrokerOpenStreamRequest, error) {
	if n.Kind == InitialNavigationPicker {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueNoSelection, Text: "picker navigation opens no stream"}
	}
	if err := n.Validate(); err != nil {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: initialNavigationRefusal(n), Text: "initial navigation is not valid"}
	}
	if err := snapshot.Validate(); err != nil {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "broker catalogue is unavailable"}
	}
	// The gabarit is validated end to end against a placeholder connection
	// identity, then stripped of it: the supervisor is the only component that
	// assigns the live connection and stream before it opens the stream.
	request, err := resolveCatalogueRequest(snapshot, pickerSelectionFromNavigation(n), initialNavigationGabaritBase)
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	request.Connection = ports.BrokerConnectionID{}
	request.Stream = 0
	return request, nil
}

// initialNavigationGabaritBase is the placeholder connection identity used to
// validate a resolved initial-navigation request end to end before the
// supervisor assigns its own live connection and stream identity. The gabarit
// returned to the caller always carries a zero connection and stream.
var initialNavigationGabaritBase = pickerResolveBase{Connection: ports.BrokerConnectionID{1}, Stream: ports.BrokerStreamID(1)}

// pickerSelectionFromNavigation projects one validated intent into the exact
// selection identity the shared resolver already understands, so the union and
// the interactive picker cannot drift into two resolution semantics.
func pickerSelectionFromNavigation(n InitialNavigation) pickerSelectionRef {
	ref := pickerSelectionRef{
		epoch:        n.Epoch,
		local:        n.Destination.Local,
		endpoint:     n.Destination.Registration.Endpoint,
		registration: n.Destination.Registration,
	}
	switch n.Kind {
	case InitialNavigationCreateNamed:
		ref.kind = pickerSelectionCreateNamed
		ref.createName = n.Name
	case InitialNavigationCreateEphemeral:
		ref.kind = pickerSelectionCreateEphemeral
	case InitialNavigationAttachExact:
		ref.kind = pickerSelectionExact
		ref.lifecycle = n.Target.LifecycleID
		ref.name = n.Target.SessionName
	}
	return ref
}

// initialNavigationRefusal classifies a failed intent validation into the
// bounded picker refusal taxonomy without exposing free text: an invalid
// creation name keeps its own code, and every other contradiction is an
// unknown-kind refusal.
func initialNavigationRefusal(n InitialNavigation) pickerCatalogueErrorCode {
	if n.Kind == InitialNavigationCreateNamed {
		if err := domain.ValidateSessionName(n.Name); err != nil {
			return pickerCatalogueInvalidName
		}
	}
	return pickerCatalogueUnknown
}
