package broker

import (
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/ports"
)

// DaemonLaunchAuthority is the policy launch token that explicitly authorizes
// starting a stopped target daemon. Ports carry it opaquely; the broker is the
// component that interprets it. Any other token, including every
// sandbox-private one, fails closed and never starts a daemon.
const DaemonLaunchAuthority = "explicit"

// RequireLocalDaemonTarget refuses a target that is not the broker-owned local
// daemon. Only that authority names a daemon this process can start, so a
// registered host is never started by mistake, even on a local carriage.
func RequireLocalDaemonTarget(target ports.BrokerDialTarget) error {
	if !target.Fence.Local {
		return daemonStartRefused("the target is not the broker-owned local daemon")
	}
	return nil
}

// ClassifyDaemonDialFailure decides whether a failed carriage dial may lead to
// a start at all. absent reports whether the transport proved the daemon is
// simply not there; that classification belongs to the carriage adapter and
// must fail closed.
//
// It returns nil only when the daemon is absent under a mode other than
// ExistingOnly. Otherwise it returns the error to report, and no lock,
// election, or process may be touched:
//
//   - ExistingOnly never starts: absence becomes BrokerErrorNoDaemon and every
//     other failure is reported unchanged.
//   - A failure that is not absence (cancellation, a rejected peer, a foreign
//     path, a permission error) is never repaired by starting another daemon.
func ClassifyDaemonDialFailure(target ports.BrokerDialTarget, dialErr error, absent bool) error {
	if dialErr == nil {
		return errors.New("broker: a running daemon is never started again")
	}
	if target.StartMode == ports.BrokerDaemonExistingOnly {
		if absent {
			return ports.BrokerError{Code: ports.BrokerErrorNoDaemon, Cause: dialErr}
		}
		return dialErr
	}
	if !absent {
		return dialErr
	}
	return nil
}

// AuthorizeDaemonLaunch is the final start decision for an absent daemon. The
// start mode is an authorization, never an obligation, and it can only narrow
// the policy: the resolved policy must also name DaemonLaunchAuthority.
func AuthorizeDaemonLaunch(target ports.BrokerDialTarget) error {
	if target.StartMode != ports.BrokerDaemonStartIfNeeded {
		return daemonStartRefused("the acquisition does not authorize starting a daemon")
	}
	if target.Policy.Launch != DaemonLaunchAuthority {
		return daemonStartRefused("the resolved policy does not authorize launching")
	}
	return nil
}

// daemonStartRefused reports a refused start as a typed policy conflict. The
// reason is local diagnostic text, never a message the daemon produced.
func daemonStartRefused(reason string) error {
	return ports.BrokerError{
		Code:  ports.BrokerErrorConflictingPolicy,
		Cause: fmt.Errorf("refusing to start the daemon: %s", reason),
	}
}
