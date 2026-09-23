package app

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
	"time"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

const brokerReadyCommand = "_broker-ready"

type brokerReadyOptions struct {
	require string
	timeout time.Duration
	maxAge  time.Duration
}

type brokerReadyResult struct {
	Schema   string            `json:"schema"`
	Status   string            `json:"status"`
	Require  string            `json:"require"`
	Reason   string            `json:"reason"`
	Pending  []string          `json:"pending"`
	Epoch    string            `json:"epoch"`
	Revision string            `json:"revision"`
	Local    *brokerReadyLocal `json:"local"`
}
type brokerReadyLocal struct {
	Identity       string `json:"identity"`
	Incarnation    string `json:"incarnation"`
	Version        string `json:"version"`
	Availability   string `json:"availability"`
	InventoryKnown bool   `json:"inventory_known"`
	LastSuccess    string `json:"last_success"`
}

func parseBrokerReadyArgs(args []string) (brokerReadyOptions, error) {
	o := brokerReadyOptions{timeout: 10 * time.Second, maxAge: 5 * time.Second}
	seen := map[string]bool{}
	for i := 0; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return o, usagef("`%s` requires a value", args[i])
		}
		name, value := args[i], args[i+1]
		if seen[name] {
			return o, usagef("duplicate `%s`", name)
		}
		seen[name] = true
		switch name {
		case "--require":
			o.require = value
		case "--timeout":
			d, err := time.ParseDuration(value)
			if err != nil || d <= 0 {
				return o, usagef("`--timeout` must be positive")
			}
			o.timeout = d
		case "--max-observation-age":
			d, err := time.ParseDuration(value)
			if err != nil || d <= 0 {
				return o, usagef("`--max-observation-age` must be positive")
			}
			o.maxAge = d
		default:
			return o, usagef("unknown `%s` option", name)
		}
	}
	if o.require != "local-authority" && o.require != "local-catalogue" {
		return o, usagef("`--require` must be local-authority or local-catalogue")
	}
	if o.require != "local-catalogue" && seen["--max-observation-age"] {
		return o, usagef("`--max-observation-age` is only valid for local-catalogue")
	}
	return o, nil
}

var brokerReadyDial = func(ctx context.Context, path string) (ports.BrokerService, error) {
	return brokeripc.Dial(ctx, path, brokeripc.Config{})
}

func runBrokerReady(ctx context.Context, o brokerReadyOptions, out io.Writer) error {
	deadline, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	path := brokeripc.SocketPath(productionBrokerLayout().Runtime)
	backoff := 5 * time.Millisecond
	for {
		if err := deadline.Err(); err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return writeReady(out, o, "canceled", "canceled", nil, 5)
			}
			return writeReady(out, o, "timeout", "deadline", nil, 3)
		}
		// A path that exists but is not a Unix socket, and a path this process
		// may not inspect, are terminal security failures: retrying them into
		// the deadline would report a foreign path as a slow broker. Only
		// proven absence is retried, which is what lets an on-demand broker
		// still come up under this probe.
		if reason, terminal := brokerReadyEndpointSecurity(path); terminal {
			return writeReady(out, o, "terminal", reason, nil, 4)
		}
		service, err := brokerReadyDial(deadline, path)
		if err != nil {
			if reason, terminal := terminalReadyReason(err); terminal {
				return writeReady(out, o, "terminal", reason, nil, 4)
			}
			select {
			case <-deadline.Done():
				continue
			case <-time.After(backoff):
			}
			if backoff < 50*time.Millisecond {
				backoff *= 2
				if backoff > 50*time.Millisecond {
					backoff = 50 * time.Millisecond
				}
			}
			continue
		}
		result := observeBrokerReady(deadline, o, service)
		_ = service.Close()
		if result != nil {
			return emitReady(out, *result)
		}
	}
}

func observeBrokerReady(ctx context.Context, o brokerReadyOptions, service ports.BrokerService) *brokerReadyResult {
	sub, err := service.Subscribe()
	if err != nil {
		r := readyResult(o, "terminal", "protocol", nil)
		return &r
	}
	defer sub.Close()
	for {
		snapshot := service.Snapshot()
		if snapshot.Epoch != 0 {
			if snapshot.Validate() != nil {
				r := readyResult(o, "terminal", "protocol", nil)
				return &r
			}
			locals := make([]ports.BrokerDaemonObservation, 0, 1)
			for _, d := range snapshot.Daemons {
				if d.Local {
					locals = append(locals, d)
				}
			}
			if len(locals) > 1 {
				r := readyResult(o, "terminal", "protocol", nil)
				return &r
			}
			if len(locals) == 1 {
				r := evaluateReady(o, snapshot, locals[0])
				if r.Status == "ready" || r.Status == "terminal" {
					return &r
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-service.Done():
			return nil
		case <-sub.Changed():
		}
	}
}

func evaluateReady(o brokerReadyOptions, s ports.BrokerSnapshot, local ports.BrokerDaemonObservation) brokerReadyResult {
	r := readyResult(o, "pending", "local-not-ready", &local)
	r.Epoch = strconv.FormatUint(uint64(s.Epoch), 10)
	r.Revision = strconv.FormatUint(uint64(s.Revision), 10)
	if o.require == "local-authority" {
		r.Status = "ready"
		r.Reason = "authority"
		r.Pending = []string{}
		return r
	}
	valid := local.Availability == domain.RemoteAvailabilityReachable && local.InventoryKnown && local.Identity.Validate() == nil && local.Incarnation.Validate() == nil && local.ProtocolVersion == protocol.Version && !local.LastSuccess.IsZero()
	age := time.Since(local.LastSuccess)
	valid = valid && age >= 0 && age <= o.maxAge
	if valid {
		r.Status = "ready"
		r.Reason = "catalogue"
		r.Pending = []string{}
	} else {
		r.Pending = []string{"local-catalogue"}
	}
	return r
}

func readyResult(o brokerReadyOptions, status, reason string, local *ports.BrokerDaemonObservation) brokerReadyResult {
	r := brokerReadyResult{Schema: "vev.broker-ready/v1", Status: status, Require: o.require, Reason: reason, Pending: []string{o.require}, Epoch: "0", Revision: "0"}
	if local != nil {
		r.Local = &brokerReadyLocal{Identity: string(local.Identity), Incarnation: hex.EncodeToString(local.Incarnation[:]), Version: strconv.FormatUint(uint64(local.ProtocolVersion), 10), Availability: local.Availability.String(), InventoryKnown: local.InventoryKnown, LastSuccess: local.LastSuccess.UTC().Format(time.RFC3339Nano)}
	}
	return r
}

// brokerReadyEndpointSecurity classifies the selected endpoint path before any
// dial. Absence stays retryable; a path that exists and is not a Unix socket,
// and a path this process may not inspect, are terminal security failures. Only
// the specific not-exist outcome is absence, so an unreadable parent, a foreign
// path, or any other inspection failure fails closed.
func brokerReadyEndpointSecurity(path string) (string, bool) {
	info, err := os.Lstat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "", false
	case err != nil:
		return "security", true
	case info.Mode()&os.ModeSocket == 0:
		return "security", true
	default:
		return "", false
	}
}

// terminalReadyReason classifies one failed dial attempt that must never be
// retried into the readiness deadline. A refusal that proves the path, the peer,
// the credentials, the configuration, or the broker conversation itself is not
// the expected broker is terminal; every other outcome, including an absent or
// refused socket, stays retryable so a broker can still come up.
func terminalReadyReason(err error) (string, bool) {
	switch {
	case err == nil:
		return "", false
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "", false
	case errors.Is(err, ipc.ErrMuxPath), errors.Is(err, ipc.ErrMuxForeignPath),
		errors.Is(err, ipc.ErrMuxPeerRejected), errors.Is(err, ipc.ErrMuxUnsupported),
		errors.Is(err, os.ErrPermission):
		return "security", true
	case errors.Is(err, brokeripc.ErrConfig):
		return "config", true
	case errors.Is(err, brokeripc.ErrProtocol), errors.Is(err, brokeripc.ErrMalformedFrame),
		errors.Is(err, ipc.ErrFrameTooLarge), errors.Is(err, ipc.ErrZeroLengthFrame):
		return "protocol", true
	default:
		return "", false
	}
}

func emitReady(w io.Writer, r brokerReadyResult) error {
	b, err := json.Marshal(r)
	if err != nil || len(b)+1 > 4096 {
		return &exitCoded{code: 1, err: errors.New("broker readiness output failure")}
	}
	if _, err = w.Write(append(b, '\n')); err != nil {
		return &exitCoded{code: 1, err: errors.New("broker readiness output failure")}
	}
	switch r.Status {
	case "ready":
		return nil
	case "timeout":
		return &exitCoded{code: 3, err: errors.New("broker readiness timeout")}
	case "terminal":
		return &exitCoded{code: 4, err: errors.New("broker readiness terminal failure")}
	case "canceled":
		return &exitCoded{code: 5, err: context.Canceled}
	default:
		return &exitCoded{code: 3, err: errors.New("broker readiness timeout")}
	}
}
func writeReady(w io.Writer, o brokerReadyOptions, status, reason string, local *ports.BrokerDaemonObservation, code int) error {
	r := readyResult(o, status, reason, local)
	err := emitReady(w, r)
	if code != 0 {
		return &exitCoded{code: code, err: fmt.Errorf("broker readiness %s", reason)}
	}
	return err
}
