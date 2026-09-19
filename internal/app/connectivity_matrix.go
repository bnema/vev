package app

// Connectivity migration matrix (Plan 001 P1.2).
//
// Every CLI entry point has exactly one owner. After the P7 cutover, all
// ordinary user-facing operations reach daemons through the per-user
// connection broker; narrowly scoped daemon-facing transport helpers stay
// infrastructure and never become alternate client façades.
//
// Ownership rules pinned here:
//   - Broker-only entries must not keep a direct daemon dialer or a second
//     host-store writer after cutover. Their CurrentDebt lists today's
//     direct-connectivity symbols to remove in the same change that
//     activates the broker owner.
//   - Broker never owns session persistence. Offline session mutation/list
//     paths (PersistMutation) become daemon-owned operations reached through
//     the broker; the persist/snapshot packages stay daemon-side.
//   - Observation never starts a broker, never undoes an explicit
//     daemon-stop, and never creates a session attachment.
//   - Transport helpers terminate transport only. They never own host
//     configuration, navigation, sessions, or picker state.
//
// The accompanying test fails when a command kind lacks a row, when a
// broker-only row declares no migration debt, when an infra row gains store
// or persistence ownership, and when direct-dial, host-store, or offline
// persistence symbols appear in files outside the declared debt set.

type connectivityOwner uint8

const (
	// connectivityBrokerOnly marks ordinary user-facing operations that go
	// through the broker façade after cutover.
	connectivityBrokerOnly connectivityOwner = iota + 1
	// connectivityTransportInfra marks narrowly scoped daemon-facing
	// transport helpers: process mechanics and carriage terminators that
	// must never become client façades.
	connectivityTransportInfra
	// connectivityLocalOnly marks commands with no daemon or session
	// connectivity at all.
	connectivityLocalOnly
)

func (o connectivityOwner) String() string {
	switch o {
	case connectivityBrokerOnly:
		return "broker-only"
	case connectivityTransportInfra:
		return "transport-infra"
	case connectivityLocalOnly:
		return "local-only"
	default:
		return "unknown"
	}
}

// connectivityEntry is one row of the migration matrix. Kind is the cmdKind
// identifier from run.go (for example "kindAttach").
type connectivityEntry struct {
	Kind string
	// Summary names the user-visible operation.
	Summary string
	Owner   connectivityOwner
	// DirectDialDebt lists today's direct-daemon dial symbols that the
	// cutover removes for broker-only entries. Empty for infra/local rows.
	DirectDialDebt []string
	// HostStoreWrite reports a RemoteHostStore constructor/writer today.
	// Only the broker owns the host store after cutover.
	HostStoreWrite bool
	// PersistMutation reports an offline session persistence read/write
	// today. These become daemon-owned operations through the broker.
	PersistMutation bool
	// Notes records cutover direction and non-obvious boundaries.
	Notes string
}

// connectivityMatrix classifies every cmdKind exactly once.
var connectivityMatrix = []connectivityEntry{
	{
		Kind:    "kindAttach",
		Summary: "interactive terminal attach/new/ephemeral, incl. remote attach, detached creation, attach preflight",
		Owner:   connectivityBrokerOnly,
		DirectDialDebt: []string{
			"localDaemonDialer.Dial (run.go)",
			"dialOnlyLocalDialer.Dial inventory control (run.go)",
			"ensureDaemonWithLifecycle detached creation (run.go createDetachedLocalSession)",
			"registry.ResolveEndpoint remote carriage (run.go runAttachWithDeps)",
			"listSessionsWithDialer attach preflight (attach_preflight.go sessionExists)",
		},
		Notes: "vev without arguments keeps creating an ephemeral session; creation is expressed as client navigation through the broker.",
	},
	{
		Kind:    "kindList",
		Summary: "session list: local, per-host, and --all",
		Owner:   connectivityBrokerOnly,
		DirectDialDebt: []string{
			"realDial listSessionsWithDialer (run.go)",
			"waitForDaemonOrLifecycle list path (run.go)",
			"RemoteCatalogClient.List over SSH (remote_hosts.go listAllSessions/listOneRemoteHost)",
		},
		PersistMutation: true,
		Notes:           "Offline persist.LoadReadOnly list path becomes a daemon-owned operation through the broker; observation never attaches.",
	},
	{
		Kind:           "kindHost",
		Summary:        "host add/rm/list",
		Owner:          connectivityBrokerOnly,
		DirectDialDebt: []string{"remoteHostDeps.hostStore (remote_hosts.go hostAdd/hostRm)"},
		HostStoreWrite: true,
		Notes:          "Second host-store writer today; broker is the sole writer after cutover. Host list is store-only (no dial).",
	},
	{
		Kind:    "kindKill",
		Summary: "kill one session, kill-all, explicit daemon-stop",
		Owner:   connectivityBrokerOnly,
		DirectDialDebt: []string{
			"waitForDaemonOrLifecycle realDial kill path (run.go)",
			"forceStopDaemonFallback (force_stop.go)",
		},
		PersistMutation: true,
		Notes:           "Offline runOfflineNamedKill mutation becomes daemon-owned via broker. Kill-all purges sessions and leaves the daemon running; daemon-stop is the distinct explicit stop.",
	},
	{
		Kind:    "kindCmd",
		Summary: "scriptable command requests, incl. remote-catalog",
		Owner:   connectivityBrokerOnly,
		DirectDialDebt: []string{
			"realDial command path (cmd.go)",
			"ensureDaemonWithLifecycle remote-catalog path (cmd.go)",
		},
		Notes: "Command tracker semantics unchanged; only the carriage moves behind the broker.",
	},
	{
		Kind:    "kindUIDriver",
		Summary: "headless UI-driver attach",
		Owner:   connectivityBrokerOnly,
		DirectDialDebt: []string{
			"localDaemonDialer launch/local paths (ui_driver.go, run.go runAttachWithDeps)",
			"configuredRemoteLaunches dialerFactory (ui_driver.go)",
		},
		Notes: "Launch-owned remote dialer and cleanup move behind the broker; UI-driver keeps no parallel carriage.",
	},
	{
		Kind:           "kindWebDaemon",
		Summary:        "browser gateway launcher",
		Owner:          connectivityBrokerOnly,
		DirectDialDebt: []string{"gateway session path via runAttachWithDeps (web_daemon.go)"},
		Notes:          "Launcher mechanics stay local; gateway session traffic goes through the broker like any client.",
	},
	{
		Kind:           "kindWebServe",
		Summary:        "browser gateway server",
		Owner:          connectivityBrokerOnly,
		DirectDialDebt: []string{"gateway session path via runAttachWithDeps (web_daemon.go)"},
		Notes:          "One client runner per WebSocket; no bypass of the broker façade.",
	},
	{
		Kind:    "kindDaemon",
		Summary: "daemon process: owns sessions, serves typed connections",
		Owner:   connectivityTransportInfra,
		Notes:   "Daemon accepts one typed connection per broker logical stream after cutover. Never aggregates other daemons, never owns the host registry (P7 removal set).",
	},
	{
		Kind:    "kindDaemonLauncher",
		Summary: "short-lived daemon spawn launcher",
		Owner:   connectivityTransportInfra,
		Notes:   "Process mechanics only; never a connectivity façade.",
	},
	{
		Kind:    "kindStdio",
		Summary: "_stdio SSH-side carriage proxy",
		Owner:   connectivityTransportInfra,
		Notes:   "Terminates SSH stdio transport on the far side; never resolves navigation or owns sessions.",
	},
	{
		Kind:    "kindQUICBootstrap",
		Summary: "_quic-bootstrap SSH-side bootstrap",
		Owner:   connectivityTransportInfra,
		Notes:   "One-time authenticated bootstrap; terminates transport, never navigation authority.",
	},
	{
		Kind:    "kindQUICProxy",
		Summary: "_quic-proxy carriage-neutral bridge",
		Owner:   connectivityTransportInfra,
		Notes:   "Blind proxy may forward raw envelopes; exposes no bytes to use cases and owns no sessions.",
	},
	{
		Kind:    "kindRemotePreview",
		Summary: "_remote-preview SSH-side preview carriage",
		Owner:   connectivityTransportInfra,
		DirectDialDebt: []string{
			"ipc.DialContext direct local dial (remote_preview.go)",
		},
		Notes: "Far-side helper only. Client-facing previews flow through broker snapshots after cutover; this helper never becomes a client façade. Direct-dial debt is recorded here rather than as broker debt because the helper itself stays infra.",
	},
	{
		Kind:    "kindUIRemoteCleanup",
		Summary: "_ui-cleanup remote-side launch cleanup",
		Owner:   connectivityTransportInfra,
		Notes:   "Scoped remote cleanup; never launches observers or brokers recursively.",
	},
	{
		Kind:    "kindWebRenew",
		Summary: "browser token renewal",
		Owner:   connectivityLocalOnly,
		Notes:   "Local control only; no daemon or session connectivity.",
	},
	{
		Kind:    "kindHelp",
		Summary: "usage text",
		Owner:   connectivityLocalOnly,
		Notes:   "No connectivity.",
	},
	{
		Kind:    "kindVersion",
		Summary: "version line",
		Owner:   connectivityLocalOnly,
		Notes:   "No connectivity.",
	},
	{
		Kind:    "kindBrokerServe",
		Summary: "hidden _broker-serve isolated offline broker sandbox",
		Owner:   connectivityLocalOnly,
		Notes:   "Foreground sandbox process over its own private root: no production runtime/state, no daemon dial, no RemoteHostStore, and no session persistence. It composes the broker under a temporary offline config and does not become a client façade or an ordinary command.",
	},
	{
		Kind:    "kindBrokerClient",
		Summary: "hidden _broker-client autonomous offline client harness",
		Owner:   connectivityLocalOnly,
		Notes:   "Explicit offline-root composition only: real broker IPC and logical streams exercise terminal and UI harnesses without changing any ordinary command or production path.",
	},
	{
		Kind:    "kindBrokerLauncher",
		Summary: "hidden _broker-launcher detached offline broker launcher",
		Owner:   connectivityLocalOnly,
		Notes:   "Process mechanics only, scoped to one operator-supplied offline root: it starts _broker-serve in a new session and exits, like the daemon launcher, and never dials a daemon, owns the host store, or mutates persistence.",
	},
	{
		Kind:    "kindBrokerStatus",
		Summary: "hidden _broker-status offline broker probe and connect-or-spawn",
		Owner:   connectivityLocalOnly,
		Notes:   "Reports the offline broker's local IPC endpoint and, with --ensure, elects one spawner under a descriptor-backed lock. It dials only the operator-supplied broker sandbox socket (never a daemon or RemoteHostStore) and never becomes a client façade or ordinary command.",
	},
	{
		Kind:    "kindBrokerMuxStdio",
		Summary: "hidden _broker-mux-stdio remote SSH stdio daemonmux bridge",
		Owner:   connectivityTransportInfra,
		DirectDialDebt: []string{
			"ipc.DialMuxContext local daemonmux carriage (broker_offline_mux.go)",
		},
		Notes: "Remote-side helper over one operator-supplied offline root: it bridges its own stdio to the single provisioned private Unix daemonmux carriage. It never starts a broker, observer, or ordinary daemon, never dials the production daemon socket, and never fabricates a daemon incarnation. Direct-dial debt is recorded here rather than as broker debt because the helper itself stays transport infra.",
	},
	{
		Kind:    "kindBrokerMuxQUICBootstrap",
		Summary: "hidden _broker-mux-quic-bootstrap remote SSH-side QUIC bootstrap",
		Owner:   connectivityTransportInfra,
		Notes:   "Starts one detached _broker-mux-quic-proxy in a new session, forwards its single readiness line, and exits. Its only child is that proxy: it never starts a broker, observer, or ordinary daemon, and one-time QUIC credentials never reach an address, log, or error.",
	},
	{
		Kind:    "kindBrokerMuxQUICProxy",
		Summary: "hidden _broker-mux-quic-proxy detached remote QUIC daemonmux bridge",
		Owner:   connectivityTransportInfra,
		DirectDialDebt: []string{
			"ipc.DialMuxContext local daemonmux carriage (broker_offline_mux.go)",
		},
		Notes: "Mints one ephemeral authenticated QUIC server, admits exactly one carriage, and bridges it to the single provisioned private Unix daemonmux carriage. It never dials the production daemon socket, never starts a daemon, and never fabricates a daemon incarnation.",
	},
}

// connectivityOwnerFor returns the matrix owner for a cmdKind identifier.
func connectivityOwnerFor(kind string) (connectivityOwner, bool) {
	for _, entry := range connectivityMatrix {
		if entry.Kind == kind {
			return entry.Owner, true
		}
	}
	return 0, false
}
