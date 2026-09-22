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
	// cutover removes for broker-only entries. Empty for infra/local rows and
	// for a broker-only row that has already completed its cutover.
	DirectDialDebt []string
	// Migrated marks a broker-only row whose cutover is complete: it keeps no
	// direct dial, no host-store write, and no persist mutation, so its debt
	// list is deliberately empty. It exists so a finished row is declared
	// explicitly instead of being mistaken for an unreviewed one.
	Migrated bool
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
		Summary: "interactive terminal attach/new/ephemeral, incl. remote attach, detached creation, and the UI-driver/web path",
		Owner:   connectivityBrokerOnly,
		DirectDialDebt: []string{
			"ensureDaemonWithLifecycle detached creation (run.go createDetachedLocalSession)",
		},
		Notes: "The ordinary terminal path is already broker-only: runAttach translates the parsed CLI target into client.InitialNavigation/Resolver, connect-or-spawns the per-user broker, and delegates to the shared runBrokerClient (run.go, broker_client.go). The old direct-dialer attach composition and its attach preflight and client host registry are removed; the UI-driver and browser compositions delegate to the shared broker client. The only remaining debt is the nested-session detached creation path, which still dials the local daemon directly and is expressed as broker navigation for ordinary attach.",
	},
	{
		Kind:     "kindList",
		Summary:  "session list: local, per-host, and --all",
		Owner:    connectivityBrokerOnly,
		Migrated: true,
		Notes:    "Cutover complete: runList reaches the per-user broker through the private connectBroker seam (run.go) and reads BrokerOperations.List for the local list or runBrokerSnapshotList for per-host/--all. It keeps no persist read, no daemon dialer, and no direct-dial list path. Remote observation never attaches.",
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
		Kind:     "kindKill",
		Summary:  "kill one session, kill-all, explicit daemon-stop",
		Owner:    connectivityBrokerOnly,
		Migrated: true,
		Notes:    "Cutover complete: runKill and requestDaemonStop reach the per-user broker through the private connectBroker seam (run.go) and drive BrokerOperations.Kill/KillAll/StopDaemon. Daemon-stop uses ExistingOnly, so it can never start what it is stopping; kill-all purges sessions and leaves the daemon running. The offline persisted-mutation path and the direct force-stop fallback are removed, so no request is ever bypassed with a direct process signal.",
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
		Kind:     "kindUIDriver",
		Summary:  "headless UI-driver attach",
		Owner:    connectivityBrokerOnly,
		Migrated: true,
		Notes:    "Cutover complete: the driver composes the process' production broker connector and the shared terminal initial-navigation translation through runUIDriverClient. It keeps no launch configuration, no dialer, no endpoint, and no daemon ownership.",
	},
	{
		Kind:     "kindUIRemoteCleanup",
		Summary:  "UI fixture cleanup through broker control",
		Owner:    connectivityBrokerOnly,
		Migrated: true,
		Notes:    "Cleanup uses the broker-owned daemon-stop operation and keeps no direct daemon carriage.",
	},
	{
		Kind:     "kindWebDaemon",
		Summary:  "browser gateway launcher",
		Owner:    connectivityBrokerOnly,
		Migrated: true,
		Notes:    "Launcher mechanics stay local. Cutover complete: the gateway server composes the shared runBrokerClient over the process' production broker connector (web_daemon.go runWebTerminalClient), so gateway session traffic goes through the broker like any client. The launcher keeps no dialer, no session carriage, and no daemon ownership.",
	},
	{
		Kind:     "kindWebServe",
		Summary:  "browser gateway server",
		Owner:    connectivityBrokerOnly,
		Migrated: true,
		Notes:    "Cutover complete: every authenticated WebSocket owns exactly one runBrokerClient over the shared production broker connector, with the virtual webterm.Terminal as that run's terminal, the no-argument local ephemeral creation as its closed initial navigation, and the standard presentation callbacks. It keeps no direct daemon dialer, no sessionwire carriage, no remote factory, and no launch configuration; disconnect cancels only that supervisor's service and logical stream.",
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
		Kind:    "kindWebRenew",
		Summary: "browser token renewal",
		Owner:   connectivityLocalOnly,
		Notes:   "Local control only; no daemon or session connectivity.",
	},
	{
		Kind:    "kindBrokerReady",
		Summary: "hidden dial-only broker readiness probe",
		Owner:   connectivityLocalOnly,
		Notes:   "Dials only the selected broker IPC socket, registers, subscribes, and reads snapshots; it never ensures, spawns, opens a logical stream, reconciles, or mutates.",
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
		Kind:    "kindProductionBrokerServe",
		Summary: "hidden production broker server",
		Owner:   connectivityLocalOnly,
		Notes:   "Foreground production broker using only productionBrokerLayout and productionBrokerConfigPath; it accepts no offline root.",
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
		Kind:    "kindProductionBrokerLauncher",
		Summary: "hidden detached production broker launcher",
		Owner:   connectivityLocalOnly,
		Notes:   "Process mechanics for the production broker only; it accepts no offline root and starts the production serve role in a new session.",
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
		Notes: "Remote-side helper over one operator-supplied offline root: it bridges its own stdio to the single provisioned private Unix daemonmux carriage. It never starts a broker, observer, or recursive helper, never dials the production daemon socket, and never fabricates a daemon incarnation. It starts the daemon behind that carriage only under the propagated --daemon-start if-needed authorization and only when its own provisioned policy permits launching; otherwise it dials only an existing carriage. Direct-dial debt is recorded here rather than as broker debt because the helper itself stays transport infra.",
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
		Notes: "Mints one ephemeral authenticated QUIC server, admits exactly one carriage, and bridges it to the single provisioned private Unix daemonmux carriage. It never dials the production daemon socket, never fabricates a daemon incarnation, and starts the daemon behind that carriage only under the propagated --daemon-start if-needed authorization and its own provisioned launch policy.",
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
