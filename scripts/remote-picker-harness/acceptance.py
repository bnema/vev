#!/usr/bin/env python3
"""Table-driven UI-driver acceptance against disposable demo-image containers.

Each scenario starts real ``vev --ui-driver`` clients against real daemons
and asserts committed outcomes (action IDs, generation changes, exact
lifecycle/session/focus context, observed published output). Sending a key
or accepting a request is never success: every scenario ends on a
postcondition ``wait`` or a verified capture.

Transport selection passes through the client process environment:
``VEV_REMOTE_TRANSPORT`` unset means QUIC, ``stdio`` means SSH stdio. The
value is forwarded into the container with ``docker exec -e`` so it actually
reaches the driver process.

Direct-remote scenarios use ``--remote ENDPOINT --session NAME`` against a
named fixture session: a named exact remote target, never an ephemeral
attach. The warm-reuse scenario keeps one client Runner across a
local -> named remote -> local -> same named remote journey and pins the
retained attachment with client debug logs, the remote daemon's attach
count, and the committed remote lifecycle.
"""

import json
import os
import subprocess
import sys
import time
import uuid

COLS = "100"
ROWS = "30"
REQUEST_TIMEOUT_S = 15
WAIT_TIMEOUT_MS = 15000

STATE_LOG_DIR = "$HOME/.local/state/vev"
CLIENT_LOG = "vev-client.log"
DAEMON_LOG = "vev-daemon.log"
STDIO_LOG = "vev-stdio.log"
REMOTE_CACHE_FILE = "remote-catalog-cache.json"
INVENTORY_TIMEOUT_S = 45
ARTIFACTS_DIR = os.environ.get("VEV_ACCEPTANCE_ARTIFACTS_DIR", "").strip()
# The warm-reuse scenario requires client debug events. Every driver gets
# debug logging unless a scenario explicitly overrides it.
DRIVER_BASE_ENV = {"VEV_LOG": "debug"}


class DriverError(Exception):
    pass


class Driver:
    """One headless client with bounded reads and reliable termination."""

    def __init__(self, container, args, env=None):
        effective = dict(DRIVER_BASE_ENV)
        if env:
            effective.update(env)
        cmd = ["docker", "exec", "-i"]
        for key in sorted(effective):
            cmd += ["-e", f"{key}={effective[key]}"]
        cmd += [container, "vev", "--ui-driver",
                "--cols", COLS, "--rows", ROWS] + args
        # The environment must be forwarded into the container (-e); passing
        # it to the docker CLI process alone never reaches the driver.
        self.process = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        self.request_id = 0
        ready = self._read("discovery")
        self.attachment = ready["attachment"]
        # Ready never promises an attachment: the generation is zero until a
        # committed attachment publishes one, and the status may be any
        # presentation. Wait for the attached publication explicitly instead
        # of assuming ready.Generation == 1.
        self.generation = ready.get("generation", 0)
        if ready.get("status") != "attached":
            self.generation = self._await_attached()

    def _await_attached(self):
        """Wait for the committed attached publication and return its generation.

        ``capture`` works in every presentation state, so polling it is safe
        while the driver is still connecting. Only a committed ``attached``
        publication with a nonzero generation is returned; anything else is
        polled again until the bounded deadline.
        """
        deadline = time.monotonic() + INVENTORY_TIMEOUT_S
        while True:
            if time.monotonic() > deadline:
                self.close()
                raise DriverError("driver never published an attached generation")
            self.request_id += 1
            self.process.stdin.write(json.dumps(dict(
                version=1, id=self.request_id, op="capture",
                attachment=self.attachment)) + "\n")
            self.process.stdin.flush()
            context = self._read("attached publication").get("context", {})
            if context.get("status") == "attached" and context.get("generation"):
                return context["generation"]

    def _read(self, what):
        deadline = time.monotonic() + REQUEST_TIMEOUT_S
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.close()
                raise DriverError(f"timed out reading {what}")
            line = self.process.stdout.readline()
            if line == "":
                raise DriverError(f"driver EOF while reading {what}")
            line = line.strip()
            if not line:
                continue
            try:
                envelope = json.loads(line)
            except json.JSONDecodeError as exc:
                raise DriverError(f"invalid driver JSON while reading {what}: {exc}") from exc
            if "error" in envelope:
                raise DriverError(f"driver error while reading {what}: {envelope['error']}")
            return envelope["result"]

    def call(self, operation, **fields):
        self.request_id += 1
        request = dict(version=1, id=self.request_id, op=operation,
                       attachment=self.attachment, **fields)
        if operation in ("keys", "text"):
            request.setdefault("generation", self.generation)
        try:
            self.process.stdin.write(json.dumps(request) + "\n")
            self.process.stdin.flush()
        except BrokenPipeError as exc:
            raise DriverError(f"driver stdin closed for {operation}") from exc
        result = self._read(operation)
        context = result.get("context", {})
        if "generation" in context:
            self.generation = context["generation"]
        return result

    def text(self):
        return self.call("capture")["text"]

    def wait_text(self, fragment, timeout_ms=WAIT_TIMEOUT_MS):
        return self.call("wait", expect={"text_contains": fragment}, timeout_ms=timeout_ms)

    def wait_focus(self, tab_id, pane_id, timeout_ms=WAIT_TIMEOUT_MS):
        return self.call("wait", expect={"focus": {
            "tab_id": tab_id, "pane_id": pane_id,
        }}, timeout_ms=timeout_ms)

    def palette(self, query, label):
        keys_result = self.call("keys", keys=["Alt+Space"])
        if query:
            text_result = self.call("text", text=query)
            if text_result.get("status") != "processed":
                raise DriverError(
                    f"palette query not processed: {json.dumps(text_result)[:500]} "
                    f"keys={json.dumps(keys_result)[:500]}")
            # Prove the query reached the palette before Enter executes the
            # selected row: the label alone is visible in the unfiltered
            # list too, so waiting on it can return before filtering lands.
            self.wait_text("> " + query)
        self.wait_text(label)
        return self.text()

    def close(self):
        process, self.process = self.process, None
        if process is None:
            return
        try:
            process.stdin.close()
        except (BrokenPipeError, ValueError):
            pass
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def check(condition, message, diagnostics):
    if not condition:
        raise DriverError(f"{message}\n diagnostics: {json.dumps(diagnostics)[:2000]}")


def docker_capture(args, timeout=30):
    return subprocess.run(["docker", *args], capture_output=True, text=True,
                          timeout=timeout)


def log_size(container, filename):
    """Byte length of one state log, so a run can read only its own events."""
    script = (f'f="{STATE_LOG_DIR}/{filename}"; '
              'if [ -f "$f" ]; then wc -c < "$f"; else echo 0; fi')
    try:
        result = docker_capture(["exec", container, "sh", "-c", script])
    except subprocess.TimeoutExpired:
        return 0
    if result.returncode != 0:
        return 0
    try:
        return int(result.stdout.strip() or "0")
    except ValueError:
        return 0


def log_since(container, filename, offset):
    """Raw log bytes appended after offset; empty when the file is absent."""
    script = (f'f="{STATE_LOG_DIR}/{filename}"; '
              f'if [ -f "$f" ]; then tail -c +{offset + 1} "$f"; fi')
    try:
        result = docker_capture(["exec", container, "sh", "-c", script])
    except subprocess.TimeoutExpired:
        return ""
    return result.stdout if result.returncode == 0 else ""


def json_events(text):
    """Parse structured vev log lines, tolerating a truncated boundary line."""
    events = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict) and "msg" in event:
            events.append(event)
    return events


def count_events(events, message, **fields):
    return sum(1 for event in events
               if event.get("msg") == message
               and all(event.get(key) == value for key, value in fields.items()))


def write_artifact(name, payload):
    """Persist scenario evidence under the configured artifacts directory.

    The payload carries only committed contexts and structured log counts:
    no credentials, key material, or raw environment are recorded.
    """
    if not ARTIFACTS_DIR:
        return
    os.makedirs(ARTIFACTS_DIR, mode=0o700, exist_ok=True)
    path = os.path.join(ARTIFACTS_DIR, name)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def await_remote_inventory(container, session, timeout_s=INVENTORY_TIMEOUT_S, interval_s=0.5):
    """Wait until the local daemon publishes ``session`` for its remote host.

    The remote monitor observes each host on a ~15s healthy cadence, so a
    freshly created remote session is invisible to the palette until the next
    observation. Wait on the daemon's persisted catalog cache (rewritten right
    after a successful observation) rather than guessing a timeout, so the
    following navigation is deterministic and near-immediate in isolation.
    """
    script = (f'f="{STATE_LOG_DIR}/{REMOTE_CACHE_FILE}"; '
              f'grep -qF "{session}" "$f" 2>/dev/null')
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            if docker_capture(["exec", container, "sh", "-c", script]).returncode == 0:
                return
        except subprocess.TimeoutExpired:
            pass
        if time.monotonic() >= deadline:
            raise DriverError(
                f"remote session {session} never reached the local inventory "
                f"within {timeout_s}s")
        time.sleep(interval_s)


def switch_route(driver, name, origin, lifecycle_id=None, timeout_ms=WAIT_TIMEOUT_MS):
    """Switch by exact qualified palette label and prove the commit.

    A named target with a known lifecycle is additionally pinned with a
    ``wait`` on that exact lifecycle, so a stale or freshly-created session
    can never satisfy the check.
    """
    driver.palette(name, f"Switch to session {name}@{origin}")
    committed = driver.call("keys", keys=["Enter"])
    check(committed.get("status") == "processed",
          f"route switch to {name}@{origin} not processed", committed)
    session = committed.get("context", {}).get("session", {})
    check(session.get("session_name") == name,
          f"route switch did not commit {name}@{origin}", committed)
    if lifecycle_id is not None:
        driver.call("wait", expect={"session": {
            "session_name": name, "lifecycle_id": lifecycle_id,
        }}, timeout_ms=timeout_ms)
    return committed["context"]


def commit_output(driver, command, sentinel):
    """Type a command, run it, and wait for output the command text lacks.

    The sentinel must not appear contiguously in the typed command; callers
    build it with shell interpolation so an echoed command line can never
    satisfy the wait.
    """
    entered = driver.call("text", text=command)
    check(entered.get("status") == "processed", "command text not processed", entered)
    driver.call("keys", keys=["Enter"])
    driver.wait_text(sentinel)


def scenario_local_palette_cycle(client_container):
    """M1: local IPC palette open/action/close with committed observation."""
    suffix = uuid.uuid4().hex[:8]
    name = "local" + suffix
    driver = Driver(client_container, ["--session", name])
    try:
        capture = driver.call("capture")
        session = capture["context"]["session"]
        check(session["session_name"] == name, "local session name mismatch", capture)
        screen = driver.palette("", "Commands")
        check("Commands" in screen, "palette did not list commands", {"screen": screen[:500]})
        opened = driver.call("keys", keys=["Escape"])
        check(opened["context"]["session"] == session, "escape changed session", opened)
        entered = driver.call("text", text="printf 'LOCAL_OK'")
        check(entered.get("status") == "processed", "text action not processed", entered)
        driver.call("keys", keys=["Enter"])
        driver.wait_text("LOCAL_OK")
        final = driver.call("capture")
        check(final["context"]["session"] == session, "local lifecycle changed", final)
        print(f"PASS local-palette-cycle session={name} lifecycle={session['lifecycle_id'][:8]}")
    finally:
        driver.close()


def scenario_direct_remote_named(client_container, remote_container, env):
    """M2/M3: direct attach to a named exact remote target, committed output.

    The remote daemon owns a pre-created named session. ``--remote remote
    --session NAME`` attaches to that exact lifecycle instead of creating an
    ephemeral one, so the daemon-owned target is pinned, not inferred.
    """
    mode = env.get("VEV_REMOTE_TRANSPORT", "quic")
    suffix = uuid.uuid4().hex[:8]
    name = "direct" + suffix
    fixture = Driver(remote_container, ["--session", name])
    fixture_capture = fixture.call("capture")
    fixture_session = fixture_capture["context"]["session"]
    fixture.close()
    check(fixture_session["session_name"] == name,
          "remote fixture did not commit the named target", fixture_capture)
    driver = Driver(client_container, ["--remote", "remote", "--session", name], env=env)
    evidence = {"scenario": "direct-remote-named", "mode": mode,
                "remote_session": name, "fixture_session": fixture_session}
    try:
        capture = driver.call("capture")
        session = capture["context"]["session"]
        check(session["session_name"] == name,
              "named remote target not committed", capture)
        check(session == fixture_session,
              "direct attach did not reuse the exact named remote lifecycle", capture)
        # The sentinel is built by the shell so the echoed command line cannot
        # satisfy the wait before the command actually runs.
        commit_output(driver, "printf 'DIRECT_%s' OK", "DIRECT_OK")
        final = driver.call("capture")
        check(final["context"]["session"] == session, "remote lifecycle changed", final)
        evidence["remote_session_context"] = session
        evidence["final_context"] = final["context"]
        print(f"PASS direct-remote-named mode={mode} session={name} "
              f"lifecycle={session['lifecycle_id'][:8]}")
    finally:
        driver.close()
        write_artifact(f"direct-remote-named-{mode}.json", evidence)


def scenario_warm_reuse(client_container, remote_container, env):
    """Warm remote reuse across one client Runner.

    Journey: local -> named remote -> local -> same named remote. The remote
    attachment is expected to suspend on the way back to local and reactivate
    warm on the return: same committed remote lifecycle, retained pane output,
    and new committed output. No-redial evidence is deterministic: the client
    debug log records exactly one suspend and one warm activation (zero
    fallbacks), and the remote daemon records exactly one attach for the
    session, so a second bootstrap or dial would be visible. When the remote
    proxy debug log is available it must also show exactly one proxy start.
    """
    mode = env.get("VEV_REMOTE_TRANSPORT", "quic")
    suffix = uuid.uuid4().hex[:8]
    local_name = "locl" + suffix
    remote_name = "remt" + suffix
    fixture = Driver(remote_container, ["--session", remote_name])
    fixture_capture = fixture.call("capture")
    fixture_session = fixture_capture["context"]["session"]
    fixture.close()
    check(fixture_session["session_name"] == remote_name,
          "remote fixture did not commit the named target", fixture_capture)
    remote_lifecycle = fixture_session["lifecycle_id"]
    # Snapshot log positions after the fixture so only events from this client
    # journey are counted.
    client_offset = log_size(client_container, CLIENT_LOG)
    remote_offset = log_size(remote_container, DAEMON_LOG)
    stdio_offset = log_size(remote_container, STDIO_LOG)
    driver = Driver(client_container, ["--session", local_name], env=env)
    evidence = {"scenario": "warm-reuse", "mode": mode,
                "local_session": local_name, "remote_session": remote_name,
                "remote_lifecycle": remote_lifecycle,
                "fixture_session": fixture_session}
    try:
        initial = driver.call("capture")["context"]
        check(initial["session"]["session_name"] == local_name,
              "local session not committed", initial)
        local_lifecycle = initial["session"]["lifecycle_id"]
        # 1. local -> named exact remote.
        await_remote_inventory(client_container, remote_name)
        switch_route(driver, remote_name, "remote", remote_lifecycle)
        first_remote = driver.call("capture")["context"]
        check(first_remote["session"] == fixture_session,
              "first remote attach did not commit the exact named lifecycle", first_remote)
        evidence["first_remote"] = first_remote
        # Committed output while the remote pane is authoritative.
        commit_output(driver, "printf 'WARM_%s' PERSIST", "WARM_PERSIST")
        # 2. remote -> local: the remote attachment suspends and stays warm.
        switch_route(driver, local_name, "local", local_lifecycle)
        local_return = driver.call("capture")["context"]
        check(local_return["session"]["session_name"] == local_name,
              "return to local did not commit", local_return)
        evidence["local_return"] = local_return
        # 3. local -> same named remote: the retained transport reactivates.
        switch_route(driver, remote_name, "remote", remote_lifecycle)
        second_remote = driver.call("capture")["context"]
        check(second_remote["session"]["lifecycle_id"] == remote_lifecycle,
              "warm return changed the remote lifecycle", second_remote)
        check(second_remote["session"] == fixture_session,
              "warm return did not reuse the exact named target", second_remote)
        evidence["second_remote"] = second_remote
        # State and new committed output both survive the warm round trip.
        driver.wait_text("WARM_PERSIST")
        commit_output(driver, "printf 'WARM_%s' AFTER", "WARM_AFTER")
        after_output = driver.call("capture")["context"]
        check(after_output["session"]["lifecycle_id"] == remote_lifecycle,
              "lifecycle changed after warm committed output", after_output)
        evidence["after_warm_output"] = after_output
    finally:
        driver.close()
        client_events = json_events(log_since(client_container, CLIENT_LOG, client_offset))
        remote_events = json_events(log_since(remote_container, DAEMON_LOG, remote_offset))
        stdio_events = json_events(log_since(remote_container, STDIO_LOG, stdio_offset))
        evidence["log_counts"] = {
            "client_suspended": count_events(
                client_events, "remote attachment suspended",
                origin="remote", session=remote_name),
            "client_warm_activated": count_events(
                client_events, "warm remote attachment activated",
                origin="remote", session=remote_name, lifecycle=remote_lifecycle),
            "client_warm_failed": count_events(
                client_events, "warm remote activation failed",
                origin="remote", session=remote_name),
            "remote_client_attached": count_events(
                remote_events, "client attached", session=remote_name),
            "remote_proxy_started": (
                count_events(stdio_events, "stdio proxy starting") +
                count_events(stdio_events, "quic proxy starting")),
        }
        write_artifact(f"warm-reuse-{mode}.json", evidence)
    counts = evidence["log_counts"]
    check(counts["client_suspended"] == 1,
          "expected exactly one remote attachment suspension", counts)
    check(counts["client_warm_activated"] == 1,
          "expected exactly one warm remote activation", counts)
    check(counts["client_warm_failed"] == 0,
          "warm activation failed or fell back to a cold dial", counts)
    check(counts["remote_client_attached"] == 1,
          "remote daemon saw a second bootstrap/attach (redial)", counts)
    if counts["remote_proxy_started"]:
        check(counts["remote_proxy_started"] == 1,
              "remote proxy started more than once (redial)", counts)
    print(f"PASS warm-reuse mode={mode} local={local_name} remote={remote_name} "
          f"lifecycle={remote_lifecycle[:8]} "
          f"attaches={counts['remote_client_attached']}")


def picker_topology(env):
    """Topology for the client-picker scenario: which daemon serves it.

    Only the topologies whose serving attachment answers the palette
    session-picker command itself are applicable. A hybrid client that
    advertises the home-picker capability is answered with a navigation
    directive instead (the R1-preserved home-picker delegation), so its
    picker is served by the home route's overlay rather than by the
    client-owned interaction; hybrid coverage stays on
    ``hybrid-exact-return``.
    """
    topology = env.get("VEV_ACCEPTANCE_TOPOLOGY", "local")
    if topology not in ("local", "direct"):
        raise SystemExit(f"unknown topology {topology}; want local or direct")
    return topology


def open_client_picker(driver, session, timeout_ms=WAIT_TIMEOUT_MS):
    """Run the palette session-picker command and wait for the client frame.

    The client owns presentation, so the frame lists the serving daemon's
    sessions with no daemon title chrome. The opening action must be
    processed: it completes only against the daemon's committed output
    boundary, never from sending the key.
    """
    # The query is the command's exact palette code: commands are searched
    # by code, so the exact match ranks ahead of every fuzzy session row
    # and the selection stays on the session-picker command instead of
    # whichever session row the query text also matched. The label is the
    # rendered command row, not its name.
    driver.palette("SSP", "Open the session picker")
    entered = driver.call("keys", keys=["Enter"])
    check(entered.get("status") == "processed", "palette enter not processed", entered)
    driver.wait_text(session, timeout_ms=timeout_ms)
    return entered


def commit_target(driver, target, before):
    """Search the observed list down to one target, then commit it.

    Row order is never assumed: the search narrows the selectable set to
    rows matching the observed name, and the committed context is asserted
    against the intended target.
    """
    searched = driver.call("keys", keys=["/"])
    check(searched.get("accepted"), "picker search not admitted", searched)
    typed = driver.call("text", text=target)
    check(typed.get("accepted"), "picker search text not admitted", typed)
    driver.wait_text(target, timeout_ms=WAIT_TIMEOUT_MS)
    committed = driver.call("keys", keys=["Enter"])
    check(committed.get("status") == "processed", "picker commit not processed", committed)
    check(committed["context"]["session"]["session_name"] == target,
          "client-picker did not commit the observed target", committed)
    check(committed["context"]["session"] != before["session"],
          "commit did not change the committed session identity", committed)
    check(committed["context"]["focus"]["pane_id"] != "", "commit published no focus", committed)
    check(committed["context"]["status"] == "attached", "commit did not publish attached status", committed)
    return committed


def scenario_client_picker_navigate(client_container, remote_container, env):
    """P6.2: client-owned navigation picker end to end, plus its cancel path."""
    topology = picker_topology(env)
    mode = env.get("VEV_REMOTE_TRANSPORT", "quic")
    suffix = uuid.uuid4().hex[:8]
    first, second, entry = (name + suffix for name in ("picka", "pickb", "pickc"))
    # Direct mode is served by the remote daemon: the picker interaction
    # belongs to the serving attachment, so its rows are the remote
    # daemon's sessions. The attachment itself is a named remote session so
    # the scenario never races an ephemeral attach for its starting point.
    serving_container = remote_container if topology == "direct" else client_container
    fixture = Driver(serving_container, ["--session", second])
    fixture.close()
    if topology == "direct":
        entry_fixture = Driver(remote_container, ["--session", entry])
        entry_fixture.close()
        driver = Driver(client_container, ["--remote", "remote", "--session", entry], env=env)
    else:
        driver = Driver(client_container, ["--session", first], env=env)
    try:
        initial = driver.call("capture")["context"]
        want_initial = entry if topology == "direct" else first
        check(initial["session"]["session_name"] == want_initial, "initial session mismatch", initial)
        # Open, search to the observed target, commit. The opener's result
        # envelope carries the boundary reached when it completed, not the
        # session at admission, so only captures are compared.
        open_client_picker(driver, second)
        before_commit = driver.call("capture")["context"]
        committed = commit_target(driver, second, before_commit)
        # Cancel subcase: reopen the picker from the committed session and
        # leave it with Escape. The source route and screen come back
        # unchanged.
        before_cancel = driver.call("capture")["context"]
        cancelled = open_client_picker(driver, second)
        check(cancelled["context"]["session"] == committed["context"]["session"],
              "reopening the picker changed the session", cancelled)
        escape = driver.call("keys", keys=["Escape"])
        check(escape.get("status") == "processed", "picker cancel not processed", escape)
        after = driver.call("capture")
        check(after["context"]["session"] == before_cancel["session"], "cancel changed the session", after)
        check(after["context"]["generation"] == before_cancel["generation"], "cancel changed the generation", after)
        print(f"PASS client-picker-navigate topology={topology} mode={mode} target={second}")
    finally:
        driver.close()


def scenario_hybrid_exact_return(client_container, remote_container, env):
    """M4/M5: local A -> local B -> remote -> exact local A with committed action."""
    mode = env.get("VEV_REMOTE_TRANSPORT", "quic")
    suffix = uuid.uuid4().hex[:8]
    first, second, remote = (name + suffix for name in ("sample", "second", "other"))
    fixture = Driver(remote_container, ["--session", remote])
    fixture.close()
    driver = Driver(client_container, ["--session", first], env=env)
    try:
        initial = driver.call("capture")["context"]["session"]
        check(initial["session_name"] == first, "initial session mismatch", initial)
        fixture = Driver(client_container, ["--session", second], env=env)
        fixture.close()
        driver.palette(second, "Switch to session " + second + "@local")
        switched = driver.call("keys", keys=["Enter"])
        check(switched["context"]["session"]["session_name"] == second,
              "local switch did not commit", switched)
        # The local daemon refreshes remote inventory on its own cadence; wait
        # for the fixture session to be published before searching the palette.
        await_remote_inventory(client_container, remote)
        driver.palette(remote, "Switch to session " + remote + "@remote")
        entered = driver.call("keys", keys=["Enter"])
        check(entered["context"]["session"]["session_name"] == remote,
              "remote switch did not commit", entered)
        label = "Switch to session " + first + "@local"
        screen = driver.palette(first, label)
        check(screen.count(label) == 1, "expected exactly one client-qualified row", {"screen": screen[:800]})
        check("Switch to session " + first + " " not in screen, "unqualified row leaked", {"screen": screen[:800]})
        selected = driver.call("keys", keys=["Enter"])
        check(selected["context"]["session"] == initial, "exact local return failed", selected)
        print(f"PASS hybrid-exact-return mode={mode} first={first}")
    finally:
        driver.close()


SCENARIOS = {
    "local-palette-cycle": lambda client, remote, env: scenario_local_palette_cycle(client),
    "direct-remote-named": scenario_direct_remote_named,
    "warm-reuse": scenario_warm_reuse,
    "hybrid-exact-return": scenario_hybrid_exact_return,
    "client-picker-navigate": scenario_client_picker_navigate,
}


def main(argv):
    if len(argv) != 4:
        raise SystemExit("usage: acceptance.py CLIENT_CONTAINER REMOTE_CONTAINER SCENARIO[@stdio|@quic]")
    client_container, remote_container, spec = argv[1], argv[2], argv[3]
    name, _, transport = spec.partition("@")
    if name not in SCENARIOS:
        raise SystemExit(f"unknown scenario {name}; want one of {sorted(SCENARIOS)}")
    env = {}
    if transport == "stdio":
        env["VEV_REMOTE_TRANSPORT"] = "stdio"
    elif transport and transport != "quic":
        raise SystemExit(f"unknown transport {transport}; want stdio or quic")
    topology = os.environ.get("VEV_ACCEPTANCE_TOPOLOGY")
    if topology:
        env["VEV_ACCEPTANCE_TOPOLOGY"] = topology
    SCENARIOS[name](client_container, remote_container, env)


if __name__ == "__main__":
    main(sys.argv)
