#!/usr/bin/env python3
"""Table-driven UI-driver acceptance against disposable demo-image containers.

Each scenario starts real ``vev --ui-driver`` clients against real daemons
and asserts committed outcomes (action IDs, generation changes, exact
lifecycle/session/focus context, observed published output). Sending a key
or accepting a request is never success: every scenario ends on a
postcondition ``wait`` or a verified capture.

Transport selection passes through the fixture process environment:
``VEV_REMOTE_TRANSPORT`` unset means UDP, ``stdio`` means SSH stdio.
Direct-remote scenarios use ``--remote`` in a fresh client container with no
local daemon; they never create a local home session as setup.
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


class DriverError(Exception):
    pass


class Driver:
    """One headless client with bounded reads and reliable termination."""

    def __init__(self, container, args, env=None):
        cmd = ["docker", "exec", "-i", container, "vev", "--ui-driver",
               "--cols", COLS, "--rows", ROWS] + args
        process_env = None
        if env:
            process_env = dict(os.environ)
            process_env.update(env)
        self.process = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            text=True, env=process_env,
        )
        self.request_id = 0
        ready = self._read("discovery")
        self.attachment = ready["attachment"]
        self.generation = ready["generation"]

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


def scenario_direct_remote_ephemeral(client_container, remote_container, env):
    """M2/M3: direct remote attach with no local daemon, committed output."""
    mode = env.get("VEV_REMOTE_TRANSPORT", "udp")
    driver = Driver(client_container, ["--remote", "remote"], env=env)
    try:
        capture = driver.call("capture")
        session = capture["context"]["session"]
        entered = driver.call("text", text="printf 'DIRECT_OK'")
        check(entered.get("status") == "processed", "remote text action not processed", entered)
        driver.call("keys", keys=["Enter"])
        driver.wait_text("DIRECT_OK")
        final = driver.call("capture")
        check(final["context"]["session"] == session, "remote lifecycle changed", final)
        print(f"PASS direct-remote-ephemeral mode={mode} lifecycle={session['lifecycle_id'][:8]}")
    finally:
        driver.close()


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
    mode = env.get("VEV_REMOTE_TRANSPORT", "udp")
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
    mode = env.get("VEV_REMOTE_TRANSPORT", "udp")
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
    "direct-remote-ephemeral": scenario_direct_remote_ephemeral,
    "hybrid-exact-return": scenario_hybrid_exact_return,
    "client-picker-navigate": scenario_client_picker_navigate,
}


def main(argv):
    if len(argv) != 4:
        raise SystemExit("usage: acceptance.py CLIENT_CONTAINER REMOTE_CONTAINER SCENARIO[@stdio|@udp]")
    client_container, remote_container, spec = argv[1], argv[2], argv[3]
    name, _, transport = spec.partition("@")
    if name not in SCENARIOS:
        raise SystemExit(f"unknown scenario {name}; want one of {sorted(SCENARIOS)}")
    env = {}
    if transport == "stdio":
        env["VEV_REMOTE_TRANSPORT"] = "stdio"
    elif transport and transport != "udp":
        raise SystemExit(f"unknown transport {transport}; want stdio or udp")
    topology = os.environ.get("VEV_ACCEPTANCE_TOPOLOGY")
    if topology:
        env["VEV_ACCEPTANCE_TOPOLOGY"] = topology
    SCENARIOS[name](client_container, remote_container, env)


if __name__ == "__main__":
    main(sys.argv)
