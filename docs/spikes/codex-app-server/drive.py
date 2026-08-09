#!/usr/bin/env python3
"""Minimal driver for `codex app-server` — the W2c spike harness.

Speaks newline-delimited JSON-RPC over stdio, records every frame in both
directions to a JSONL transcript, and runs one named scenario per invocation.

Safety: threads are started read-only with approvalPolicy "untrusted", so the
model cannot touch the filesystem without an approval request we control, and
this driver denies every approval unless the scenario says otherwise.
"""

import json
import os
import pathlib
import select
import subprocess
import sys
import threading
import time

SCRATCH = pathlib.Path(__file__).resolve().parent
# Per-invocation feature overrides; never `codex features enable`, which would
# persist into the user's config.toml.
EXTRA_ARGS = []
TRANSCRIPTS = SCRATCH / "transcripts"


class AppServer:
    def __init__(self, transcript_name, cwd):
        TRANSCRIPTS.mkdir(parents=True, exist_ok=True)
        self.path = TRANSCRIPTS / f"{transcript_name}.jsonl"
        self.log = self.path.open("w")
        self.cwd = str(cwd)
        self.proc = subprocess.Popen(
            ["codex", "app-server", "--stdio"] + EXTRA_ARGS,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self._next_id = 0
        self._stderr = []
        threading.Thread(target=self._drain_stderr, daemon=True).start()

    def _drain_stderr(self):
        for line in self.proc.stderr:
            self._stderr.append(line.rstrip())

    def _record(self, direction, msg):
        self.log.write(json.dumps({"dir": direction, "t": round(time.time(), 3), "msg": msg}) + "\n")
        self.log.flush()

    def send(self, msg):
        self._record("out", msg)
        self.proc.stdin.write(json.dumps(msg) + "\n")
        self.proc.stdin.flush()

    def request(self, method, params):
        self._next_id += 1
        rid = self._next_id
        self.send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        return rid

    def notify(self, method, params=None):
        msg = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        self.send(msg)

    def respond(self, rid, result):
        self.send({"jsonrpc": "2.0", "id": rid, "result": result})

    def read(self, timeout=120):
        """Next frame, or None on timeout/EOF.

        select() on the pipe rather than a bare readline, so a stalled server
        surfaces as a timeout instead of hanging the harness forever.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            ready, _, _ = select.select([self.proc.stdout], [], [], max(0.1, deadline - time.time()))
            if not ready:
                return None
            line = self.proc.stdout.readline()
            if not line:
                return None
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                self._record("in-unparsed", line)
                continue
            self._record("in", msg)
            return msg
        return None

    def close(self):
        try:
            self.proc.terminate()
            self.proc.wait(timeout=5)
        except Exception:
            self.proc.kill()
        self.log.close()
        if self._stderr:
            (TRANSCRIPTS / f"{self.path.stem}.stderr.txt").write_text("\n".join(self._stderr))


def initialize(srv):
    rid = srv.request(
        "initialize",
        {
            "clientInfo": {"name": "horde-spike", "version": "0.1.0", "title": "Horde W2c spike"},
            "capabilities": {"experimentalApi": True},
        },
    )
    while True:
        msg = srv.read(timeout=30)
        if msg is None:
            raise SystemExit("no response to initialize")
        if msg.get("id") == rid:
            srv.notify("initialized")
            return msg


def pump(srv, until_methods, deny_approvals=True, timeout=180, on_request=None):
    """Read until one of `until_methods` notifications arrives.

    Answers server->client approval requests along the way, which is the whole
    point of the exercise.
    """
    seen = []
    approvals = []
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = srv.read(timeout=max(1, int(deadline - time.time())))
        if msg is None:
            break
        method = msg.get("method")
        if method:
            seen.append(method)
        # Server -> client request (has both id and method).
        if method and "id" in msg:
            approvals.append(method)
            if on_request:
                result = on_request(method, msg.get("params") or {})
            else:
                result = {"decision": "decline" if deny_approvals else "accept"}
            srv.respond(msg["id"], result)
            continue
        if method in until_methods:
            return seen, approvals, msg
    return seen, approvals, None


STATE = SCRATCH / "state.json"
_UNTRUSTED = SCRATCH / "untrusted_path.txt"
# A "trusted" project makes Codex skip approvals entirely, so the approval
# probes must run somewhere Codex has not recorded trust for.
SANDBOX = pathlib.Path(_UNTRUSTED.read_text().strip()) if _UNTRUSTED.exists() else SCRATCH / "sandbox"
SANDBOX_MODE = ["read-only"]


def load_state():
    return json.loads(STATE.read_text()) if STATE.exists() else {}


def save_state(**kw):
    s = load_state()
    s.update(kw)
    STATE.write_text(json.dumps(s, indent=1))


def await_response(srv, rid, timeout=60):
    """Read until the response to `rid`, answering nothing along the way."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = srv.read(timeout=max(1, int(deadline - time.time())))
        if msg is None:
            return None
        if msg.get("id") == rid and ("result" in msg or "error" in msg):
            return msg
    return None


GRANULAR = {
    "granular": {
        "mcp_elicitations": True,
        "rules": True,
        "sandbox_approval": True,
        "request_permissions": True,
        "skill_approval": True,
    }
}


def start_thread(srv, approval_policy="untrusted"):
    SANDBOX.mkdir(parents=True, exist_ok=True)
    (SANDBOX / "probe.txt").write_text("horde probe file\n")
    rid = srv.request(
        "thread/start",
        {
            "cwd": str(SANDBOX),
            "approvalPolicy": GRANULAR if approval_policy == "granular" else approval_policy,
            "approvalsReviewer": "user",
            "sandbox": SANDBOX_MODE[0],
        },
    )
    return await_response(srv, rid)


def scenario_initialize(args):
    srv = AppServer("01-initialize", SCRATCH)
    try:
        print(json.dumps(initialize(srv), indent=2)[:2000])
    finally:
        srv.close()


def scenario_freeprobes(args):
    """Everything answerable without spending a model turn."""
    srv = AppServer("02-free-probes", SCRATCH)
    try:
        initialize(srv)
        for method, params in [
            ("model/list", {}),
            ("account/rateLimits/read", {}),
            ("account/read", {}),
            ("mcpServerStatus/list", {}),
            ("permissionProfile/list", {}),
        ]:
            rid = srv.request(method, params)
            resp = await_response(srv, rid, timeout=30)
            body = json.dumps(resp)[:600] if resp else "(no response)"
            print(f"---- {method}\n{body}\n")
    finally:
        srv.close()


def scenario_turn(args):
    """A full headless turn: start a thread, send one turn, stream to completion."""
    srv = AppServer("03-basic-turn", SCRATCH)
    try:
        initialize(srv)
        started = start_thread(srv)
        thread = (started or {}).get("result", {})
        thread_id = thread.get("thread", {}).get("id") if isinstance(thread.get("thread"), dict) else thread.get("thread")
        print("thread/start ->", json.dumps(thread)[:500])
        save_state(thread_id=thread_id, model=thread.get("model"))

        srv.request(
            "turn/start",
            {"threadId": thread_id, "input": [{"type": "text", "text": "Reply with exactly the token HORDE_OK and nothing else."}]},
        )
        seen, approvals, final = pump(srv, {"turn/completed"}, timeout=180)
        print("notifications seen:", json.dumps(seen))
        print("approval requests:", json.dumps(approvals))
        print("turn/completed:", json.dumps(final)[:1500])
    finally:
        srv.close()


PROMPTS = {
    "echo": "Run the shell command: echo horde-approval-probe . Then tell me whether it ran.",
    # Writing must escape a read-only sandbox, which is where Codex escalates.
    "write": "Create a file named horde-escalation-probe.txt in the current directory containing the word ESCALATED. Use a shell command.",
    # Network is blocked in every Codex sandbox tier, so this is the escape
    # that must escalate rather than silently succeed.
    "net": "Run this exact shell command and report the output: curl -sS -o /dev/null -w '%{http_code}' https://example.com",
    # Writing OUTSIDE the workspace root is the sandbox escape workspace-write
    # cannot satisfy on its own, so this is where Codex must ask.
    "escape": "Run this exact shell command: printf ESCALATED > /private/tmp/horde-escape-probe.txt . Then report whether it succeeded.",
}


def scenario_approval(args):
    """The load-bearing one: can Horde intercept and decide a tool call?"""
    decision = (args[0] if args else "decline")
    if len(args) > 3:
        SANDBOX_MODE[0] = args[3]
    if len(args) > 4 and args[4] == "flags":
        EXTRA_ARGS.extend(["--enable", "exec_permission_approvals", "--enable", "request_permissions_tool"])
    srv = AppServer(f"05-approval-{decision}-{args[1] if len(args) > 1 else 'echo'}-{args[2] if len(args) > 2 else 'untrusted'}-{'flags' if EXTRA_ARGS else 'noflags'}", SCRATCH)
    captured = []

    def answer(method, params):
        captured.append({"method": method, "params": params})
        return {"decision": decision}

    try:
        initialize(srv)
        started = start_thread(srv, approval_policy=(args[2] if len(args) > 2 else "untrusted"))
        thread = (started or {}).get("result", {})
        thread_id = thread.get("thread", {}).get("id") if isinstance(thread.get("thread"), dict) else thread.get("thread")
        print("thread/start err:", json.dumps((started or {}).get("error"))[:300])
        srv.request(
            "turn/start",
            {
                "threadId": thread_id,
                "input": [{"type": "text", "text": PROMPTS[args[1] if len(args) > 1 else "echo"]}],
            },
        )
        seen, approvals, final = pump(srv, {"turn/completed"}, timeout=240, on_request=answer)
        print("approval requests intercepted:", json.dumps(approvals))
        for c in captured:
            print("  ->", json.dumps(c)[:900])
        print("notifications seen:", json.dumps(sorted(set(seen))))
        print("turn/completed:", json.dumps(final)[:1200])
    finally:
        srv.close()


def scenario_resume(args):
    """Does a thread survive process death with its context intact?"""
    thread_id = load_state().get("thread_id")
    if not thread_id:
        raise SystemExit("run the `turn` scenario first")
    srv = AppServer("05-resume", SCRATCH)
    try:
        initialize(srv)
        rid = srv.request("thread/resume", {"threadId": thread_id, "cwd": str(SANDBOX), "sandbox": "read-only"})
        resumed = await_response(srv, rid, timeout=60)
        print("thread/resume ->", json.dumps(resumed)[:600])
        srv.request(
            "turn/start",
            {"threadId": thread_id, "input": [{"type": "text", "text": "What exact token did I ask you to reply with a moment ago? Answer with just that token."}]},
        )
        seen, approvals, final = pump(srv, {"turn/completed"}, timeout=180)
        print("notifications seen:", json.dumps(sorted(set(seen))))
        print("turn/completed:", json.dumps(final)[:1200])
    finally:
        srv.close()


SCENARIOS = {
    "initialize": scenario_initialize,
    "freeprobes": scenario_freeprobes,
    "turn": scenario_turn,
    "approval": scenario_approval,
    "resume": scenario_resume,
}


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "initialize"
    SCENARIOS[name](sys.argv[2:])
