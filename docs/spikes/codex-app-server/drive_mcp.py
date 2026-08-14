#!/usr/bin/env python3
"""Capture Codex's MCP tool-call elicitation flow as golden transcripts.

The rmcp dual-era spike measured this interception point but its captures were
lost (docs/spikes/rmcp-dual-era.md, note at the top). This driver re-records it
against the installed codex-cli, with the stub MCP server in ``mcp_stub.py`` as
the upstream, so the codec has preserved bytes to be held to.

Usage:
    drive_mcp.py <transcript-name> <accept|decline> <granular|on-request|untrusted>

Records ``{"dir","t","msg"}`` envelopes (dir is the driver's socket view: "in"
is what came in to the driver, i.e. harness output — HarnessOutputKey::In).
The stub's own log is written next to the transcript as ``<name>.mcp-stub.jsonl``
so the capture proves whether any byte of the tool call reached the MCP server
before the elicitation was answered.
"""

import json
import pathlib
import select
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
TRANSCRIPTS = HERE / "transcripts"

GRANULAR = {
    "granular": {
        "mcp_elicitations": True,
        "rules": True,
        "sandbox_approval": True,
    }
}


class AppServer:
    def __init__(self, transcript_name, extra_args):
        TRANSCRIPTS.mkdir(parents=True, exist_ok=True)
        self.path = TRANSCRIPTS / f"{transcript_name}.jsonl"
        self.log = self.path.open("w")
        self.proc = subprocess.Popen(
            ["codex", "app-server", "--stdio"] + extra_args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self._next_id = 0

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


def await_response(srv, rid, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = srv.read(timeout=max(1, int(deadline - time.time())))
        if msg is None:
            return None
        if msg.get("id") == rid and ("result" in msg or "error" in msg):
            return msg
    return None


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        return 2
    name, decision, policy = sys.argv[1], sys.argv[2], sys.argv[3]

    sandbox = HERE / "sandbox"
    sandbox.mkdir(parents=True, exist_ok=True)
    stub_log = TRANSCRIPTS / f"{name}.mcp-stub.jsonl"
    stub_log.unlink(missing_ok=True)

    extra = [
        "-c", f'mcp_servers.spike.command="{sys.executable}"',
        "-c", f'mcp_servers.spike.args=["{HERE / "mcp_stub.py"}"]',
        "-c", f'mcp_servers.spike.env={{HORDE_MCP_STUB_LOG = "{stub_log}"}}',
    ]

    srv = AppServer(name, extra)
    answered = []
    try:
        rid = srv.request(
            "initialize",
            {
                "clientInfo": {"name": "horde-w5b", "version": "0.1.0"},
                "capabilities": {"experimentalApi": True},
            },
        )
        if await_response(srv, rid, timeout=30) is None:
            print("NO-INITIALIZE-RESPONSE")
            return 1
        srv.notify("initialized")

        rid = srv.request(
            "thread/start",
            {
                "cwd": str(sandbox),
                "approvalPolicy": GRANULAR if policy == "granular" else policy,
                "approvalsReviewer": "user",
                "sandbox": "read-only",
            },
        )
        started = await_response(srv, rid, timeout=60)
        result = (started or {}).get("result") or {}
        thread = result.get("threadId") or (result.get("thread") or {}).get("id")
        if not thread:
            print("NO-THREAD", json.dumps(started)[:400])
            return 1

        srv.request(
            "turn/start",
            {
                "threadId": thread,
                "input": [{
                    "type": "text",
                    "text": "Call the horde_probe tool from the spike MCP server "
                            "with token set to HORDE_MCP_OK, then report exactly "
                            "what it returned.",
                }],
            },
        )

        deadline = time.time() + 240
        while time.time() < deadline:
            msg = srv.read(timeout=30)
            if msg is None:
                continue
            method = msg.get("method")
            if method and "id" in msg:
                # A server->client request. The elicitation result field is
                # `action` (an MCP elicitation result); sandbox/exec approvals
                # use `decision`. Sending the wrong shape is a deserialization
                # error, which the rmcp spike measured.
                if "elicitation" in method:
                    result = ({"action": "accept", "content": {}}
                              if decision == "accept" else {"action": "decline"})
                else:
                    result = {"decision": decision}
                answered.append(method)
                srv.respond(msg["id"], result)
                continue
            if method == "turn/completed":
                print("TURN-COMPLETED answered:", json.dumps(answered))
                return 0
            if method == "error":
                print("ERROR-NOTIFICATION", json.dumps(msg.get("params"))[:400])
        print("TIMEOUT answered:", json.dumps(answered))
        return 1
    finally:
        srv.close()


if __name__ == "__main__":
    raise SystemExit(main())
