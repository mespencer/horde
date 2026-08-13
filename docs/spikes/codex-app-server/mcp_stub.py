#!/usr/bin/env python3
"""A minimal stdio MCP server, for capturing Codex's MCP tool-call elicitation.

Serves exactly one tool, ``horde_probe``, which echoes its ``token`` argument.
Every request and response is appended to the file named by
``HORDE_MCP_STUB_LOG`` so a capture can prove whether a byte of the tool call
reached the server before Horde answered the elicitation (the load-bearing
property of the interception point — see docs/adr/0005 amendment).

Speaks MCP 2025-06-18-era JSON-RPC over stdio: initialize, notifications,
tools/list, tools/call. Nothing else is needed to make codex-cli call a tool.
"""

import json
import sys
import os
import time


def log(direction, msg):
    path = os.environ.get("HORDE_MCP_STUB_LOG")
    if not path:
        return
    with open(path, "a") as f:
        f.write(json.dumps({"dir": direction, "t": round(time.time(), 3), "msg": msg}) + "\n")


def reply(rid, result):
    msg = {"jsonrpc": "2.0", "id": rid, "result": result}
    log("out", msg)
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        log("in", msg)
        method = msg.get("method")
        rid = msg.get("id")
        if method == "initialize":
            reply(rid, {
                "protocolVersion": msg.get("params", {}).get("protocolVersion", "2025-06-18"),
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "horde-mcp-stub", "version": "0.1.0"},
            })
        elif method == "tools/list":
            reply(rid, {"tools": [{
                "name": "horde_probe",
                "description": "Echoes the token argument back, prefixed with PROBE:.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"token": {"type": "string"}},
                    "required": ["token"],
                },
            }]})
        elif method == "tools/call":
            token = (msg.get("params", {}).get("arguments") or {}).get("token", "")
            reply(rid, {"content": [{"type": "text", "text": f"PROBE:{token}"}],
                        "isError": False})
        elif rid is not None:
            reply(rid, {})
        # Notifications need no reply.


if __name__ == "__main__":
    main()
