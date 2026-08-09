#!/usr/bin/env python3
"""Minimal stdio MCP server exposing one permission-prompt tool.

Decision is read from $HORDE_PERM_DECISION ("allow" | "deny"). Every request and
response is appended as JSON to $HORDE_PERM_LOG. Stands in for the horde MCP
endpoint that the permission chain (DESIGN.md section 10) will terminate at.
"""
import json, os, sys, datetime, time

LOG = os.environ.get("HORDE_PERM_LOG", "/tmp/horde-perm.log")
DECISION = os.environ.get("HORDE_PERM_DECISION", "allow")
DENY_MSG = os.environ.get("HORDE_PERM_DENY_MSG", "Denied by horde permission chain (spike W1c)")
DELAY = float(os.environ.get("HORDE_PERM_DELAY", "0"))
MUTATE = os.environ.get("HORDE_PERM_MUTATE", "")

def log(kind, payload):
    with open(LOG, "a") as f:
        f.write(json.dumps({"ts": datetime.datetime.now().isoformat(), "kind": kind, "payload": payload}) + "\n")

TOOL = {
    "name": "permission_prompt",
    "description": "Horde permission chain terminus: approves or denies a proposed tool call.",
    "inputSchema": {
        "type": "object",
        "properties": {
            "tool_name": {"type": "string"},
            "input": {"type": "object"},
            "tool_use_id": {"type": "string"},
        },
        "required": ["tool_name", "input"],
    },
}

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
    log("send", obj)

def handle(req):
    method = req.get("method")
    rid = req.get("id")
    if method == "initialize":
        pv = req.get("params", {}).get("protocolVersion", "2025-06-18")
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "protocolVersion": pv,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "horde-perm-spike", "version": "0.1.0"},
        }}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": rid, "result": {"tools": [TOOL]}}
    if method == "tools/call":
        params = req.get("params", {})
        args = params.get("arguments", {})
        if DELAY:
            log("delay", {"seconds": DELAY})
            time.sleep(DELAY)
        if DECISION == "deny":
            decision = {"behavior": "deny", "message": DENY_MSG}
        else:
            new_input = dict(args.get("input", {}))
            if MUTATE:
                k, v = MUTATE.split("=", 1)
                new_input[k] = v
            decision = {"behavior": "allow", "updatedInput": new_input}
        log("decision", {"called_tool": params.get("name"), "args": args, "decision": decision})
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "content": [{"type": "text", "text": json.dumps(decision)}],
            "isError": False,
        }}
    if rid is None:
        return None  # notification
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"method not found: {method}"}}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
    except json.JSONDecodeError:
        continue
    log("recv", req)
    resp = handle(req)
    if resp is not None:
        send(resp)
