#!/usr/bin/env python3
"""W12c probe driver: claude under `--permission-prompt-tool stdio`.

Spawns claude in stream-json mode with the stdio permission route, sends N user
messages on one stdin, answers every `control_request` the CLI writes to stdout,
and records both directions to JSONL in the transcripts' `{"dir","t","msg"}`
envelope.

Policy, by environment:

- `PROBE_DECISION` — `allow` (default), `deny`, `mutate`, `remember`
- `PROBE_MUTATE`   — `key=value` written into `updatedInput` under `mutate`
- `PROBE_ANSWER`   — the answer given to every AskUserQuestion question
                     (default: the first option's label)
- `PROBE_INIT`     — `1` sends the SDK's `initialize` control request first
- `PROBE_DELAY`    — seconds to stall before answering a permission request
- `DRIVE_TIMEOUT`  — seconds to wait for each turn's result (default 180)

Everything after the prompts is passed through to the CLI.
"""
import json, os, subprocess, sys, threading, time


def main():
    out_path = sys.argv[1]
    n_turns = int(sys.argv[2])
    prompts = sys.argv[3:3 + n_turns]
    extra = sys.argv[3 + n_turns:]

    cmd = ["claude", "-p", "--input-format", "stream-json", "--output-format", "stream-json",
           "--verbose", "--model", "haiku", "--setting-sources", "", "--strict-mcp-config",
           "--permission-prompt-tool", "stdio"] + extra
    print("CMD:", " ".join(repr(c) for c in cmd), file=sys.stderr)

    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True, bufsize=1)
    fout = open(out_path, "w")
    lock = threading.Lock()
    results = []
    done = threading.Event()

    def record(direction, msg):
        with lock:
            fout.write(json.dumps({"dir": direction, "t": round(time.time(), 3), "msg": msg}) + "\n")
            fout.flush()

    def send(msg):
        record("in", msg)
        p.stdin.write(json.dumps(msg) + "\n")
        p.stdin.flush()

    def answer(req):
        request_id = req["request_id"]
        body = req.get("request", {})
        subtype = body.get("subtype")
        if subtype != "can_use_tool":
            send({"type": "control_response", "response": {
                "subtype": "error", "request_id": request_id,
                "error": f"probe does not handle {subtype}"}})
            return
        delay = float(os.environ.get("PROBE_DELAY", "0"))
        if delay:
            time.sleep(delay)
        tool = body.get("tool_name")
        tool_input = body.get("input", {})
        decision = os.environ.get("PROBE_DECISION", "allow")
        if tool == "AskUserQuestion":
            answers = {}
            for q in tool_input.get("questions", []):
                default = (q.get("options") or [{"label": "yes"}])[0].get("label")
                answers[q.get("question")] = os.environ.get("PROBE_ANSWER", default)
            response = {"behavior": "allow",
                        "updatedInput": {**tool_input, "answers": answers}}
        elif decision == "deny":
            response = {"behavior": "deny", "message": "denied by the W12c probe"}
        elif decision == "mutate":
            key, _, value = os.environ.get("PROBE_MUTATE", "content=REWRITTEN").partition("=")
            response = {"behavior": "allow", "updatedInput": {**tool_input, key: value}}
        elif decision == "remember":
            response = {"behavior": "allow", "updatedInput": tool_input,
                        "updatedPermissions": [{"type": "addRules", "rules": [{"toolName": tool}],
                                                "behavior": "allow", "destination": "session"}]}
        else:
            response = {"behavior": "allow", "updatedInput": tool_input}
        send({"type": "control_response", "response": {
            "subtype": "success", "request_id": request_id, "response": response}})

    def reader():
        for line in p.stdout:
            line = line.rstrip("\n")
            if not line:
                continue
            m = json.loads(line)
            record("out", m)
            if m.get("type") == "control_request":
                threading.Thread(target=answer, args=(m,), daemon=True).start()
            if m.get("type") == "result":
                results.append(m)
                done.set()
        done.set()

    th = threading.Thread(target=reader, daemon=True)
    th.start()

    if os.environ.get("PROBE_INIT") == "1":
        send({"type": "control_request", "request_id": "probe-init",
              "request": {"subtype": "initialize", "hooks": None}})
        time.sleep(2)

    for i, prompt in enumerate(prompts):
        done.clear()
        send({"type": "user", "message": {"role": "user",
                                          "content": [{"type": "text", "text": prompt}]}})
        if not done.wait(timeout=int(os.environ.get("DRIVE_TIMEOUT", "180"))):
            print(f"TIMEOUT waiting for result of turn {i}", file=sys.stderr)
            break
        print(f"turn {i} -> result: {json.dumps(results[-1].get('result'))[:200]}", file=sys.stderr)

    p.stdin.close()
    try:
        p.wait(timeout=30)
    except subprocess.TimeoutExpired:
        print("process did NOT exit after stdin close; killing", file=sys.stderr)
        p.kill()
    err = p.stderr.read()
    if err.strip():
        print("STDERR:", err, file=sys.stderr)
    print("EXIT CODE:", p.returncode, file=sys.stderr)
    print("RESULTS:", len(results), file=sys.stderr)
    fout.close()


main()
