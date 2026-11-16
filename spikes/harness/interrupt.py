#!/usr/bin/env python3
"""Interrupt probe: start a long turn, fire a control_request interrupt mid-flight,
then try to keep using the SAME process, and report the session id for resume testing."""
import json, subprocess, sys, threading, time, os

out_path = sys.argv[1]
mode = sys.argv[2]  # control | stdin-close | sigint

cmd = ["claude", "-p", "--input-format", "stream-json", "--output-format", "stream-json",
       "--verbose", "--model", "haiku", "--setting-sources", "", "--strict-mcp-config",
       "--tools", ""]
p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE, text=True, bufsize=1)
fout = open(out_path, "w")
state = {"session_id": None, "saw_assistant": False, "results": []}
first_assistant = threading.Event()
got_result = threading.Event()

def rec(direction, msg):
    fout.write(json.dumps({"dir": direction, "t": round(time.time(), 3), "msg": msg}) + "\n")
    fout.flush()

def reader():
    for line in p.stdout:
        line = line.strip()
        if not line:
            continue
        m = json.loads(line)
        rec("out", m)
        if m.get("session_id"):
            state["session_id"] = m["session_id"]
        if m.get("type") == "assistant" and not state["saw_assistant"]:
            state["saw_assistant"] = True
            first_assistant.set()
        if m.get("type") == "result":
            state["results"].append(m)
            got_result.set()
    print("[reader] stdout closed", file=sys.stderr)

threading.Thread(target=reader, daemon=True).start()

def send(msg):
    rec("in", msg)
    p.stdin.write(json.dumps(msg) + "\n")
    p.stdin.flush()

long_prompt = os.environ.get("INTERRUPT_PROMPT", "Count from 1 to 300. Print each number on its own line, "
               "with the word 'tick' after it. Do not stop early.")
send({"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": long_prompt}]}})

first_assistant.wait(timeout=120)
time.sleep(float(os.environ.get("INTERRUPT_DELAY", "2.0")))  # let some output accumulate
print(f"[probe] first assistant seen; firing interrupt via {mode}", file=sys.stderr)

if mode == "control":
    send({"type": "control_request", "request_id": "horde-int-1", "request": {"subtype": "interrupt"}})
elif mode == "stdin-close":
    p.stdin.close()
elif mode == "sigint":
    p.send_signal(2)

# See whether a result arrives after the interrupt
if got_result.wait(timeout=90):
    r = state["results"][-1]
    print(f"[probe] result after interrupt: subtype={r.get('subtype')} is_error={r.get('is_error')} "
          f"stop_reason={r.get('stop_reason')} terminal_reason={r.get('terminal_reason')}", file=sys.stderr)
else:
    print("[probe] NO result event within 90s of interrupt", file=sys.stderr)

# Can we keep using the same process?
if mode != "stdin-close":
    print("[probe] attempting follow-up turn on SAME process", file=sys.stderr)
    got_result.clear()
    try:
        send({"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": "Reply with exactly: STILL_ALIVE"}]}})
        if got_result.wait(timeout=90):
            print(f"[probe] follow-up result: {json.dumps(state['results'][-1].get('result'))[:120]}", file=sys.stderr)
        else:
            print("[probe] follow-up produced NO result", file=sys.stderr)
    except Exception as e:
        print(f"[probe] follow-up send failed: {e!r}", file=sys.stderr)
    try:
        p.stdin.close()
    except Exception:
        pass

try:
    p.wait(timeout=30)
except subprocess.TimeoutExpired:
    print("[probe] process did not exit; killing", file=sys.stderr)
    p.kill()
print("[probe] exit code:", p.returncode, file=sys.stderr)
print("[probe] SESSION_ID:", state["session_id"], file=sys.stderr)
err = p.stderr.read()
if err.strip():
    print("[probe] STDERR:", err[:2000], file=sys.stderr)
fout.close()
