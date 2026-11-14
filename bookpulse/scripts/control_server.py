"""Small control server in the spark container, so the app page "Điều khiển demo" (/setting) can run the demo
commands from buttons instead of `docker compose exec spark ...`.

The app runs in another container and cannot start processes in this one, so docker-compose.yml starts this
server as the spark container's main process (it replaces `sleep infinity`; `docker compose exec spark ...`
works as before). It only runs the fixed commands in ACTIONS, with numeric parameters checked against a range;
it is reachable only on the compose network (port 8600 is not published to the host) and has no other API.

    GET  /status            -> {"stream_running": bool, "tasks": {name: {running, returncode, started, log}}}
    GET  /log/<name>        -> {"log": full output of the last run of <name>}
    POST /run/<name>        body: JSON parameters -> starts the command in the background
    POST /stop_stream       -> SIGINT to the stream job (SIGKILL after 30 s), as reset_streaming.py does
Output of each command goes to /tmp/bookpulse-control/<name>.log (inside the container, not in the repo).
"""
import json
import os
import random
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from common.fake import FAKE_CSV
from common.settings import ROOT
from common.stream_job import STREAM_JOB, stream_running
from common.stream_job import stop_stream as stop_stream_job

PORT = int(os.getenv("CONTROL_PORT", "8600"))
LOG_DIR = Path("/tmp/bookpulse-control")
PY = sys.executable


def num(params, key, default, lo, hi, cast=int):
    value = cast(params.get(key, default))
    if not lo <= value <= hi:
        raise ValueError(f"{key} must be between {lo} and {hi}")
    return str(value)


# name -> function(params) -> argv. Nothing outside this table can be run.
ACTIONS = {
    "start_stream": lambda p: [PY, STREAM_JOB],
    "reset": lambda p: [PY, "scripts/reset_streaming.py"],
    "rate_recs": lambda p: [PY, "scripts/rate_from_recs.py", "--user", num(p, "user", 1, 1, 10**7),
                            "--n", num(p, "n", 2, 1, 10), "--rating", num(p, "rating", 5, 1, 5)],
    "late": lambda p: [PY, "streaming/producer.py", "--file", str(FAKE_CSV),
                       "--limit", num(p, "limit", 900, 50, 5000), "--rate", num(p, "rate", 20, 5, 100),
                       "--late", num(p, "late", 0.05, 0.01, 0.5, float),
                       "--seed", num(p, "seed", random.randrange(10**6), 0, 10**9)],
    "kafka_audit": lambda p: [PY, "scripts/kafka_audit.py"],
    "nosql_evidence": lambda p: [PY, "scripts/nosql_evidence.py"],
    # the whole loop stream archive -> clean -> train ALS -> write_recs -> stream job back (about 3 minutes on 12 cores, 6-8 on 4)
    "retrain": lambda p: [PY, "scripts/retrain.py", "--user", num(p, "user", 1, 1, 10**7),
                          "--eval-users", num(p, "eval_users", 1000, 100, 10000)]
                         + (["--exclude-simulated"] if p.get("exclude_simulated") else []),
}
TASKS = {}  # name -> {"proc", "started", "log"}
LOCK = threading.Lock()


def tail(path, lines):
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return ""
    return "\n".join(text.splitlines()[-lines:]) if lines else text[-200_000:]


def task_state(name, lines=40):
    t = TASKS.get(name)
    if t is None:
        return None
    rc = t["proc"].poll()  # also reaps a finished child
    return {"running": rc is None, "returncode": rc, "started": t["started"],
            "elapsed": round(time.time() - t["started"]), "log": tail(t["log"], lines)}


def start(name, params):
    with LOCK:
        if name not in ACTIONS:
            return 404, {"error": f"unknown action {name}"}
        busy = [n for n in TASKS if task_state(n)["running"]]
        if name in busy:
            return 409, {"error": f"{name} is already running"}
        if name == "start_stream" and stream_running():
            return 409, {"error": "the stream job is already running"}
        if name == "reset" and [n for n in busy if n != "start_stream"]:
            return 409, {"error": f"wait until {', '.join(busy)} has finished"}
        # retrain stops and restarts the stream job itself and needs the RAM: nothing else may run meanwhile
        if name == "retrain" and [n for n in busy if n != "start_stream"]:
            return 409, {"error": f"wait until {', '.join(n for n in busy if n != 'start_stream')} has finished"}
        if "retrain" in busy and name != "retrain":
            return 409, {"error": "retrain is running (about 3 minutes); wait until it has finished"}
        try:
            argv = ACTIONS[name](params)
        except (TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log = LOG_DIR / f"{name}.log"
        with open(log, "w", encoding="utf-8") as out:
            out.write("$ python " + " ".join(argv[1:]) + "\n")
            out.flush()
            proc = subprocess.Popen(argv, cwd=ROOT, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                    start_new_session=True)
        TASKS[name] = {"proc": proc, "started": time.time(), "log": log}
        return 200, {"started": name, "pid": proc.pid}


def stop_stream():
    if not stream_running():
        return 409, {"error": "the stream job is not running"}

    threading.Thread(target=stop_stream_job, daemon=True).start()
    return 200, {"stopping": True}


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/status":
            with LOCK:
                tasks = {n: task_state(n) for n in TASKS}
            self.reply(200, {"stream_running": stream_running(), "tasks": tasks})
        elif self.path.startswith("/log/"):
            state = task_state(self.path[len("/log/"):], lines=0)
            self.reply(200 if state else 404, {"log": state["log"] if state else ""})
        else:
            self.reply(404, {"error": "not found"})

    def do_POST(self):
        size = int(self.headers.get("Content-Length") or 0)
        try:
            params = json.loads(self.rfile.read(size) or b"{}")
        except json.JSONDecodeError:
            return self.reply(400, {"error": "body must be JSON"})
        if self.path == "/stop_stream":
            self.reply(*stop_stream())
        elif self.path.startswith("/run/"):
            self.reply(*start(self.path[len("/run/"):], params if isinstance(params, dict) else {}))
        else:
            self.reply(404, {"error": "not found"})

    def log_message(self, *args):  # the app polls /status every few seconds: keep the container log quiet
        pass


if __name__ == "__main__":
    print(f"BookPulse control server on :{PORT} (actions: {', '.join(ACTIONS)})", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
