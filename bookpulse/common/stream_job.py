"""Find and stop the Structured Streaming job (streaming/stream_stats.py) inside the spark container.

Shared by scripts/control_server.py (buttons on the app page /setting), scripts/reset_streaming.py and
scripts/retrain.py, which all need "is the job running?" and "stop it cleanly, kill it if it does not stop".
The job may have been started by any of them or by `docker compose exec`, so it is found by its command line.
"""
import subprocess
import time

STREAM_JOB = "streaming/stream_stats.py"


def stream_running():
    return subprocess.run(["pgrep", "-f", STREAM_JOB], capture_output=True).returncode == 0


def stop_stream(wait_seconds=30):
    """SIGINT (the job stops its queries and writes its progress file), SIGKILL if it is still there after wait_seconds.

    Returns a short description of what happened, for logs and reports."""
    if not stream_running():
        return "was not running"
    subprocess.run(["pkill", "-INT", "-f", STREAM_JOB])
    for _ in range(wait_seconds):
        if not stream_running():
            return "stopped (SIGINT)"
        time.sleep(1)
    subprocess.run(["pkill", "-9", "-f", STREAM_JOB])
    time.sleep(1)
    return f"killed (SIGKILL after {wait_seconds} s)"
