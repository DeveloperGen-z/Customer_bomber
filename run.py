#!/usr/bin/env python3
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

BASE = Path(__file__).resolve().parent
PORT = int(os.environ.get("PORT", os.environ.get("GATEWAY_PORT", "5000")))
env = os.environ.copy()
env["GATEWAY_PORT"] = str(PORT)
env["GATEWAY_URL"] = f"http://127.0.0.1:{PORT}"
processes = []

def log(msg):
    print(f"[RUN] {msg}", flush=True)

def start(name, script):
    log(f"Starting {name}: {script}")
    p = subprocess.Popen([sys.executable, "-u", str(BASE / script)], cwd=str(BASE), env=env)
    processes.append((name, p))
    return p

def wait_for_gateway(timeout=60):
    url = f"http://127.0.0.1:{PORT}/api/status"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                if 200 <= r.status < 500:
                    log(f"Gateway ready on port {PORT}")
                    return True
        except Exception:
            pass
        time.sleep(0.25)
    return False

def stop_all(*_):
    log("Stopping services...")
    for name, p in processes:
        if p.poll() is None:
            log(f"Stopping {name}")
            try: p.terminate()
            except Exception: pass
    deadline = time.time() + 8
    while time.time() < deadline and any(p.poll() is None for _, p in processes):
        time.sleep(0.2)
    for name, p in processes:
        if p.poll() is None:
            try: p.kill()
            except Exception: pass
    raise SystemExit(0)

signal.signal(signal.SIGTERM, stop_all)
signal.signal(signal.SIGINT, stop_all)

def main():
    for filename in ("app.py", "gateway_bot.py"):
        if not (BASE / filename).exists():
            raise FileNotFoundError(f"{filename} not found")
    log(f"BASE={BASE}")
    log(f"PORT={PORT}")
    flask = start("Flask gateway", "app.py")
    if not wait_for_gateway():
        log("Gateway did not become ready within 60 seconds.")
        stop_all()
    bot = start("Telegram bot", "gateway_bot.py")
    log("Flask + Telegram bot are running in the same Render service.")
    while True:
        if flask.poll() is not None:
            log(f"Flask exited with code {flask.returncode}")
            stop_all()
        if bot.poll() is not None:
            log(f"Telegram bot exited with code {bot.returncode}")
            stop_all()
        time.sleep(1)

if __name__ == "__main__":
    main()
