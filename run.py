#!/usr/bin/env python3
"""Render one-command launcher for the Flask gateway + Telegram bot.

Start everything with:
    python run.py

Render provides PORT. The launcher maps the Flask gateway to that port and
points the Telegram bot at the same local HTTP server.
"""
import os
import signal
import subprocess
import sys
import time
from urllib.request import urlopen

BASE = os.path.dirname(os.path.abspath(__file__))
APP = os.path.join(BASE, "app_fast.py")
BOT = os.path.join(BASE, "bot_fast.py")

children = []


def log(msg):
    print(f"[RUNNER] {msg}", flush=True)


def stop_all(*_):
    log("Stopping gateway + bot...")
    for p in reversed(children):
        if p.poll() is None:
            try:
                p.terminate()
            except Exception:
                pass
    deadline = time.time() + 8
    for p in reversed(children):
        if p.poll() is None:
            try:
                remaining = max(0.1, deadline - time.time())
                p.wait(timeout=remaining)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass
    raise SystemExit(0)


def gateway_ready(url, timeout=25):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urlopen(url + "/api/status", timeout=2) as r:
                return 200 <= r.status < 500
        except Exception:
            time.sleep(0.25)
    return False


def start(name, script, env):
    if not os.path.exists(script):
        raise FileNotFoundError(script)
    log(f"Starting {name}: {os.path.basename(script)}")
    p = subprocess.Popen(
        [sys.executable, "-u", script],
        cwd=BASE,
        env=env,
    )
    children.append(p)
    return p


def main():
    port = int(os.environ.get("PORT", os.environ.get("GATEWAY_PORT", "5000")))
    local_url = f"http://127.0.0.1:{port}"

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["GATEWAY_PORT"] = str(port)
    env["GATEWAY_URL"] = local_url

    signal.signal(signal.SIGTERM, stop_all)
    signal.signal(signal.SIGINT, stop_all)

    app_proc = start("Flask gateway", APP, env)
    log(f"Waiting for gateway on {local_url} ...")
    if not gateway_ready(local_url):
        log("Gateway did not become ready.")
        stop_all()

    bot_proc = start("Telegram bot", BOT, env)
    log(f"ALL SYSTEMS RUNNING — {local_url}")

    # Simple supervisor: if either process dies, restart it.
    while True:
        time.sleep(1)

        if app_proc.poll() is not None:
            log(f"Gateway exited with code {app_proc.returncode}; restarting...")
            app_proc = start("Flask gateway", APP, env)
            if not gateway_ready(local_url, timeout=25):
                log("Gateway restart failed; stopping bot and retrying.")
                if bot_proc.poll() is None:
                    bot_proc.terminate()
                time.sleep(2)
                continue

        if bot_proc.poll() is not None:
            log(f"Bot exited with code {bot_proc.returncode}; restarting...")
            bot_proc = start("Telegram bot", BOT, env)


if __name__ == "__main__":
    main()
