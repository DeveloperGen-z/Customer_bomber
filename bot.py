#!/usr/bin/env python3
"""
Gateway Bot — pro Telegram console for your SMS gateway.

Built on pyTelegramBotAPI (telebot) + your points / referral /
force-subscribe framework, wired to the Flask gateway over HTTP.

WHY HTTP instead of importing app.py:
    Flask aur bot alag processes hote hain. Agar bot app.py import kare to
    dono ke paas apna alag TRACKER hota aur live progress kabhi update nahi
    hota. HTTP se dono ko EK hi source of truth milta hai — wahi numbers jo
    website dikhati hai.
"""
import json
import os
import re
import shutil
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import requests
from requests.adapters import HTTPAdapter

import telebot
from telebot import types

# ═══════════════════════════════════════════════════════════════
#  CONFIG — sirf BOT_TOKEN aur ADMIN_IDS environment se aate hain.
#  Baaki sab niche hardcoded hai, isliye export karne ki zaroorat nahi.
# ═══════════════════════════════════════════════════════════════
TOKEN = os.environ.get('BOT_TOKEN', '').strip()
ADMIN_IDS = {int(x) for x in os.environ.get('ADMIN_IDS', '').replace(' ', '').split(',')
             if x.strip().lstrip('-').isdigit()}

GATEWAY_URL = 'http://127.0.0.1:5000'
REQUIRED_CHANNELS = ['earnflowspidy']          # single channel

DB_FILE = 'users_db.json'
START_POINTS = 20
REFERRAL_BONUS = 5
COST_PER_MSG = 1        # 1 message = 1 point
COST_RETRY = 2
COOLDOWN = 25

MAX_COUNT = 2000        # ek request me max kitne messages

BAR, EMPTY, BAR_LEN = '▰', '▱', 14
PHONE_RE = re.compile(r'^\+?[1-9]\d{6,14}$')
TICK = 0.8

PENDING = {}      # chat_id -> {'stage': 'phone'|'message', 'phone': str, 'msg': str}
WATCHING = {}     # chat_id -> job_id
JOIN_CACHE = {}   # user_id -> (ts, ok)   — Telegram rate limits bachane ke liye
BLAST_LOCK = threading.Lock()
HTTP_LOCAL = threading.local()

bot = telebot.TeleBot(TOKEN)


def log(msg=''):
    print(f'[{time.strftime("%H:%M:%S")}] {msg}', flush=True)


def esc(s):
    return (str(s if s is not None else '')
            .replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'))


# ═══════════════════════════════════════════════════════════════
#  GATEWAY HTTP CLIENT  (single source of truth = Flask process)
# ═══════════════════════════════════════════════════════════════
class GatewayError(Exception):
    pass


def _http_session():
    session = getattr(HTTP_LOCAL, 'session', None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=0)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({'Accept': 'application/json', 'Connection': 'keep-alive'})
        HTTP_LOCAL.session = session
    return session

def api(path, payload=None, timeout=30):
    url = GATEWAY_URL + path
    try:
        session = _http_session()
        if payload is None:
            r = session.get(url, timeout=timeout)
        else:
            r = session.post(url, json=payload, timeout=timeout)
        raw = r.text
        try:
            body = r.json()
        except Exception:
            body = {'ok': False, 'error': raw[:160]}
        if r.status_code >= 400:
            return body if isinstance(body, dict) else {'ok': False, 'error': f'HTTP {r.status_code}'}
        return body if isinstance(body, dict) else {'ok': False, 'error': 'Invalid gateway response'}
    except requests.RequestException as e:
        raise GatewayError('Gateway server se connect nahi hua.\n'
                           '`python3 app.py` chal rahi hai?') from e
    except Exception as e:
        raise GatewayError(str(e))


def is_admin(uid):
    return bool(ADMIN_IDS) and uid in ADMIN_IDS


def valid_phone(v):
    clean = str(v or '').strip()
    for ch in ' -()':
        clean = clean.replace(ch, '')
    return bool(PHONE_RE.match(clean))


def clamp_count(v, default=1):
    """Kitne messages — 1..MAX_COUNT. Kuch bhi aaye safe."""
    try:
        n = int(float(str(v).strip().rstrip('x×').strip()))
    except (TypeError, ValueError):
        return default
    return max(1, min(MAX_COUNT, n))


def blast_cost(count):
    """1 message = 1 point. 55 messages = 55 points."""
    return max(1, int(count or 1)) * COST_PER_MSG


def pool_capacity():
    """Poore pool me kitne SIM cards hain — 1 SIM = 1 message."""
    try:
        d = api('/api/devices', timeout=12)
        return len(d.get('devices') or []) * 2
    except Exception:
        return 0


def bar(fraction):
    filled = int(BAR_LEN * max(0.0, min(1.0, fraction)))
    return BAR * filled + EMPTY * (BAR_LEN - filled)


# ═══════════════════════════════════════════════════════════════
#  DATABASE  (atomic + auto backup)
# ═══════════════════════════════════════════════════════════════
def load_db():
    if not os.path.exists(DB_FILE):
        return {}
    try:
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        shutil.copy(DB_FILE, DB_FILE + '.corrupt')
        log(f'!! DB corrupt — {DB_FILE}.corrupt me backup')
        return {}


def save_db(db):
    if os.path.exists(DB_FILE):
        shutil.copy(DB_FILE, DB_FILE + '.bak')
    tmp = DB_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(db, f, indent=2, ensure_ascii=False)
    os.replace(tmp, DB_FILE)


def new_user(uid, name='', username=''):
    return {'points': START_POINTS, 'name': name, 'username': username,
            'joined': int(time.time()), 'ref': '', 'referred': 0,
            'spent': 0, 'earned': START_POINTS, 'cooldown': 0, 'history': []}


def get_user(uid, name='', username=''):
    db = load_db()
    if str(uid) not in db:
        db[str(uid)] = new_user(uid, name, username)
        save_db(db)
    return db[str(uid)]


def add_points(uid, amount, reason):
    db = load_db()
    u = db.get(str(uid))
    if not u:
        return
    u['points'] = max(0, u['points'] + amount)
    if amount > 0:
        u['earned'] = u.get('earned', 0) + amount
    u.setdefault('history', []).append(
        {'t': int(time.time()), 'd': reason, 'x': amount})
    u['history'] = u['history'][-50:]
    save_db(db)


def spend(uid, amount, reason):
    """Points katte hain. False = balance nahi tha."""
    db = load_db()
    u = db.get(str(uid))
    if not u or u.get('points', 0) < amount:
        return False
    u['points'] -= amount
    u['spent'] = u.get('spent', 0) + amount
    u.setdefault('history', []).append(
        {'t': int(time.time()), 'd': reason, 'x': -amount})
    u['history'] = u['history'][-50:]
    save_db(db)
    return True


def set_cooldown(uid):
    db = load_db()
    u = db.get(str(uid))
    if u:
        u['cooldown'] = int(time.time())
        save_db(db)


def cooldown_left(uid):
    u = get_user(uid)
    return max(0, int(u.get('cooldown', 0) + COOLDOWN - time.time()))


# ═══════════════════════════════════════════════════════════════
#  FORCE SUBSCRIBE  (60s cache — Telegram rate limit se bachao)
# ═══════════════════════════════════════════════════════════════
MEMBER_STATUSES = ('member', 'administrator', 'creator', 'owner')


def join_prompt_kb(miss):
    kb = types.InlineKeyboardMarkup(row_width=1)
    for ch in miss:
        clean_ch = ch.replace('@', '')
        kb.add(types.InlineKeyboardButton(f'📢 Join @{clean_ch}', url=f'https://t.me/{clean_ch}'))
    kb.add(types.InlineKeyboardButton('✅ I joined — check again',
                                      callback_data='recheck'))
    return kb


def missing_channels(user_id):
    """Sirf TABHI block karo jab Telegram CONFIRM kare ki user member nahi hai."""
    if is_admin(user_id):
        return []
    cached = JOIN_CACHE.get(user_id)
    if cached and time.time() - cached[0] < 60:
        return [] if cached[1] else list(REQUIRED_CHANNELS)
    for ch in REQUIRED_CHANNELS:
        try:
            # FIX 1: API needs @ to verify channels properly
            target_ch = ch if ch.startswith('@') else f'@{ch}'
            st = bot.get_chat_member(target_ch, user_id).status
        except Exception as e:
            log(f'! join-check unavailable for @{ch}: {type(e).__name__}: {e}')
            JOIN_CACHE[user_id] = (time.time(), True)
            return []
        if st not in MEMBER_STATUSES:
            JOIN_CACHE[user_id] = (time.time(), False)
            return list(REQUIRED_CHANNELS)
    JOIN_CACHE[user_id] = (time.time(), True)
    return []


def join_gate(message):
    """Missing channel ho to message rok do. True = aage badho."""
    miss = missing_channels(message.chat.id)
    if not miss:
        return True
    bot.send_message(message.chat.id,
                     '🚨 <b>Bot use karne ke liye channel join karein</b>',
                     parse_mode='HTML', reply_markup=join_prompt_kb(miss))
    return False


def main_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(types.KeyboardButton('📡 Send Message'),
           types.KeyboardButton('📊 Live Status'))
    kb.add(types.KeyboardButton('💎 My Points'),
           types.KeyboardButton('🔗 Referral Link'))
    kb.add(types.KeyboardButton('ℹ️ Help'))
    return kb


def help_text():
    return (
        '<b>Gateway Bot</b> — pro SMS console\n\n'
        f'💎 New user ko <b>{START_POINTS}</b> points\n'
        f'👥 Referral pe <b>+{REFERRAL_BONUS}</b> points\n'
        f'✉️ <b>{COST_PER_MSG} point = 1 message</b>\n'
        f'📶 1 SIM = 1 message · auto queue supported\n\n'
        f'<b>Commands</b>\n'
        f'<code>/status</code>     gateway + last blast\n'
        f'<code>/send</code>       blast bhejo\n'
        f'<code>/stats</code>      REAL live delivery progress\n'
        f'<code>/failed</code>     kaun fail hua\n'
        f'<code>/retry</code>      sirf fail hue (<b>{COST_RETRY}</b> pts)\n'
        f'<code>/history</code>    pichle blasts\n'
        f'<code>/balance</code>    points + cooldown\n'
        f'<code>/referral</code>   apna link\n\n'
        f'<i>Send: number → message → kitne message.\n'
        f'1 SIM = 1 message. Pool se zyada maanga to bacha hua QUEUE me '
        f'chalta hai.\n'
        f'Har blast par {COOLDOWN}s cooldown.</i>'
    )


# ═══════════════════════════════════════════════════════════════
#  RENDERERS
# ═══════════════════════════════════════════════════════════════
def live_text(j):
    requested = j.get('requested', j.get('total', 0))
    capacity = j.get('capacity', '?')
    dispatched = j.get('dispatched', 0)
    percent = j.get('percent') or 0
    queued = j.get('queued') or 0
    return (
        '⚡ <b>BLAST LIVE</b>\n\n'
        f'{bar(percent / 100)}  <b>{percent}%</b>\n\n'
        f'🎯 Requested  <b>{requested}</b>\n'
        f'📡 SIM Pool   <b>{capacity}</b>\n'
        f'🚀 Dispatched <b>{dispatched}</b>\n'
        f'✅ Delivered  <b>{j.get("sent", 0)}</b> / {requested}\n'
        f'❌ Failed     <b>{j.get("failed", 0)}</b>\n'
        f'📊 In flight  <b>{j.get("remaining", 0)}</b>\n'
        f'⏳ Queue      <b>{queued}</b>\n'
        f'⚡ Speed      <b>{j.get("rate", 0)}/s</b>\n'
        f'⏱ Elapsed    <b>{j.get("elapsed", 0)}s</b>'
    )


def done_text(j, title):
    return (
        f'{title}\n\n'
        f'To          <code>{esc(j["to"])}</code>\n'
        f'Maanga      <b>{j.get("requested", j["total"])}</b> · '
        f'SIM pool <b>{j.get("capacity", "?")}</b>\n'
        f'✅ Delivered <b>{j["sent"]}</b> / {j["total"]}\n'
        f'❌ Failed    <b>{j["failed"]}</b>\n'
        f'📈 Success   <b>{j["success"] if j.get("success") is not None else "—"}%</b>\n'
        f'⏱ {j["elapsed"]}s'
    )


def failure_text(j):
    bad = [d for d in j.get('devices', []) if d.get('failed')]
    if not bad:
        return '✅ Is blast me <b>koi failure nahi</b>.'
    out = [f'❌ <b>Failures</b> — {len(bad)} device(s), '
           f'{sum(d["failed"] for d in bad)} message(s)', '']
    for d in bad:
        out.append(f'{"🟢" if d["status"] else "🔴"} <b>{esc(d["name"])}</b>  '
                   f'<code>{esc(d["id"][:18])}</code>')
        batt = d.get('batteryPercent')
        out.append(f'    ❌ {d["failed"]} failed · ✅ {d["sent"]} sent · '
                   f'🔋 {batt if batt is not None else "—"}%')
    out += ['', '<i>Offline devices har blast me poora fail hote hain. '
                'Unke workers band karo, phir /retry se dobara bhejo.</i>']
    return '\n'.join(out)


def devices_text(devices):
    if not devices:
        return '📭 <b>No devices</b>\n\nKoi gateway online nahi.'
    online = sum(1 for d in devices if d.get('status'))
    out = [f'📡 <b>Device pool</b> — {len(devices)} total · 🟢 {online} online', '']
    for d in devices[:25]:
        batt = d.get('batteryPercent')
        out += [f'{"🟢" if d["status"] else "🔴"} <b>{esc(d["name"])}</b>',
                f'    <code>{esc(d["id"][:18])}</code> · {esc(d.get("model", "—"))}',
                f'    📞 {esc(d.get("phoneNumber", "—"))} · '
                f'{"🔋 %s%%" % batt if batt is not None else "🔋 —"}', '']
    if len(devices) > 25:
        out.append(f'…aur {len(devices) - 25} devices')
    return '\n'.join(out)


# ═══════════════════════════════════════════════════════════════
#  START / ACCOUNT
# ═══════════════════════════════════════════════════════════════
@bot.message_handler(commands=['start'])
def cmd_start(message):
    uid = message.chat.id
    get_user(uid, message.from_user.first_name or '',
             message.from_user.username or '')

    parts = (message.text or '').split()
    if len(parts) > 1 and parts[1].isdigit() and parts[1] != str(uid):
        db = load_db()
        inviter = parts[1]
        if inviter in db and db[inviter].get('ref') != str(uid):
            db[inviter]['referred'] = db[inviter].get('referred', 0) + 1
            me = db[str(uid)]
            if not me.get('ref'):
                me['ref'] = inviter
            save_db(db)
            add_points(inviter, REFERRAL_BONUS, 'referral')
            try:
                bot.send_message(inviter,
                                 f'🎉 <b>Referral Success!</b>\n\n'
                                 f'+{REFERRAL_BONUS} points aa gaye.',
                                 parse_mode='HTML')
            except Exception:
                pass

    if not join_gate(message):
        return
    u = get_user(uid)
    bot.send_message(uid,
                     f'👋 <b>Welcome to Gateway Bot</b>\n\n'
                     f'💎 Balance: <b>{u["points"]}</b> points\n'
                     f'✉️ <b>{COST_PER_MSG}</b> point = 1 message\n\n'
                     + help_text(),
                     parse_mode='HTML', reply_markup=main_menu())


@bot.message_handler(commands=['help'])
def cmd_help(message):
    if not join_gate(message):
        return
    bot.send_message(message.chat.id, help_text(), parse_mode='HTML',
                     reply_markup=main_menu())


@bot.message_handler(commands=['balance', 'points'])
def cmd_balance(message):
    if not join_gate(message):
        return
    u = get_user(message.chat.id)
    left = cooldown_left(message.chat.id)
    cd = f'\n⏳ Cooldown: <b>{left}s</b>' if left else '\n✅ Blast ready'
    bot.send_message(message.chat.id,
                     f'💎 <b>{u["points"]}</b> points\n\n'
                     f'✉️ Messages bhej sakte ho: <b>{u["points"]}</b>\n'
                     f'📤 Earned <b>{u.get("earned", 0)}</b> · '
                     f'📥 Spent <b>{u.get("spent", 0)}</b>\n'
                     f'👥 Referred <b>{u.get("referred", 0)}</b>' + cd,
                     parse_mode='HTML', reply_markup=main_menu())


@bot.message_handler(commands=['referral'])
def cmd_referral(message):
    uid = message.chat.id
    link = f'https://t.me/{bot.get_me().username}?start={uid}'
    bot.send_message(uid,
                     f'🚀 <b>Your referral link</b>\n\n'
                     f'<code>{link}</code>\n\n'
                     f'1 referral = <b>+{REFERRAL_BONUS}</b> points\n'
                     f'{COST_PER_MSG} point = 1 message',
                     parse_mode='HTML', reply_markup=main_menu())


@bot.message_handler(commands=['top'])
def cmd_top(message):
    db = load_db()
    rows = sorted(db.items(), key=lambda kv: kv[1].get('points', 0), reverse=True)[:10]
    if not rows:
        bot.send_message(message.chat.id, 'Abhi koi user nahi.')
        return
    out = ['🏆 <b>Top 10</b>', '']
    for i, (uid, u) in enumerate(rows, 1):
        out.append(f'{i}. <code>{esc(uid)}</code> — <b>{u.get("points", 0)}</b> pts')
    bot.send_message(message.chat.id, '\n'.join(out), parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════
#  GATEWAY COMMANDS
# ═══════════════════════════════════════════════════════════════
@bot.message_handler(commands=['status'])
def cmd_status(message):
    if not join_gate(message):
        return
    try:
        st = api('/api/status', timeout=8)
        dev = api('/api/devices', timeout=12)
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}', parse_mode='HTML')
        return

    if not st.get('configured'):
        bot.send_message(message.chat.id,
                         '🟠 Gateway online, Firebase connected nahi.\n\n'
                         '<code>/connect https://proj.firebaseio.com SECRET</code>',
                         parse_mode='HTML')
        return

    devices = dev.get('devices') or []
    online = sum(1 for d in devices if d.get('status'))
    lines = ['🟢 <b>Gateway online</b>', '',
             'Firebase  ✅ connected',
             f'Devices   <b>{len(devices)}</b>',
             f'Online    <b>{online}</b>',
             f'Workers   <b>{st.get("workers", "?")}</b>']
    job = (api('/api/delivery', timeout=8) or {}).get('job')
    if job and job.get('total'):
        lines += ['', f'Last blast: ✅ <b>{job["sent"]}/{job["total"]}</b> · '
                      f'❌ <b>{job["failed"]}</b> · {job["elapsed"]}s']
        if job.get('failed_tasks'):
            lines.append(f'Retryable: <b>{job["failed_tasks"]}</b> → /retry')
    bot.send_message(message.chat.id, '\n'.join(lines), parse_mode='HTML',
                     reply_markup=main_menu())


@bot.message_handler(commands=['connect'])
def cmd_connect(message):
    if not is_admin(message.chat.id):
        bot.send_message(message.chat.id, '⛔ Admin only.')
        return
    parts = (message.text or '').split()
    if len(parts) < 3:
        bot.send_message(message.chat.id,
                         'Usage:\n<code>/connect https://proj.firebaseio.com SECRET</code>')
        return
    try:
        res = api('/api/firebase',
                  {'url': parts[1], 'key': ' '.join(parts[2:])}, timeout=20)
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}', parse_mode='HTML')
        return
    if res.get('ok'):
        bot.send_message(message.chat.id, '✅ Firebase connected')
    else:
        bot.send_message(message.chat.id,
                         f'❌ {esc(res.get("error", "failed"))}', parse_mode='HTML')


@bot.message_handler(commands=['devices'])
def cmd_devices(message):
    if not join_gate(message):
        return
    try:
        res = api('/api/devices', timeout=15)
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}', parse_mode='HTML')
        return
    if not res.get('ok'):
        bot.send_message(message.chat.id, f'❌ {esc(res.get("error", "failed"))}',
                         parse_mode='HTML')
        return
    bot.send_message(message.chat.id, devices_text(res['devices']), parse_mode='HTML',
                     reply_markup=main_menu())


@bot.message_handler(commands=['stats'])
def cmd_stats(message):
    if not join_gate(message):
        return
    try:
        job = (api('/api/delivery', timeout=10) or {}).get('job')
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}', parse_mode='HTML')
        return
    if not job or not job.get('total'):
        bot.send_message(message.chat.id, 'ℹ️ Abhi koi blast nahi hua.')
        return
    if job['running']:
        bot.send_message(message.chat.id, live_text(job), parse_mode='HTML')
        return
    title = ('⚠️ <b>INTERRUPTED</b>' if job.get('interrupted')
             else '✅ <b>COMPLETE</b>' if not job['failed'] and job['sent']
             else '⚠️ <b>DONE WITH ERRORS</b>')
    kb = None
    if job.get('failed_tasks'):
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton(
            f'🔁 Retry {job["failed_tasks"]} failed', callback_data='retry_all'))
    bot.send_message(message.chat.id, done_text(job, title),
                     parse_mode='HTML', reply_markup=kb)


@bot.message_handler(commands=['failed'])
def cmd_failed(message):
    if not join_gate(message):
        return
    try:
        job = (api('/api/delivery', timeout=10) or {}).get('job')
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}', parse_mode='HTML')
        return
    if not job or not job.get('total'):
        bot.send_message(message.chat.id, 'ℹ️ Abhi koi blast nahi hua.')
        return
    bot.send_message(message.chat.id, failure_text(job), parse_mode='HTML')


@bot.message_handler(commands=['history'])
def cmd_history(message):
    if not join_gate(message):
        return
    try:
        res = api('/api/delivery', timeout=10)
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}', parse_mode='HTML')
        return
    jobs = res.get('jobs') or []
    job = res.get('job')
    if not jobs:
        bot.send_message(message.chat.id, 'ℹ️ Koi blast history nahi.')
        return
    out = [f'🗂 <b>Last {len(jobs)} blast(s)</b>', '']
    for jid in jobs[-8:]:
        if jid == (job or {}).get('id'):
            out.append(f'• <code>{jid}</code> — ✅ {job["sent"]}/{job["total"]} · '
                       f'❌ {job["failed"]}')
        else:
            out.append(f'• <code>{jid}</code>')
    bot.send_message(message.chat.id, '\n'.join(out), parse_mode='HTML')


@bot.message_handler(commands=['retry'])
def cmd_retry(message):
    if not join_gate(message):
        return
    u = get_user(message.chat.id)
    if u['points'] < COST_RETRY:
        bot.send_message(message.chat.id,
                         f'❌ <b>Not enough points!</b>\n\n'
                         f'Chahiye: {COST_RETRY} · Balance: {u["points"]}\n'
                         f'Referral se free points kamao 👉 /referral',
                         parse_mode='HTML')
        return
    only = (message.text or '').split()
    only = only[1] if len(only) > 1 else None
    try:
        res = api('/api/message-retry', {'only': only}, timeout=20)
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}', parse_mode='HTML')
        return
    if not res.get('ok'):
        bot.send_message(message.chat.id, f'❌ {esc(res.get("error", "failed"))}',
                         parse_mode='HTML')
        return
    spend(message.chat.id, COST_RETRY, f'retry {res["retried"]}')
    set_cooldown(message.chat.id)
    log(f'RETRY by {message.chat.id}: {res["retried"]} messages, job={res["job_id"]}')
    sent = bot.send_message(message.chat.id,
                            f'🔁 <b>RETRY STARTED</b>\n\n'
                            f'Devices  <b>{res["devices_count"]}</b>\n'
                            f'Retrying <b>{res["retried"]}</b> message(s)\n\n'
                            f'💎 -{COST_RETRY} points',
                            parse_mode='HTML')
    threading.Thread(target=watch, args=(message.chat.id, sent.message_id,
                                         res['job_id']), daemon=True).start()


# ═══════════════════════════════════════════════════════════════
#  SEND FLOW
# ═══════════════════════════════════════════════════════════════
def plan_text(count, balance=None):
    cost = blast_cost(count)
    lines = [
        f'💎 Cost: <b>{cost}</b> points',
        f'📨 {count} message(s) = <b>{cost}</b> point(s)',
        '✨ Benefit: quantity send karte hi auto-send + live tracking',
    ]
    if balance is not None:
        lines.append(f'💰 Balance: <b>{balance}</b> · Max: <b>{balance // COST_PER_MSG}</b>')
    return '\n'.join(lines)


def start_blast(uid, to, msg, count):
    """Quantity receive hote hi direct API call; no Ready/Blast button."""
    count = clamp_count(count)
    cost = blast_cost(count)
    u = get_user(uid)

    if not valid_phone(to):
        bot.send_message(uid, '❌ Number valid nahi.')
        return False
    if not msg:
        bot.send_message(uid, '❌ Message empty nahi ho sakta.')
        return False
    if cost > u.get('points', 0):
        bot.send_message(uid,
                         f'❌ <b>Points kam hain</b>\n\n'
                         f'Chahiye: <b>{cost}</b> · Balance: <b>{u.get("points", 0)}</b>',
                         parse_mode='HTML')
        return False
    left = cooldown_left(uid)
    if left:
        bot.send_message(uid, f'⏳ Abhi <b>{left}s</b> cooldown baaki hai.', parse_mode='HTML')
        return False
    # Same user ke liye ek hi active job — live message overwrite nahi hoga.
    if uid in WATCHING:
        bot.send_message(uid, '⚠️ Aapka previous blast abhi live hai. Pehle uska result complete hone dein.')
        return False
    if not BLAST_LOCK.acquire(blocking=False):
        bot.send_message(uid, '⚠️ Gateway abhi busy hai. Thodi der baad try karein.')
        return False

    # Immediate visual feedback; this is NOT a confirmation step.
    live_msg = bot.send_message(
        uid,
        '⚡ <b>STARTING BLAST…</b>\n\n'
        f'🎯 Requested <b>{count}</b>\n'
        f'💎 Cost <b>{cost}</b> points\n'
        f'📨 Message <code>{esc(msg[:120])}</code>\n\n'
        '<i>Gateway se live status connect ho raha hai…</i>',
        parse_mode='HTML')

    try:
        res = api('/api/message',
                  {'to': to, 'message': msg, 'count': count}, timeout=30)
    except Exception as e:
        BLAST_LOCK.release()
        try:
            bot.edit_message_text(
                f'❌ <b>SEND FAILED</b>\n\n{esc(str(e))}',
                chat_id=uid, message_id=live_msg.message_id, parse_mode='HTML')
        except Exception:
            pass
        return False

    if not res.get('ok'):
        BLAST_LOCK.release()
        try:
            bot.edit_message_text(
                f'❌ <b>SEND FAILED</b>\n\n{esc(res.get("error", "failed"))}',
                chat_id=uid, message_id=live_msg.message_id, parse_mode='HTML')
        except Exception:
            pass
        return False

    spend(uid, cost, f'send {count} to {to}')
    set_cooldown(uid)
    PENDING.pop(uid, None)
    WATCHING[uid] = res['job_id']
    log(f'BLAST by {uid}: to={to} count={count} cost={cost} '
        f'capacity={res.get("capacity")} dispatched={res.get("dispatched")} '
        f'queued={res.get("queued")} job={res["job_id"]}')

    threading.Thread(target=watch,
                     args=(uid, live_msg.message_id, res['job_id']), daemon=True).start()
    return True


def ask_count(uid):
    u = get_user(uid)
    balance = u.get('points', 0)
    bot.send_message(uid,
        '🔢 <b>Kitne message bhejne hain?</b>\n\n'
        + plan_text(1, balance) + '\n\n'
        + '<i>Bas quantity type karo — 1, 2, 3, 10…\n'
          'Send karte hi direct blast start ho jayega. Koi confirmation button nahi.</i>',
        parse_mode='HTML')


@bot.message_handler(commands=['send'])
def cmd_send(message):
    if not join_gate(message):
        return
    PENDING[message.chat.id] = {'stage': 'phone'}
    bot.send_message(message.chat.id,
        '📱 <b>Recipient number</b> bhejo…\n\n'
        '<i>Example: +91960742919</i>\n\n'
        f'💎 <b>{COST_PER_MSG}</b> point = 1 message', parse_mode='HTML')


@bot.message_handler(func=lambda m: m.text == '📡 Send Message')
def menu_send(message):
    cmd_send(message)


@bot.message_handler(func=lambda m: m.text == '💎 My Points')
def menu_points(message):
    cmd_balance(message)


@bot.message_handler(func=lambda m: m.text == '🔗 Referral Link')
def menu_ref(message):
    cmd_referral(message)


@bot.message_handler(func=lambda m: m.text == '📊 Live Status')
def menu_stats(message):
    cmd_stats(message)


@bot.message_handler(func=lambda m: m.text == 'ℹ️ Help')
def menu_help(message):
    cmd_help(message)


@bot.message_handler(func=lambda m: True)
def handle_text(message):
    """Guided flow: number -> message -> quantity -> automatic blast."""
    text = (message.text or '').strip()
    if not text or text.startswith('/'):
        return
    st = PENDING.get(message.chat.id)
    if not st:
        if message.from_user and message.from_user.is_bot:
            return
        bot.send_message(message.chat.id, help_text(), parse_mode='HTML', reply_markup=main_menu())
        return
    if not join_gate(message):
        return
    if st['stage'] == 'phone':
        if not valid_phone(text):
            bot.send_message(message.chat.id, '❌ Number valid nahi. Example: <code>+91960742919</code>', parse_mode='HTML')
            return
        PENDING[message.chat.id] = {'stage': 'message', 'phone': text}
        bot.send_message(message.chat.id, f'✅ <code>{esc(text)}</code>\n\nAb message likh do…', parse_mode='HTML')
        return
    if st['stage'] == 'message':
        if len(text) > 500:
            bot.send_message(message.chat.id, '❌ Message 500 characters se chhota hona chahiye.')
            return
        PENDING[message.chat.id] = {'stage': 'count', 'phone': st['phone'], 'msg': text}
        ask_count(message.chat.id)
        return
    if st['stage'] == 'count':
        if not text.isdigit():
            bot.send_message(message.chat.id, '❌ Sirf quantity number type karo. Example: <code>5</code>', parse_mode='HTML')
            return
        start_blast(message.chat.id, st['phone'], st['msg'], clamp_count(text))
        return
    PENDING.pop(message.chat.id, None)


# ═══════════════════════════════════════════════════════════════
#  FIXED CALLBACK HANDLERS (Errors 1 & 2 resolved below)
# ═══════════════════════════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data == 'recheck')
def cb_recheck(call):
    # FIX 2: Swapped answer_call_query with answer_callback_query
    JOIN_CACHE.pop(call.from_user.id, None)
    uid = call.message.chat.id
    mid = call.message.message_id
    miss = missing_channels(uid)
    
    if not miss:
        bot.answer_callback_query(call.id, '✅ Verified')
        try:
            bot.edit_message_text(
                '✅ <b>Verified!</b>\n\nAb bot poora use kar sakte ho 👇',
                uid, mid, parse_mode='HTML')
        except Exception:
            pass
        return
        
    bot.answer_callback_query(call.id, 'Abhi bhi nahi dikh raha')
    try:
        bot.edit_message_reply_markup(uid, mid, reply_markup=join_prompt_kb(miss))
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
#  LIVE TRACKING (separate thread)
# ═══════════════════════════════════════════════════════════════
def watch(chat_id, message_id, job_id):
    """Website-style live tracker: same Telegram message gets updated until done."""
    try:
        last_text = None
        for _ in range(600):
            try:
                job = (api(f'/api/delivery?job={job_id}', timeout=10) or {}).get('job')
            except GatewayError:
                time.sleep(2)
                continue
            if not job:
                time.sleep(TICK)
                continue

            text_now = live_text(job)
            if text_now != last_text and not job.get('complete') and not job.get('interrupted'):
                try:
                    bot.edit_message_text(text_now, chat_id=chat_id, message_id=message_id,
                                          parse_mode='HTML')
                    last_text = text_now
                except Exception:
                    pass

            if job.get('complete') or job.get('interrupted'):
                break
            time.sleep(TICK)

        job = (api(f'/api/delivery?job={job_id}', timeout=10) or {}).get('job')
        if not job:
            return

        if job.get('interrupted'):
            title = '⚠️ <b>BLAST INTERRUPTED</b>'
        elif not job.get('failed') and job.get('sent'):
            title = '✅ <b>SUCCESSFULLY SENT</b>'
        elif job.get('sent'):
            title = '⚠️ <b>SENT WITH ERRORS</b>'
        else:
            title = '❌ <b>SEND FAILED</b>'

        text = done_text(job, title)
        # Final summary me exact request / SIM capacity / dispatch bhi rahe.
        text += (
            '\n\n'
            f'🎯 Requested <b>{job.get("requested", job.get("total", 0))}</b> · '
            f'📡 SIM Pool <b>{job.get("capacity", "?")}</b> · '
            f'🚀 Dispatched <b>{job.get("dispatched", 0)}</b>'
        )

        bad = [d for d in job.get('devices', []) if d.get('failed')]
        if bad:
            text += '\n\n❌ Failed: ' + ', '.join(esc(d['name']) for d in bad[:5])

        kb = None
        if job.get('failed_tasks'):
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton(
                f'🔁 Retry {job["failed_tasks"]} failed ({COST_RETRY} pts)',
                callback_data='retry_all'))
            kb.add(types.InlineKeyboardButton('📊 Failures', callback_data='show_failed'))

        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=message_id,
                                  parse_mode='HTML', reply_markup=kb)
        except Exception:
            bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=kb)
    finally:
        WATCHING.pop(chat_id, None)
        BLAST_LOCK.release()


@bot.callback_query_handler(func=lambda c: c.data == 'retry_all')
def cb_retry(call):
    uid = call.message.chat.id
    mid = call.message.message_id
    u = get_user(uid)
    if u.get('points', 0) < COST_RETRY:
        bot.answer_callback_query(call.id, '❌ Not enough points')
        return
    left = cooldown_left(uid)
    if left:
        bot.answer_callback_query(call.id, f'⏳ {left}s cooldown')
        return
    if not BLAST_LOCK.acquire(blocking=False):
        bot.answer_callback_query(call.id, '⚠️ Ek blast already chal raha hai')
        return
    try:
        bot.answer_callback_query(call.id, '🔁 Retry started')
        bot.edit_message_text('🔁 <b>RETRY STARTED</b>', chat_id=uid, message_id=mid, parse_mode='HTML')
        res = api('/api/message-retry', {'only': None}, timeout=30)
    except Exception as e:
        BLAST_LOCK.release()
        bot.edit_message_text(f'❌ {esc(str(e))}', chat_id=uid, message_id=mid, parse_mode='HTML')
        return
    if not res.get('ok'):
        BLAST_LOCK.release()
        bot.edit_message_text(f'❌ {esc(res.get("error", "failed"))}', chat_id=uid, message_id=mid, parse_mode='HTML')
        return
    spend(uid, COST_RETRY, f'retry {res.get("retried", 0)}')
    set_cooldown(uid)
    WATCHING[uid] = res['job_id']
    log(f'RETRY by {uid}: {res.get("retried", 0)} messages, job={res["job_id"]}')
    threading.Thread(target=watch, args=(uid, mid, res['job_id']), daemon=True).start()


@bot.callback_query_handler(func=lambda c: c.data == 'show_failed')
def cb_show_failed(call):
    # FIX 2: Swapped answer_call_query with answer_callback_query
    bot.answer_callback_query(call.id)
    try:
        job = (api('/api/delivery', timeout=10) or {}).get('job')
    except GatewayError as e:
        bot.send_message(call.message.chat.id, f'❌ {esc(str(e))}', parse_mode='HTML')
        return
    bot.send_message(call.message.chat.id, failure_text(job) if job else '—', parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════
#  BOOT
# ═══════════════════════════════════════════════════════════════
bot.set_my_commands([
    types.BotCommand('start', 'Menu + referral'),
    types.BotCommand('balance', 'Points + cooldown'),
    types.BotCommand('referral', 'Apna link'),
    types.BotCommand('status', 'Gateway + last blast'),
    types.BotCommand('send', 'Blast bhejo'),
    types.BotCommand('stats', 'Live delivery progress'),
    types.BotCommand('failed', 'Kaun fail hua'),
    types.BotCommand('retry', 'Sirf fail hue dobara'),
    types.BotCommand('history', 'Pichle blasts'),
    types.BotCommand('top', 'Leaderboard'),
    types.BotCommand('help', 'All commands'),
])


def main():
    if not TOKEN:
        log('❌ BOT_TOKEN missing.\n   export BOT_TOKEN="123456:ABC-your-botfather-token"')
        raise SystemExit(1)
    log(f'🤖 Gateway Bot v3  →  {GATEWAY_URL}')
    log('   channels: ' + (', '.join('@' + c for c in REQUIRED_CHANNELS) or 'none'))
    log(f'   points: start {START_POINTS} · refer +{REFERRAL_BONUS} · '
        f'{COST_PER_MSG}/message · retry -{COST_RETRY}')
    log(f'   admins: {sorted(ADMIN_IDS) or "none (only public commands)"}')

    probe = api('/api/status', timeout=6)
    log('✅ Gateway reachable' if isinstance(probe, dict) else '⚠️ Gateway odd reply')

    try:
        bot.infinity_polling(skip_pending=True)
    except TypeError:
        bot.infinity_polling()


if __name__ == '__main__':
    main()
