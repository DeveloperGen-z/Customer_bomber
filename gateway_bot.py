#!/usr/bin/env python3
"""
Gateway Bot — Premium Telegram console for your SMS gateway.
Smart Parallel Dispatch · Hinglish HUD · Full Admin Toolkit · Streak Rewards
"""
import json
import os
import re
import shutil
import threading
import time

import requests
from requests.adapters import HTTPAdapter

import telebot
from telebot import types

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════
TOKEN = os.environ.get('BOT_TOKEN', '').strip()
ADMIN_IDS = {int(x) for x in os.environ.get('ADMIN_IDS', '').replace(' ', '').split(',')
             if x.strip().lstrip('-').isdigit()}

GATEWAY_URL = os.environ.get('GATEWAY_URL', 'http://127.0.0.1:5000').rstrip('/')
REQUIRED_CHANNELS = ['earnflowspidy']

DB_FILE = 'users_db.json'
START_POINTS = 20
REFERRAL_BONUS = 5
COST_PER_MSG = 1
COST_RETRY = 2
DAILY_BONUS = 5
MAX_STREAK_BONUS = 20

COOLDOWN = 3
TICK = 0.8
MAX_COUNT = 2000

BAR, EMPTY, BAR_LEN = '▰', '▱', 14
PHONE_RE = re.compile(r'^\+?[1-9]\d{6,14}$')

PENDING = {}
WATCHING = {}
JOIN_CACHE = {}
BLAST_LOCK = threading.Lock()
HTTP_LOCAL = threading.local()

bot = telebot.TeleBot(TOKEN, parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════
def log(msg=''):
    print(f'[{time.strftime("%H:%M:%S")}] {msg}', flush=True)


def esc(s):
    return (str(s if s is not None else '')
            .replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'))


def is_admin(uid):
    return bool(ADMIN_IDS) and uid in ADMIN_IDS


def valid_phone(v):
    clean = str(v or '').strip()
    for ch in ' -()':
        clean = clean.replace(ch, '')
    return bool(PHONE_RE.match(clean))


def clamp_count(v, default=1):
    try:
        n = int(float(str(v).strip().rstrip('x×').strip()))
    except (TypeError, ValueError):
        return default
    return max(1, min(MAX_COUNT, n))


def blast_cost(count):
    return max(1, int(count or 1)) * COST_PER_MSG


def bar(fraction):
    filled = int(BAR_LEN * max(0.0, min(1.0, fraction)))
    return BAR * filled + EMPTY * (BAR_LEN - filled)


def humanize(ts):
    return time.strftime('%d %b %Y', time.localtime(ts)) if ts else '—'


def fmt_ago(ts):
    if not ts:
        return '—'
    d = int(time.time() - ts)
    if d < 60: return f'{d}s ago'
    if d < 3600: return f'{d // 60}m ago'
    if d < 86400: return f'{d // 3600}h ago'
    return f'{d // 86400}d ago'


def user_level(spent):
    if spent < 100: return '🥉 Bronze'
    if spent < 500: return '🥈 Silver'
    if spent < 2000: return '🥇 Gold'
    if spent < 5000: return '💎 Platinum'
    return '👑 Diamond'


# ═══════════════════════════════════════════════════════════════
#  GATEWAY HTTP CLIENT
# ═══════════════════════════════════════════════════════════════
class GatewayError(Exception):
    pass


def _http_session():
    session = getattr(HTTP_LOCAL, 'session', None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=64, max_retries=0)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({'Accept': 'application/json', 'Connection': 'keep-alive'})
        HTTP_LOCAL.session = session
    return session


def api(path, payload=None, timeout=15):
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
        raise GatewayError('🔌 <b>Gateway Connection Error</b>\n'
                           'Server offline lag raha hai. Admin se contact karein.') from e
    except Exception as e:
        raise GatewayError(str(e))


def get_online_devices():
    try:
        res = api('/api/devices', timeout=10)
        if not res.get('ok'):
            return []
        return [d for d in (res.get('devices') or []) if d.get('status')]
    except Exception as e:
        log(f'! get_online_devices failed: {e}')
        return []


def device_short_names(devices, limit=4):
    out = []
    for d in devices[:limit]:
        n = d.get('name') or d.get('model') or 'SIM'
        out.append(esc(str(n)[:18]))
    if len(devices) > limit:
        out.append(f'+{len(devices) - limit} more')
    return out


def build_sim_pool_line(devices):
    n = len(devices)
    if n == 0:
        return '📡 <b>SIM Pool:</b> ❌ none online'
    line = f'📡 <b>SIM Pool:</b> {n} device(s) online'
    names = device_short_names(devices, 4)
    if names:
        line += '\n   ├ ' + '\n   ├ '.join(names)
    return line


# ═══════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════
def load_db():
    if not os.path.exists(DB_FILE):
        return {}
    try:
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        shutil.copy(DB_FILE, DB_FILE + '.corrupt')
        log(f'!! DB corrupt — backup saved to {DB_FILE}.corrupt')
        return {}


def save_db(db):
    if os.path.exists(DB_FILE):
        shutil.copy(DB_FILE, DB_FILE + '.bak')
    tmp = DB_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(db, f, indent=2, ensure_ascii=False)
    os.replace(tmp, DB_FILE)


def new_user(uid, name='', username=''):
    return {
        'points': START_POINTS, 'name': name, 'username': username,
        'joined': int(time.time()), 'ref': '', 'referred': 0,
        'spent': 0, 'earned': START_POINTS, 'cooldown': 0,
        'last_bonus': 0, 'streak': 0, 'history': [],
        'numbers': [], 'banned': False, 'ban_reason': '',
    }


def get_user(uid, name='', username=''):
    db = load_db()
    key = str(uid)
    if key not in db:
        db[key] = new_user(uid, name, username)
        save_db(db)
    return db[key]


def add_points(uid, amount, reason):
    db = load_db()
    u = db.get(str(uid))
    if not u:
        return
    u['points'] = max(0, u['points'] + amount)
    if amount > 0:
        u['earned'] = u.get('earned', 0) + amount
    u.setdefault('history', []).append({'t': int(time.time()), 'd': reason, 'x': amount})
    u['history'] = u['history'][-50:]
    save_db(db)


def spend(uid, amount, reason):
    db = load_db()
    u = db.get(str(uid))
    if not u or u.get('points', 0) < amount:
        return False
    u['points'] -= amount
    u['spent'] = u.get('spent', 0) + amount
    u.setdefault('history', []).append({'t': int(time.time()), 'd': reason, 'x': -amount})
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


def remember_number(uid, number):
    db = load_db()
    u = db.get(str(uid))
    if not u:
        return
    nums = u.setdefault('numbers', [])
    if number in nums:
        nums.remove(number)
    nums.insert(0, number)
    u['numbers'] = nums[:5]
    save_db(db)


def is_banned(uid):
    if is_admin(uid):
        return False
    return bool(get_user(uid).get('banned'))


# ═══════════════════════════════════════════════════════════════
#  MAINTENANCE MODE
# ═══════════════════════════════════════════════════════════════
def maintenance_on():
    return bool(load_db().get('__maintenance__'))


def set_maintenance(state):
    db = load_db()
    db['__maintenance__'] = bool(state)
    save_db(db)


# ═══════════════════════════════════════════════════════════════
#  FORCE SUBSCRIBE
# ═══════════════════════════════════════════════════════════════
MEMBER_STATUSES = ('member', 'administrator', 'creator', 'owner')


def join_prompt_kb(miss):
    kb = types.InlineKeyboardMarkup(row_width=1)
    for ch in miss:
        clean_ch = ch.replace('@', '')
        kb.add(types.InlineKeyboardButton(f'📢 Join @{clean_ch}', url=f'https://t.me/{clean_ch}'))
    kb.add(types.InlineKeyboardButton('✅ Verify Membership', callback_data='recheck'))
    return kb


def missing_channels(user_id):
    if is_admin(user_id):
        return []
    cached = JOIN_CACHE.get(user_id)
    if cached and time.time() - cached[0] < 60:
        return [] if cached[1] else list(REQUIRED_CHANNELS)
    for ch in REQUIRED_CHANNELS:
        try:
            target_ch = ch if ch.startswith('@') else f'@{ch}'
            st = bot.get_chat_member(target_ch, user_id).status
        except Exception as e:
            log(f'! join-check unavailable for @{ch}: {e}')
            JOIN_CACHE[user_id] = (time.time(), True)
            return []
        if st not in MEMBER_STATUSES:
            JOIN_CACHE[user_id] = (time.time(), False)
            return list(REQUIRED_CHANNELS)
    JOIN_CACHE[user_id] = (time.time(), True)
    return []


def join_gate(message):
    miss = missing_channels(message.chat.id)
    if not miss:
        return True
    bot.send_message(message.chat.id,
                     '🔒 <b>Access Denied</b>\n\n'
                     'Bot use karne ke liye pehle hamare official channel join karein!',
                     reply_markup=join_prompt_kb(miss))
    return False


# ═══════════════════════════════════════════════════════════════
#  KEYBOARDS
# ═══════════════════════════════════════════════════════════════
def main_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(types.KeyboardButton('📡 Send Message'), types.KeyboardButton('📊 Live Status'))
    kb.add(types.KeyboardButton('💎 My Wallet'),    types.KeyboardButton('🎁 Daily Bonus'))
    kb.add(types.KeyboardButton('🔗 Refer & Earn'), types.KeyboardButton('👤 My Profile'))
    kb.add(types.KeyboardButton('🏆 Leaderboard'),  types.KeyboardButton('ℹ️ Help'))
    return kb


def admin_menu():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(types.InlineKeyboardButton('👥 User Lookup', callback_data='adm:user'),
           types.InlineKeyboardButton('📊 Bot Stats',   callback_data='adm:stats'))
    kb.add(types.InlineKeyboardButton('📣 Broadcast',    callback_data='adm:bc'),
           types.InlineKeyboardButton('🚫 Ban Manager', callback_data='adm:ban'))
    kb.add(types.InlineKeyboardButton('🔧 Maintenance',  callback_data='adm:maint'),
           types.InlineKeyboardButton('📜 Command List', callback_data='adm:cmds'))
    return kb


# ═══════════════════════════════════════════════════════════════
#  RENDERERS
# ═══════════════════════════════════════════════════════════════
def live_text(j):
    requested = j.get('requested', j.get('total', 0))
    capacity = j.get('capacity', '?')
    sent = j.get('sent', 0)
    failed = j.get('failed', 0)
    remaining = j.get('remaining', 0)
    percent = j.get('percent') or 0
    rate = j.get('rate', 0)
    elapsed = j.get('elapsed', 0)
    return (
        '⚡ <b>BLAST IN PROGRESS</b>\n'
        f'{"─" * 22}\n\n'
        f'{bar(percent / 100)}  <b>{percent}%</b>\n\n'
        f'🎯 <b>Target:</b> {requested} messages\n'
        f'📡 <b>SIM Slots:</b> {capacity}\n'
        f'✅ <b>Delivered:</b> {sent}\n'
        f'❌ <b>Failed:</b> {failed}\n'
        f'⏳ <b>Remaining:</b> {remaining}\n'
        f'⚡ <b>Speed:</b> {rate}/s\n'
        f'⏱ <b>Elapsed:</b> {elapsed}s\n\n'
        '<i>🔄 Parallel across devices, no overlap.</i>'
    )


def done_text(j, title):
    requested = j.get("requested", j["total"])
    delivered = j.get("sent", 0)
    failed = j.get("failed", 0)
    missing = max(0, requested - delivered - failed)
    out = (
        f'{title}\n'
        f'{"─" * 22}\n\n'
        f'👤 <b>To:</b> <code>{esc(j["to"])}</code>\n'
        f'🎯 <b>Requested:</b> {requested}\n'
        f'📡 <b>SIM Slots:</b> {j.get("capacity", "?")}\n'
        f'✅ <b>Delivered:</b> {delivered} / {requested}\n'
        f'❌ <b>Failed:</b> {failed}\n'
        f'📈 <b>Delivery Rate:</b> {j["success"] if j.get("success") is not None else "—"}%\n'
        f'⏱ <b>Total Time:</b> {j["elapsed"]}s'
    )
    if missing > 0:
        out += f'\n\n🔍 <b>Undelivered:</b> {missing}'
    return out


def failure_text(j):
    bad = [d for d in j.get('devices', []) if d.get('failed')]
    if not bad:
        return '✅ <b>Perfect Delivery!</b>\nKoi failures nahi mile.'
    out = [f'❌ <b>Failure Report</b> — {len(bad)} device(s) impacted', '']
    for d in bad:
        out.append(f'{"🟢" if d["status"] else "🔴"} <b>{esc(d["name"])}</b>')
        out.append(f'   ↳ ❌ {d["failed"]} fails | ✅ {d["sent"]} sent')
    out += ['', '<i>Offline devices automatically fail. Turn them on and use /retry.</i>']
    return '\n'.join(out)


def devices_text(devices):
    if not devices:
        return '📭 <b>No Devices Found</b>\nGateway server par koi device connect nahi hai.'
    online = sum(1 for d in devices if d.get('status'))
    out = [f'📡 <b>Gateway Pool</b> — {len(devices)} Total | 🟢 {online} Online', '']
    for d in devices[:25]:
        batt = d.get('batteryPercent')
        out += [f'{"🟢" if d["status"] else "🔴"} <b>{esc(d["name"])}</b>',
                f'   ├ Model: {esc(d.get("model", "—"))}',
                f'   └ 📞 {esc(d.get("phoneNumber", "—"))} | 🔋 {batt if batt is not None else "—"}%', '']
    if len(devices) > 25:
        out.append(f'<i>...and {len(devices) - 25} more devices hidden.</i>')
    return '\n'.join(out)


def profile_text(u, uid):
    level = user_level(u.get('spent', 0))
    return (
        '👤 <b>YOUR PROFILE</b>\n'
        f'{"─" * 22}\n\n'
        f'🪪 <b>Name:</b> {esc(u.get("name") or "Anonymous")}\n'
        f'🆔 <b>ID:</b> <code>{uid}</code>\n'
        f'🏅 <b>Level:</b> {level}\n'
        f'💎 <b>Balance:</b> {u.get("points", 0)} pts\n'
        f'📈 <b>Earned:</b> {u.get("earned", 0)} pts\n'
        f'📉 <b>Spent:</b> {u.get("spent", 0)} pts\n'
        f'👥 <b>Referrals:</b> {u.get("referred", 0)}\n'
        f'🔥 <b>Streak:</b> {u.get("streak", 0)} days\n'
        f'📅 <b>Joined:</b> {humanize(u.get("joined"))}\n'
        f'⏱ <b>Last Seen:</b> {fmt_ago(u.get("cooldown"))}'
    )


def help_text():
    return (
        '🤖 <b>Gateway Bot</b>\n'
        f'{"─" * 22}\n\n'
        f'🎁 <b>Welcome Bonus:</b> {START_POINTS} Points\n'
        f'👥 <b>Referral Bonus:</b> +{REFERRAL_BONUS} per friend\n'
        f'💵 <b>Rate:</b> {COST_PER_MSG} point = 1 SMS\n'
        f'🔥 <b>Daily Streak:</b> +{DAILY_BONUS} to +{MAX_STREAK_BONUS} pts/day\n\n'
        '<b>📌 User Commands:</b>\n'
        '🔹 /send — Start new SMS blast\n'
        '🔹 /stats — Live progress tracker\n'
        '🔹 /status — Server & gateway info\n'
        '🔹 /devices — View gateway devices\n'
        '🔹 /failed — View failed delivery logs\n'
        '🔹 /retry — Retry failed numbers\n'
        '🔹 /history — Your past campaigns\n'
        '🔹 /balance — Wallet summary\n'
        '🔹 /profile — Full profile card\n'
        '🔹 /referral — Earn link\n'
        '🔹 /top — Leaderboard\n'
        '🔹 /extra — Claim daily bonus\n'
        '🔹 /affords — Capacity calculator\n'
        '🔹 /deepthink — Account analytics\n'
        '🔹 /recent — Recent numbers\n\n'
        '<i>Tip: Support ke liye admin se contact karein.</i>'
    )


# ═══════════════════════════════════════════════════════════════
#  START / WELCOME
# ═══════════════════════════════════════════════════════════════
@bot.message_handler(commands=['start'])
def cmd_start(message):
    uid = message.chat.id

    if maintenance_on() and not is_admin(uid):
        bot.send_message(uid,
                         '🛠 <b>Maintenance Mode</b>\n\n'
                         'Bot abhi update ho raha hai. Thodi der me try karein.',
                         reply_markup=types.ReplyKeyboardRemove())
        return

    get_user(uid, message.from_user.first_name or '', message.from_user.username or '')

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
                                 f'🎉 <b>New Referral!</b>\n\nAapko <b>+{REFERRAL_BONUS}</b> points mil gaye hain.',
                                 parse_mode='HTML')
            except Exception:
                pass

    if is_banned(uid):
        bot.send_message(uid, '🚫 <b>You are banned.</b>\nContact admin for support.')
        return

    if not join_gate(message):
        return

    u = get_user(uid)
    bot.send_chat_action(uid, 'typing')
    bot.send_message(
        uid,
        f'👋 <b>Welcome, {esc(message.from_user.first_name)}!</b>\n'
        f'{"─" * 22}\n\n'
        f'💎 <b>Wallet:</b> {u["points"]} points\n'
        f'✉️ <b>Rate:</b> {COST_PER_MSG} point = 1 Message\n\n'
        '⚡ <b>Getting Started:</b>\n'
        '├ 📡 Tap <b>Send Message</b> to start a blast\n'
        '├ 🎁 Claim your <b>Daily Bonus</b> every day\n'
        '├ 🔗 Share your <b>Referral Link</b> to earn\n'
        '└ 📊 Track live with <b>Live Status</b>\n\n'
        '<i>Niche menu se apna campaign shuru karein!</i>',
        reply_markup=main_menu())


@bot.message_handler(commands=['help'])
def cmd_help(message):
    if not join_gate(message):
        return
    bot.send_chat_action(message.chat.id, 'typing')
    bot.send_message(message.chat.id, help_text(), reply_markup=main_menu())


# ═══════════════════════════════════════════════════════════════
#  WALLET / PROFILE / REFERRAL / TOP
# ═══════════════════════════════════════════════════════════════
@bot.message_handler(commands=['balance', 'points'])
def cmd_balance(message):
    if not join_gate(message):
        return
    u = get_user(message.chat.id)
    left = cooldown_left(message.chat.id)
    cd = f'\n⏳ <b>Cooldown:</b> {left}s left' if left else '\n✅ <b>Status:</b> Ready to Blast'
    bot.send_message(
        message.chat.id,
        '💳 <b>WALLET DASHBOARD</b>\n'
        f'{"─" * 22}\n\n'
        f'💎 <b>Balance:</b> {u["points"]} Points\n'
        f'📤 <b>Total Earned:</b> {u.get("earned", 0)}\n'
        f'📥 <b>Total Spent:</b> {u.get("spent", 0)}\n'
        f'👥 <b>Referrals:</b> {u.get("referred", 0)}\n'
        f'🎯 <b>Can Send:</b> {u["points"] // COST_PER_MSG} messages' + cd,
        reply_markup=main_menu())


@bot.message_handler(commands=['profile'])
def cmd_profile(message):
    if not join_gate(message):
        return
    u = get_user(message.chat.id)
    bot.send_message(message.chat.id, profile_text(u, message.chat.id), reply_markup=main_menu())


@bot.message_handler(commands=['referral'])
def cmd_referral(message):
    uid = message.chat.id
    link = f'https://t.me/{bot.get_me().username}?start={uid}'
    bot.send_message(
        uid,
        '🚀 <b>REFER & EARN</b>\n'
        f'{"─" * 22}\n\n'
        'Tap to copy your link:\n'
        f'<code>{link}</code>\n\n'
        f'🎁 <b>You get:</b> +{REFERRAL_BONUS} points\n'
        f'🎁 <b>Friend gets:</b> {START_POINTS} points free\n\n'
        '<i>Jitne zyada dost, utne zyada points!</i>',
        reply_markup=main_menu())


@bot.message_handler(commands=['top'])
def cmd_top(message):
    db = load_db()
    rows = [(k, v) for k, v in db.items() if isinstance(v, dict) and 'points' in v]
    rows = sorted(rows, key=lambda kv: kv[1].get('points', 0), reverse=True)[:10]
    if not rows:
        bot.send_message(message.chat.id, '📭 Abhi leaderboard empty hai.')
        return
    medals = ['🥇', '🥈', '🥉']
    out = ['🏆 <b>TOP 10 SPAMMERS</b>', '─' * 22, '']
    for i, (uid, u) in enumerate(rows):
        name = u.get('name', 'User') or 'User'
        tag = medals[i] if i < 3 else f'<b>{i+1}.</b>'
        out.append(f'{tag} {esc(name)} — 💎 <b>{u.get("points", 0)}</b> pts')
    bot.send_message(message.chat.id, '\n'.join(out), reply_markup=main_menu())


@bot.message_handler(commands=['recent'])
def cmd_recent(message):
    if not join_gate(message):
        return
    u = get_user(message.chat.id)
    nums = u.get('numbers', [])
    if not nums:
        bot.send_message(message.chat.id, '📭 Abhi tak koi number use nahi kiya.')
        return
    out = ['📞 <b>RECENT NUMBERS</b>', '─' * 22, '']
    for i, n in enumerate(nums, 1):
        out.append(f'{i}. <code>{esc(n)}</code>')
    out += ['', '<i>Tip: /send se naya blast start karein.</i>']
    bot.send_message(message.chat.id, '\n'.join(out))


# ═══════════════════════════════════════════════════════════════
#  DAILY BONUS WITH STREAK
# ═══════════════════════════════════════════════════════════════
@bot.message_handler(commands=['extra', 'daily'])
def cmd_extra(message):
    if not join_gate(message):
        return
    db = load_db()
    uid = str(message.chat.id)
    u = db.get(uid)
    if not u:
        return
    now = time.time()
    last = u.get('last_bonus', 0)
    streak = u.get('streak', 0)

    if now - last >= 86400:
        if last and now - last <= 172800:
            streak += 1
        else:
            streak = 1
        bonus = min(DAILY_BONUS + (streak - 1) * 2, MAX_STREAK_BONUS)
        u['points'] += bonus
        u['earned'] = u.get('earned', 0) + bonus
        u['last_bonus'] = now
        u['streak'] = streak
        u.setdefault('history', []).append({'t': int(now), 'd': 'daily_bonus', 'x': bonus})
        u['history'] = u['history'][-50:]
        save_db(db)
        bot.send_message(
            message.chat.id,
            '🎁 <b>DAILY BONUS CLAIMED!</b>\n'
            f'{"─" * 22}\n\n'
            f'💰 <b>Bonus:</b> +{bonus} points\n'
            f'🔥 <b>Streak:</b> {streak} day(s)\n'
            f'💎 <b>New Balance:</b> {u["points"]} pts\n\n'
            f'<i>Kal wapas aayein, streak badhao aur zyada points payein!</i>',
            reply_markup=main_menu())
    else:
        left = int(86400 - (now - last))
        h, rem = divmod(left, 3600)
        m, _ = divmod(rem, 60)
        bot.send_message(
            message.chat.id,
            '⏳ <b>BONUS ALREADY CLAIMED</b>\n'
            f'{"─" * 22}\n\n'
            f'🕒 Next claim in: <b>{h}h {m}m</b>\n'
            f'🔥 <b>Current Streak:</b> {streak} day(s)\n\n'
            '<i>Streak todna mat, roz claim karo!</i>',
            reply_markup=main_menu())


# ═══════════════════════════════════════════════════════════════
#  EXTRA FEATURES
# ═══════════════════════════════════════════════════════════════
@bot.message_handler(commands=['deepthink'])
def cmd_deepthink(message):
    if not join_gate(message):
        return
    bot.send_chat_action(message.chat.id, 'typing')
    u = get_user(message.chat.id)
    total_spent = u.get('spent', 0)
    total_earned = u.get('earned', 0)
    history = u.get('history', [])
    blasts = sum(1 for h in history if 'send' in str(h.get('d', '')))
    bot.send_message(
        message.chat.id,
        '🧠 <b>DEEP ANALYTICS</b>\n'
        f'{"─" * 22}\n\n'
        f'🏅 <b>Level:</b> {user_level(total_spent)}\n'
        f'💸 <b>Points Burned:</b> {total_spent}\n'
        f'💰 <b>Points Generated:</b> {total_earned}\n'
        f'🚀 <b>Campaigns Run:</b> {blasts}\n'
        f'👥 <b>Referrals:</b> {u.get("referred", 0)}\n'
        f'🔥 <b>Streak:</b> {u.get("streak", 0)} days\n\n'
        '<i>Conclusion: Engagement high hai. Blasting continue rakhein!</i>')


@bot.message_handler(commands=['affords'])
def cmd_affords(message):
    if not join_gate(message):
        return
    u = get_user(message.chat.id)
    bal = u.get('points', 0)
    can_send = bal // COST_PER_MSG
    devices = get_online_devices()
    n = len(devices)

    sim_note = ''
    if n == 0:
        sim_note = '\n\n❌ <b>No device online</b> — blast kaam nahi karega.'
    else:
        sim_note = f'\n\n💡 <b>{n} device(s)</b> online — parallel blast chalega.'

    kb = types.InlineKeyboardMarkup(row_width=1)
    if can_send > 0 and n > 0:
        kb.add(types.InlineKeyboardButton(f'🚀 Blast {can_send} msgs now', callback_data='start_blast_quick'))
    kb.add(types.InlineKeyboardButton('🔗 Refer for more points', callback_data='show_ref'))
    bot.send_message(
        message.chat.id,
        '🧮 <b>CAPACITY CALCULATOR</b>\n'
        f'{"─" * 22}\n\n'
        f'💎 <b>Balance:</b> {bal} Points\n'
        f'📩 <b>You can afford:</b> <b>{can_send}</b> SMS\n'
        f'📡 <b>Devices Online:</b> {n}\n'
        f'⚡ <b>Rate:</b> {COST_PER_MSG} point/SMS'
        + sim_note,
        reply_markup=kb)


# ═══════════════════════════════════════════════════════════════
#  GATEWAY COMMANDS
# ═══════════════════════════════════════════════════════════════
@bot.message_handler(commands=['status'])
def cmd_status(message):
    if not join_gate(message):
        return
    bot.send_chat_action(message.chat.id, 'typing')
    try:
        st = api('/api/status', timeout=8)
        dev = api('/api/devices', timeout=12)
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}')
        return

    if not st.get('configured'):
        bot.send_message(
            message.chat.id,
            '🟠 Gateway online hai par Firebase connect nahi hai.\n\n'
            'Admin: <code>/connect URL KEY</code>')
        return

    devices = dev.get('devices') or []
    online = sum(1 for d in devices if d.get('status'))
    mode = st.get('mode', 'smart_parallel')
    interval = st.get('send_interval', '—')
    sims_per = st.get('sims_per_device', '—')

    lines = [
        '🟢 <b>GATEWAY STATUS</b>',
        '─' * 22, '',
        '🔥 <b>Firebase:</b> ✅ Connected',
        f'📱 <b>Devices:</b> {len(devices)} (🟢 {online} Online)',
        f'⚙️ <b>Mode:</b> {esc(str(mode).replace("_", " ").title())}',
        f'⏱ <b>Interval:</b> {esc(str(interval))}s',
        f'📶 <b>SIM/Device:</b> {esc(str(sims_per))}',
    ]
    if online == 0:
        lines.append('❌ <b>No device online!</b> Blast kaam nahi karega.')
    try:
        job = (api('/api/delivery', timeout=8) or {}).get('job')
        if job and job.get('total'):
            lines += ['',
                      f'📝 <b>Last Blast:</b> ✅ {job["sent"]}/{job["total"]} | ❌ {job["failed"]} | ⏱ {job["elapsed"]}s']
            if job.get('failed_tasks'):
                lines.append(f'🔄 <b>Retryable:</b> {job["failed_tasks"]}')
    except Exception:
        pass
    bot.send_message(message.chat.id, '\n'.join(lines), reply_markup=main_menu())


@bot.message_handler(commands=['connect'])
def cmd_connect(message):
    if not is_admin(message.chat.id):
        bot.send_message(message.chat.id, '⛔ <b>Admin only command.</b>')
        return
    parts = (message.text or '').split()
    if len(parts) < 3:
        bot.send_message(message.chat.id,
                         '⚠️ <b>Usage:</b>\n<code>/connect https://proj.firebaseio.com SECRET</code>')
        return
    try:
        res = api('/api/firebase', {'url': parts[1], 'key': ' '.join(parts[2:])}, timeout=20)
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}')
        return
    if res.get('ok'):
        bot.send_message(message.chat.id, '✅ <b>Firebase connected successfully!</b>')
    else:
        bot.send_message(message.chat.id, f'❌ {esc(res.get("error", "failed"))}')


@bot.message_handler(commands=['devices'])
def cmd_devices(message):
    if not join_gate(message):
        return
    bot.send_chat_action(message.chat.id, 'typing')
    try:
        res = api('/api/devices', timeout=15)
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}')
        return
    if not res.get('ok'):
        bot.send_message(message.chat.id, f'❌ {esc(res.get("error", "failed"))}')
        return
    bot.send_message(message.chat.id, devices_text(res['devices']), reply_markup=main_menu())


@bot.message_handler(commands=['stats'])
def cmd_stats(message):
    if not join_gate(message):
        return
    try:
        job = (api('/api/delivery', timeout=10) or {}).get('job')
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}')
        return
    if not job or not job.get('total'):
        bot.send_message(message.chat.id,
                         'ℹ️ <b>No Blast Found</b>\nAbhi tak koi campaign start nahi hua.')
        return
    if job['running']:
        bot.send_message(message.chat.id, live_text(job))
        return
    title = ('⚠️ <b>BLAST INTERRUPTED</b>' if job.get('interrupted')
             else '✅ <b>BLAST COMPLETE</b>' if not job['failed'] and job['sent']
             else '⚠️ <b>BLAST DONE (WITH ERRORS)</b>')
    kb = None
    if job.get('failed_tasks'):
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton(f'🔁 Retry {job["failed_tasks"]} Failed',
                                          callback_data='retry_all'))
    bot.send_message(message.chat.id, done_text(job, title), reply_markup=kb)


@bot.message_handler(commands=['failed'])
def cmd_failed(message):
    if not join_gate(message):
        return
    bot.send_chat_action(message.chat.id, 'typing')
    try:
        job = (api('/api/delivery', timeout=10) or {}).get('job')
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}')
        return
    if not job or not job.get('total'):
        bot.send_message(message.chat.id, 'ℹ️ Abhi koi blast nahi hua.')
        return
    bot.send_message(message.chat.id, failure_text(job))


@bot.message_handler(commands=['history'])
def cmd_history(message):
    if not join_gate(message):
        return
    bot.send_chat_action(message.chat.id, 'typing')
    try:
        res = api('/api/delivery', timeout=10)
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}')
        return
    jobs = res.get('jobs') or []
    job = res.get('job')
    if not jobs:
        bot.send_message(message.chat.id,
                         'ℹ️ <b>No History</b>\nAapne abhi tak koi blast nahi kiya.')
        return
    out = ['🗂 <b>RECENT CAMPAIGNS</b>', '─' * 22, '']
    for jid in jobs[-8:]:
        if jid == (job or {}).get('id'):
            out.append(f'🔹 <code>{jid}</code>\n   ✅ {job["sent"]}/{job["total"]} | ❌ {job["failed"]}')
        else:
            out.append(f'🔹 <code>{jid}</code>')
    bot.send_message(message.chat.id, '\n'.join(out))


@bot.message_handler(commands=['retry'])
def cmd_retry(message):
    if not join_gate(message):
        return
    u = get_user(message.chat.id)
    if u['points'] < COST_RETRY:
        bot.send_message(
            message.chat.id,
            f'❌ <b>Insufficient Balance</b>\n\n'
            f'Cost: {COST_RETRY} pts | Your Balance: {u["points"]} pts\n'
            f'<i>Tip: Refer friends 👉 /referral</i>')
        return
    only = (message.text or '').split()
    only = only[1] if len(only) > 1 else None
    try:
        res = api('/api/message-retry', {'only': only}, timeout=20)
    except GatewayError as e:
        bot.send_message(message.chat.id, f'❌ {esc(str(e))}')
        return
    if not res.get('ok'):
        bot.send_message(message.chat.id, f'❌ {esc(res.get("error", "failed"))}')
        return
    spend(message.chat.id, COST_RETRY, f'retry {res["retried"]}')
    set_cooldown(message.chat.id)
    log(f'RETRY by {message.chat.id}: {res["retried"]} msgs, job={res["job_id"]}')
    sent = bot.send_message(
        message.chat.id,
        '🔁 <b>RETRY INITIATED</b>\n'
        f'{"─" * 22}\n\n'
        f'📱 <b>Devices Active:</b> {res["devices_count"]}\n'
        f'🔄 <b>Retrying:</b> {res["retried"]} messages\n\n'
        f'💸 <b>Cost:</b> -{COST_RETRY} points')
    threading.Thread(target=watch, args=(message.chat.id, sent.message_id, res['job_id']), daemon=True).start()


# ═══════════════════════════════════════════════════════════════
#  SEND FLOW
# ═══════════════════════════════════════════════════════════════
def plan_text(count, balance=None):
    cost = blast_cost(count)
    lines = [f'💎 <b>Cost:</b> {cost} points', f'📨 <b>Volume:</b> {count} message(s)']
    if balance is not None:
        lines.append(f'💰 <b>Balance:</b> {balance} (Max: {balance // COST_PER_MSG})')
    return '\n'.join(lines)


def estimate_time(count, devices):
    """Estimate blast time based on smart parallel mode."""
    if not devices:
        return None
    try:
        st = api('/api/status', timeout=5)
        interval = float(st.get('send_interval', 2.0))
    except Exception:
        interval = 2.0
    n = len(devices)
    # Each device handles count/n messages, with interval gap
    per_dev = max(1, (count + n - 1) // n)
    return int(per_dev * interval)


def start_blast(uid, to, msg, count):
    count = clamp_count(count)
    cost = blast_cost(count)
    u = get_user(uid)

    if not valid_phone(to):
        bot.send_message(uid, '❌ <b>Invalid Number Format.</b>')
        return False
    if not msg:
        bot.send_message(uid, '❌ <b>Message cannot be empty.</b>')
        return False
    if cost > u.get('points', 0):
        bot.send_message(
            uid,
            f'❌ <b>Insufficient Balance</b>\n\n'
            f'Required: <b>{cost}</b> | Wallet: <b>{u.get("points", 0)}</b>\n'
            f'Use /referral to earn more points.')
        return False
    left = cooldown_left(uid)
    if left:
        bot.send_message(uid, f'⏳ <b>Cooldown:</b> Please wait {left}s before next blast.')
        return False
    if uid in WATCHING:
        bot.send_message(uid, '⚠️ <b>Blast in Progress</b>\nPichla blast chal raha hai, wait karein.')
        return False
    if not BLAST_LOCK.acquire(blocking=False):
        bot.send_message(uid, '⚠ <b>Gateway Busy</b>\nThodi der me try karein.')
        return False

    try:
        online_devices = get_online_devices()
    except Exception:
        online_devices = []

    if len(online_devices) == 0:
        BLAST_LOCK.release()
        bot.send_message(
            uid,
            '❌ <b>BLAST CANCELLED</b>\n'
            f'{"─" * 22}\n\n'
            'Gateway me koi bhi device online nahi hai.\n'
            'Pehle admin se device connect karwayein.\n\n'
            '<i>Aapke points deduct nahi kiye gaye.</i>')
        return False

    sim_line = build_sim_pool_line(online_devices)
    est = estimate_time(count, online_devices)

    live_msg = bot.send_message(
        uid,
        '⚡ <b>INITIALIZING BLAST...</b>\n\n'
        f'{sim_line}\n'
        f'🎯 <b>Target:</b> {count} messages\n'
        f'💸 <b>Cost:</b> {cost} points\n'
        f'⏱ <b>Est. Time:</b> ~{est}s\n'
        f'📝 <b>Message:</b> <code>{esc(msg[:120])}</code>\n\n'
        '<i>🔗 Connecting to gateway servers...</i>')

    try:
        res = api('/api/message', {'to': to, 'message': msg, 'count': count}, timeout=30)
    except Exception as e:
        BLAST_LOCK.release()
        try:
            bot.edit_message_text(f'❌ <b>CONNECTION FAILED</b>\n\n{esc(str(e))}',
                                  chat_id=uid, message_id=live_msg.message_id)
        except Exception:
            pass
        return False

    if not res.get('ok'):
        BLAST_LOCK.release()
        try:
            bot.edit_message_text(f'❌ <b>SERVER ERROR</b>\n\n{esc(res.get("error", "failed"))}',
                                  chat_id=uid, message_id=live_msg.message_id)
        except Exception:
            pass
        return False

    spend(uid, cost, f'send {count} to {to}')
    set_cooldown(uid)
    remember_number(uid, to)
    PENDING.pop(uid, None)
    WATCHING[uid] = res['job_id']
    log(f'BLAST {uid}: to={to} count={count} cost={cost} '
        f'devices={len(online_devices)} job={res["job_id"]}')

    threading.Thread(target=watch, args=(uid, live_msg.message_id, res['job_id']), daemon=True).start()
    return True


def ask_count(uid):
    u = get_user(uid)
    devices = get_online_devices()
    n = len(devices)
    interval = 2.0
    try:
        st = api('/api/status', timeout=5)
        interval = float(st.get('send_interval', 2.0))
    except Exception:
        pass

    if n == 0:
        safe_hint = '❌ <b>Koi device online nahi hai</b> — pehle admin se device on karwayein.'
    else:
        safe_hint = (f'💡 <b>{n} device(s) online</b>\n'
                     f'   ⏱ Interval: ~{interval}s per device\n'
                     f'   🔄 Parallel across devices')

    bot.send_message(
        uid,
        '🔢 <b>Kitne messages bhejne hain?</b>\n\n'
        + plan_text(1, u.get('points', 0)) + '\n\n'
        + safe_hint + '\n\n'
        '👉 <i>Sirf number type karein (e.g. 10, 50, 100).</i>')


@bot.message_handler(commands=['send'])
def cmd_send(message):
    if not join_gate(message):
        return
    PENDING[message.chat.id] = {'stage': 'phone'}
    bot.send_message(
        message.chat.id,
        '📱 <b>RECIPIENT NUMBER</b>\n'
        f'{"─" * 22}\n\n'
        'Enter karein (with country code):\n'
        '<i>Example: +919876543210</i>\n\n'
        f'💡 Rate: {COST_PER_MSG} point = 1 Message\n\n'
        '<i>Cancel karne ke liye /cancel bhejein.</i>')


@bot.message_handler(commands=['cancel'])
def cmd_cancel(message):
    PENDING.pop(message.chat.id, None)
    bot.send_message(message.chat.id, '✅ <b>Cancelled.</b>', reply_markup=main_menu())


# menu button handlers
@bot.message_handler(func=lambda m: m.text == '📡 Send Message')
def menu_send(message):
    cmd_send(message)


@bot.message_handler(func=lambda m: m.text == '💎 My Wallet')
def menu_points(message):
    cmd_balance(message)


@bot.message_handler(func=lambda m: m.text == '🔗 Refer & Earn')
def menu_ref(message):
    cmd_referral(message)


@bot.message_handler(func=lambda m: m.text == '📊 Live Status')
def menu_stats(message):
    cmd_stats(message)


@bot.message_handler(func=lambda m: m.text == '🎁 Daily Bonus')
def menu_bonus(message):
    cmd_extra(message)


@bot.message_handler(func=lambda m: m.text == '👤 My Profile')
def menu_profile(message):
    cmd_profile(message)


@bot.message_handler(func=lambda m: m.text == '🏆 Leaderboard')
def menu_top(message):
    cmd_top(message)


@bot.message_handler(func=lambda m: m.text == 'ℹ️ Help')
def menu_help(message):
    cmd_help(message)


@bot.message_handler(func=lambda m: True)
def handle_text(message):
    text = (message.text or '').strip()
    if not text or text.startswith('/'):
        return

    if maintenance_on() and not is_admin(message.chat.id):
        bot.send_message(message.chat.id, '🛠 Bot maintenance me hai. Thodi der baad try karein.')
        return

    if is_banned(message.chat.id):
        return

    st = PENDING.get(message.chat.id)
    if not st:
        if message.from_user and message.from_user.is_bot:
            return
        bot.send_message(message.chat.id, help_text(), reply_markup=main_menu())
        return
    if not join_gate(message):
        return

    if st['stage'] == 'phone':
        if not valid_phone(text):
            bot.send_message(message.chat.id,
                             '❌ <b>Invalid Number</b>\nValid format use karein (e.g., +919876543210).')
            return
        PENDING[message.chat.id] = {'stage': 'message', 'phone': text}
        bot.send_message(message.chat.id,
                         f'✅ <b>Number Saved</b>\n<code>{esc(text)}</code>\n\n'
                         '📝 <b>Ab apna message type karein:</b>')
        return

    if st['stage'] == 'message':
        if len(text) > 500:
            bot.send_message(message.chat.id, '❌ Message max 500 characters.')
            return
        PENDING[message.chat.id] = {'stage': 'count', 'phone': st['phone'], 'msg': text}
        ask_count(message.chat.id)
        return

    if st['stage'] == 'count':
        if not text.isdigit():
            bot.send_message(message.chat.id, '❌ Sirf digits type karein (e.g. 5, 20, 100).')
            return
        start_blast(message.chat.id, st['phone'], st['msg'], clamp_count(text))
        return

    PENDING.pop(message.chat.id, None)


# ═══════════════════════════════════════════════════════════════
#  CALLBACK HANDLERS
# ═══════════════════════════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data == 'recheck')
def cb_recheck(call):
    JOIN_CACHE.pop(call.from_user.id, None)
    uid = call.message.chat.id
    mid = call.message.message_id
    miss = missing_channels(uid)
    if not miss:
        bot.answer_callback_query(call.id, '✅ Verification Successful!')
        try:
            bot.edit_message_text('✅ <b>Channel Verified!</b>\n\nAap poora bot use kar sakte hain 👇',
                                  uid, mid)
        except Exception:
            pass
        return
    bot.answer_callback_query(call.id, '❌ Abhi tak join nahi kiya!')
    try:
        bot.edit_message_reply_markup(uid, mid, reply_markup=join_prompt_kb(miss))
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == 'start_blast_quick')
def cb_start_blast_quick(call):
    bot.answer_callback_query(call.id)
    cmd_send(call.message)


@bot.callback_query_handler(func=lambda c: c.data == 'show_ref')
def cb_show_ref(call):
    bot.answer_callback_query(call.id)
    cmd_referral(call.message)


@bot.callback_query_handler(func=lambda c: c.data == 'retry_all')
def cb_retry(call):
    uid = call.message.chat.id
    mid = call.message.message_id
    u = get_user(uid)
    if u.get('points', 0) < COST_RETRY:
        bot.answer_callback_query(call.id, '❌ Not enough points', show_alert=True)
        return
    left = cooldown_left(uid)
    if left:
        bot.answer_callback_query(call.id, f'⏳ Wait {left}s', show_alert=True)
        return
    if not BLAST_LOCK.acquire(blocking=False):
        bot.answer_callback_query(call.id, '⚠️ Server busy', show_alert=True)
        return
    try:
        bot.answer_callback_query(call.id, '🔁 Retry Initiated!')
        bot.edit_message_text('🔁 <b>RETRY STARTED...</b>', chat_id=uid, message_id=mid)
        res = api('/api/message-retry', {'only': None}, timeout=30)
    except Exception as e:
        BLAST_LOCK.release()
        bot.edit_message_text(f'❌ <b>ERROR:</b> {esc(str(e))}', chat_id=uid, message_id=mid)
        return
    if not res.get('ok'):
        BLAST_LOCK.release()
        bot.edit_message_text(f'❌ <b>FAILED:</b> {esc(res.get("error", "failed"))}',
                              chat_id=uid, message_id=mid)
        return
    spend(uid, COST_RETRY, f'retry {res.get("retried", 0)}')
    set_cooldown(uid)
    WATCHING[uid] = res['job_id']
    log(f'RETRY {uid}: {res.get("retried", 0)} msgs, job={res["job_id"]}')
    threading.Thread(target=watch, args=(uid, mid, res['job_id']), daemon=True).start()


@bot.callback_query_handler(func=lambda c: c.data == 'show_failed')
def cb_show_failed(call):
    bot.answer_callback_query(call.id)
    try:
        job = (api('/api/delivery', timeout=10) or {}).get('job')
    except GatewayError as e:
        bot.send_message(call.message.chat.id, f'❌ {esc(str(e))}')
        return
    bot.send_message(call.message.chat.id, failure_text(job) if job else '📭 No data available.')


# ═══════════════════════════════════════════════════════════════
#  ADMIN PANEL CALLBACKS
# ═══════════════════════════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data.startswith('adm:'))
def cb_admin(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, '⛔ Admin only', show_alert=True)
        return
    action = call.data.split(':', 1)[1]
    bot.answer_callback_query(call.id)

    if action == 'cmds':
        bot.send_message(
            call.message.chat.id,
            '📜 <b>ADMIN COMMANDS</b>\n'
            f'{"─" * 22}\n\n'
            '👥 <b>User Management:</b>\n'
            '<code>/userinfo &lt;uid&gt;</code>\n'
            '<code>/addpoints &lt;uid&gt; &lt;amt&gt;</code>\n'
            '<code>/removepoints &lt;uid&gt; &lt;amt&gt;</code>\n'
            '<code>/setpoints &lt;uid&gt; &lt;amt&gt;</code>\n'
            '<code>/resetuser &lt;uid&gt;</code>\n'
            '<code>/ban &lt;uid&gt; [reason]</code>\n'
            '<code>/unban &lt;uid&gt;</code>\n'
            '<code>/banned</code>\n\n'
            '📣 <b>Broadcast:</b>\n'
            '<code>/broadcast &lt;msg&gt;</code>\n'
            '<code>/giveall &lt;amt&gt;</code>\n\n'
            '🔧 <b>System:</b>\n'
            '<code>/botstats</code>\n'
            '<code>/maintenance on|off</code>\n'
            '<code>/connect &lt;url&gt; &lt;key&gt;</code>\n'
            '<code>/admin</code>')
        return

    if action == 'user':
        bot.send_message(call.message.chat.id,
                         '👥 <b>User Lookup</b>\n\nUse: <code>/userinfo &lt;uid&gt;</code>')
        return

    if action == 'stats':
        cmd_botstats(call.message)
        return

    if action == 'bc':
        bot.send_message(call.message.chat.id,
                         '📣 <b>Broadcast</b>\n\nUse: <code>/broadcast Your message here</code>')
        return

    if action == 'ban':
        bot.send_message(call.message.chat.id,
                         '🚫 <b>Ban Manager</b>\n\n'
                         '<code>/ban &lt;uid&gt; [reason]</code>\n'
                         '<code>/unban &lt;uid&gt;</code>\n'
                         '<code>/banned</code>')
        return

    if action == 'maint':
        state = 'off' if maintenance_on() else 'on'
        set_maintenance(state == 'on')
        bot.send_message(call.message.chat.id,
                         f'🔧 <b>Maintenance:</b> {"🟢 ON" if state == "on" else "🔴 OFF"}')
        return


# ═══════════════════════════════════════════════════════════════
#  ADMIN COMMANDS
# ═══════════════════════════════════════════════════════════════
@bot.message_handler(commands=['admin'])
def cmd_admin(message):
    if not is_admin(message.chat.id):
        bot.send_message(message.chat.id, '⛔ <b>You are not an admin.</b>')
        return
    bot.send_message(
        message.chat.id,
        '👑 <b>ADMIN CONTROL PANEL</b>\n'
        f'{"─" * 22}\n\n'
        f'🔧 <b>Maintenance:</b> {"🟢 ON" if maintenance_on() else "🔴 OFF"}\n'
        f'📊 <b>Admins:</b> {len(ADMIN_IDS)}\n'
        f'⚙️ <b>Gateway Mode:</b> Smart Parallel\n\n'
        '<i>Niche buttons se sections open karein.</i>',
        reply_markup=admin_menu())


@bot.message_handler(commands=['addpoints'])
def cmd_addpoints(message):
    if not is_admin(message.chat.id):
        return
    parts = (message.text or '').split()
    if len(parts) < 3:
        bot.send_message(message.chat.id, "⚠️ <b>Usage:</b> <code>/addpoints 123456789 50</code>")
        return
    try:
        uid, amt = parts[1], int(parts[2])
        add_points(uid, amt, 'admin_add')
        bot.send_message(message.chat.id, f"✅ Added {amt} points to <code>{uid}</code>.")
        try:
            bot.send_message(uid, f"🎁 <b>Gift Received!</b>\n\nAdmin ne aapko <b>{amt}</b> points diye hain.")
        except Exception:
            pass
    except ValueError:
        bot.send_message(message.chat.id, "❌ Amount must be a number.")


@bot.message_handler(commands=['removepoints'])
def cmd_removepoints(message):
    if not is_admin(message.chat.id):
        return
    parts = (message.text or '').split()
    if len(parts) < 3:
        bot.send_message(message.chat.id, "⚠️ <b>Usage:</b> <code>/removepoints 123456789 50</code>")
        return
    try:
        uid, amt = parts[1], int(parts[2])
        spend(uid, amt, 'admin_remove')
        bot.send_message(message.chat.id, f"✅ Removed {amt} points from <code>{uid}</code>.")
    except ValueError:
        bot.send_message(message.chat.id, "❌ Amount must be a number.")


@bot.message_handler(commands=['setpoints'])
def cmd_setpoints(message):
    if not is_admin(message.chat.id):
        return
    parts = (message.text or '').split()
    if len(parts) < 3:
        bot.send_message(message.chat.id, "⚠️ <b>Usage:</b> <code>/setpoints 123456789 500</code>")
        return
    try:
        uid, amt = parts[1], int(parts[2])
        db = load_db()
        if uid not in db:
            bot.send_message(message.chat.id, "❌ User not found.")
            return
        db[uid]['points'] = amt
        save_db(db)
        bot.send_message(message.chat.id, f"✅ Set <code>{uid}</code> points to <b>{amt}</b>.")
    except ValueError:
        bot.send_message(message.chat.id, "❌ Amount must be a number.")


@bot.message_handler(commands=['resetuser'])
def cmd_resetuser(message):
    if not is_admin(message.chat.id):
        return
    parts = (message.text or '').split()
    if len(parts) < 2:
        bot.send_message(message.chat.id, "⚠️ <b>Usage:</b> <code>/resetuser 123456789</code>")
        return
    uid = parts[1]
    db = load_db()
    if uid not in db:
        bot.send_message(message.chat.id, "❌ User not found.")
        return
    name = db[uid].get('name', '')
    username = db[uid].get('username', '')
    db[uid] = new_user(uid, name, username)
    save_db(db)
    bot.send_message(message.chat.id, f"✅ Reset user <code>{uid}</code>.")


@bot.message_handler(commands=['userinfo'])
def cmd_userinfo(message):
    if not is_admin(message.chat.id):
        return
    parts = (message.text or '').split()
    if len(parts) < 2:
        bot.send_message(message.chat.id, "⚠️ <b>Usage:</b> <code>/userinfo 123456789</code>")
        return
    uid = parts[1]
    db = load_db()
    u = db.get(uid)
    if not u:
        bot.send_message(message.chat.id, "❌ User not found.")
        return
    text = (
        f'👤 <b>USER INFO</b>\n{"─" * 22}\n\n'
        f'🆔 <b>ID:</b> <code>{uid}</code>\n'
        f'🪪 <b>Name:</b> {esc(u.get("name", "N/A"))}\n'
        f'📛 <b>Username:</b> @{esc(u.get("username", "N/A"))}\n'
        f'💎 <b>Points:</b> {u.get("points", 0)}\n'
        f'📤 <b>Earned:</b> {u.get("earned", 0)}\n'
        f'📥 <b>Spent:</b> {u.get("spent", 0)}\n'
        f'👥 <b>Referrals:</b> {u.get("referred", 0)}\n'
        f'🔥 <b>Streak:</b> {u.get("streak", 0)} days\n'
        f'🚫 <b>Banned:</b> {"Yes — " + esc(u.get("ban_reason", "")) if u.get("banned") else "No"}\n'
        f'📅 <b>Joined:</b> {humanize(u.get("joined"))}')
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['broadcast'])
def cmd_broadcast(message):
    if not is_admin(message.chat.id):
        return
    msg_text = (message.text or '').replace('/broadcast', '', 1).strip()
    if not msg_text:
        bot.send_message(message.chat.id, "⚠️ <b>Usage:</b> <code>/broadcast Hello everyone!</code>")
        return
    db = load_db()
    users = [k for k, v in db.items() if isinstance(v, dict) and 'points' in v]
    bot.send_message(message.chat.id, f"🚀 Broadcasting to {len(users)} users...")
    ok, fail = 0, 0
    for uid in users:
        try:
            bot.send_message(uid, f"📢 <b>Admin Announcement</b>\n\n{msg_text}")
            ok += 1
            time.sleep(0.05)
        except Exception:
            fail += 1
    bot.send_message(message.chat.id,
                     f"✅ <b>Broadcast Complete</b>\n\nDelivered: {ok}\nFailed: {fail}")


@bot.message_handler(commands=['giveall'])
def cmd_giveall(message):
    if not is_admin(message.chat.id):
        return
    parts = (message.text or '').split()
    if len(parts) < 2:
        bot.send_message(message.chat.id, "⚠️ <b>Usage:</b> <code>/giveall 10</code>")
        return
    try:
        amt = int(parts[1])
    except ValueError:
        bot.send_message(message.chat.id, "❌ Amount must be a number.")
        return
    db = load_db()
    count = 0
    for k, v in db.items():
        if isinstance(v, dict) and 'points' in v:
            v['points'] = v.get('points', 0) + amt
            v['earned'] = v.get('earned', 0) + amt
            v.setdefault('history', []).append({'t': int(time.time()), 'd': 'admin_giveall', 'x': amt})
            v['history'] = v['history'][-50:]
            count += 1
    save_db(db)
    bot.send_message(message.chat.id, f"✅ Gave <b>{amt}</b> points to <b>{count}</b> users.")
    for k, v in db.items():
        if isinstance(v, dict) and 'points' in v and k != str(message.chat.id):
            try:
                bot.send_message(k, f"🎁 <b>Gift from Admin!</b>\n\n+{amt} points added to your wallet.")
                time.sleep(0.05)
            except Exception:
                pass


@bot.message_handler(commands=['ban'])
def cmd_ban(message):
    if not is_admin(message.chat.id):
        return
    parts = (message.text or '').split()
    if len(parts) < 2:
        bot.send_message(message.chat.id, "⚠️ <b>Usage:</b> <code>/ban 123456789 [reason]</code>")
        return
    uid = parts[1]
    reason = ' '.join(parts[2:]) or 'No reason'
    db = load_db()
    if uid not in db:
        bot.send_message(message.chat.id, "❌ User not found.")
        return
    db[uid]['banned'] = True
    db[uid]['ban_reason'] = reason
    save_db(db)
    bot.send_message(message.chat.id, f"🚫 Banned <code>{uid}</code>\nReason: {esc(reason)}")


@bot.message_handler(commands=['unban'])
def cmd_unban(message):
    if not is_admin(message.chat.id):
        return
    parts = (message.text or '').split()
    if len(parts) < 2:
        bot.send_message(message.chat.id, "⚠️ <b>Usage:</b> <code>/unban 123456789</code>")
        return
    uid = parts[1]
    db = load_db()
    if uid not in db:
        bot.send_message(message.chat.id, "❌ User not found.")
        return
    db[uid]['banned'] = False
    db[uid]['ban_reason'] = ''
    save_db(db)
    bot.send_message(message.chat.id, f"✅ Unbanned <code>{uid}</code>.")


@bot.message_handler(commands=['banned'])
def cmd_banned(message):
    if not is_admin(message.chat.id):
        return
    db = load_db()
    banned = [(k, v) for k, v in db.items() if isinstance(v, dict) and v.get('banned')]
    if not banned:
        bot.send_message(message.chat.id, '✅ No banned users.')
        return
    out = ['🚫 <b>BANNED USERS</b>', '─' * 22, '']
    for uid, u in banned:
        out.append(f'• <code>{uid}</code> — {esc(u.get("name", "?"))}\n   ↳ {esc(u.get("ban_reason", ""))}')
    bot.send_message(message.chat.id, '\n'.join(out))


@bot.message_handler(commands=['botstats'])
def cmd_botstats(message):
    if not is_admin(message.chat.id):
        return
    db = load_db()
    users = [v for v in db.values() if isinstance(v, dict) and 'points' in v]
    total_points = sum(u.get('points', 0) for u in users)
    total_spent = sum(u.get('spent', 0) for u in users)
    total_earned = sum(u.get('earned', 0) for u in users)
    banned = sum(1 for u in users if u.get('banned'))
    active = sum(1 for u in users if time.time() - u.get('cooldown', 0) < 86400)
    bot.send_message(
        message.chat.id,
        '📊 <b>BOT STATISTICS</b>\n'
        f'{"─" * 22}\n\n'
        f'👥 <b>Total Users:</b> {len(users)}\n'
        f'🟢 <b>Active (24h):</b> {active}\n'
        f'🚫 <b>Banned:</b> {banned}\n'
        f'💎 <b>Points in Circulation:</b> {total_points}\n'
        f'📥 <b>Total Spent:</b> {total_spent}\n'
        f'📤 <b>Total Earned:</b> {total_earned}\n'
        f'🔧 <b>Maintenance:</b> {"🟢 ON" if maintenance_on() else "🔴 OFF"}')
    try:
        st = api('/api/status', timeout=6)
        dev = api('/api/devices', timeout=8)
        devices = dev.get('devices') or []
        online = sum(1 for d in devices if d.get('status'))
        bot.send_message(
            message.chat.id,
            '🌐 <b>GATEWAY</b>\n'
            f'{"─" * 22}\n\n'
            f'🔥 <b>Firebase:</b> {"✅" if st.get("configured") else "❌"}\n'
            f'📱 <b>Devices:</b> {len(devices)} (🟢 {online})\n'
            f'⚙️ <b>Mode:</b> {esc(str(st.get("mode", "smart_parallel")).replace("_", " ").title())}\n'
            f'⏱ <b>Interval:</b> {esc(str(st.get("send_interval", "—")))}s')
    except Exception:
        pass


@bot.message_handler(commands=['maintenance'])
def cmd_maintenance(message):
    if not is_admin(message.chat.id):
        return
    parts = (message.text or '').split()
    if len(parts) < 2 or parts[1].lower() not in ('on', 'off'):
        bot.send_message(message.chat.id, "⚠️ <b>Usage:</b> <code>/maintenance on|off</code>")
        return
    state = parts[1].lower() == 'on'
    set_maintenance(state)
    bot.send_message(message.chat.id,
                     f'🔧 <b>Maintenance:</b> {"🟢 ON" if state else "🔴 OFF"}')


# ═══════════════════════════════════════════════════════════════
#  LIVE TRACKING
# ═══════════════════════════════════════════════════════════════
def watch(chat_id, message_id, job_id):
    """Poll /api/delivery until job complete. No per-device display."""
    try:
        last_text = None
        for _ in range(1800):  # 30 min max
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
                    bot.edit_message_text(text_now, chat_id=chat_id, message_id=message_id)
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
            title = '⚠️ <b>BLAST STOPPED/INTERRUPTED</b>'
        elif not job.get('failed') and job.get('sent'):
            title = '✅ <b>100% SUCCESSFUL BLAST</b>'
        elif job.get('sent'):
            title = '⚠️ <b>BLAST COMPLETED WITH ERRORS</b>'
        else:
            title = '❌ <b>BLAST FAILED COMPLETELY</b>'

        text = done_text(job, title)

        kb = None
        if job.get('failed_tasks'):
            kb = types.InlineKeyboardMarkup(row_width=1)
            kb.add(types.InlineKeyboardButton(
                f'🔁 Retry {job["failed_tasks"]} ({COST_RETRY} pts)',
                callback_data='retry_all'))

        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
        except Exception:
            bot.send_message(chat_id, text, reply_markup=kb)
    finally:
        WATCHING.pop(chat_id, None)
        try:
            BLAST_LOCK.release()
        except RuntimeError:
            pass


# ═══════════════════════════════════════════════════════════════
#  BOOT
# ═══════════════════════════════════════════════════════════════
bot.set_my_commands([
    types.BotCommand('start', 'Open Menu'),
    types.BotCommand('send', 'Start SMS Campaign'),
    types.BotCommand('balance', 'Check Wallet'),
    types.BotCommand('profile', 'My Profile'),
    types.BotCommand('referral', 'Refer & Earn'),
    types.BotCommand('extra', 'Daily Bonus'),
    types.BotCommand('status', 'Gateway Status'),
    types.BotCommand('devices', 'Online Devices'),
    types.BotCommand('stats', 'Live Progress'),
    types.BotCommand('failed', 'Failure Logs'),
    types.BotCommand('history', 'Campaign History'),
    types.BotCommand('top', 'Leaderboard'),
    types.BotCommand('deepthink', 'Account Analytics'),
    types.BotCommand('affords', 'Capacity Calculator'),
    types.BotCommand('help', 'Help'),
    types.BotCommand('admin', 'Admin Panel'),
])


def main():
    if not TOKEN:
        log('❌ BOT_TOKEN missing.')
        log('   export BOT_TOKEN="123456:ABC-your-botfather-token"')
        raise SystemExit(1)
    log(f'🤖 Gateway Bot → {GATEWAY_URL}')
    log('   channels: ' + (', '.join('@' + c for c in REQUIRED_CHANNELS) or 'none'))
    log(f'   points: start {START_POINTS} · refer +{REFERRAL_BONUS} · '
        f'{COST_PER_MSG}/msg · retry -{COST_RETRY}')
    log(f'   admins: {sorted(ADMIN_IDS) or "none"}')

    try:
        probe = api('/api/status', timeout=6)
        if isinstance(probe, dict):
            mode = probe.get('mode', 'smart_parallel')
            interval = probe.get('send_interval', '—')
            log(f'✅ Gateway reachable · mode={mode} · interval={interval}s')
        else:
            log('⚠ Gateway error')
    except Exception as e:
        log(f'⚠ Gateway probe failed: {e}')

    # Version-safe polling
    log('🚀 Bot polling started...')
    try:
        bot.infinity_polling(skip_pending=True)
    except TypeError:
        try:
            bot.infinity_polling()
        except TypeError:
            bot.polling(none_stop=True, skip_pending=True)


if __name__ == '__main__':
    main()
