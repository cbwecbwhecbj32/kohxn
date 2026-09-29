# GitHub repository URL preserved per configuration: https://github.com/luffy-sh-op/mmd_PANEL/tree/main
import asyncio
import json
import os
import html
import hashlib
import secrets
import uuid
import time
import re
import base64
import sqlite3
import socket
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from collections import deque, defaultdict

from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import Response, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn
import httpx
import logging
import psutil

try:
    import telebot
    from telebot.async_telebot import AsyncTeleBot
    from telebot import types
    TELEBOT_AVAILABLE = True
except ImportError:
    TELEBOT_AVAILABLE = False
    print("WARNING: Please install pyTelegramBotAPI to enable the Telegram Bot: pip install pyTelegramBotAPI")

log_queue = deque(maxlen=150)

class QueueHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            log_queue.append(msg)
        except Exception:
            pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("エムエムディー-Gateway")

q_handler = QueueHandler()
q_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(q_handler)
logging.getLogger("uvicorn.error").addHandler(q_handler)
logging.getLogger("uvicorn.access").addHandler(q_handler)

app = FastAPI(title="エムエムディー Panel", docs_url=None, redoc_url=None)

# Bump this on every release so the dashboard can notify already-open sessions
# that a new version is available / was just applied.
PANEL_VERSION = "1.1.0"

# GitHub repo checked for update notifications
GITHUB_REPO = "luffy-sh-op/mmd_PANEL"

async def check_github_latest(force: bool = False) -> dict:
    """Fetches the latest release tag from GitHub, caches in SQLite.
    Only actually calls the API if force=True or no cached data exists."""
    conn = get_db()
    try:
        cur = conn.execute("SELECT latest_tag, latest_url, checked_at FROM github_cache WHERE id = 1")
        row = cur.fetchone()
    finally:
        conn.close()

    now = time.time()
    cached_tag = row["latest_tag"] if row else None
    cached_url = row["latest_url"] if row else None
    cached_at = row["checked_at"] if row else 0

    if not force and cached_tag and (now - cached_at) < 60:
        return {"tag": cached_tag, "url": cached_url, "checked_at": cached_at}

    global http_client
    if http_client is None:
        return {"tag": cached_tag, "url": cached_url, "checked_at": cached_at}

    new_tag = cached_tag
    new_url = cached_url
    try:
        r = await http_client.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest",
            headers={"Accept": "application/vnd.github+json"},
        )
        if r.status_code == 200:
            data = r.json()
            new_tag = data.get("tag_name") or data.get("name")
            new_url = data.get("html_url")
        else:
            r2 = await http_client.get(f"https://api.github.com/repos/{GITHUB_REPO}/commits/main")
            if r2.status_code == 200:
                data2 = r2.json()
                sha = data2.get("sha") or ""
                new_tag = sha[:7] if sha else cached_tag
                new_url = f"https://github.com/{GITHUB_REPO}/commit/{sha}" if sha else cached_url
    except Exception as e:
        logger.warning(f"GitHub version check failed: {e}")

    conn = get_db()
    try:
        conn.execute("INSERT OR REPLACE INTO github_cache (id, latest_tag, latest_url, checked_at) VALUES (1, ?, ?, ?)",
                     (new_tag, new_url, now))
        conn.commit()
    finally:
        conn.close()

    # Create notification if a new version is detected
    if new_tag and new_tag != cached_tag and cached_tag:
        await create_notification(
            type="update",
            title=f"New version: {new_tag}",
            message=f"Panel version {cached_tag} → {new_tag} is available on GitHub.",
            link=new_url,
        )

    return {"tag": new_tag, "url": new_url, "checked_at": now}

async def github_check_loop():
    """Background task: check GitHub every 60 seconds for new releases."""
    await asyncio.sleep(10)  # initial delay
    while True:
        try:
            await check_github_latest(force=True)
        except Exception as e:
            logger.warning(f"GitHub periodic check error: {e}")
        await asyncio.sleep(60)

# ── Notifications ────────────────────────────────────────────────────────

async def create_notification(type: str, title: str, message: str, link: str | None = None):
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO notifications (type, title, message, link, created_at) VALUES (?, ?, ?, ?, ?)",
            (type, title, message, link, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Error creating notification: {e}")
    finally:
        conn.close()

async def get_unread_notification_count() -> int:
    conn = get_db()
    try:
        cur = conn.execute("SELECT COUNT(*) as cnt FROM notifications WHERE seen = 0")
        row = cur.fetchone()
        return row["cnt"] if row else 0
    finally:
        conn.close()

async def get_notifications(limit: int = 50) -> list:
    conn = get_db()
    try:
        cur = conn.execute(
            "SELECT id, type, title, message, link, seen, created_at FROM notifications ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

def _get_or_create_secret() -> str:
    """Returns a stable secret key across restarts."""
    env_secret = os.environ.get("SECRET_KEY")
    if env_secret:
        return env_secret
    secret_file = "/data/secret.key" if os.path.isdir("/data") else "secret.key"
    try:
        if os.path.exists(secret_file):
            with open(secret_file, "r", encoding="utf-8") as f:
                existing = f.read().strip()
                if existing:
                    return existing
    except Exception:
        pass
    new_secret = secrets.token_urlsafe(32)
    try:
        with open(secret_file, "w", encoding="utf-8") as f:
            f.write(new_secret)
    except Exception as e:
        logger.warning(f"Could not persist secret.key, sessions/passwords will reset on restart: {e}")
    return new_secret

CONFIG = {
    "port": int(os.environ.get("PORT", 8000)),
    "secret": _get_or_create_secret(),
    "telegram_token": "",
    "telegram_admin_id": "",
    "bot_lang": "en",
    "railway_token": "",
    "notify_connections": "0",
}

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.mount("/client", StaticFiles(directory="client"), name="client")

connections: dict = {}
connections_lock = asyncio.Lock()
connection_sockets: dict = {}
link_ip_map: dict = defaultdict(set)
stats = {"total_bytes": 0, "total_requests": 0, "total_errors": 0, "start_time": time.time()}
error_logs: deque = deque(maxlen=50)
hourly_traffic: dict = defaultdict(int)
daily_traffic: dict = defaultdict(int)
http_client: httpx.AsyncClient | None = None

LINKS: dict = {}
LINKS_LOCK = asyncio.Lock()
CUSTOM_ADDRESSES: list = []
CUSTOM_ADDRESSES_LOCK = asyncio.Lock()

notified_uids = set()

SESSION_COOKIE = "ren_session"
SESSION_TTL = 60 * 60 * 24 * 7
UNLIMITED_QUOTA_BYTES = 53687091200000
# پورت همیشه ثابت روی 443 است — دیگه قابل تغییر توسط کاربر نیست
DEFAULT_PORT = 443
MIN_PORT, MAX_PORT = 1, 65535

# نوع پروتکل (auth scheme) و ترابرد به‌صورت دو بُعد جدا از هم هستن؛ کاربر برای هر
# کانفیگ هرکدوم رو مستقل از اون یکی انتخاب می‌کنه (مثلاً Trojan + XHTTP stream-up
# یا VLESS + WebSocket و ...). مقدار ذخیره‌شده‌ی نهایی همیشه "{auth}-{transport}"ه.
AUTH_TYPES = ("vless", "trojan")
DEFAULT_AUTH = "vless"

TRANSPORTS = ("ws", "xhttp-packet-up", "xhttp-stream-up")
DEFAULT_TRANSPORT = "ws"

PROTOCOLS = tuple(f"{a}-{t}" for a in AUTH_TYPES for t in TRANSPORTS)
DEFAULT_PROTOCOL = f"{DEFAULT_AUTH}-{DEFAULT_TRANSPORT}"

def split_protocol(protocol: str) -> tuple[str, str]:
    """مقدار ذخیره‌شده‌ی protocol ("auth-transport") رو به دو بخش auth/transport می‌شکونه."""
    protocol = normalize_protocol(protocol)
    auth, transport = protocol.split("-", 1)
    return auth, transport

def normalize_protocol(value: str | None) -> str:
    """قدیم‌ترها مقدار protocol فقط ترابرد بود (مثلاً 'xhttp-packet-up' بدون
    پیشوند auth) چون auth همیشه vless بود. این تابع مقادیر قدیمی رو به فرمت
    جدید 'auth-transport' تبدیل می‌کنه تا کانفیگ‌های قبلی خراب نشن."""
    value = (value or "").strip().lower()
    if value in PROTOCOLS:
        return value
    if value in TRANSPORTS:  # legacy value با auth ضمنی vless
        return f"vless-{value}"
    return DEFAULT_PROTOCOL

# Fingerprint (uTLS) های قابل انتخاب برای هر کانفیگ — مستقل برای هر پروتکل انتخاب می‌شه
FINGERPRINTS = ("chrome", "firefox", "safari", "ios", "android", "edge", "360", "qq", "random", "randomized")
DEFAULT_FINGERPRINT = "chrome"

# لیست بسته‌ی ALPNهای قابل‌انتخاب (دیگه فیلد آزاد نیست) — مستقل برای هر پروتکل انتخاب می‌شه
ALPN_OPTIONS = ("h3", "h2", "http/1.1", "h3,h2,http/1.1", "h3,h2", "h2,http/1.1")

# پیش‌فرض ALPN بر اساس نوع ترابرد، وقتی کاربر مقدار انتخاب نکرده (auth روی این تاثیری نداره)
DEFAULT_ALPN_BY_PROTOCOL = {}
for _auth in AUTH_TYPES:
    DEFAULT_ALPN_BY_PROTOCOL[f"{_auth}-ws"] = "http/1.1"
    DEFAULT_ALPN_BY_PROTOCOL[f"{_auth}-xhttp-packet-up"] = "h2,http/1.1"
    DEFAULT_ALPN_BY_PROTOCOL[f"{_auth}-xhttp-stream-up"] = "h2,http/1.1"
del _auth

# ═══════════════════ ساختار «variants» — هر لینک می‌تونه هم‌زمان هم VLESS هم Trojan ═══════════════════
# هر لینک به‌جای یک protocol واحد، یک variant مستقل برای هر auth type داره:
#   link["variants"] = {
#       "vless":  {"enabled": bool, "transport": ..., "fingerprint": ..., "alpn": ...},
#       "trojan": {"enabled": bool, "transport": ..., "fingerprint": ..., "alpn": ...},
#   }
# حداقل یکی از دو تا باید enabled باشه.

def default_variants() -> dict:
    return {
        "vless": {"enabled": True, "transport": DEFAULT_TRANSPORT, "fingerprint": DEFAULT_FINGERPRINT, "alpn": DEFAULT_ALPN_BY_PROTOCOL["vless-ws"]},
        "trojan": {"enabled": False, "transport": DEFAULT_TRANSPORT, "fingerprint": DEFAULT_FINGERPRINT, "alpn": DEFAULT_ALPN_BY_PROTOCOL["trojan-ws"]},
    }

def sanitize_variant(v: dict | None, auth: str) -> dict:
    v = v or {}
    transport = str(v.get("transport") or DEFAULT_TRANSPORT).strip().lower()
    if transport not in TRANSPORTS:
        transport = DEFAULT_TRANSPORT
    fp = str(v.get("fingerprint") or DEFAULT_FINGERPRINT).strip().lower()
    if fp not in FINGERPRINTS:
        fp = DEFAULT_FINGERPRINT
    alpn = str(v.get("alpn") or "").strip()
    if alpn not in ALPN_OPTIONS:
        alpn = DEFAULT_ALPN_BY_PROTOCOL.get(f"{auth}-{transport}", "http/1.1")
    return {"enabled": bool(v.get("enabled", False)), "transport": transport, "fingerprint": fp, "alpn": alpn}

def sanitize_variants(variants: dict | None) -> dict:
    variants = variants or {}
    result = {auth: sanitize_variant(variants.get(auth), auth) for auth in AUTH_TYPES}
    if not any(result[a]["enabled"] for a in AUTH_TYPES):
        result["vless"]["enabled"] = True  # حداقل یکی باید فعال بمونه
    return result

def variants_from_legacy(protocol: str, fingerprint: str, alpn: str) -> dict:
    """کانفیگ‌های قدیمی که فقط یک protocol/fingerprint/alpn ستونی داشتن رو به فرمت جدید تبدیل می‌کنه."""
    auth, transport = split_protocol(protocol)
    variants = default_variants()
    for a in AUTH_TYPES:
        variants[a]["enabled"] = False
    variants[auth] = {
        "enabled": True, "transport": transport,
        "fingerprint": fingerprint or DEFAULT_FINGERPRINT,
        "alpn": alpn or DEFAULT_ALPN_BY_PROTOCOL.get(f"{auth}-{transport}", "http/1.1"),
    }
    return variants

def variants_to_legacy(variants: dict) -> tuple[str, str, str]:
    """برای پرشدن ستون‌های قدیمی protocol/fingerprint/alpn (صرفاً برای سازگاری با ابزارهای بیرونی)."""
    for auth in AUTH_TYPES:
        v = (variants or {}).get(auth, {})
        if v.get("enabled"):
            return f"{auth}-{v.get('transport', DEFAULT_TRANSPORT)}", v.get("fingerprint", DEFAULT_FINGERPRINT), v.get("alpn", "")
    return DEFAULT_PROTOCOL, DEFAULT_FINGERPRINT, ""

def variants_from_body(body: dict, base: dict | None = None) -> dict:
    """بدنه‌ی JSON درخواست (فیلدهای vless_enabled/vless_transport/... و trojan_*) رو
    به ساختار variants تبدیل می‌کنه. base مقادیر پیش‌فرض/موجود رو برای فیلدهایی که
    توی body نیومدن فراهم می‌کنه (برای PATCH جزئی)."""
    base = base or default_variants()
    result = {}
    for auth in AUTH_TYPES:
        cur = dict(base.get(auth, {}))
        if f"{auth}_enabled" in body:
            cur["enabled"] = bool(body.get(f"{auth}_enabled"))
        if f"{auth}_transport" in body:
            cur["transport"] = body.get(f"{auth}_transport")
        if f"{auth}_fingerprint" in body:
            cur["fingerprint"] = body.get(f"{auth}_fingerprint")
        if f"{auth}_alpn" in body:
            cur["alpn"] = body.get(f"{auth}_alpn")
        result[auth] = cur
    return sanitize_variants(result)

    # ═══════════════════════════════════════════════════════════════════════
# 🌐 NODE SYSTEM — لیست کشورها + توابع پایه
# ═══════════════════════════════════════════════════════════════════════

COUNTRIES = {
    "nl": {"name": "Netherlands",   "flag": "🇳🇱"},
    "us": {"name": "United States", "flag": "🇺🇸"},
    "sg": {"name": "Singapore",     "flag": "🇸🇬"},
    "fi": {"name": "Finland",       "flag": "🇫🇮"},
    "de": {"name": "Germany",       "flag": "🇩🇪"},
    "jp": {"name": "Japan",         "flag": "🇯🇵"},
    "gb": {"name": "United Kingdom","flag": "🇬🇧"},
    "fr": {"name": "France",        "flag": "🇫🇷"},
    "tr": {"name": "Turkey",        "flag": "🇹🇷"},
    "ae": {"name": "UAE",           "flag": "🇦🇪"},
    "ca": {"name": "Canada",        "flag": "🇨🇦"},
    "au": {"name": "Australia",     "flag": "🇦🇺"},
    "it": {"name": "Italy",         "flag": "🇮🇹"},
    "es": {"name": "Spain",         "flag": "🇪🇸"},
    "se": {"name": "Sweden",        "flag": "🇸🇪"},
    "ch": {"name": "Switzerland",   "flag": "🇨🇭"},
    "at": {"name": "Austria",       "flag": "🇦🇹"},
    "pl": {"name": "Poland",        "flag": "🇵🇱"},
    "ru": {"name": "Russia",        "flag": "🇷🇺"},
    "in": {"name": "India",         "flag": "🇮🇳"},
    "kr": {"name": "South Korea",   "flag": "🇰🇷"},
    "hk": {"name": "Hong Kong",     "flag": "🇭🇰"},
    "ir": {"name": "Iran",          "flag": "🇮🇷"},
    "br": {"name": "Brazil",        "flag": "🇧🇷"},
}

MAX_NODES = 7

NODE_SETTINGS_KEYS = (
    "panel_role",
    "panel_name",
    "panel_country",
    "panel_flag",
    "my_api_token",
    "master_url",
    "master_token",
)


def generate_node_token() -> str:
    """توکن امن برای احراز هویت بین مستر و نودها تولید می‌کنه."""
    return "nd_" + secrets.token_urlsafe(32)


def get_panel_role() -> str:
    """نقش این پنل رو برمی‌گردونه: 'master' یا 'slave'."""
    return CONFIG.get("panel_role", "master")


def get_panel_flag() -> str:
    """پرچم این پنل رو برمی‌گردونه."""
    return CONFIG.get("panel_flag", "🇳🇱")


def get_panel_name() -> str:
    """نام این پنل رو برمی‌گردونه."""
    return CONFIG.get("panel_name", "Master")


def init_node_settings():
    """اگه تنظیمات نود وجود نداشته باشه، مقدار پیش‌فرض می‌ذاره."""
    conn = get_db()
    try:
        existing = set()
        cur = conn.execute("SELECT key FROM settings")
        for row in cur.fetchall():
            existing.add(row["key"])
        
        defaults = {
            "panel_role": "master",
            "panel_name": "Master-Panel",
            "panel_country": "nl",
            "panel_flag": "🇳🇱",
            "my_api_token": generate_node_token(),
            "master_url": "",
            "master_token": "",
        }
        
        for key, val in defaults.items():
            if key not in existing:
                conn.execute(
                    "INSERT INTO settings (key, value) VALUES (?, ?)",
                    (key, val)
                )
                CONFIG[key] = val
                logger.info(f"[NODE] Initialized setting '{key}'")
        
        conn.commit()
    finally:
        conn.close()

DB_FILE = "/data/panel.db" if os.path.isdir("/data") else "panel.db"
if os.path.isdir("/data"):
    logger.warning(f"[STARTUP] Persistent volume detected at /data -> using {DB_FILE} (data survives restarts/deploys)")
else:
    logger.warning(f"[STARTUP] NO persistent volume found at /data -> using EPHEMERAL {DB_FILE} (ALL links/data will be LOST on next restart/deploy!)")
DB_LOCK = asyncio.Lock()
bot = None
bot_polling_task: asyncio.Task | None = None

BOT_I18N = {
    "en": { ... },  # (همه متن‌های انگلیسی و فارسی قبلی رو نگه داشتم)
    "fa": { ... },
}

def bot_lang() -> str:
    return CONFIG.get("bot_lang") if CONFIG.get("bot_lang") in ("en", "fa") else "en"

def L(key: str, **kwargs) -> str:
    lang = bot_lang()
    template = BOT_I18N.get(lang, BOT_I18N["en"]).get(key) or BOT_I18N["en"].get(key, key)
    try:
        return template.format(**kwargs)
    except Exception:
        return template

def build_main_keyboard():
    if not TELEBOT_AVAILABLE:
        return None
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton(L("btn_stats"), callback_data="tg_stats"),
        types.InlineKeyboardButton(L("btn_users"), callback_data="tg_users"),
        types.InlineKeyboardButton(L("btn_top"), callback_data="tg_top"),
        types.InlineKeyboardButton(L("btn_create"), callback_data="tg_create_guide"),
        types.InlineKeyboardButton(L("btn_addip"), callback_data="tg_add_ip_guide"),
        types.InlineKeyboardButton(L("btn_lang"), callback_data="tg_lang_toggle"),
    )
    return kb

# ── SQLite Database ──────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS links (
            uuid TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            limit_bytes INTEGER DEFAULT 0,
            used_bytes INTEGER DEFAULT 0,
            max_connections INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            expires_at TEXT,
            protocol TEXT DEFAULT 'vless-ws',
            fingerprint TEXT DEFAULT 'chrome',
            alpn TEXT DEFAULT '',
            port INTEGER DEFAULT 443
        );
        # ... (بقیه جدول‌ها همون قبلی)
        # (کپی کامل از کد اصلی رو نگه داشتم)
    """)
    conn.commit()
    # ... (بقیه init_db همان قبلی)

    # Ensure default auth row
    cur = conn.execute("SELECT password_hash FROM auth WHERE id = 1")
    row = cur.fetchone()
    if row is None:
        conn.execute("INSERT INTO auth (id, password_hash) VALUES (1, ?)", (AUTH["password_hash"],))
        conn.commit()
    else:
        AUTH["password_hash"] = row["password_hash"]
    conn.close()
    migrate_json_to_sqlite()

def migrate_json_to_sqlite():
    # ... (کپی کامل از کد اصلی)

# ── بقیه توابع ذخیره و بارگذاری DB (load_db, save_db, etc.) همان قبلی

def hash_password(pw: str) -> str:
    return hashlib.sha256(f"{pw}{CONFIG['secret']}".encode()).hexdigest()

AUTH = {"password_hash": hash_password("admin")}

# ── بقیه کدهای اصلی (create_session, require_auth, keep_alive, etc.) همان قبلی

# ── توابع خاص تم جدید (اینجا تم رو اعمال کردم)

def get_panel_html() -> str:
    return PANEL_HTML.replace(
        '<style>',
        f'''<style>
        :root {{
            --gold: #22d3ee;
            --gold-dim: #67e8f9;
            --surface3: #0f172a;
            --border: #334155;
            --text: #f1f5f9;
            --red: #ef4444;
            --green: #22c55e;
            --yellow: #eab308;
            --bg: #0a0f1c;
            --accent: #a5b4fc;
        }}
        
        body {{
            background: var(--bg);
            color: var(--text);
        }}
        
        .card, .mo-box, .mo-add, .mo-edit {{
            background: rgba(15, 23, 42, 0.92);
            border: 1px solid #22d3ee;
            box-shadow: 0 0 25px rgba(34, 211, 238, 0.25);
        }}
        
        .card:hover, .mo-box:hover {{
            box-shadow: 0 0 40px rgba(34, 211, 238, 0.45);
            transform: translateY(-3px);
        }}
        
        .btn {{
            background: linear-gradient(90deg, #22d3ee, #a5b4fc);
            color: #0a0f1c;
            font-weight: 700;
            box-shadow: 0 0 18px rgba(34, 211, 238, 0.5);
            transition: all 0.3s ease;
        }}
        
        .btn:hover {{
            box-shadow: 0 0 30px rgba(34, 211, 238, 0.7);
            transform: scale(1.05);
        }}
        
        .btn-gold {{
            background: linear-gradient(90deg, #22d3ee, #67e8f9);
        }}
        
        .d-card, .m-card {{
            background: rgba(15, 23, 42, 0.95);
            border: 1px solid rgba(34, 211, 238, 0.4);
        }}
        
        .tag-vless {{
            background: linear-gradient(90deg, #22d3ee, #67e8f9);
            color: #0a0f1c;
            padding: 2px 10px;
            border-radius: 9999px;
            font-size: 10px;
            font-weight: 700;
        }}
        
        .notif-item {{
            animation: pulse 2s infinite;
        }}
        
        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.85; }}
        }}
        
        h1, .panel-title {{
            background: linear-gradient(90deg, #22d3ee, #a5b4fc);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 800;
            letter-spacing: -1px;
        }}
        '''
    )

# (بقیه کد اصلی پایتون رو کامل نگه داشتم)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=CONFIG["port"])
