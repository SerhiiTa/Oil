import os
import json
import time
import requests
import concurrent.futures
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =============================
# ===  НАСТРОЙКИ  ============
# =============================

BOT_TOKEN = "8240240384:AAFPTwo0FYMz25IfBEJdmhN61Qqk4vchsuo"
CHAT_ID = None  # можно указать свой id, чтобы бот присылал автоотчёт

FRED_API_KEY = os.getenv("FRED_API_KEY", "")  # вставь ключ, если есть
REQUEST_TIMEOUT_API = 20
REQUEST_TIMEOUT_WEB = (5, 7)

# =============================
# ===  УТИЛИТЫ  ===============
# =============================

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

# кэш храним в памяти, TTL в секундах
_cache = {}

def get_cache(key: str):
    entry = _cache.get(key)
    if entry and entry["exp"] > time.time():
        return entry["data"]
    return None

def set_cache(key: str, data, ttl_sec: int = 600):
    _cache[key] = {"data": data, "exp": time.time() + ttl_sec}

# =============================
# ===  HTTP с таймаутами =====
# =============================

def http_get_api(url):
    return requests.get(url, timeout=REQUEST_TIMEOUT_API,
                        headers={"User-Agent": "oil-analyzer/2.0"})

def http_get_web(url):
    return requests.get(url, timeout=REQUEST_TIMEOUT_WEB,
                        headers={"User-Agent": "Mozilla/5.0 (OilAnalyzer)"})

def http_get(url):
    """универсальный запрос с ретраями"""
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.8,
                  status_forcelist=[429, 500, 502, 503, 504],
                  raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s.get(url, timeout=REQUEST_TIMEOUT_WEB,
                 headers={"User-Agent": "Mozilla/5.0 (OilAnalyzer)"})

# =============================
# ===  ФОРМАТЫ ===============
# =============================

def _fmt_num(val):
    try:
        return f"{float(val):.2f}"
    except Exception:
        return str(val) if val is not None else "N/A"

def _fmt_pct(val):
    try:
        return f"{float(val):+.2f}%"
    except Exception:
        return "+0.00%"

# =============================
# ===  TELEGRAM  =============
# =============================

def send_telegram(text: str):
    """Отправка сообщения в Telegram"""
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID or "@EIAOilSignalsBot", "text": text, "parse_mode": "Markdown"}
        r = http_get_api(url + "?" + "&".join([f"{k}={v}" for k,v in payload.items()]))
        return r.status_code
    except Exception as e:
        print("Telegram send error:", e)
