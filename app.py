# app.py
# ============================
#  OIL ANALYZER v7 (stable)
#  Источники: EIA, Baker Hughes, CFTC, Yahoo Finance, FRED
#  Команды: /help /summary /prices /eia /baker /cot /macro
#  Маршруты: /, /health, /data, /analyze, /cron/daily, /telegram
# ============================

import os
import sys
import json
import math
import time
import traceback
import concurrent.futures
from datetime import datetime, timezone, timedelta

import requests
import yfinance as yf
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request

# ====== ENV ======
# Все ключи только из переменных окружения!
EIA_API_KEY        = os.getenv("EIA_API_KEY", "")
FRED_API_KEY       = os.getenv("FRED_API_KEY", "")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# Сетевые настройки
REQUEST_TIMEOUT = 20
UA = {"User-Agent": "oil-analyzer/3.0 (+https://render.com)"}

# Память для простого кэша
CACHE = {}

app = Flask(__name__)

# ====== helpers ======
def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def http_get(url):
    return requests.get(url, timeout=REQUEST_TIMEOUT, headers=UA)

def http_post(url, payload):
    return requests.post(url, json=payload, timeout=REQUEST_TIMEOUT, headers=UA)

# ====== cache ======
def get_cache(key):
    row = CACHE.get(key)
    if not row:
        return None
    if datetime.now(timezone.utc) > row["ts"] + timedelta(seconds=row["ttl"]):
        return None
    return row["data"]

def set_cache(key, data, ttl_sec):
    CACHE[key] = {"ts": datetime.now(timezone.utc), "ttl": ttl_sec, "data": data}

# ====== formatting ======
def _num(x, nd=2, default="N/A"):
    try:
        return f"{float(x):,.{nd}f}"
    except Exception:
        return default

def _pct(x, nd=2):
    try:
        return f"{float(x):+.{nd}f}%"
    except Exception:
        return f"{0:+.{nd}f}%"

# ====== telegram ======
def send_telegram(html_text, chat_id=None):
    if not TELEGRAM_BOT_TOKEN:
        return False
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": html_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        r = http_post(url, payload)
        return r.ok
    except Exception:
        return False
        # ====== EIA ======
def get_eia_weekly():
    """
    Weekly EIA Petroleum Summary — Crude Oil (EPC0)
    Кэш: 6 часов.
    Возвращает красиво оформленный отчёт для Telegram.
    """
    if not EIA_API_KEY:
        return {"error": "EIA_API_KEY missing"}

    cached = get_cache("eia")
    if cached:
        return cached

    try:
        url = (
            "https://api.eia.gov/v2/petroleum/sum/sndw/data/"
            f"?api_key={EIA_API_KEY}"
            "&frequency=weekly"
            "&data[0]=value"
            "&facets[product][]=EPC0"
            "&sort[0][column]=period&sort[0][direction]=desc"
            "&offset=0&length=5"
        )

        js = http_get(url).json()
        records = (js.get("response") or {}).get("data") or []
        if not records:
            return {"error": "No EIA Crude Oil records found"}

        # ==== Извлечение ключевых показателей ====
        data = {}
        for r in records:
            sdesc = r.get("series-description", "")
            val = r.get("value")
            unit = r.get("units", "")
            if "Ending Stocks" in sdesc:
                data["stocks"] = (val, unit, sdesc)
            elif "Imports" in sdesc:
                data["imports"] = (val, unit, sdesc)
            elif "Production" in sdesc:
                data["production"] = (val, unit, sdesc)

        # ==== Формирование красивого отчёта ====
        period = records[0].get("period", "N/A")
        report = (
            f"🛢 **EIA Crude Oil Weekly Report ({period})**\n"
            f"──────────────────────────────\n"
        )

        if "stocks" in data:
            val, u, _ = data["stocks"]
            report += f"📦 **Stocks:** {val or 'N/A'} {u}\n"
        if "imports" in data:
            val, u, _ = data["imports"]
            report += f"🚢 **Imports:** {val or 'N/A'} {u}\n"
        if "production" in data:
            val, u, _ = data["production"]
            report += f"⚙️ **Production:** {val or 'N/A'} {u}\n"

        # ==== Аналитика (AI Summary) ====
        analysis = "\n📈 **AI Summary:**\n"
        try:
            stocks_val = float(data.get("stocks", [0])[0] or 0)
            prod_val = float(data.get("production", [0])[0] or 0)
            if stocks_val > 420000:
                analysis += "• High crude inventories may pressure prices slightly.\n"
            else:
                analysis += "• Lower inventories support a bullish tone.\n"
            if prod_val > 400:
                analysis += "• Production remains stable → balanced market.\n"
            else:
                analysis += "• Production decline supports upside potential.\n"
        except Exception:
            analysis += "• Not enough data for full evaluation.\n"

        # ==== Собираем итог ====
        report += analysis
        result = {
            "period": period,
            "raw": data,
            "report": report
        }

        set_cache("eia", result, 21600)
        return result

    except Exception as e:
        return {"error": f"EIA fetch error: {e}"}

# ====== Baker Hughes ======
def get_baker_hughes():
    """
    Сниппет со страницы https://rigcount.bakerhughes.com/
    Берём текст вокруг ключевых слов; кэш на сутки.
    """
    cached = get_cache("baker")
    if cached:
        return cached
    try:
        html = http_get("https://rigcount.bakerhughes.com/").text
        soup = BeautifulSoup(html, "html.parser")
        txt = soup.get_text(" ", strip=True)
        # Ищем быстрые маркеры
        anchors = ["U.S.", "Canada", "International", "Rig Count"]
        snippet = None
        for a in anchors:
            if a in txt:
                i = txt.find(a)
                snippet = txt[max(0, i - 80) : i + 300]
                break
        out = {"snippet": (snippet or txt[:400]).strip(), "source": "Baker Hughes (Rig Count)"}
    except Exception as e:
        out = {"error": f"baker: {e}"}
    set_cache("baker", out, 86400)
    return out

# ====== CFTC (Disaggregated Futures + Options) ======
CFTC_FUT = "https://www.cftc.gov/dea/futures/petroleum_lf.htm"
CFTC_FOP = "https://www.cftc.gov/dea/options/petroleum_lof.htm"

# Названия секций, которые точно есть в отчёте (по твоим скринам)
CFTC_KEYS = [
    "WTI–PHYSICAL", "WTI-PHYSICAL", "WTI PHYSICAL",
    "WTI FINANCIAL CRUDE OIL",
    "BRENT LAST DAY",
    "GASOLINE RBOB",
    "NY HARBOR ULSD",
    "WTI HOUSTON ARGUS/WIT TR MO",
    "WTI MIDLAND ARGUS VS WTI TRADE",
    "USGC HSFO (PLATTS)",
]

def _cftc_extract(txt):
    # Очищаем спец-переносы
    t = txt.replace("\r", "").replace("\x00", "")
    found = []
    for key in CFTC_KEYS:
        if key in t:
            idx = t.find(key)
            block = t[max(0, idx-140): idx+1200]
            found.append(block.strip())
    return "\n\n".join(found) if found else ""

def get_cftc():
    """
    Тянем 2 страницы (Futures Only + Futures&Options).
    Сливаем короткие выжимки по ключевым разделам.
    Кэш: сутки.
    """
    cached = get_cache("cftc")
    if cached:
        return cached
    try:
        res = []
        for url in (CFTC_FUT, CFTC_FOP):
            try:
                html = http_get(url).text
                soup = BeautifulSoup(html, "html.parser")
                txt = soup.get_text("\n", strip=True)
                chunk = _cftc_extract(txt)
                if chunk:
                    res.append(chunk)
            except Exception:
                continue
        snippet = "\n\n".join(res).strip() or "No petroleum sections parsed."
        out = {"snippet": snippet, "source": "CFTC Disaggregated (Fut/Fut+Opt)"}
    except Exception as e:
        out = {"error": f"cftc: {e}"}
    set_cache("cftc", out, 86400)
    return out

# ====== FRED (CPI, Fed Funds) ======
def get_fred():
    if not FRED_API_KEY:
        return {"error": "FRED_API_KEY missing"}
    cached = get_cache("fred")
    if cached:
        return cached
    try:
        u1 = f"https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL&api_key={FRED_API_KEY}&file_type=json"
        u2 = f"https://api.stlouisfed.org/fred/series/observations?series_id=FEDFUNDS&api_key={FRED_API_KEY}&file_type=json"
        cpi = http_get(u1).json()["observations"][-1]
        rate = http_get(u2).json()["observations"][-1]
        out = {"CPI": float(cpi["value"]), "CPI_date": cpi["date"], "FedRate": float(rate["value"]), "Rate_date": rate["date"]}
        set_cache("fred", out, 43200)
        return out
    except Exception as e:
        return {"error": f"fred: {e}"}

# ====== Yahoo Finance (WTI & DXY) ======
def _last_close_series(ticker, period="3d", interval="1h"):
    """
    Стабильный способ получить последнюю и предыдущую свечу.
    Сначала Ticker().history, затем fallback на yf.download.
    """
    try:
        h = yf.Ticker(ticker).history(period=period, interval=interval)
        if h is None or h.dropna().empty:
            h = yf.download(ticker, period=period, interval=interval, progress=False)
        h = h.dropna()
        if len(h) >= 2:
            return float(h["Close"].iloc[-1]), float(h["Close"].iloc[-2])
        if len(h) == 1:
            v = float(h["Close"].iloc[-1])
            return v, v
    except Exception:
        pass
    return None, None

def get_prices():
    """
    Возвращает {'WTI', 'WTI_change', 'DXY', 'DXY_change'}.
    DXY пробуем по ^DXY, затем DX-Y.NYB, затем ETF UUP.
    Кэш: 10 минут.
    """
    cached = get_cache("prices")
    if cached:
        # возврат сразу, если свежие данные (<10 мин)
        return cached

    out = {"WTI": None, "WTI_change": None, "DXY": None, "DXY_change": None, "source": "Yahoo Finance"}

    w_last, w_prev = _last_close_series("CL=F")
    # цепочка фоллбеков по DXY
    d_last, d_prev = _last_close_series("^DXY")
    if d_last is None:
        d_last, d_prev = _last_close_series("DX-Y.NYB")
    if d_last is None:
        d_last, d_prev = _last_close_series("UUP")  # ETF Dollar Index

    if w_last is not None and w_prev is not None:
        out["WTI"] = round(w_last, 2)
        out["WTI_change"] = round((w_last - w_prev) / w_prev * 100, 2) if w_prev else 0.0
    if d_last is not None and d_prev is not None:
        out["DXY"] = round(d_last, 2)
        out["DXY_change"] = round((d_last - d_prev) / d_prev * 100, 2) if d_prev else 0.0

    # если совсем ничего — вернуть последний кэш, если есть
    if (out["WTI"] is None and out["DXY"] is None) and cached:
        return cached

    set_cache("prices", out, 600)
    return out
    # ====== AI (OpenAI) ======
# По умолчанию используем gpt-4o-mini — быстрее и дешевле.
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ====== CFTC ANALYSIS MODULE (ADD-ON) ======
import re

def analyze_cftc_snippet(snippet: str) -> str:
    """
    Мини-анализ CFTC для краткого дневного отчёта.
    Извлекает базовые числа (Producers, Managed Money Long/Short) и делает быструю интерпретацию.
    """
    if not snippet:
        return "CFTC: данные отсутствуют."

    # Ищем первые три числа в тексте (обычно это producers, long, short)
    nums = re.findall(r"([\d\.]+)", snippet)
    if len(nums) < 3:
        return "CFTC: не удалось извлечь позиции."

    try:
        producers = float(nums[0])
        money_long = float(nums[1])
        money_short = float(nums[2])
        diff = money_long - money_short

        if diff > 10:
            sentiment = "🟩 Bullish — фонды наращивают длинные позиции."
        elif diff < -10:
            sentiment = "🟥 Bearish — фонды увеличивают короткие позиции."
        else:
            sentiment = "⚪ Neutral — баланс длинных и коротких позиций."

        return (
            f"📊 <b>CFTC Snapshot</b>\n"
            f"• Producers: {producers:.1f}%\n"
            f"• Managed Money (Long): {money_long:.1f}%\n"
            f"• Managed Money (Short): {money_short:.1f}%\n"
            f"{sentiment}"
        )
    except Exception as e:
        return f"CFTC parsing error: {e}"


def gpt_analyze_cftc(full_text: str) -> str:
    """
    Глубокий анализ полного еженедельного отчёта CFTC.
    GPT сам делает вывод о динамике длинных/коротких позиций и рыночных настроениях.
    """
    if not OPENAI_API_KEY:
        return "⚠️ Нет ключа OpenAI. GPT-анализ недоступен."

    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

    prompt = (
        "Ты опытный аналитик рынка нефти. Проанализируй этот полный текст отчёта CFTC "
        "(Commitments of Traders) по нефти WTI.\n"
        "Выдели ключевые изменения позиций Managed Money и Producers по сравнению с прошлой неделей, "
        "сделай вывод о настроении рынка (Bullish / Bearish / Neutral) и краткий прогноз на неделю.\n\n"
        "Отчёт:\n" + full_text
    )

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты аналитик CFTC. Пиши коротко и по делу, в виде отчёта."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=600,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"GPT error: {e}"

# ====== ROUTES ======
@app.route("/")
def index():
    return jsonify({"ok": True, "message": "Oil Analyzer Bot is running", "time": utc_now()})

@app.route("/health")
def health():
    return jsonify({"ok": True, "time": utc_now()})

@app.route("/data")
def data_endpoint():
    mode = request.args.get("mode", "summary")
    return jsonify(collect(mode))

@app.route("/analyze")
def analyze_endpoint():
    mode = request.args.get("mode", "summary")
    return jsonify(run_once(mode))

@app.route("/cron/daily")
def cron_daily():
    """
    Отправка автоотчёта. По условию: ежедневно в 08:00 America/Chicago.
    Рендер-триггер: настроить CRON в Render на 13:00 UTC (летом) / 14:00 UTC (зимой) или просто дергать этот URL.
    """
    res = run_once("summary", chat_id=TELEGRAM_CHAT_ID)
    return jsonify({"ok": True, "result": res})

# ====== TELEGRAM WEBHOOK ======
@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    try:
        upd = request.get_json(force=True, silent=True) or {}
        msg = upd.get("message") or upd.get("edited_message") or {}
        chat_id = str(msg.get("chat", {}).get("id", "")) or TELEGRAM_CHAT_ID
        text = (msg.get("text") or "").strip().lower()

        if text in ("/start", "/help"):
            help_txt = (
                "🛢 <b>Oil Analyzer Bot</b>\n\n"
                "Команды:\n"
                "• /summary — полный отчёт (EIA, Baker, CFTC, Macro, Market, AI)\n"
                "• /prices — быстрый апдейт WTI & DXY\n"
                "• /eia — последний weekly-срез EIA\n"
                "• /baker — сниппет Baker Hughes\n"
                "• /cot — CFTC petroleum (disaggregated) сниппет\n"
                "• /macro — CPI & FedRate (FRED)\n"
                "\n⏰ Автоотчёт ежедневно в 08:00 America/Chicago (через Render CRON)."
            )
            send_telegram(help_txt, chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/prices"):
            pr = get_prices()
            send_telegram(fmt_prices(pr), chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/summary"):
            res = run_once("summary", chat_id=chat_id)
            return jsonify({"ok": True, "result": res})

        if text.startswith("/eia"):
            e = get_eia_weekly()
            if "error" in e:
                send_telegram(f"⚠️ {e['error']}", chat_id=chat_id)
            else:
                send_telegram(e.get("report", "No EIA data available."), chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/baker"):
            b = get_baker_hughes()
            send_telegram(f"Baker raw:\n<code>{json.dumps(b, ensure_ascii=False)}</code>", chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/cot"):
            c = get_cftc()
            send_telegram(f"CFTC raw:\n<code>{(c.get('snippet') or '')[:3900]}</code>", chat_id=chat_id)
            return jsonify({"ok": True})
        if text.startswith("/cot_full"):
            c = get_cftc()  # уже существующая функция, которая достаёт файл или snippet
            snippet = c.get("snippet") or ""
            send_telegram("⌛ Анализирую полный отчёт CFTC...", chat_id=chat_id)

            # запускаем GPT-анализ
            ai_report = gpt_analyze_cftc(snippet)
            send_telegram(ai_report, chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/macro"):
            m = get_fred()
            send_telegram(f"FRED raw:\n<code>{json.dumps(m, ensure_ascii=False)}</code>", chat_id=chat_id)
            return jsonify({"ok": True})

        send_telegram("Неизвестная команда. Введите /help", chat_id=chat_id)
        return jsonify({"ok": True})

    except Exception as e:
        # не роняем вебхук — всегда 200
        send_telegram(f"Internal error:\n<code>{traceback.format_exc()[:1500]}</code>")
        return jsonify({"ok": False, "error": str(e)}), 200

# ====== RUN ======
if __name__ == "__main__":
    if len(sys.argv) > 1:
        # локальный запуск: python app.py summary
        mode = sys.argv[1].lower()
        print(json.dumps(run_once(mode), ensure_ascii=False, indent=2))
    else:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")), debug=False)
        
