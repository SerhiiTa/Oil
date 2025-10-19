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
import re
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
REQUEST_TIMEOUT = 40
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
# ====== Baker Hughes ======
import re

def get_baker_hughes():
    """
    Сниппет со страницы https://rigcount.bakerhughes.com/
    Берём текст вокруг ключевых слов; кэш на сутки.
    Пытаемся вытащить U.S./Canada/International и их изменения.
    """
    cached = get_cache("baker")
    if cached:
        return cached

    try:
        html = http_get("https://rigcount.bakerhughes.com/").text
        soup = BeautifulSoup(html, "html.parser")
        txt = soup.get_text(" ", strip=True)

        # Нормализуем пробелы
        txt_norm = re.sub(r"\s+", " ", txt)

        # Ищем компактный фрагмент вокруг ключей
        anchors = ["U.S.", "Canada", "International", "Rig Count"]
        snippet = None
        for a in anchors:
            if a in txt_norm:
                i = txt_norm.find(a)
                snippet = txt_norm[max(0, i - 80): i + 320]
                break
        snippet = (snippet or txt_norm[:400]).strip()

        # Пытаемся вытащить числа и дельты
        # Пример шаблона: "U.S. 17 Oct 2025 548 +1"
        rec = {
            "us": None, "us_delta": None,
            "canada": None, "canada_delta": None,
            "intl": None, "intl_delta": None,
            "as_of": None
        }

        m_us = re.search(r"U\.S\.\s+(\d{1,2}\s+\w+\s+\d{4})\s+(\d+)\s+([+\-]\d+)", txt_norm)
        if m_us:
            rec["as_of"] = rec["as_of"] or m_us.group(1)
            rec["us"] = int(m_us.group(2)); rec["us_delta"] = int(m_us.group(3))

        m_ca = re.search(r"Canada\s+(\d{1,2}\s+\w+\s+\d{4})\s+(\d+)\s+([+\-]\d+)", txt_norm)
        if m_ca:
            rec["as_of"] = rec["as_of"] or m_ca.group(1)
            rec["canada"] = int(m_ca.group(2)); rec["canada_delta"] = int(m_ca.group(3))

        m_int = re.search(r"International\s+([A-Z][a-z]{2,9}\s+\d{4})\s+(\d+)\s+([+\-]\d+)", txt_norm)
        if m_int:
            # Международный блок часто помесячный (например, "Sept 2025")
            rec["intl"] = int(m_int.group(2)); rec["intl_delta"] = int(m_int.group(3))

        # Сентимент: ориентируемся на U.S. дельту, если есть
        if rec["us_delta"] is not None:
            if rec["us_delta"] > 0:
                sentiment = "🟥 Bearish — рост числа вышек может увеличить предложение нефти."
            elif rec["us_delta"] < 0:
                sentiment = "🟩 Bullish — сокращение вышек может сдержать предложение."
            else:
                sentiment = "⚪ Neutral — без изменения числа вышек."
        else:
            sentiment = "⚪ Neutral — данных по дельте недостаточно."

        out = {
            "snippet": snippet,
            "source": "Baker Hughes (Rig Count)",
            "as_of": rec["as_of"],
            "us": rec["us"], "us_delta": rec["us_delta"],
            "canada": rec["canada"], "canada_delta": rec["canada_delta"],
            "intl": rec["intl"], "intl_delta": rec["intl_delta"],
            "sentiment": sentiment,
        }
    except Exception as e:
        out = {"error": f"baker: {e}"}

    # Кэшируем на сутки (даже если парсинг частичный — пригодится)
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
# ===== FORMAT PRICES =====
def fmt_prices(pr):
    if not pr:
        return "💹 Market data unavailable."

    wti = pr.get("WTI")
    dxy = pr.get("DXY")
    wti_ch = pr.get("WTI_change")
    dxy_ch = pr.get("DXY_change")

    lines = [
        "💹 <b>Market Update</b>",
        f"🛢 WTI: ${_num(wti)} (24h {_pct(wti_ch)})",
        f"💵 DXY: {_num(dxy)} (24h {_pct(dxy_ch)})",
    ]
    return "\n".join(lines)
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
                "🛢 <b>Oil Analyzer Bot — команды</b>\n\n"
                "📊 <b>Основные отчёты:</b>\n"
                "• /summary — полный AI-отчёт (EIA, Baker, CFTC, Macro, Prices)\n"
                "• /prices — быстрый апдейт по WTI и DXY\n\n"
                "🧾 <b>Источники данных:</b>\n"
                "• /eia — последний отчёт EIA (Weekly Petroleum Status)\n"
                "• /baker — последние данные Baker Hughes (буровые установки)\n"
                "• /cot — короткий CFTC raw-срез\n"
                "• /cot_full — полный AI-анализ CFTC (Commitments of Traders)\n"
                "• /macro — макроэкономика (CPI, Fed Funds Rate)\n\n"
                "🤖 <b>AI Аналитика:</b>\n"
                "• Автоотчёт ежедневно в 08:00 America/Chicago (через Render CRON)\n"
                "• Используется модель <code>gpt-4o-mini</code> с низкой температурой (стабильный вывод)\n\n"
                "💬 <b>Советы:</b>\n"
                "— Команды можно вводить без регистра (/Summary = /summary)\n"
                "— Используй /cot_full раз в неделю для глубокого отчёта CFTC\n"
                "— /summary собирает всё воедино и делает торговый план."
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
# ====== AI (OpenAI) ======
def gpt_analyze(payload, prices):
    """
    Генерация торгового плана и сводки.
    Если API-ключа нет — вернём краткий rule-based план.
    """
    def rule_based():
        px = prices or {}
        wti = px.get("WTI")
        ch = px.get("WTI_change")
        dxy = px.get("DXY_change")
        if wti is None:
            rec = "NEUTRAL"
        else:
            score = (ch or 0) - (dxy or 0)
            rec = "BUY" if score > 0 else "SELL" if score < 0 else "NEUTRAL"
        if wti:
            vol = max(abs(ch or 0), 0.6) / 100.0
            tgt = wti * (1 + (0.018 if rec == "BUY" else -0.018))
            stp = wti * (1 - (0.009 if rec == "BUY" else -0.009))
            tgt = round(tgt, 2)
            stp = round(stp, 2)
        else:
            tgt = stp = None

        lines = [
            f"🔴 <b>EIA Oil Report Analysis</b>",
            f"🎯 <b>{rec}</b>",
            f"💰 Цена WTI: {('$'+_num(wti)) if wti else 'N/A'}",
            "",
            "<b>Торговый план:</b>",
            f"🎯 Цель: {('$'+_num(tgt)) if tgt else 'Не определена'}",
            f"⛔ Стоп: {('$'+_num(stp)) if stp else 'Не определен'}",
        ]
        return "\n".join(lines)

    if not OPENAI_API_KEY:
        return rule_based()

    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        prompt = (
            "Ты кратко и чётко анализируешь рынок нефти. Используй факты из блоков ниже и выдай:\n"
            "1) Рекомендацию BUY/SELL/NEUTRAL\n"
            "2) Торговый план: цель и стоп (динамические, опирайся на текущую цену WTI)\n"
            "3) 2–4 фактора (буллеты) по EIA/Baker/CFTC/Macro/Prices\n"
            "4) Короткий итог на 24–72 часа.\n\n"
            "Данные:\n"
            + json.dumps(payload, ensure_ascii=False)
        )

        msg = [
            {"role": "system", "content": "Ты дисциплинированный рыночный аналитик. Коротко, по делу."},
            {"role": "user", "content": prompt},
        ]
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=msg,
            temperature=0.25,
            max_tokens=600,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"GPT error: {e}"
# ====== FORMAT MAIN SUMMARY ======
def fmt_summary(payload, analysis=None):
    lines = [f"🧾 <b>Oil Report: SUMMARY</b>", f"🕒 {utc_now()}"]

    # ===== BAKER HUGHES =====
    baker = payload.get("baker") or {}

    # Если есть отдельный форматтер fmt_baker — используем его
    if 'fmt_baker' in globals():
        lines += ["", fmt_baker(baker)]
    else:
        snippet = baker.get("snippet")
        sentiment = baker.get("sentiment")
        if snippet:
            lines += [
                "\n🛠 <b>Baker Hughes Rig Count</b>",
                f"• {snippet[:300]}{'...' if len(snippet) > 300 else ''}",
            ]
            if sentiment:
                lines.append(sentiment)
        else:
            lines += ["\n🛠 <b>Baker Hughes:</b> данные не получены."]

    # ====== EIA ======
    eia = payload.get("eia") or {}
    if isinstance(eia, dict) and "raw" in eia:
        raw = eia["raw"]
        period = eia.get("period", "N/A")

        stocks_val = raw.get("stocks", ["N/A", ""])[0]
        imports_val = raw.get("imports", ["N/A", ""])[0]
        prod_val = raw.get("production", ["N/A", ""])[0]
        stocks_unit = raw.get("stocks", ["", ""])[1]
        imports_unit = raw.get("imports", ["", ""])[1]
        prod_unit = raw.get("production", ["", ""])[1]

        try:
            s_val = float(stocks_val)
            p_val = float(prod_val)
            if s_val > 820000 and p_val > 400:
                sentiment = "🟥 <b>Bearish:</b> High inventories & steady output may pressure prices."
            elif s_val < 780000 and p_val < 400:
                sentiment = "🟩 <b>Bullish:</b> Falling stocks & reduced output support upside."
            else:
                sentiment = "⚪ <b>Neutral:</b> Balanced crude market."
        except Exception:
            sentiment = "⚪ <b>Neutral:</b> Data incomplete."

        lines += [
            "\n📦 <b>EIA Weekly Crude Snapshot</b>",
            f"• Period: {period}",
            f"• Stocks: {_num(stocks_val)} {stocks_unit}",
            f"• Imports: {_num(imports_val)} {imports_unit}",
            f"• Production: {_num(prod_val)} {prod_unit}",
            f"{sentiment}",
        ]

    # ====== CFTC ======
    cftc_txt = payload.get("cftc_interpretation")
    if cftc_txt:
        lines += ["\n📊 <b>CFTC</b>", cftc_txt]

    # ====== MACRO ======
    fred = payload.get("fred") or {}
    if isinstance(fred, dict) and fred:
        lines += [
            "\n🏦 <b>Macro (FRED)</b>",
            f"• CPI: {_num(fred.get('CPI'))}",
            f"• Fed Funds: {_num(fred.get('FedRate'))}%",
        ]

    # ====== MARKET ======
    pr = payload.get("prices") or {}
    if isinstance(pr, dict) and pr:
        lines += [
            "\n💹 <b>Market</b>",
            f"🛢 WTI: ${_num(pr.get('WTI'))} (24h {_pct(pr.get('WTI_change'))})",
            f"💵 DXY: {_num(pr.get('DXY'))} (24h {_pct(pr.get('DXY_change'))})",
        ]

    # ====== AI ======
    if analysis:
        lines += [
            "\n🧠 <b>AI Analysis</b>",
            analysis,
        ]

    return "\n".join(lines)
# ====== RUN ======
# ====== RUN ONCE (MAIN SUMMARY BUILDER) ======
def run_once(mode="summary", chat_id=None):
    """
    Генерирует полный сводный отчёт (summary) или другие режимы при необходимости.
    """
    try:
        # Подтягиваем все данные
        eia = get_eia_weekly()
        baker = get_baker_hughes()
        cftc = get_cftc()
        fred = get_fred()
        prices = get_prices()

        # Формируем payload
        payload = {
            "eia": eia,
            "baker": baker,
            "cftc": cftc,
            "fred": fred,
            "prices": prices,
        }

        # добавляем быстрый анализ CFTC (если доступен snippet)
        payload["cftc_interpretation"] = analyze_cftc_snippet(cftc.get("snippet", ""))

        # Анализ AI
        analysis = gpt_analyze(payload, prices)

        # Формируем общий текстовый отчёт
        report = fmt_summary(payload, analysis=analysis)

        # Отправляем в Telegram, если есть chat_id
        if chat_id:
            send_telegram(report, chat_id=chat_id)

        return report

    except Exception as e:
        err_msg = f"❌ run_once error: {e}"
        if chat_id:
            send_telegram(err_msg, chat_id=chat_id)
        return err_msg
if __name__ == "__main__":
    if len(sys.argv) > 1:
        # локальный запуск: python app.py summary
        mode = sys.argv[1].lower()
        print(json.dumps(run_once(mode), ensure_ascii=False, indent=2))
    else:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")), debug=False)
        
