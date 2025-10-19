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
# ====== EIA ======
def get_eia_weekly():
    """
    EIA Weekly Petroleum Status (Crude Stocks, Production, Imports, Refinery Input)
    Кэш: 6 часов.
    """
    if not EIA_API_KEY:
        return {"error": "EIA_API_KEY missing"}

    cached = get_cache("eia")
    if cached:
        return cached

    try:
        # Фильтруем только основные нефтяные ряды
        url = (
            "https://api.eia.gov/v2/petroleum/wps/data/"
            f"?api_key={EIA_API_KEY}"
            "&frequency=weekly"
            "&data[0]=value"
            "&facets[series][]=PET.WCRSTUS1.W"  # Stocks
            "&facets[series][]=PET.WCRFPUS2.W"  # Production
            "&facets[series][]=PET.WCEIMUS2.W"  # Imports
            "&facets[series][]=PET.WPULEUS3.W"  # Refinery Input
            "&sort[0][column]=period&sort[0][direction]=desc"
            "&offset=0&length=4"
        )

        js = http_get(url).json()
        records = (js.get("response") or {}).get("data") or []

        if not records:
            return {"error": "No EIA records found"}

        # Готовим список ключевых показателей
        summary = []
        for r in records:
            summary.append({
                "period": r.get("period"),
                "series": r.get("series-description"),
                "value": r.get("value"),
                "units": r.get("units", ""),
            })

        out = {"period": records[0].get("period"), "summary": summary}
        set_cache("eia", out, 21600)  # 6 часов

        return out

    except Exception as e:
        return {"error": f"eia: {e}"}

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

def gpt_analyze(payload, prices):
    """
    Генерация торгового плана и сводки.
    Если API-ключа нет — вернём краткий rule-based план.
    """
    # Бэкап-логика без GPT (чтобы всегда был план)
    def rule_based():
        px = prices or {}
        wti = px.get("WTI")
        ch = px.get("WTI_change")
        dxy = px.get("DXY_change")
        # простая евкалиптика: WTI↑ & DXY↓ → BUY, иначе SELL, если нет данных → NEUTRAL
        if wti is None:
            rec = "NEUTRAL"
        else:
            score = (ch or 0) - (dxy or 0)
            rec = "BUY" if score > 0 else "SELL" if score < 0 else "NEUTRAL"
        # динамические таргет/стоп
        if wti:
            vol = max(abs(ch or 0), 0.6) / 100.0  # грубая «вола» от % изменения
            tgt = wti * (1 + (0.018 if rec == "BUY" else -0.018))  # ~1.8%
            stp = wti * (1 - (0.009 if rec == "BUY" else -0.009))  # ~0.9% в противоположную сторону
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

# ====== message formatting ======
def fmt_prices(pr):
    w = _num(pr.get("WTI"))
    wc = _pct(pr.get("WTI_change"))
    d = _num(pr.get("DXY"))
    dc = _pct(pr.get("DXY_change"))
    return (
        "💹 <b>DXY & WTI update</b>\n"
        f"🕒 {utc_now()}\n\n"
        f"🛢 WTI: <b>${w}</b> (24h {wc})\n"
        f"💵 DXY: <b>{d}</b> (24h {dc})"
    )

def fmt_summary(payload, analysis=None):
    lines = [f"🧾 <b>Oil Report: SUMMARY</b>", f"🕒 {utc_now()}"]

    # EIA
    e = (payload.get("eia") or {})
    rr = e.get("raw") or {}
    if rr:
        lines += [
            "\n📦 <b>EIA</b>",
            f"• Period: {rr.get('period','N/A')}",
            f"• Region: {rr.get('area-name','U.S. or PADD') or rr.get('area','N/A')}",
            f"• Product: {rr.get('product-name','N/A') or rr.get('product','N/A')}",
            f"• Value: {_num(rr.get('value'))} {rr.get('units','')}".strip(),
        ]

    # Baker
    b = payload.get("baker") or {}
    if b.get("snippet"):
        s = b["snippet"].strip()
        s = (s[:400] + "…") if len(s) > 400 else s
        lines += ["", "🏗️ <b>Baker Hughes</b>", s]

    # CFTC
    c = payload.get("cftc") or {}
    if c.get("snippet"):
        s = c["snippet"].strip()
        s = (s[:800] + "…") if len(s) > 800 else s
        lines += ["", "📊 <b>CFTC</b>", f"<code>{s}</code>"]

    # Macro
    m = payload.get("fred") or {}
    if m and "CPI" in m:
        lines += [
            "", "🏦 <b>Macro (FRED)</b>",
            f"• CPI: {_num(m.get('CPI'))}",
            f"• Fed Funds: {_num(m.get('FedRate'))}%",
        ]

    # Market
    p = payload.get("prices") or {}
    lines += [
        "", "💹 <b>Market</b>",
        f"🛢 WTI: ${_num(p.get('WTI'))} (24h {_pct(p.get('WTI_change'))})",
        f"💵 DXY: {_num(p.get('DXY'))} (24h {_pct(p.get('DXY_change'))})",
    ]

    if analysis:
        lines += ["", "🧠 <b>AI Analysis</b>", analysis]

    return "\n".join(lines)
    # ====== сбор данных ======
def collect(mode="summary"):
    mode = (mode or "summary").lower()
    data = {"timestamp": utc_now(), "mode": mode}

    jobs = []
    if mode in ("summary", "prices"): jobs.append(("prices", get_prices))
    if mode in ("summary", "eia"):    jobs.append(("eia", get_eia_weekly))
    if mode in ("summary", "baker"):  jobs.append(("baker", get_baker_hughes))
    if mode in ("summary", "cftc", "cot"):    jobs.append(("cftc", get_cftc))
    if mode in ("summary", "macro", "fred"):  jobs.append(("fred", get_fred))

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fn): key for key, fn in jobs}
        for f in concurrent.futures.as_completed(futs):
            k = futs[f]
            try:
                data[k] = f.result()
            except Exception as e:
                data[k] = {"error": f"{k}: {e}"}
    return data

def run_once(mode="summary", chat_id=None):
    payload = collect(mode)
    analysis = None
    if mode == "summary":
        analysis = gpt_analyze(payload, payload.get("prices") or {})
    msg = fmt_summary(payload, analysis)
    ok = send_telegram(msg, chat_id=chat_id)
    return {"ok": True, "sent": ok, "payload": payload, "analysis": analysis}

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
            send_telegram(f"EIA raw:\n<code>{json.dumps(e, ensure_ascii=False)}</code>", chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/baker"):
            b = get_baker_hughes()
            send_telegram(f"Baker raw:\n<code>{json.dumps(b, ensure_ascii=False)}</code>", chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/cot"):
            c = get_cftc()
            send_telegram(f"CFTC raw:\n<code>{(c.get('snippet') or '')[:3900]}</code>", chat_id=chat_id)
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
        
