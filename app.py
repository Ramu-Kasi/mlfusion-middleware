import os
import sys
import time
import threading
import requests
import pandas as pd

from flask import Flask, request, jsonify, render_template_string, redirect
from datetime import datetime, date
import pytz

from dhanhq import dhanhq, DhanContext, DhanLogin

app = Flask(__name__)

# =============================================================================
# CONFIG
# =============================================================================
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
DHAN_API_KEY = os.environ.get("DHAN_API_KEY")
DHAN_API_SECRET = os.environ.get("DHAN_API_SECRET")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

IST = pytz.timezone("Asia/Kolkata")

# =============================================================================
# GLOBAL STATE (same as AllColumnsFrozen)
# =============================================================================
dhan = None
dhan_context = None

TRADE_HISTORY = []
OPEN_TRADE_REF = None

AUTH_ALERT_SENT_TODAY = False
WATCHDOG_STARTED = False

DHAN_API_STATUS = {
    "state": "UNKNOWN",
    "message": "Checking..."
}

# =============================================================================
# UTIL
# =============================================================================
def log_now(msg):
    sys.stderr.write(f"[ALGO_ENGINE] {msg}\n")
    sys.stderr.flush()

# =============================================================================
# TELEGRAM (NEW)
# =============================================================================
def notify_oauth_expired():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": (
                    "⚠️ Dhan OAuth expired\n\n"
                    "Re-authorize before market opens:\n"
                    "https://mlfusion-middleware.onrender.com/oauth/start"
                )
            },
            timeout=5
        )
    except Exception as e:
        log_now(f"Telegram error: {e}")

def is_auth_expired(resp=None, exc=None):
    txt = ""
    if resp:
        txt = str(resp).lower()
    if exc:
        txt = str(exc).lower()
    return any(k in txt for k in ["token", "auth", "expired", "unauthorized", "invalid"])

# =============================================================================
# TRADE ACTIVE DAYS (unchanged logic)
# =============================================================================
def calculate_trade_days(entry_date_str, exit_date_str=None):
    entry_date = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
    end_date = (
        datetime.strptime(exit_date_str, "%Y-%m-%d").date()
        if exit_date_str else datetime.now(IST).date()
    )
    return (end_date - entry_date).days + 1

# =============================================================================
# OAUTH WATCHDOG (SAFE)
# =============================================================================
def oauth_daily_check():
    global AUTH_ALERT_SENT_TODAY, dhan

    while True:
        now = datetime.now(IST)

        if now.hour == 0 and now.minute < 5:
            AUTH_ALERT_SENT_TODAY = False

        if dhan is None:
            time.sleep(30)
            continue

        if now.hour == 8 and now.minute == 45 and not AUTH_ALERT_SENT_TODAY:
            try:
                resp = dhan.get_profile()
                if resp.get("status") != "success" and is_auth_expired(resp=resp):
                    notify_oauth_expired()
                    AUTH_ALERT_SENT_TODAY = True
            except Exception as e:
                if is_auth_expired(exc=e):
                    notify_oauth_expired()
                    AUTH_ALERT_SENT_TODAY = True
            time.sleep(70)

        time.sleep(30)

# =============================================================================
# OAUTH
# =============================================================================
@app.route("/oauth/start")
def oauth_start():
    dhan_login = DhanLogin(CLIENT_ID)
    consent_app_id = dhan_login.generate_login_session(
        DHAN_API_KEY,
        DHAN_API_SECRET
    )
    return redirect(
        "https://auth.dhan.co/login/consentApp-login"
        f"?consentAppId={consent_app_id}"
    )

@app.route("/oauth/callback")
def oauth_callback():
    global dhan, dhan_context, WATCHDOG_STARTED

    try:
        token_id = request.args.get("token_id") or request.args.get("tokenId")
        if not token_id:
            return "OAuth failed: tokenId missing", 400

        dhan_login = DhanLogin(CLIENT_ID)
        token_resp = dhan_login.consume_token_id(
            token_id=token_id,
            app_id=DHAN_API_KEY,
            app_secret=DHAN_API_SECRET
        )

        access_token = token_resp.get("accessToken")
        if not access_token:
            raise Exception("Invalid OAuth response")

        dhan_context = DhanContext(
            client_id=CLIENT_ID,
            access_token=access_token
        )
        dhan = dhanhq(dhan_context)

        if not WATCHDOG_STARTED:
            threading.Thread(
                target=oauth_daily_check,
                daemon=True
            ).start()
            WATCHDOG_STARTED = True

        log_now("OAuth successful – token active")
        return "✅ Dhan OAuth successful. You may close this window."

    except Exception as e:
        log_now(f"OAuth error: {e}")
        return f"OAuth error: {str(e)}", 500

# =============================================================================
# API STATUS
# =============================================================================
def check_dhan_api_status():
    if dhan is None:
        DHAN_API_STATUS["state"] = "ERROR"
        DHAN_API_STATUS["message"] = "OAuth required"
        return

    try:
        resp = dhan.get_positions()
        if resp.get("status") == "success":
            DHAN_API_STATUS["state"] = "ACTIVE"
            DHAN_API_STATUS["message"] = "Active (OAUTH)"
        else:
            DHAN_API_STATUS["state"] = "ERROR"
            DHAN_API_STATUS["message"] = "API Error"
    except Exception:
        DHAN_API_STATUS["state"] = "ERROR"
        DHAN_API_STATUS["message"] = "API Error"

# =============================================================================
# 🔥 MLFUSION WEBHOOK — RESTORED VERBATIM
# =============================================================================
@app.route("/mlfusion", methods=["POST"])
def mlfusion():
    global OPEN_TRADE_REF
    data = request.get_json(force=True)
    msg = data.get("message", "").upper()
    price = float(data.get("price", 0))

    log_now(f"MLFUSION ALERT | {msg} @ {price}")

    detect_forced_exit()

    sec, strike, qty, expiry_used = get_atm_id(price, msg)
    if not sec:
        return jsonify({"error": "ATM not found"}), 400

    resp = dhan.place_order(
        security_id=sec,
        exchange="NFO",
        transaction_type="BUY" if msg == "BUY" else "SELL",
        quantity=qty,
        order_type="MARKET",
        product_type="M"
    )

    trade = build_trade_row(
        msg=msg,
        price=price,
        strike=strike,
        expiry_used=expiry_used,
        resp=resp
    )

    TRADE_HISTORY.append(trade)
    OPEN_TRADE_REF = trade if trade["Status"] == "OPEN" else None

    return jsonify({"status": "ok"})

# =============================================================================
# DASHBOARD
# =============================================================================
@app.route("/")
def dashboard():
    check_dhan_api_status()
    return render_template_string(
        DASHBOARD_HTML,
        history=TRADE_HISTORY,
        api_state=DHAN_API_STATUS["state"],
        api_message=DHAN_API_STATUS["message"],
        last_run=datetime.now(IST).strftime("%H:%M:%S")
    )

# =============================================================================
# DASHBOARD HTML — ALL COLUMNS
# =============================================================================
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="refresh" content="60">
<style>
body { font-family: Arial; background:#f5f6f7; padding:20px }
table { border-collapse: collapse; width:100%; }
th, td { border:1px solid #ccc; padding:6px; text-align:center; }
th { background:#222; color:white; }
.green { color:#0a7d0a; font-weight:bold }
.red { color:#b30000; font-weight:bold }
.status-ok { background:#28a745; color:white; padding:4px 10px; border-radius:10px }
.status-err { background:#f0ad4e; color:white; padding:4px 10px; border-radius:10px }
</style>
</head>
<body>

<div style="margin-bottom:10px">
<b>Dhan API:</b>
<span class="{{ 'status-ok' if api_state=='ACTIVE' else 'status-err' }}">
{{ api_message }}
</span>
&nbsp;&nbsp; Last Check: {{ last_run }} IST
</div>

<table>
<tr>
<th>Date</th><th>Time</th><th>Price</th><th>Strike</th><th>Type</th>
<th>Expiry Used</th><th>Lot</th><th>Premium</th>
<th>Entry</th><th>Exit</th>
<th>Points</th><th>PnL ₹</th><th>Trade Days</th>
<th>Status</th><th>Remarks</th>
</tr>

{% for row in history %}
<tr>
<td>{{ row.Date }}</td>
<td>{{ row.Time }}</td>
<td>{{ row.Price }}</td>
<td>{{ row.Strike }}</td>
<td>{{ row.Type }}</td>
<td>{{ row.Expiry }}</td>
<td>{{ row.Lot }}</td>
<td>{{ row.Premium }}</td>
<td>{{ row.Entry }}</td>
<td>{{ row.Exit }}</td>
<td>{{ row.Points }}</td>
<td class="{{ 'green' if row.PnL >= 0 else 'red' }}">{{ row.PnL }}</td>
<td>{{ row.TradeDays }}</td>
<td>{{ row.Status }}</td>
<td>{{ row.Remarks }}</td>
</tr>
{% endfor %}
</table>

</body>
</html>
"""

# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
