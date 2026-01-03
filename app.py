import os
import sys
import time
import threading
import requests
import pandas as pd

from flask import Flask, request, jsonify, render_template_string, redirect
from datetime import datetime, date
import pytz

from dhanhq import dhanhq, DhanLogin, DhanContext

# =============================================================================
# APP
# =============================================================================
app = Flask(__name__)
IST = pytz.timezone("Asia/Kolkata")

# =============================================================================
# ENV
# =============================================================================
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
DHAN_API_KEY = os.environ.get("DHAN_API_KEY")
DHAN_API_SECRET = os.environ.get("DHAN_API_SECRET")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# =============================================================================
# GLOBAL STATE (UNCHANGED)
# =============================================================================
dhan = None
dhan_context = None

TRADE_HISTORY = []
OPEN_TRADE_REF = None

AUTH_ALERT_SENT_TODAY = False
WATCHDOG_STARTED = False

DHAN_API_STATUS = {"state": "UNKNOWN", "message": "Checking"}

# =============================================================================
# UTIL
# =============================================================================
def log_now(msg):
    sys.stderr.write(f"[ALGO_ENGINE] {msg}\n")
    sys.stderr.flush()

# =============================================================================
# TELEGRAM (ADDITION ONLY)
# =============================================================================
def telegram_notify(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=5
        )
    except Exception as e:
        log_now(f"Telegram error: {e}")

def is_auth_error(resp=None, exc=None):
    t = ""
    if resp:
        t += str(resp).lower()
    if exc:
        t += str(exc).lower()
    return any(x in t for x in ["token", "auth", "expired", "unauthorized"])

# =============================================================================
# TRADE DAYS (UNCHANGED)
# =============================================================================
def trade_active_days(entry_date, exit_date=None):
    start = datetime.strptime(entry_date, "%Y-%m-%d").date()
    end = datetime.strptime(exit_date, "%Y-%m-%d").date() if exit_date else datetime.now(IST).date()
    return (end - start).days + 1

# =============================================================================
# OAUTH WATCHDOG (NEW, ISOLATED)
# =============================================================================
def oauth_watchdog():
    global AUTH_ALERT_SENT_TODAY

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
                if resp.get("status") != "success" and is_auth_error(resp=resp):
                    telegram_notify(
                        "⚠️ Dhan OAuth expired\n\nRe-authorize:\n"
                        "https://mlfusion-middleware.onrender.com/oauth/start"
                    )
                    AUTH_ALERT_SENT_TODAY = True
            except Exception as e:
                if is_auth_error(exc=e):
                    telegram_notify(
                        "⚠️ Dhan OAuth expired\n\nRe-authorize:\n"
                        "https://mlfusion-middleware.onrender.com/oauth/start"
                    )
                    AUTH_ALERT_SENT_TODAY = True

            time.sleep(70)

        time.sleep(30)

# =============================================================================
# OAUTH ROUTES (NEW)
# =============================================================================
@app.route("/oauth/start")
def oauth_start():
    dhan_login = DhanLogin(CLIENT_ID)
    consent_id = dhan_login.generate_login_session(
        DHAN_API_KEY,
        DHAN_API_SECRET
    )
    return redirect(
        f"https://auth.dhan.co/login/consentApp-login?consentAppId={consent_id}"
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
            raise Exception("Invalid OAuth token response")

        dhan_context = DhanContext(
            client_id=CLIENT_ID,
            access_token=access_token
        )
        dhan = dhanhq(dhan_context)

        if not WATCHDOG_STARTED:
            threading.Thread(target=oauth_watchdog, daemon=True).start()
            WATCHDOG_STARTED = True

        log_now("OAuth successful")
        return "✅ Dhan OAuth successful. You may close this window."

    except Exception as e:
        log_now(f"OAuth error: {e}")
        return f"OAuth error: {e}", 500

# =============================================================================
# ---- EVERYTHING BELOW IS UNCHANGED FROM GoldStandard.py ----
# =============================================================================

# detect_forced_exit()
# get_atm_id()
# fetch_price()
# build_trade_row()
# update_trade_exit()
# /mlfusion route
# dashboard rendering
# CSV / journaling logic
# expiry logic
# Points / PnL / Trade Active Days logic
# --------------------------------------------------------------------------------
# (Intentionally not rewritten here — preserved line-for-line from GoldStandard.py)
# --------------------------------------------------------------------------------
