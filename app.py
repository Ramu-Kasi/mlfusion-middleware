import os
import sys
import time
import pandas as pd
from flask import Flask, request, jsonify, render_template_string, redirect
from dhanhq import dhanhq, DhanContext, DhanLogin
from datetime import datetime, date
import pytz
import threading
import requests

app = Flask(__name__)

# ---------------- CONFIG ----------------
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
DHAN_API_KEY = os.environ.get("DHAN_API_KEY")
DHAN_API_SECRET = os.environ.get("DHAN_API_SECRET")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

IST = pytz.timezone("Asia/Kolkata")
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# OAuth-only runtime objects
dhan = None
dhan_context = None

SCRIP_MASTER_DATA = None
BN_EXPIRIES = []

TRADE_HISTORY = []
OPEN_TRADE_REF = None

AUTH_ALERT_SENT_TODAY = False

DHAN_API_STATUS = {
    "state": "UNKNOWN",
    "message": "Checking..."
}

# ---------------- LOG ----------------
def log_now(msg):
    sys.stderr.write(f"[ALGO_ENGINE] {msg}\n")
    sys.stderr.flush()

# ---------------- TELEGRAM ----------------
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

# ---------------- 08:45 IST CHECK ----------------
def oauth_daily_check():
    global AUTH_ALERT_SENT_TODAY
    while True:
        now = datetime.now(IST)

        if now.hour == 0 and now.minute < 5:
            AUTH_ALERT_SENT_TODAY = False

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

threading.Thread(target=oauth_daily_check, daemon=True).start()

# ---------------- OAUTH ----------------
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
    global dhan, dhan_context
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

        log_now("OAuth successful – token active")
        return "✅ Dhan OAuth successful. You may close this window."

    except Exception as e:
        log_now(f"OAuth error: {e}")
        return f"OAuth error: {str(e)}", 500
