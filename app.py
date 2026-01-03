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

AUTH_MODE = "OAUTH"
AUTH_STATUS = "OK"

IST = pytz.timezone("Asia/Kolkata")

# -------- INITIAL EMPTY CONTEXT (OAuth only) --------
dhan_context = None
dhan = None

# ---------------- STATE ----------------
AUTH_ALERT_SENT_TODAY = False

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None
BN_EXPIRIES = []

TRADE_HISTORY = []
OPEN_TRADE_REF = None

DHAN_API_STATUS = {
    "state": "UNKNOWN",
    "message": "Checking..."
}

# ---------------- LOG ----------------
def log_now(msg):
    sys.stderr.write(f"[ALGO_ENGINE] {msg}\n")
    sys.stderr.flush()

# ---------------- NOTIFICATION ----------------
def notify_oauth_expired():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log_now("Telegram credentials missing. Skipping notification.")
        return

    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": (
                    "⚠️ Dhan OAuth expired\n\n"
                    "Please re-authorize before market:\n"
                    "https://mlfusion-middleware.onrender.com/oauth/start"
                )
            },
            timeout=5
        )
    except Exception as e:
        log_now(f"Telegram notify error: {e}")

def is_auth_expired(resp=None, exc=None):
    text = ""
    if resp:
        text = str(resp).lower()
    if exc:
        text = str(exc).lower()

    return any(k in text for k in [
        "token",
        "auth",
        "expired",
        "unauthorized",
        "invalid"
    ])

# ---------------- DAILY 8:45 CHECK ----------------
def oauth_daily_check():
    global AUTH_ALERT_SENT_TODAY

    while True:
        now = datetime.now(IST)

        # Reset flag shortly after midnight
        if now.hour == 0 and now.minute < 5:
            AUTH_ALERT_SENT_TODAY = False

        # Run ONLY at 08:45 IST
        if now.hour == 8 and now.minute == 45 and not AUTH_ALERT_SENT_TODAY:
            try:
                resp = dhan.get_profile()
                if resp.get("status") != "success":
                    if is_auth_expired(resp=resp):
                        notify_oauth_expired()
                        AUTH_ALERT_SENT_TODAY = True
            except Exception as e:
                if is_auth_expired(exc=e):
                    notify_oauth_expired()
                    AUTH_ALERT_SENT_TODAY = True

            time.sleep(70)  # avoid double trigger

        time.sleep(30)

threading.Thread(target=oauth_daily_check, daemon=True).start()

# ---------------- OAUTH ROUTES ----------------
@app.route("/oauth/start")
def oauth_start():
    dhan_login = DhanLogin(CLIENT_ID)

    consent_app_id = dhan_login.generate_login_session(
        DHAN_API_KEY,
        DHAN_API_SECRET
    )

    consent_url = (
        "https://auth.dhan.co/login/consentApp-login"
        f"?consentAppId={consent_app_id}"
    )

    log_now(f"Redirecting to Dhan OAuth: {consent_url}")
    return redirect(consent_url)

@app.route("/oauth/callback")
def oauth_callback():
    global dhan, dhan_context, AUTH_MODE, AUTH_STATUS

    try:
        token_id = request.args.get("token_id") or request.args.get("tokenId")
        if not token_id:
            return "OAuth failed: token_id missing", 400

        dhan_login = DhanLogin(CLIENT_ID)

        token_response = dhan_login.consume_token_id(
            token_id=token_id,
            app_id=DHAN_API_KEY,
            app_secret=DHAN_API_SECRET
        )

        access_token = token_response.get("accessToken")
        if not access_token:
            raise Exception(f"Invalid token response: {token_response}")

        dhan_context = DhanContext(
            client_id=CLIENT_ID,
            access_token=access_token
        )

        dhan = dhanhq(dhan_context)

        AUTH_MODE = "OAUTH"
        AUTH_STATUS = "OK"

        log_now("OAuth successful – switched to OAuth token")
        return "✅ Dhan OAuth successful. You may close this window."

    except Exception as e:
        AUTH_STATUS = "FAILED"
        log_now(f"OAuth error: {e}")
        return f"OAuth error: {str(e)}", 500

# ---------------- API HEALTH ----------------
def check_dhan_api_status():
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

# ---------------- DASHBOARD ----------------
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

# ---------------- UI ----------------
DASHBOARD_HTML = '''
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="refresh" content="60">
<style>
body{font-family:sans-serif;background:#f0f2f5;padding:20px}
.status-active{background:#28a745;color:#fff;padding:2px 10px;border-radius:10px}
.status-error{background:#f0ad4e;color:#fff;padding:2px 10px;border-radius:10px}
</style>
</head>
<body>

<div>
<b>Dhan API:</b>
<span class="{{ 'status-active' if api_state=='ACTIVE' else 'status-error' }}">
{{ api_message }}
</span>
&nbsp;&nbsp;
Last Check: {{ last_run }} IST
</div>

</body>
</html>
'''

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
