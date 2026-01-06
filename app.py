import os
import sys
import time
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime, date, timedelta
import pytz
import threading

app = Flask(__name__)

# ---------------- CONFIG ----------------
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

IST = pytz.timezone("Asia/Kolkata")
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# ---------------- TOKEN TIMING (NEW LOGIC) ----------------
# Token validity is assumed as 24 hours from app start time
TOKEN_VALIDITY_HOURS = 24
TOKEN_GENERATED_AT = datetime.now(IST)

def get_token_expiry_status():
    """
    Calculates how much time is left before token expiry.
    Used ONLY for dashboard visibility.
    """
    expiry_time = TOKEN_GENERATED_AT + timedelta(hours=TOKEN_VALIDITY_HOURS)
    remaining = expiry_time - datetime.now(IST)

    if remaining.total_seconds() <= 0:
        return {"state": "EXPIRED", "label": "Expired – Update Required"}
    elif remaining.total_seconds() <= 2 * 3600:
        hrs = remaining.seconds // 3600
        mins = (remaining.seconds % 3600) // 60
        return {"state": "SOON", "label": f"Expiring Soon ({hrs}h {mins}m)"}
    else:
        hrs = remaining.seconds // 3600
        mins = (remaining.seconds % 3600) // 60
        return {"state": "ACTIVE", "label": f"Expires in {hrs}h {mins}m"}

# ---------------- DATA ----------------
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

# ---------------- SCRIP MASTER ----------------
def load_scrip_master():
    global SCRIP_MASTER_DATA
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)

        inst = next(c for c in df.columns if "INSTRUMENT" in c.upper())
        sym  = next(c for c in df.columns if "SYMBOL" in c.upper())
        exp  = next(c for c in df.columns if "EXPIRY_DATE" in c.upper())

        mask = (
            df[inst].str.contains("OPTIDX", na=False) &
            df[sym].str.contains("BANKNIFTY", case=False, na=False) &
            ~df[sym].str.contains("BANKEX", case=False, na=False)
        )

        SCRIP_MASTER_DATA = df[mask].copy()
        SCRIP_MASTER_DATA[exp] = pd.to_datetime(SCRIP_MASTER_DATA[exp], errors="coerce")
        SCRIP_MASTER_DATA.dropna(subset=[exp], inplace=True)

        refresh_bn_expiries()
        log_now("Scrip master loaded/refreshed")

    except Exception as e:
        log_now(f"Scrip load error: {e}")

def periodic_scrip_refresh():
    while True:
        time.sleep(24 * 60 * 60)
        load_scrip_master()

threading.Thread(target=load_scrip_master, daemon=True).start()
threading.Thread(target=periodic_scrip_refresh, daemon=True).start()

# ---------------- EXPIRY ----------------
def refresh_bn_expiries():
    global BN_EXPIRIES
    exp = next(c for c in SCRIP_MASTER_DATA.columns if "EXPIRY_DATE" in c.upper())
    BN_EXPIRIES = sorted(SCRIP_MASTER_DATA[exp].unique())

def get_current_and_next_expiry():
    today = date.today()
    future = [e for e in BN_EXPIRIES if e.date() >= today]
    if len(future) >= 2:
        return future[0], future[1]
    if len(future) == 1:
        return future[0], future[0]
    return None, None

def get_active_expiry_details():
    curr, nxt = get_current_and_next_expiry()
    if not curr:
        return "—", None
    dte = (curr.date() - date.today()).days
    active = nxt if dte <= 5 else curr
    return active.strftime("%d-%b-%Y"), dte

# ---------------- ATM ----------------
def get_atm_id(price, signal):
    base = round(price / 100) * 100
    strike, opt = ((base - 100, "CE") if "BUY" in signal else (base + 100, "PE"))

    cols = SCRIP_MASTER_DATA.columns
    sc = next(c for c in cols if "STRIKE" in c.upper())
    tc = next(c for c in cols if "OPTION_TYPE" in c.upper())
    ec = next(c for c in cols if "EXPIRY_DATE" in c.upper())
    ic = next(c for c in cols if "SECURITY" in c.upper() or "TOKEN" in c.upper())

    curr, nxt = get_current_and_next_expiry()
    dte = (curr.date() - date.today()).days if curr else 99
    expiry = nxt if dte <= 5 else curr

    row = SCRIP_MASTER_DATA[
        (SCRIP_MASTER_DATA[sc] == strike) &
        (SCRIP_MASTER_DATA[tc] == opt) &
        (SCRIP_MASTER_DATA[ec] == expiry)
    ]

    if row.empty:
        return None, strike, 30, "—"

    return str(int(row.iloc[0][ic])), strike, 30, expiry.strftime("%d-%b-%Y")

# ---------------- PRICE ----------------
def fetch_price(sec_id=None):
    try:
        tb = dhan.get_trade_book()
        if tb.get("status") != "success":
            return None
        for t in reversed(tb["data"]):
            if sec_id is None or str(t["securityId"]) == str(sec_id):
                return float(t["tradedPrice"])
    except Exception:
        pass
    return None

# ---------------- API HEALTH ----------------
def check_dhan_api_status():
    try:
        resp = dhan.get_positions()
        if resp.get("status") == "success":
            DHAN_API_STATUS["state"] = "ACTIVE"
            DHAN_API_STATUS["message"] = "Active"
        else:
            DHAN_API_STATUS["state"] = "ERROR"
            DHAN_API_STATUS["message"] = "API Error"
    except Exception:
        DHAN_API_STATUS["state"] = "ERROR"
        DHAN_API_STATUS["message"] = "API Error"

# ---------------- EXIT OPPOSITE POSITION (NEW LOGIC) ----------------
def exit_opposite_position(expected_type):
    """
    Ensures any opposite position is closed BEFORE entering a new one.
    Returns True if safe to proceed, False if exit failed.
    """
    global OPEN_TRADE_REF

    if not OPEN_TRADE_REF or OPEN_TRADE_REF["type"] == expected_type:
        return True  # Nothing to exit

    log_now("Opposite position detected, attempting exit first")

    try:
        # Exit by selling existing position at market
        dhan.place_order(
            security_id=OPEN_TRADE_REF["security_id"],
            exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.SELL,
            quantity=OPEN_TRADE_REF["lot_size"],
            order_type=dhan.MARKET,
            product_type=dhan.MARGIN,
            price=0
        )

        time.sleep(1)  # Ensure exit happens before new entry

        OPEN_TRADE_REF["exit_price"] = fetch_price(OPEN_TRADE_REF["security_id"])
        OPEN_TRADE_REF["status"] = "CLOSED"
        OPEN_TRADE_REF["remarks"] = "EXIT ON REVERSAL"

        OPEN_TRADE_REF = None
        return True

    except Exception as e:
        log_now(f"Exit failed: {e}")
        return False

# ---------------- ROUTES ----------------
@app.route("/")
def dashboard():
    check_dhan_api_status()
    token_info = get_token_expiry_status()
    expiry, dte = get_active_expiry_details()

    return render_template_string(
        DASHBOARD_HTML,
        history=TRADE_HISTORY,
        api_state=DHAN_API_STATUS["state"],
        api_message=DHAN_API_STATUS["message"],
        token=token_info,
        active_expiry=expiry,
        expiry_danger=(dte is not None and dte <= 5),
        last_run=datetime.now(IST).strftime("%H:%M:%S")
    )

@app.route("/mlfusion", methods=["POST"])
def mlfusion():
    global OPEN_TRADE_REF

    data = request.get_json(force=True)
    msg = data.get("message", "").upper()
    price = float(data.get("price", 0))

    log_now(f"MLFUSION ALERT | {msg} @ {price}")

    expected_type = "CE" if "BUY" in msg else "PE"

    # NEW LOGIC: exit opposite position before entry
    if not exit_opposite_position(expected_type):
        return jsonify({"error": "Exit failed, trade blocked"}), 400

    sec, strike, qty, expiry_used = get_atm_id(price, msg)
    if not sec:
        return jsonify({"error": "ATM not found"}), 400

    resp = dhan.place_order(
        security_id=sec,
        exchange_segment=dhan.NSE_FNO,
        transaction_type=dhan.BUY,
        quantity=qty,
        order_type=dhan.MARKET,
        product_type=dhan.MARGIN,
        price=0
    )

    success = resp.get("status") == "success"

    trade = {
        "date": datetime.now(IST).strftime("%d-%b-%Y"),
        "time": datetime.now(IST).strftime("%H:%M:%S"),
        "price": price,
        "strike": strike,
        "type": expected_type,
        "expiry_used": expiry_used,
        "lot_size": qty,
        "entry_price": "—",
        "exit_price": "—",
        "status": "OPEN" if success else "REJECTED",
        "remarks": str(resp),
        "security_id": sec
    }

    if success:
        trade["entry_price"] = fetch_price(sec)
        OPEN_TRADE_REF = trade

    TRADE_HISTORY.insert(0, trade)
    return jsonify(trade), 200

# ---------------- UI ----------------
DASHBOARD_HTML = '''
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="refresh" content="60">
<style>
.status-active{background:#28a745;color:#fff;padding:2px 8px;border-radius:8px}
.status-soon{background:#f0ad4e;color:#fff;padding:2px 8px;border-radius:8px}
.status-expired{background:#d9534f;color:#fff;padding:2px 8px;border-radius:8px}
</style>
</head>
<body>

<b>Dhan API:</b> {{ api_message }}
&nbsp;&nbsp;
<b>Token:</b>
<span class="
{% if token.state=='ACTIVE' %}status-active
{% elif token.state=='SOON' %}status-soon
{% else %}status-expired{% endif %}
">
{{ token.label }}
</span>

</body>
</html>
'''

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
