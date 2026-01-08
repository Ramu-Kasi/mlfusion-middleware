import os
import sys
import time
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime, date
import pytz
import threading

app = Flask(__name__)

# ================= CONFIG =================
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

IST = pytz.timezone("Asia/Kolkata")
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# ================= GLOBALS =================
SCRIP_MASTER_DATA = None
BN_EXPIRIES = []
TRADE_HISTORY = []
OPEN_TRADE_REF = None

DHAN_API_STATUS = {"state": "UNKNOWN", "message": "Checking..."}

# ================= LOG =================
def log_now(msg):
    sys.stderr.write(f"[ALGO_ENGINE] {msg}\n")
    sys.stderr.flush()

# ================= SCRIP MASTER =================
def load_scrip_master():
    global SCRIP_MASTER_DATA, BN_EXPIRIES
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)

        inst = next(c for c in df.columns if "INSTRUMENT" in c.upper())
        sym  = next(c for c in df.columns if "SYMBOL" in c.upper())
        exp  = next(
            c for c in df.columns
            if "EXPIRY_DATE" in c.upper() or "EXPIRY" in c.upper()
        )

        mask = (
            df[inst].str.contains("OPTIDX", na=False) &
            df[sym].str.contains("BANKNIFTY", case=False, na=False) &
            ~df[sym].str.contains("BANKEX", case=False, na=False)
        )

        SCRIP_MASTER_DATA = df[mask].copy()
        SCRIP_MASTER_DATA[exp] = pd.to_datetime(
            SCRIP_MASTER_DATA[exp], errors="coerce"
        )
        SCRIP_MASTER_DATA.dropna(subset=[exp], inplace=True)

        BN_EXPIRIES = sorted(SCRIP_MASTER_DATA[exp].unique())

        log_now(f"✓ Scrip master loaded | BN Expiries: {len(BN_EXPIRIES)}")

    except Exception as e:
        log_now(f"✗ Scrip load error: {e}")

def periodic_scrip_refresh():
    while True:
        time.sleep(24 * 60 * 60)
        load_scrip_master()

# Load immediately + keep refreshing
load_scrip_master()
threading.Thread(target=periodic_scrip_refresh, daemon=True).start()

# ================= EXPIRY =================
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

# ================= ATM =================
def get_atm_id(price, signal):
    try:
        base = round(price / 100) * 100
        strike, opt = (base - 100, "CE") if "BUY" in signal else (base + 100, "PE")

        cols = SCRIP_MASTER_DATA.columns
        sc = next(c for c in cols if "STRIKE" in c.upper())
        tc = next(c for c in cols if "OPTION" in c.upper())
        ec = next(c for c in cols if "EXPIRY" in c.upper())
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

    except Exception as e:
        log_now(f"ATM error: {e}")
        return None, None, 30, "—"

# ================= PRICE =================
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

# ================= API HEALTH =================
def check_dhan_api_status():
    try:
        r = dhan.get_positions()
        if r.get("status") == "success":
            DHAN_API_STATUS.update(state="ACTIVE", message="Active")
        else:
            DHAN_API_STATUS.update(state="ERROR", message="API Error")
    except Exception:
        DHAN_API_STATUS.update(state="ERROR", message="API Error")

# ================= VERIFY CLOSE =================
def verify_position_closed(security_id, retries=2):
    for _ in range(retries):
        time.sleep(0.15)
        r = dhan.get_positions()
        if r.get("status") != "success":
            continue
        for p in r.get("data", []):
            if str(p.get("securityId")) == str(security_id):
                if p.get("netQty", 0) != 0:
                    return False
        return True
    return False

# ================= ACTUAL BN POSITIONS =================
def get_actual_bn_positions():
    try:
        r = dhan.get_positions()
        if r.get("status") != "success":
            return []
        out = []
        for p in r.get("data", []):
            net = p.get("netQty", 0)
            if net == 0:
                continue
            sym = p.get("tradingSymbol", "").upper()
            if "BANKNIFTY" not in sym or "BANKEX" in sym:
                continue
            t = "CE" if "CE" in sym else "PE" if "PE" in sym else None
            if t:
                out.append({
                    "type": t,
                    "security_id": str(p["securityId"]),
                    "quantity": abs(net),
                    "symbol": sym
                })
        return out
    except Exception:
        return []

# ================= ATOMIC SWITCH =================
def atomic_switch(expected_type):
    """
    Returns: ALLOW | BLOCK | ABORT
    """
    global OPEN_TRADE_REF

    actual = get_actual_bn_positions()

    if not actual:
        OPEN_TRADE_REF = None
        return "ALLOW"

    same_type = any(p["type"] == expected_type for p in actual)
    opposite = "PE" if expected_type == "CE" else "CE"
    opp_pos = next((p for p in actual if p["type"] == opposite), None)

    if same_type and not opp_pos:
        log_now(f"■ SAME DIRECTION {expected_type} already open → BLOCK")
        return "BLOCK"

    if opp_pos:
        r = dhan.place_order(
            security_id=opp_pos["security_id"],
            exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.SELL,
            quantity=opp_pos["quantity"],
            order_type=dhan.MARKET,
            product_type=dhan.MARGIN,
            price=0
        )

        if r.get("status") != "success":
            return "ABORT"

        if not verify_position_closed(opp_pos["security_id"]):
            return "ABORT"

        OPEN_TRADE_REF = None
        time.sleep(0.1)
        return "ALLOW"

    return "ALLOW"

# ================= ROUTES =================
@app.route("/")
def dashboard():
    check_dhan_api_status()
    expiry, dte = get_active_expiry_details()
    return render_template_string(
        DASHBOARD_HTML,
        history=TRADE_HISTORY,
        api_state=DHAN_API_STATUS["state"],
        api_message=DHAN_API_STATUS["message"],
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

    expected_type = "CE" if "BUY" in msg else "PE"
    decision = atomic_switch(expected_type)

    if decision == "BLOCK":
        return jsonify({"status": "ignored"}), 200
    if decision == "ABORT":
        return jsonify({"error": "Atomic switch failed"}), 400

    sec, strike, qty, expiry_used = get_atm_id(price, msg)
    if not sec:
        return jsonify({"error": "ATM not found"}), 400

    r = dhan.place_order(
        security_id=sec,
        exchange_segment=dhan.NSE_FNO,
        transaction_type=dhan.BUY,
        quantity=qty,
        order_type=dhan.MARKET,
        product_type=dhan.MARGIN,
        price=0
    )

    trade = {
        "date": datetime.now(IST).strftime("%d-%b-%Y"),
        "time": datetime.now(IST).strftime("%H:%M:%S"),
        "price": price,
        "strike": strike,
        "type": expected_type,
        "expiry_used": expiry_used,
        "lot_size": qty,
        "premium_paid": "—",
        "entry_price": "—",
        "exit_price": "—",
        "status": "OPEN" if r.get("status") == "success" else "REJECTED",
        "remarks": str(r),
        "security_id": sec
    }

    if trade["status"] == "OPEN":
        time.sleep(0.3)
        entry = fetch_price(sec)
        trade["entry_price"] = entry
        trade["premium_paid"] = round(entry * qty, 2) if entry else "—"
        OPEN_TRADE_REF = trade

    TRADE_HISTORY.insert(0, trade)
    return jsonify(trade), 200

# ================= DASHBOARD HTML =================
DASHBOARD_HTML = '''
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="refresh" content="60">
<style>
body{font-family:sans-serif;background:#f0f2f5;padding:20px}
.status-bar{background:#fff;padding:15px;border-radius:8px;display:flex;gap:20px;align-items:center}
.status-active{background:#28a745;color:#fff;padding:2px 10px;border-radius:10px}
.status-expired{background:#d9534f;color:#fff;padding:2px 10px;border-radius:10px}
.expiry-danger{color:#d9534f;font-weight:bold}
.journal-title{font-family:Georgia,serif;font-size:21px;color:#b08d57}
table{width:100%;border-collapse:collapse;background:#fff;margin-top:20px}
th{background:#333;color:#fff;padding:10px}
td{padding:10px;border-bottom:1px solid #eee}
</style>
</head>
<body>

<div class="status-bar">
<b>Dhan API:</b>
<span class="{% if api_state=='ACTIVE' %}status-active{% else %}status-expired{% endif %}">
{{ api_message }}
</span>

&nbsp;&nbsp;<b>Active BN Expiry:</b>
<span class="{{ 'expiry-danger' if expiry_danger else '' }}">{{ active_expiry }}</span>

<div class="journal-title" style="margin:0 auto;">Ramu's Magic Journal</div>
<div>Last Check: {{ last_run }} IST</div>
</div>

<table>
<tr>
<th>Date</th><th>Time</th><th>Price</th><th>Strike</th><th>Type</th>
<th>Expiry</th><th>Lot</th><th>Premium</th><th>Entry</th><th>Exit</th>
<th>Status</th><th>Remarks</th>
</tr>

{% for t in history %}
<tr>
<td>{{t.date}}</td><td>{{t.time}}</td><td>{{t.price}}</td><td>{{t.strike}}</td>
<td>{{t.type}}</td><td>{{t.expiry_used}}</td><td>{{t.lot_size}}</td>
<td>{{t.premium_paid}}</td><td>{{t.entry_price}}</td><td>{{t.exit_price}}</td>
<td>{{t.status}}</td><td>{{t.remarks}}</td>
</tr>
{% endfor %}
</table>

</body>
</html>
'''

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
