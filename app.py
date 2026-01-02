import os
import sys
import time
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime
import pytz
import threading

app = Flask(__name__)

# --- CONFIGURATION ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None
BN_EXPIRIES = []

TRADE_HISTORY = []
OPEN_TRADE_REF = None

IST = pytz.timezone("Asia/Kolkata")

def log_now(msg):
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

# --- LOAD SCRIP MASTER ---
def load_scrip_master():
    global SCRIP_MASTER_DATA
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)

        inst_col = next(c for c in df.columns if 'INSTRUMENT' in c.upper())
        sym_col  = next(c for c in df.columns if 'SYMBOL' in c.upper())
        exch_col = next((c for c in df.columns if 'EXCHANGE' in c.upper()), None)
        exp_col  = next(c for c in df.columns if 'EXPIRY_DATE' in c.upper())

        mask = (
            df[inst_col].str.contains('OPTIDX', na=False) &
            df[sym_col].str.contains('BANKNIFTY', case=False, na=False) &
            ~df[sym_col].str.contains('BANKEX', case=False, na=False)
        )

        if exch_col:
            mask &= df[exch_col].str.contains('NSE', case=False, na=False)

        SCRIP_MASTER_DATA = df[mask].copy()
        SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(
            SCRIP_MASTER_DATA[exp_col], errors='coerce'
        )
        SCRIP_MASTER_DATA.dropna(subset=[exp_col], inplace=True)

        refresh_bn_expiries()
        log_now("Scrip master loaded / refreshed")

    except Exception as e:
        log_now(f"SCRIP LOAD ERROR: {e}")

threading.Thread(target=load_scrip_master, daemon=True).start()

# --- DAILY AUTO REFRESH (24 HOURS) ---
def periodic_scrip_refresh():
    while True:
        time.sleep(24 * 60 * 60)
        load_scrip_master()

threading.Thread(target=periodic_scrip_refresh, daemon=True).start()

# --- EXPIRY UTILITIES ---
def refresh_bn_expiries():
    global BN_EXPIRIES
    try:
        exp_col = next(c for c in SCRIP_MASTER_DATA.columns if 'EXPIRY_DATE' in c.upper())
        BN_EXPIRIES = sorted(SCRIP_MASTER_DATA[exp_col].unique())
    except Exception as e:
        log_now(f"EXPIRY REFRESH ERROR: {e}")

def get_current_and_next_expiry():
    today = datetime.now(IST).date()
    future = [e for e in BN_EXPIRIES if e.date() >= today]

    if len(future) >= 2:
        return future[0], future[1]
    elif len(future) == 1:
        return future[0], future[0]
    return None, None

def get_active_expiry_details():
    curr, nxt = get_current_and_next_expiry()
    if not curr:
        return "—", None

    today = datetime.now(IST).date()
    dte = (curr.date() - today).days
    active = nxt if dte <= 5 else curr

    return active.strftime("%d-%b-%Y"), dte

# --- ATM CONTRACT SELECTION ---
def get_atm_id(price, signal):
    try:
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty:
            return None, None, 30, "—"

        base = round(price / 100) * 100
        strike, opt_type = (base - 100, "CE") if "BUY" in signal else (base + 100, "PE")

        cols = SCRIP_MASTER_DATA.columns
        strike_col = next(c for c in cols if 'STRIKE' in c.upper())
        type_col   = next(c for c in cols if 'OPTION_TYPE' in c.upper())
        exp_col    = next(c for c in cols if 'EXPIRY_DATE' in c.upper())
        id_col     = next(c for c in cols if 'SECURITY' in c.upper() or 'TOKEN' in c.upper())

        curr, nxt = get_current_and_next_expiry()
        today = datetime.now(IST).date()

        selected = curr
        if curr:
            dte = (curr.date() - today).days
            if dte <= 5 and nxt:
                selected = nxt

        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) &
            (SCRIP_MASTER_DATA[type_col] == opt_type) &
            (SCRIP_MASTER_DATA[exp_col] == selected)
        ]

        if not match.empty:
            return (
                str(int(match.iloc[0][id_col])),
                strike,
                30,
                selected.strftime("%d-%b-%Y")
            )

        return None, strike, 30, "—"

    except Exception as e:
        log_now(f"ATM ERROR: {e}")
        return None, None, 30, "—"

# --- PRICE FETCH ---
def fetch_price(security_id):
    try:
        time.sleep(0.6)
        tb = dhan.get_trade_book()
        if tb.get('status') != 'success':
            return None
        for t in reversed(tb['data']):
            if str(t['securityId']) == str(security_id):
                return float(t['tradedPrice'])
    except Exception:
        return None

# --- SURGICAL REVERSAL ---
def surgical_reversal(signal):
    global OPEN_TRADE_REF
    try:
        pos = dhan.get_positions()
        if pos.get('status') != 'success':
            return

        for p in pos['data']:
            if "BANKNIFTY" in p['tradingSymbol'] and int(p['netQty']) != 0:
                dhan.place_order(
                    security_id=p['securityId'],
                    exchange_segment=p['exchangeSegment'],
                    transaction_type=dhan.SELL,
                    quantity=abs(int(p['netQty'])),
                    order_type=dhan.MARKET,
                    product_type=dhan.MARGIN,
                    price=0
                )

                if OPEN_TRADE_REF:
                    OPEN_TRADE_REF['exit_price'] = fetch_price(p['securityId']) or "—"
                    OPEN_TRADE_REF['status'] = "CLOSED"
                    OPEN_TRADE_REF = None
                return
    except Exception:
        return

# --- ROUTES ---
@app.route('/')
def dashboard():
    now = datetime.now(IST).strftime("%H:%M:%S")
    expiry, dte = get_active_expiry_details()
    danger = dte is not None and dte <= 5

    return render_template_string(
        DASHBOARD_HTML,
        history=TRADE_HISTORY,
        last_run=now,
        active_expiry=expiry,
        expiry_danger=danger
    )

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    global OPEN_TRADE_REF
    data = request.get_json(force=True)
    msg = data['message'].upper()
    price = float(data['price'])

    surgical_reversal(msg)

    sec_id, strike, qty, expiry_used = get_atm_id(price, msg)

    order_resp = dhan.place_order(
        security_id=sec_id,
        exchange_segment=dhan.NSE_FNO,
        transaction_type=dhan.BUY,
        quantity=qty,
        order_type=dhan.MARKET,
        product_type=dhan.MARGIN,
        price=0
    )

    success = order_resp.get("status") == "success"

    trade = {
        "time": datetime.now(IST).strftime("%H:%M:%S"),
        "price": price,
        "strike": strike,
        "type": "CE" if "BUY" in msg else "PE",
        "expiry_used": expiry_used,
        "lot_size": qty,
        "premium_paid": "—",
        "entry_price": "—",
        "exit_price": "—",
        "status": "OPEN" if success else "REJECTED",
        "remarks": str(order_resp)
    }

    if success:
        entry = fetch_price(sec_id)
        trade["entry_price"] = entry or "—"
        trade["premium_paid"] = round(entry * qty, 2) if entry else "—"
        OPEN_TRADE_REF = trade

    TRADE_HISTORY.insert(0, trade)
    return jsonify(trade), 200

# --- UI ---
DASHBOARD_HTML = """<!DOCTYPE html>
<html>
<head>
<title>MLFusion Jan2-2026</title>
<meta http-equiv="refresh" content="60">
<style>
body{font-family:sans-serif;background:#f0f2f5;padding:20px}
.status-bar{background:#fff;padding:15px;border-radius:8px;display:flex;gap:25px;box-shadow:0 1px 3px rgba(0,0,0,.1);margin-bottom:20px}
.status-active{background:#28a745;color:#fff;padding:2px 10px;border-radius:10px;font-weight:bold}
.expiry-safe{color:#222;font-weight:bold}
.expiry-danger{color:#d9534f;font-weight:bold}
table{width:100%;border-collapse:collapse;background:#fff;border-radius:8px}
th{background:#333;color:#fff;padding:12px;text-align:left}
td{padding:12px;border-bottom:1px solid #eee}
.ce-text{color:#28a745;font-weight:bold}
.pe-text{color:#d9534f;font-weight:bold}
</style>
</head>
<body>
<div class="status-bar">
<b>Dhan API:</b><span class="status-active">Active</span>
<b>Active BN Expiry:</b>
<span class="{{ 'expiry-danger' if expiry_danger else 'expiry-safe' }}">
{{ active_expiry }}
</span>
<span style="margin-left:auto">Last Check: {{ last_run }} IST</span>
</div>

<h3>Trade History</h3>
<table>
<thead>
<tr>
<th>Time</th><th>Price</th><th>Strike</th><th>Type</th>
<th>Expiry Used</th><th>Lot Size</th><th>Premium Paid</th>
<th>Entry Price</th><th>Exit Price</th>
<th>Status</th><th>Remarks</th>
</tr>
</thead>
<tbody>
{% for t in history %}
<tr>
<td>{{t.time}}</td><td>{{t.price}}</td><td>{{t.strike}}</td>
<td class="{{'ce-text' if t.type=='CE' else 'pe-text'}}">{{t.type}}</td>
<td>{{t.expiry_used}}</td>
<td>{{t.lot_size}}</td><td>{{t.premium_paid}}</td>
<td>{{t.entry_price}}</td><td>{{t.exit_price}}</td>
<td>{{t.status}}</td><td>{{t.remarks}}</td>
</tr>
{% endfor %}
</tbody>
</table>
</body>
</html>"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
