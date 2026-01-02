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

# --- 1. CONFIGURATION ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None
TRADE_HISTORY = []
OPEN_TRADE_REF = None  # <-- tracks current open trade

def log_now(msg):
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

# --- 2. DYNAMIC SCRIP MASTER ---
def load_scrip_master():
    global SCRIP_MASTER_DATA
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        sym_col = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)
        exch_col = next((c for c in df.columns if 'EXCHANGE' in c.upper()), None)
        exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)

        if inst_col and sym_col:
            mask = (
                (df[inst_col].str.contains('OPTIDX', na=False)) &
                (df[sym_col].str.contains('BANKNIFTY', case=False, na=False)) &
                (~df[sym_col].str.contains('BANKEX', case=False, na=False))
            )
            if exch_col:
                mask &= df[exch_col].str.contains('NSE', case=False, na=False)

            SCRIP_MASTER_DATA = df[mask].copy()
            if exp_col:
                SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
                SCRIP_MASTER_DATA.dropna(subset=[exp_col], inplace=True)

    except Exception as e:
        log_now(f"SCRIP LOAD ERROR: {e}")

threading.Thread(target=load_scrip_master, daemon=True).start()

def get_atm_id(price, signal):
    try:
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty:
            return None, None, 30

        base = round(price / 100) * 100
        strike, opt_type = (base - 100, "CE") if "BUY" in signal else (base + 100, "PE")

        cols = SCRIP_MASTER_DATA.columns
        strike_col = next(c for c in cols if 'STRIKE' in c.upper())
        type_col = next(c for c in cols if 'OPTION_TYPE' in c.upper())
        exp_col = next(c for c in cols if 'EXPIRY_DATE' in c.upper())
        id_col = next(c for c in cols if 'SECURITY' in c.upper() or 'TOKEN' in c.upper())

        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) &
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ].sort_values(exp_col)

        if not match.empty:
            return str(int(match.iloc[0][id_col])), strike, 30

        return None, strike, 30
    except Exception:
        return None, None, 30

# --- UI (UNCHANGED) ---
DASHBOARD_HTML = """<!DOCTYPE html>
<html>
<head>
<title>MLFusion Jan1-2026</title>
<meta http-equiv="refresh" content="60">
<style>
body{font-family:sans-serif;background:#f0f2f5;padding:20px}
.status-bar{background:#fff;padding:15px;border-radius:8px;display:flex;box-shadow:0 1px 3px rgba(0,0,0,.1);margin-bottom:20px}
.status-active{background:#28a745;color:#fff;padding:2px 10px;border-radius:10px;font-weight:bold}
table{width:100%;border-collapse:collapse;background:#fff;border-radius:8px}
th{background:#333;color:#fff;padding:12px;text-align:left}
td{padding:12px;border-bottom:1px solid #eee}
.ce-text{color:#28a745;font-weight:bold}
.pe-text{color:#d9534f;font-weight:bold}
</style>
</head>
<body>
<div class="status-bar">
<b>Dhan API Status:</b>&nbsp;<span class="status-active">Active</span>
<span style="margin-left:auto">Last Check: {{ last_run }} (IST)</span>
</div>

<h3>Trade History (Jan1-2026 Version)</h3>
<table>
<thead>
<tr>
<th>Time</th><th>Price</th><th>Strike</th><th>Type</th>
<th>Lot Size</th><th>Premium Paid</th>
<th>Entry Price</th><th>Exit Price</th>
<th>Status</th><th>Remarks</th>
</tr>
</thead>
<tbody>
{% for t in history %}
<tr>
<td>{{t.time}}</td><td>{{t.price}}</td><td>{{t.strike}}</td>
<td class="{{'ce-text' if t.type=='CE' else 'pe-text'}}">{{t.type}}</td>
<td>{{t.lot_size}}</td><td>{{t.premium_paid}}</td>
<td>{{t.entry_price}}</td><td>{{t.exit_price}}</td>
<td>{{t.status}}</td><td>{{t.remarks}}</td>
</tr>
{% endfor %}
</tbody>
</table>
</body>
</html>"""

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

# --- REVERSAL WITH EXIT UPDATE ---
def surgical_reversal(signal):
    global OPEN_TRADE_REF
    try:
        pos = dhan.get_positions()
        if pos.get('status') != 'success':
            return False

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

                exit_price = fetch_price(p['securityId'])

                if OPEN_TRADE_REF:
                    OPEN_TRADE_REF['exit_price'] = exit_price or "—"
                    OPEN_TRADE_REF['status'] = "CLOSED"
                    OPEN_TRADE_REF = None
                return True
        return False
    except Exception:
        return False

# --- ROUTES ---
@app.route('/')
def dashboard():
    now = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M:%S")
    return render_template_string(DASHBOARD_HTML, history=TRADE_HISTORY, last_run=now)

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    global OPEN_TRADE_REF
    data = request.get_json(force=True)
    msg = data['message'].upper()
    price = float(data['price'])

    was_rev = surgical_reversal(msg)
    sec_id, strike, qty = get_atm_id(price, msg)

    order = dhan.place_order(
        security_id=sec_id,
        exchange_segment=dhan.NSE_FNO,
        transaction_type=dhan.BUY,
        quantity=qty,
        order_type=dhan.MARKET,
        product_type=dhan.MARGIN,
        price=0
    )

    entry_price = fetch_price(sec_id)
    premium = round(entry_price * qty, 2) if entry_price else "—"

    trade = {
        "time": datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M:%S"),
        "price": price,
        "strike": strike,
        "type": "CE" if "BUY" in msg else "PE",
        "lot_size": qty,
        "premium_paid": premium,
        "entry_price": entry_price or "—",
        "exit_price": "—",
        "status": "OPEN",
        "remarks": f"Opened {strike}"
    }

    TRADE_HISTORY.insert(0, trade)
    OPEN_TRADE_REF = trade

    return jsonify(trade), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
