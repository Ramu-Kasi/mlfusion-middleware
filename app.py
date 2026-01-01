import os
import sys
import time
import pandas as pd
import threading
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime
import pytz

# --- 1. INITIALIZATION (Port locked to 5000) ---
app = Flask(__name__)
PORT = 5000 

CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None
TRADE_HISTORY = []

def log_now(msg):
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

# --- 2. BACKGROUND CSV LOADING (Ensures Port 5000 opens immediately) ---
def load_scrip_master():
    global SCRIP_MASTER_DATA
    try:
        log_now("BOOT: Background Download Started...")
        # Download heavy CSV in separate thread
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        mask = (
            (df['SEM_INSTRUMENT_NAME'].str.contains('OPTIDX', na=False)) & 
            (df['SEM_SYMBOL_NAME'].str.contains('BANKNIFTY', case=False, na=False)) &
            (df['SEM_EXCHANGE_ID'].str.contains('NSE', case=False, na=False))
        )
        SCRIP_MASTER_DATA = df[mask].copy()
        if 'SEM_EXPIRY_DATE' in SCRIP_MASTER_DATA.columns:
            SCRIP_MASTER_DATA['SEM_EXPIRY_DATE'] = pd.to_datetime(SCRIP_MASTER_DATA['SEM_EXPIRY_DATE'], errors='coerce')
            SCRIP_MASTER_DATA = SCRIP_MASTER_DATA.dropna(subset=['SEM_EXPIRY_DATE'])
        log_now("BOOT: Background Load Complete. Engine Ready on Port 5000.")
    except Exception as e:
        log_now(f"BOOT ERROR: {e}")

# Start background thread so Flask can bind to port 5000 instantly
threading.Thread(target=load_scrip_master).start()

def get_atm_id(price, signal):
    # Wait logic: if signal hits while still downloading, poll for 30s
    attempts = 0
    while SCRIP_MASTER_DATA is None and attempts < 6:
        log_now("SIGNAL RECEIVED: Waiting for Scrip Master...")
        time.sleep(5)
        attempts += 1
        
    try:
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty: return None, None, 30
        base_strike = round(float(price) / 100) * 100
        if "BUY" in signal.upper():
            strike, opt_type = base_strike - 100, "CE"
        else:
            strike, opt_type = base_strike + 100, "PE"
            
        match = SCRIP_MASTER_DATA[(SCRIP_MASTER_DATA['SEM_STRIKE_PRICE'] == strike) & 
                                  (SCRIP_MASTER_DATA['SEM_OPTION_TYPE'] == opt_type)].copy()
        
        if not match.empty:
            today = pd.Timestamp(datetime.now().date())
            match = match[match['SEM_EXPIRY_DATE'] >= today].sort_values(by='SEM_EXPIRY_DATE', ascending=True)
            if not match.empty:
                return str(int(match.iloc[0]['SEM_SMST_SECURITY_ID'])), strike, 30
        return None, strike, 30
    except Exception: return None, None, 30

# --- 3. DASHBOARD UI ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>MLFusion v1.4 (Port 5000)</title>
    <meta http-equiv="refresh" content="30">
    <style>
        body { font-family: sans-serif; background-color: #f0f2f5; padding: 20px; }
        .status-bar { background: white; padding: 15px; border-radius: 8px; display: flex; align-items: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; }
        .status-active { background-color: #28a745; color: #fff; padding: 2px 10px; border-radius: 10px; font-weight: bold; }
        .status-loading { background-color: #ffc107; color: #000; padding: 2px 10px; border-radius: 10px; font-weight: bold; }
        table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; }
        th { background: #333; color: white; padding: 12px; text-align: left; }
        td { padding: 12px; border-bottom: 1px solid #eee; }
    </style>
</head>
<body>
    <div class="status-bar">
        <b>Dhan API:</b> &nbsp; <span class="status-active">Active</span> &nbsp;
        <b>Engine Status:</b> &nbsp; {% if loading %}<span class="status-loading">Downloading Data...</span>{% else %}<span class="status-active">Ready</span>{% endif %}
        <span style="margin-left: auto;">IST: {{ last_run }}</span>
    </div>
    <h3>Live Trade Log (v1.4)</h3>
    <table>
        <thead><tr><th>Time (IST)</th><th>Price</th><th>Strike</th><th>Type</th><th>Status</th><th>Remarks</th></tr></thead>
        <tbody>
            {% for trade in history %}
            <tr><td>{{ trade.time }}</td><td>{{ trade.price }}</td><td>{{ trade.strike }}</td><td>{{ trade.type }}</td><td>{{ trade.status }}</td><td>{{ trade.remarks }}</td></tr>
            {% endfor %}
        </tbody>
    </table>
</body>
</html>
"""

# --- 4. SURGICAL REVERSAL ---
def surgical_reversal(signal_type):
    was_closed = False
    try:
        positions_resp = dhan.get_positions()
        if positions_resp.get('status') == 'success':
            for pos in positions_resp.get('data', []):
                symbol = pos.get('tradingSymbol', '').upper()
                net_qty = int(pos.get('netQty', 0))
                if "BANKNIFTY" in symbol and net_qty != 0:
                    is_call, is_put = "CE" in symbol, "PE" in symbol
                    if (signal_type == "BUY" and is_put) or (signal_type == "SELL" and is_call):
                        dhan.place_order(security_id=pos['securityId'], exchange_segment=pos['exchangeSegment'], transaction_type=dhan.SELL if net_qty > 0 else dhan.BUY, quantity=abs(net_qty), order_type=dhan.MARKET, product_type=dhan.MARGIN, price=0)
                        was_closed = True
        return was_closed
    except Exception: return False

# --- 5. ROUTES ---
@app.route('/')
def dashboard():
    now_ist = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M:%S")
    return render_template_string(DASHBOARD_HTML, history=TRADE_HISTORY, last_run=now_ist, loading=(SCRIP_MASTER_DATA is None))

@
