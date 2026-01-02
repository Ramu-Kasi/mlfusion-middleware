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

def log_now(msg):
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

# --- 2. DYNAMIC SCRIP MASTER ---
def load_scrip_master():
    global SCRIP_MASTER_DATA
    try:
        log_now("ASYNC: Loading Scrip Master in background...")
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
                mask = mask & (df[exch_col].str.contains('NSE', case=False, na=False))

            SCRIP_MASTER_DATA = df[mask].copy()
            if exp_col:
                SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
                SCRIP_MASTER_DATA = SCRIP_MASTER_DATA.dropna(subset=[exp_col])
        log_now("BOOT: Jan1-2026 Scrip Master Ready.")
    except Exception as e:
        log_now(f"BOOT ERROR: {e}")

threading.Thread(target=load_scrip_master, daemon=True).start()

def get_atm_id(price, signal):
    try:
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty: return None, None, 15
        base_strike = round(float(price) / 100) * 100
        if "BUY" in signal.upper():
            strike, opt_type = base_strike - 100, "CE"
        else:
            strike, opt_type = base_strike + 100, "PE"
            
        cols = SCRIP_MASTER_DATA.columns
        strike_col = next((c for c in cols if 'STRIKE' in c.upper()), None)
        type_col = next((c for c in cols if 'OPTION_TYPE' in c.upper()), None)
        exp_col = next((c for c in cols if 'EXPIRY_DATE' in c.upper()), None)
        id_col = next((c for c in cols if 'SMST_SECURITY_ID' in c.upper()), 
                     next((c for c in cols if 'TOKEN' in c.upper()), None))

        match = SCRIP_MASTER_DATA[(SCRIP_MASTER_DATA[strike_col] == strike) & (SCRIP_MASTER_DATA[type_col] == opt_type)].copy()
        if not match.empty:
            today = pd.Timestamp(datetime.now().date())
            match = match[match[exp_col] >= today].sort_values(by=exp_col, ascending=True)
            if not match.empty:
                return str(int(match.iloc[0][id_col])), strike, 15
        return None, strike, 15
    except Exception: return None, None, 15

# --- 3. DASHBOARD UI ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>MLFusion Jan1-2026</title>
    <meta http-equiv="refresh" content="60">
    <style>
        body { font-family: sans-serif; background-color: #f0f2f5; padding: 20px; }
        .status-bar { background: white; padding: 15px; border-radius: 8px; display: flex; align-items: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; }
        .status-active { background-color: #28a745; color: #fff; padding: 2px 10px; border-radius: 10px; font-weight: bold; }
        table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; }
        th { background: #333; color: white; padding: 12px; text-align: left; }
        td { padding: 12px; border-bottom: 1px solid #eee; }
        .ce-text { color: #28a745; font-weight: bold; }
        .pe-text { color: #d9534f; font-weight: bold; }
    </style>
</head>
<body>
    <div class="status-bar">
        <b>Dhan API Status:</b> &nbsp; <span class="status-active">Active</span>
        <span style="margin-left: auto;">Last Check: {{ last_run }} (IST)</span>
    </div>
    <h3>Trade History (Jan1-2026 Version)</h3>
    <table>
        <thead>
            <tr>
                <th>Time (IST)</th><th>Price</th><th>Strike</th><th>Type</th>
                <th>Lots</th><th>Premium Paid</th><th>Entry Price</th><th>Exit Price</th>
                <th>Status</th><th>PnL</th><th>Remarks</th>
            </tr>
        </thead>
        <tbody>
            {% for trade in history %}
            <tr>
                <td>{{ trade.time }}</td>
                <td>{{ trade.price }}</td>
                <td>{{ trade.strike }}</td>
                <td class="{{ 'ce-text' if trade.type == 'CE' else 'pe-text' }}">{{ trade.type }}</td>
                <td>{{ trade.lots }}</td>
                <td>{{ trade.premium }}</td>
                <td>{{ trade.entry }}</td>
                <td>{{ trade.exit }}</td>
                <td style="font-weight:bold;">{{ trade.status }}</td>
                <td style="color: {{ 'green' if trade.pnl >= 0 else 'red' }}">{{ trade.pnl }}</td>
                <td>{{ trade.remarks }}</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</body>
</html>
"""

# --- 4. SURGICAL REVERSAL ---
def surgical_reversal(signal_type, current_price):
    was_rev = False
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
                        
                        for trade in TRADE_HISTORY:
                            if trade['status'] == 'OPEN' and (trade['type'] in symbol):
                                trade['status'] = 'CLOSED'
                                trade['exit'] = current_price
                                trade['pnl'] = round((current_price - trade['entry']) * abs(net_qty), 2)
                        was_rev = True
        return was_rev
    except Exception: return False

# --- 5. ROUTES ---
@app.route('/')
def dashboard():
    now_ist = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M:%S")
    return render_template_string(DASHBOARD_HTML, history=TRADE_HISTORY, last_run=now_ist)

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json(force=True, silent=True)
    if not data: return jsonify({"status": "no data"}), 400
    msg, price = data.get('message', '').upper(), float(data.get('price', 0))
    
    was_rev = surgical_reversal(msg, price)
    time.sleep(0.5) 
    
    sec_id, strike, qty = get_atm_id(price, msg)
    if not sec_id: return jsonify({"status": "error", "remarks": "Scrip ID not found"}), 404
    
    order_res = dhan.place_order(security_id=sec_id, exchange_segment=dhan.NSE_FNO, transaction_type=dhan.BUY, quantity=qty, order_type=dhan.MARKET, product_type=dhan.MARGIN, price=0)
    
    trade_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M:%S")
    curr_type = "CE" if "BUY" in msg else "PE"
    opp_type = "PE" if "BUY" in msg else "CE"
    
    if order_res.get('status') == 'success':
        remark = f"Closed {opp_type} & Opened {curr_type} {strike}" if was_rev else f"Opened {curr_type} {strike}"
        status_entry = {
            "time": trade_time, "price": price, "strike": strike, "type": curr_type, 
            "lots": int(qty/15), "premium": round(price * qty, 2), "entry": price, 
            "exit": "-", "status": "OPEN", "pnl": 0.0, "remarks": remark
        }
    else:
        status_entry = {
            "time": trade_time, "price": price, "strike": strike, "type": curr_type, 
            "lots": 0, "premium": 0, "entry": 0, "exit": "-", "status": "FAILED", 
            "pnl": 0.0, "remarks": order_res.get('remarks', 'Entry Failed')
        }

    TRADE_HISTORY.insert(0, status_entry)
    return jsonify(status_entry), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
