import os
import sys
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime
import pytz

app = Flask(__name__)

# --- 1. CONFIGURATION ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None
TRADE_HISTORY = []

def log_now(msg):
    """Force logs to show in Render immediately"""
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

# --- 2. SCRIP MASTER LOGIC (Dynamic ID Fetching) ---
def load_scrip_master():
    global SCRIP_MASTER_DATA
    log_now("BOOT: Loading CSV and applying STRICT Bank Nifty filters...")
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
                mask = mask & (df[exch_col].str.contains('NSE', case=False, na=False))

            SCRIP_MASTER_DATA = df[mask].copy()
            if exp_col:
                SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
                SCRIP_MASTER_DATA = SCRIP_MASTER_DATA.dropna(subset=[exp_col])
            
            log_now(f"BOOT: Success! {len(SCRIP_MASTER_DATA)} Bank Nifty contracts loaded.")
    except Exception as e:
        log_now(f"CRITICAL BOOT ERROR: {e}")

load_scrip_master()

def get_atm_id(price, signal):
    try:
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty: 
            return None, None, 30
        
        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        cols = SCRIP_MASTER_DATA.columns
        strike_col = next((c for c in cols if 'STRIKE' in c.upper()), None)
        type_col = next((c for c in cols if 'OPTION_TYPE' in c.upper()), None)
        exp_col = next((c for c in cols if 'EXPIRY_DATE' in c.upper()), None)
        id_col = next((c for c in cols if 'SMST_SECURITY_ID' in c.upper()), 
                     next((c for c in cols if 'TOKEN' in c.upper()), None))

        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & 
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ].copy()
        
        if not match.empty:
            today = pd.Timestamp(datetime.now().date())
            match = match[match[exp_col] >= today]
            match = match.sort_values(by=exp_col, ascending=True)
            if not match.empty:
                row = match.iloc[0]
                return str(int(row[id_col])), strike, 30
        return None, strike, 30
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None, 30

# --- 3. DASHBOARD UI (Strictly Closed Triple-Quotes) ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>MLFusion Trading Bot</title>
    <meta http-equiv="refresh" content="60">
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f0f2f5; margin: 0; padding: 20px; }
        .status-bar { background: white; padding: 15px; border-radius: 8px; display: flex; align-items: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; gap: 20px; }
        .status-active { background-color: #28a745; color: #fff; padding: 2px 10px; border-radius: 10px; font-size: 0.9em; font-weight: bold; }
        table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        th { background: #333; color: white; padding: 12px; text-align: left; }
        td { padding: 12px; border-bottom: 1px solid #eee; }
    </style>
</head>
<body>
    <div class="status-bar">
        <b>Dhan API Status:</b>
        <span class="status-active">Active</span>
        <span style="margin-left: auto;">Last Check: {{ last_run }} (IST)</span>
    </div>
    <h3>Trade History</h3>
    <table>
        <thead>
            <tr><th>Time (IST)</th><th>Price</th><th>Strike</th><th>Type</th><th>Status</th><th>Remarks</th></tr>
        </thead>
        <tbody>
            {% for trade in history %}
            <tr>
                <td>{{ trade.time }}</td>
                <td>{{ trade.price }}</td>
                <td>{{ trade.strike }}</td>
                <td>{{ trade.type }}</td>
                <td>{{ trade.status }}</td>
                <td>{{ trade.remarks }}</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</body>
</html>
"""

# --- 4. SURGICAL REVERSAL ---
def surgical_reversal(signal_type):
    try:
        positions_resp = dhan.get_positions()
        if positions_resp.get('status') == 'success':
            for pos in positions_resp.get('data', []):
                symbol = pos.get('tradingSymbol', '').upper()
                net_qty = int(pos.get('netQty', 0))
                if "BANKNIFTY" in symbol and net_qty != 0:
                    is_call = "CE" in symbol
                    is_put = "PE" in symbol
                    if (signal_type == "BUY" and is_put) or (signal_type == "SELL" and is_call):
                        exit_side = dhan.SELL if net_qty > 0 else dhan.BUY
                        # Every bracket checked character-by-character
                        dhan.place_order(
                            security_id=pos['securityId'],
                            exchange_segment=pos['exchangeSegment'],
                            transaction_type=exit_side,
                            quantity=abs(net_qty),
                            order_type=dhan.MARKET,
                            product_type=dhan.MARGIN,
                            price=0
                        )
        return True
    except Exception: return False

# --- 5. ROUTES ---
@app.route('/')
def dashboard():
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist).strftime("%H:%M:%S")
    return render_template_string(DASHBOARD_HTML, history=TRADE_HISTORY, last_run=now_ist)

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json(force=True, silent=True)
    if not data: return jsonify({"status": "no data"}), 400
    
    # Matches 'message': 'SELL' from your screenshot
    msg = data.get('message', data.get('signal', '')).upper()
    price = float(data.get('price', 0))
    
    # 1. Close opposing
    surgical_reversal(msg)
    
    # 2. Get Dynamic ID (with Lot size 30)
    sec_id, strike, qty = get_atm_id(price, msg)
    
    if not sec_id:
        return jsonify({"status": "error", "remarks": "Scrip ID not found"}), 404

    # 3. Execute Order
    entry_side = dhan.BUY if "BUY" in msg else dhan.SELL
    order_res = dhan.place_order(
        security_id=sec_id,
        exchange_segment=dhan.NSE_FNO,
        transaction_type=entry_side,
        quantity=qty,
        order_type=dhan.MARKET,
        product_type=dhan.MARGIN,
        price=0
    )
    
    ist = pytz.timezone('Asia/Kolkata')
    trade_time = datetime.now(ist).strftime("%H:%M:%S")
    
    status_entry = {
        "time": trade_time,
        "price": price,
        "strike": strike,
        "type": "CE" if "BUY" in msg else "PE",
        "status": "success" if order_res.get('status') == 'success' else "failure",
        "remarks": order_res.get('remarks', 'Executed')
    }
    TRADE_HISTORY.insert(0, status_entry)
    return jsonify(status_entry), 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
