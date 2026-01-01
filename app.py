import os, sys, time, pytz
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime

# --- 1. INITIALIZATION ---
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

# --- 2. STABLE DATA LOADING ---
def load_scrip_master():
    global SCRIP_MASTER_DATA
    try:
        log_now("BOOT: Loading Scrip Master...")
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        mask = (
            (df['SEM_INSTRUMENT_NAME'].str.contains('OPTIDX', na=False)) & 
            (df['SEM_SYMBOL_NAME'].str.contains('BANKNIFTY', case=False, na=False)) &
            (df['SEM_EXCHANGE_ID'].str.contains('NSE', case=False, na=False))
        )
        SCRIP_MASTER_DATA = df[mask].copy()
        log_now(f"BOOT: Ready. Scrips Found: {len(SCRIP_MASTER_DATA)}")
    except Exception as e:
        log_now(f"BOOT ERROR: {e}")

load_scrip_master()

def get_atm_id(price, signal):
    try:
        if SCRIP_MASTER_DATA is None: return None, None
        base_strike = round(float(price) / 100) * 100
        strike = (base_strike - 100) if "BUY" in signal.upper() else (base_strike + 100)
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        match = SCRIP_MASTER_DATA[(SCRIP_MASTER_DATA['SEM_STRIKE_PRICE'] == strike) & 
                                  (SCRIP_MASTER_DATA['SEM_OPTION_TYPE'] == opt_type)].copy()
        
        if not match.empty:
            return str(int(match.iloc[0]['SEM_SMST_SECURITY_ID'])), strike
        return None, strike
    except: return None, None

# --- 3. STABLE REVERSAL LOGIC ---
def surgical_reversal():
    try:
        pos = dhan.get_positions()
        if pos.get('status') == 'success':
            for p in pos.get('data', []):
                if "BANKNIFTY" in p.get('tradingSymbol', '').upper() and int(p.get('netQty', 0)) != 0:
                    dhan.place_order(
                        security_id=p['securityId'], 
                        exchange_segment=p['exchangeSegment'], 
                        transaction_type=dhan.SELL if int(p['netQty']) > 0 else dhan.BUY, 
                        quantity=abs(int(p['netQty'])), 
                        order_type=dhan.MARKET, 
                        product_type=dhan.MARGIN, 
                        price=0
                    )
    except: pass

# --- 4. DASHBOARD UI ---
@app.route('/')
def dashboard():
    now_ist = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M:%S")
    return render_template_string("""
    <html><head><title>MLFusion v1.0 Stable</title><meta http-equiv="refresh" content="30"></head>
    <body style="font-family:sans-serif; padding:20px;">
    <h3>Trade History (Jan1-2026 Version)</h3>
    <table border="1" style="width:100%; border-collapse:collapse;">
        <tr style="background:#eee;"><th>Time (IST)</th><th>Price</th><th>Strike</th><th>Type</th><th>Status</th></tr>
        {% for t in history %}
        <tr><td>{{t.time}}</td><td>{{t.price}}</td><td>{{t.strike}}</td><td>{{t.type}}</td><td>{{t.status}}</td></tr>
        {% endfor %}
    </table></body></html>""", history=TRADE_HISTORY, last_run=now_ist)

# --- 5. EXECUTION ROUTE ---
@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json(force=True, silent=True)
    if not data: return jsonify({"status": "no data"}), 400
    
    msg, price = data.get('message', '').upper(), float(data.get('price', 0))
    
    surgical_reversal()
    time.sleep(1)
    
    sec_id, strike = get_atm_id(price, msg)
    if not sec_id: return jsonify({"status": "error"}), 404
    
    order = dhan.place_order(
        security_id=sec_id, exchange_segment=dhan.NSE_FNO, 
        transaction_type=dhan.BUY, quantity=30, 
        order_type=dhan.MARKET, product_type=dhan.MARGIN, price=0
    )
    
    trade_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M:%S")
    entry = {
        "time": trade_time, "price": price, "strike": strike, 
        "type": "CE" if "BUY" in msg else "PE", 
        "status": "success" if order.get('status') == 'success' else "failed"
    }
    TRADE_HISTORY.insert(0, entry)
    return jsonify(entry), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=PORT)
