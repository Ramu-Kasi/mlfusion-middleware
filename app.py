import os, sys, time, threading, pytz
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime

# --- 1. DEFINE APP FIRST (Prevents NameError) ---
app = Flask(__name__)
PORT = 5000 

# --- 2. CONFIGURATION ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None
TRADE_HISTORY = []

def log_now(msg):
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

# --- 3. BACKGROUND DATA LOADER ---
def load_scrip_master():
    global SCRIP_MASTER_DATA
    try:
        log_now("BOOT: Background Loading Started...")
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
        log_now("BOOT: Background Loading Complete.")
    except Exception as e:
        log_now(f"BOOT ERROR: {e}")

# Start background thread immediately
threading.Thread(target=load_scrip_master, daemon=True).start()

# --- 4. CORE FUNCTIONS ---
def get_atm_id(price, signal):
    attempts = 0
    while SCRIP_MASTER_DATA is None and attempts < 12:
        log_now("SIGNAL RECEIVED: Data still loading, waiting 5s...")
        time.sleep(5)
        attempts += 1
    try:
        if SCRIP_MASTER_DATA is None: return None, None, 30
        base_strike = round(float(price) / 100) * 100
        strike = (base_strike - 100) if "BUY" in signal.upper() else (base_strike + 100)
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        match = SCRIP_MASTER_DATA[(SCRIP_MASTER_DATA['SEM_STRIKE_PRICE'] == strike) & (SCRIP_MASTER_DATA['SEM_OPTION_TYPE'] == opt_type)].copy()
        if not match.empty:
            today = pd.Timestamp(datetime.now().date())
            match = match[match['SEM_EXPIRY_DATE'] >= today].sort_values(by='SEM_EXPIRY_DATE', ascending=True)
            if not match.empty:
                return str(int(match.iloc[0]['SEM_SMST_SECURITY_ID'])), strike, 30
        return None, strike, 30
    except Exception: return None, None, 30

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
    return render_template_string("""
    <html><head><title>MLFusion v1.6</title><meta http-equiv="refresh" content="30"><style>
    body { font-family: sans-serif; background: #f0f2f5; padding: 20px; }
    table { width: 100%; border-collapse: collapse; background: white; }
    th, td { padding: 12px; border: 1px solid #eee; text-align: left; }
    th { background: #333; color: white; }
    </style></head><body>
    <h3>Trade Log (IST: {{ last_run }})</h3>
    <table><thead><tr><th>Time</th><th>Price</th><th>Strike</th><th>Type</th><th>Status</th><th>Remarks</th></tr></thead>
    <tbody>{% for t in history %}<tr><td>{{t.time}}</td><td>{{t.price}}</td><td>{{t.strike}}</td><td>{{t.type}}</td><td>{{t.status}}</td><td>{{t.remarks}}</td></tr>{% endfor %}</tbody>
    </table></body></html>""", history=TRADE_HISTORY, last_run=now_ist)

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json(force=True, silent=True)
    if not data: return jsonify({"status": "no data"}), 400
    msg, price = data.get('message', '').upper(), float(data.get('price', 0))
    was_reversed = surgical_reversal(msg)
    time.sleep(0.5)
    sec_id, strike, qty = get_atm_id(price, msg)
    if not sec_id: return jsonify({"remarks": "Scrip ID not found"}), 404
    order_res = dhan.place_order(security_id=sec_id, exchange_segment=dhan.NSE_FNO, transaction_type=dhan.BUY, quantity=qty, order_type=dhan.MARKET, product_type=dhan.MARGIN, price=0)
    trade_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M:%S")
    curr_t, opp_t = ("CE", "PE") if "BUY" in msg else ("PE", "CE")
    if order_res.get('status') == 'success':
        remark = f"Closed {opp_t} & Opened {curr_t} {strike}" if was_reversed else f"Opened {curr_t} {strike}"
    else:
        remark = order_res.get('remarks', order_res.get('err_msg', 'Entry Failed'))
    entry = {"time": trade_time, "price": price, "strike": strike, "type": curr_t, "status": "success" if order_res.get('status') == 'success' else "failure", "remarks": remark}
    TRADE_HISTORY.insert(0, entry)
    return jsonify(entry), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=PORT)
