import os, sys, time, threading, pytz
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime

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

def load_scrip_master():
    global SCRIP_MASTER_DATA
    try:
        log_now("BOOT: Background Loading Started...")
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # --- FLEXIBLE COLUMN DETECTION ---
        # We check which column name Dhan is currently using for Symbol
        sym_col = next((c for c in df.columns if c in ['SEM_SYMBOL_NAME', 'SEM_TRADING_SYMBOL', 'SYMBOL']), None)
        inst_col = next((c for c in df.columns if c in ['SEM_INSTRUMENT_NAME', 'INSTRUMENT']), None)
        exch_col = next((c for c in df.columns if c in ['SEM_EXCHANGE_ID', 'EXCHANGE']), None)
        
        if not sym_col:
            log_now(f"CRITICAL: Could not find Symbol column. Available: {list(df.columns[:5])}")
            return

        mask = (
            (df[inst_col].str.contains('OPTIDX', na=False)) & 
            (df[sym_col].str.contains('BANKNIFTY', case=False, na=False)) &
            (df[exch_col].str.contains('NSE', case=False, na=False))
        )
        SCRIP_MASTER_DATA = df[mask].copy()
        
        # Standardize naming for our internal logic
        SCRIP_MASTER_DATA.rename(columns={sym_col: 'SEM_SYMBOL_NAME', inst_col: 'SEM_INSTRUMENT_NAME'}, inplace=True)
        
        log_now(f"BOOT: Success. Filtered {len(SCRIP_MASTER_DATA)} BankNifty contracts.")
    except Exception as e:
        log_now(f"BOOT ERROR: {str(e)}")

threading.Thread(target=load_scrip_master, daemon=True).start()

# ... [get_atm_id, surgical_reversal, and routes remain identical to v1.6] ...

def get_atm_id(price, signal):
    attempts = 0
    while SCRIP_MASTER_DATA is None and attempts < 12:
        time.sleep(5); attempts += 1
    try:
        if SCRIP_MASTER_DATA is None: return None, None, 30
        base_strike = round(float(price) / 100) * 100
        strike = (base_strike - 100) if "BUY" in signal.upper() else (base_strike + 100)
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        # Dhan CSV usually uses 'SEM_OPTION_TYPE' and 'SEM_STRIKE_PRICE'
        match = SCRIP_MASTER_DATA[(SCRIP_MASTER_DATA['SEM_STRIKE_PRICE'] == strike) & 
                                  (SCRIP_MASTER_DATA['SEM_OPTION_TYPE'] == opt_type)].copy()
        
        if not match.empty:
            return str(int(match.iloc[0]['SEM_SMST_SECURITY_ID'])), strike, 30
        return None, strike, 30
    except Exception as e:
        log_now(f"STRIKE ERROR: {e}")
        return None, None, 30

@app.route('/')
def dashboard():
    now_ist = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M:%S")
    return render_template_string("""
    <html><head><title>MLFusion v1.7</title><meta http-equiv="refresh" content="30"><style>
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
    remark = f"Opened {curr_t} {strike}"
    if order_res.get('status') == 'success' and was_reversed:
        remark = f"Closed {opp_t} & {remark}"
    elif order_res.get('status') != 'success':
        remark = order_res.get('remarks', 'Entry Failed')
    entry = {"time": trade_time, "price": price, "strike": strike, "type": curr_t, "status": "success" if order_res.get('status') == 'success' else "failure", "remarks": remark}
    TRADE_HISTORY.insert(0, entry)
    return jsonify(entry), 200

def surgical_reversal(signal_type):
    try:
        pos = dhan.get_positions()
        if pos.get('status') == 'success':
            for p in pos.get('data', []):
                if "BANKNIFTY" in p['tradingSymbol'].upper() and int(p['netQty']) != 0:
                    dhan.place_order(security_id=p['securityId'], exchange_segment=p['exchangeSegment'], transaction_type=dhan.SELL if int(p['netQty']) > 0 else dhan.BUY, quantity=abs(int(p['netQty'])), order_type=dhan.MARKET, product_type=dhan.MARGIN, price=0)
                    return True
        return False
    except: return False

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=PORT)
