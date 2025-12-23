import os
import sys
import time
import pandas as pd
from flask import Flask, request, jsonify
from dhanhq import dhanhq
from datetime import datetime

# 1. INITIALIZE APP
app = Flask(__name__)

# 2. CONFIGURATION & STATE
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None
TRADE_HISTORY = [] 

def log_now(msg):
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def check_token_status():
    """Checks if the Dhan token is currently active."""
    try:
        profile = dhan.get_fund_limits()
        if profile.get('status') == 'success':
            return "Active", "#28a745"
        return "Expired/Invalid", "#dc3545"
    except Exception:
        return "Connection Error", "#ffc107"

def load_scrip_master():
    """Initial boot to load contract IDs."""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Loading Master CSV...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        sym_col = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)
        id_col = next((c for c in df.columns if 'SMST_SECURITY_ID' in c.upper() or 'SECURITY_ID' in c.upper()), None)
        strike_col = next((c for c in df.columns if 'STRIKE' in c.upper()), None)
        type_col = next((c for c in df.columns if 'OPTION_TYPE' in c.upper()), None)
        exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)

        mask = (df[inst_col].str.contains('OPTIDX', na=False)) & (df[sym_col].str.contains('BANKNIFTY', case=False, na=False))
        SCRIP_MASTER_DATA = df[mask][[id_col, strike_col, type_col, exp_col]].copy()
        SCRIP_MASTER_DATA[id_col] = SCRIP_MASTER_DATA[id_col].astype(str).str.split('.').str[0]
        SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
        log_now("BOOT: Success.")
    except Exception as e:
        log_now(f"BOOT ERROR: {e}")

load_scrip_master()

def get_itm_id(price, signal):
    """Finds the 100-point ITM contract for current signal."""
    try:
        if SCRIP_MASTER_DATA is None: return None, None, "N/A"
        atm_strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        strike = (atm_strike - 100) if opt_type == "CE" else (atm_strike + 100)
        
        matches = SCRIP_MASTER_DATA[(SCRIP_MASTER_DATA.iloc[:,1] == strike) & (SCRIP_MASTER_DATA.iloc[:,2] == opt_type)].copy()
        if not matches.empty:
            matches = matches.sort_values(by=matches.columns[3])
            row = matches.iloc[0]
            return str(row[matches.columns[0]]), strike, row[matches.columns[3]].strftime('%Y-%m-%d')
        return None, strike, "N/A"
    except:
        return None, None, "N/A"

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json(force=True, silent=True)
    if not data: return jsonify({"status": "error"}), 400
    
    msg, price = data.get("message", "").upper(), data.get("price", 0)
    sec_id, strike, expiry = get_itm_id(price, msg)
    
    trade_info = {"time": datetime.now().strftime("%H:%M:%S"), "price": price, "strike": strike, "type": "CE" if "BUY" in msg else "PE", "expiry": expiry, "status": "Executing", "remarks": ""}
    
    try:
        order = dhan.place_order(tag='MLFusion', transaction_type=dhan.BUY, exchange_segment=dhan.NSE_FNO, product_type=dhan.MARGIN, order_type=dhan.MARKET, validity=dhan.DAY, security_id=sec_id, quantity=35, price=0)
        trade_info["status"] = order.get('status', 'failure')
        trade_info["remarks"] = order.get('remarks', 'OK')
        TRADE_HISTORY.append(trade_info)
        return jsonify(order)
    except Exception as e:
        trade_info["status"] = "Error"
        trade_info["remarks"] = str(e)
        TRADE_HISTORY.append(trade_info)
        return jsonify({"status": "error"}), 500

@app.route('/')
def dashboard():
    status_text, status_color = check_token_status()
    
    # HTML template using placeholders like [[STATUS]] to avoid brace-related SyntaxErrors
    template = """
    <html><head><title>MLFusion Live</title><meta http-equiv="refresh" content="60">
    <style>
        body { font-family: sans-serif; margin: 40px; background: #f4f4f9; }
        .status-bar { background: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; display: flex; justify-content: space-between; align-items: center; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .status-pill { padding: 5px 15px; border-radius: 20px; color: white; font-weight: bold; background-color: [[COLOR]]; }
        table { width: 100%; border-collapse: collapse; background: white; }
        th, td { padding: 12px; border: 1px solid #ddd; text-align: left; }
        th { background: #333; color: white; }
        .CE { color: green; font-weight: bold; }
        .PE { color: red; font-weight: bold; }
    </style></head><body>
    <div class="status-bar">
        <div><strong>Dhan API Status:</strong> <span class="status-pill">[[STATUS]]</span></div>
        <div style="font-size: 12px; color: #666;">Refreshes every 60s</div>
    </div>
    <h2>Trade History</h2>
    <table><tr><th>Time</th><th>Price</th><th>Strike</th><th>Type</th><th>Expiry</th><th>Status</th><th>Remarks</th></tr>
    [[ROWS]]
    </table></body></html>
    """
    
    rows_html = ""
    for t in reversed(TRADE_HISTORY):
        rows_html += f"<tr><td>{t['time']}</td><td>{t['price']}</td><td>{t['strike']}</td><td class='{t['type']}'>{t['type']}</td><td>{t['expiry']}</td><td>{t['status']}</td><td>{t['remarks']}</td></tr>"
    
    content = template.replace("[[STATUS]]", status_text).replace("[[COLOR]]", status_color).replace("[[ROWS]]", rows_html)
    return content

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
