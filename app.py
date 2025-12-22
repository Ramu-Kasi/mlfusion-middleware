import os
import sys
import pandas as pd
from flask import Flask, request, jsonify
from dhanhq import dhanhq
from apscheduler.schedulers.background import BackgroundScheduler

# 1. INITIALIZE APP
app = Flask(__name__)

# 2. CONFIGURATION
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# GLOBAL CACHE
SCRIP_MASTER_DATA = None

def log_now(msg):
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def load_scrip_master():
    """Downloads and finds columns dynamically to prevent 'KeyError'"""
    global SCRIP_MASTER_DATA
    log_now("REFRESH: Downloading Scrip Master...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # DYNAMIC COLUMN FINDER: Prevent the error seen in image_4acce4.png
        cols = df.columns
        inst_col = next((c for c in cols if 'INSTRUMENT' in c.upper()), None)
        und_id_col = next((c for c in cols if 'UNDERLYING_SECURITY_ID' in c.upper()), None)
        
        if inst_col and und_id_col:
            # Filter for Bank Nifty (25) Options
            filtered = df[
                (df[inst_col].str.contains('OPTIDX', na=False)) & 
                (df[und_id_col] == 25)
            ].copy()
            
            # Pre-find Expiry column and convert to dates
            exp_col = next((c for c in cols if 'EXPIRY' in c.upper() and 'DATE' in c.upper()), None)
            if exp_col:
                filtered[exp_col] = pd.to_datetime(filtered[exp_col], errors='coerce')
            
            SCRIP_MASTER_DATA = filtered
            log_now(f"SUCCESS: {len(SCRIP_MASTER_DATA)} contracts in cache.")
            return True
        else:
            log_now("CRITICAL ERROR: Could not find required columns in CSV.")
            return False
    except Exception as e:
        log_now(f"CACHE ERROR: {e}")
        return False

# Initial Load
load_scrip_master()

# 3. SCHEDULER: 24h Refresh
scheduler = BackgroundScheduler()
scheduler.add_job(func=load_scrip_master, trigger="interval", hours=24)
scheduler.start()

def get_atm_id(price, signal):
    """Robust lookup using flexible column matching"""
    global SCRIP_MASTER_DATA
    try:
        if not price or str(price).lower() == "none":
            return None, None, None
            
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty:
            log_now("DATA MISSING: Attempting blocking fetch...")
            if not load_scrip_master(): return None, None, None

        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        # Dynamically find the columns we need for filtering
        cols = SCRIP_MASTER_DATA.columns
        strike_col = next((c for c in cols if 'STRIKE' in c.upper()), None)
        type_col = next((c for c in cols if 'OPTION_TYPE' in c.upper()), None)
        id_col = next((c for c in cols if 'SECURITY_ID' in c.upper() or 'TOKEN' in c.upper()), None)
        exp_col = next((c for c in cols if 'EXPIRY' in c.upper() and 'DATE' in c.upper()), None)
        lot_col = next((c for c in cols if 'LOT' in c.upper()), None)

        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & 
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ].copy()
        
        if not match.empty:
            # Sort by Expiry and pick nearest
            if exp_col:
                match = match.dropna(subset=[exp_col]).sort_values(by=exp_col)
            
            row = match.iloc[0]
            final_id = str(int(row[id_col]))
            qty = int(row[lot_col]) if lot_col else 35
            
            log_now(f"MATCH: {row.get('SEM_TRADING_SYMBOL', 'Contract')} -> ID {final_id}, Qty {qty}")
            return final_id, strike, qty
            
        return None, strike, 35
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None, 35

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    raw = request.get_data(as_text=True)
    log_now(f"SIGNAL: {raw}")
    try:
        data = request.get_json(force=True, silent=True)
        if not data: return jsonify({"status": "error", "message": "No JSON"}), 400

        sec_id, strike, qty = get_atm_id(data.get("price"), data.get("message", ""))
        
        if not sec_id:
            log_now(f"FAILED: Strike {strike} not found.")
            return jsonify({"status": "not_found"}), 404

        order = dhan.place_order(
            security_id=sec_id, exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.BUY, quantity=qty, 
            order_type=dhan.MARKET, product_type=dhan.MARGIN,
            price=0, validity='DAY'
        )
        log_now(f"DHAN: {order}")
        return jsonify(order), 200
    except Exception as e:
        log_now(f"ERROR: {e}")
        return jsonify({"status": "error"}), 500

@app.route('/')
def health(): return "ACTIVE", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
