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
    """Simple, direct download and cache"""
    global SCRIP_MASTER_DATA
    log_now("REFRESH: Downloading Scrip Master...")
    try:
        # We download the full CSV and only keep Bank Nifty rows to save memory
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Filter for Bank Nifty (25) Index Options (OPTIDX)
        SCRIP_MASTER_DATA = df[
            (df['SEM_INSTRUMENT_NAME'].str.contains('OPTIDX', na=False)) & 
            (df['SEM_UNDERLYING_SECURITY_ID'] == 25)
        ].copy()
        
        # Pre-convert expiry to ensure sorting works instantly later
        SCRIP_MASTER_DATA['SEM_EXPIRY_DATE'] = pd.to_datetime(SCRIP_MASTER_DATA['SEM_EXPIRY_DATE'], errors='coerce')
        
        log_now(f"SUCCESS: {len(SCRIP_MASTER_DATA)} contracts cached.")
        return True
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
    """Simple lookup from memory"""
    global SCRIP_MASTER_DATA
    try:
        if not price or str(price).lower() == "none":
            return None, None, None
            
        # Emergency Fetch if memory is empty
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty:
            log_now("DATA MISSING: Fetching now...")
            if not load_scrip_master(): return None, None, None

        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        # Filter from the pre-filtered cache
        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA['SEM_STRIKE_PRICE'] == strike) & 
            (SCRIP_MASTER_DATA['SEM_OPTION_TYPE'] == opt_type)
        ].copy()
        
        if not match.empty:
            # Sort by date and pick nearest
            match = match.dropna(subset=['SEM_EXPIRY_DATE']).sort_values(by='SEM_EXPIRY_DATE')
            row = match.iloc[0]
            
            final_id = str(int(row['SEM_SMST_SECURITY_ID']))
            qty = int(row['SEM_LOT_UNITS']) if 'SEM_LOT_UNITS' in row else 35
            
            log_now(f"MATCH: {row['SEM_TRADING_SYMBOL']} -> ID {final_id}, Qty {qty}")
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
        if not data: return jsonify({"status": "error"}), 400

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
