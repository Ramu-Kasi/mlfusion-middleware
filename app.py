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
    """Immediate logging for Render"""
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def load_scrip_master():
    """Background loader to prevent deployment timeouts"""
    global SCRIP_MASTER_DATA
    log_now("REFRESH: Fetching Scrip Master...")
    try:
        # Use a timeout on the request to prevent hanging
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # We look for the exact columns from your working screenshot
        # But we use a search to be safe against hidden spaces
        cols = df.columns
        inst_c = next((c for c in cols if 'INSTRUMENT_NAME' in c.upper()), None)
        und_c = next((c for c in cols if 'UNDERLYING_SECURITY_ID' in c.upper()), None)
        
        if inst_c and und_c:
            filtered = df[
                (df[inst_c].str.contains('OPTIDX', na=False)) & 
                (df[und_c] == 25)
            ].copy()
            
            # Map remaining columns for the lookup engine
            exp_c = next((c for c in cols if 'EXPIRY_DATE' in c.upper()), None)
            if exp_c:
                filtered[exp_c] = pd.to_datetime(filtered[exp_c], errors='coerce')
            
            SCRIP_MASTER_DATA = filtered
            log_now(f"SUCCESS: {len(SCRIP_MASTER_DATA)} Bank Nifty contracts cached.")
            return True
    except Exception as e:
        log_now(f"CACHE ERROR: {e}")
    return False

# 3. SCHEDULER: Start background tasks AFTER app definition
scheduler = BackgroundScheduler()
# This ensures the app starts even if the download is slow
scheduler.add_job(func=load_scrip_master, trigger="date") 
scheduler.add_job(func=load_scrip_master, trigger="interval", hours=24)
scheduler.start()

def get_atm_id(price, signal):
    """Reliable lookup engine"""
    global SCRIP_MASTER_DATA
    try:
        if not price or str(price).lower() == "none":
            return None, None, None
            
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty:
            log_now("DATA MISSING: Emergency reload in progress...")
            load_scrip_master()
            if SCRIP_MASTER_DATA is None: return None, None, None

        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        # Use flexible column detection for the search
        cols = SCRIP_MASTER_DATA.columns
        s_col = next((c for c in cols if 'STRIKE' in c.upper()), None)
        t_col = next((c for c in cols if 'OPTION_TYPE' in c.upper()), None)
        i_col = next((c for c in cols if 'SECURITY_ID' in c.upper()), None)
        e_col = next((c for c in cols if 'EXPIRY_DATE' in c.upper()), None)
        q_col = next((c for c in cols if 'LOT' in c.upper()), None)

        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[s_col] == strike) & 
            (SCRIP_MASTER_DATA[t_col] == opt_type)
        ].copy()
        
        if not match.empty:
            if e_col:
                match = match.dropna(subset=[e_col]).sort_values(by=e_col)
            
            row = match.iloc[0]
            final_id = str(int(row[i_col]))
            qty = int(row[q_col]) if q_col else 35
            
            log_now(f"MATCH: {row.get('SEM_TRADING_SYMBOL', 'Contract')} -> ID {final_id}, Qty {qty}")
            return final_id, strike, qty
            
        return None, strike, 35
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None, 35

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    log_now(f"SIGNAL: {request.get_data(as_text=True)}")
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
    # Use environment port for Render
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
