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
    """Force logs to show in Render immediately"""
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def load_scrip_master():
    """Downloads and filters CSV once, then caches in memory"""
    global SCRIP_MASTER_DATA
    log_now("REFRESH: Fetching and Caching Scrip Master...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Core Filter Logic maintained
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        und_col = next((c for c in df.columns if 'UNDERLYING_SECURITY_ID' in c.upper()), None)
        
        if inst_col and und_col:
            filtered_df = df[
                (df[inst_col].str.contains('OPTIDX', na=False)) & 
                (df[und_col] == 25)
            ].copy()
            
            # Pre-convert dates for instant sorting
            exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)
            if exp_col:
                filtered_df[exp_col] = pd.to_datetime(filtered_df[exp_col], errors='coerce')
            
            SCRIP_MASTER_DATA = filtered_df
            log_now(f"SUCCESS: Cached {len(SCRIP_MASTER_DATA)} Bank Nifty contracts.")
    except Exception as e:
        log_now(f"CACHE ERROR: {e}")

# Initial Load on Startup
load_scrip_master()

# 3. SCHEDULER: Refresh cache every 24 hours
scheduler = BackgroundScheduler()
scheduler.add_job(func=load_scrip_master, trigger="interval", hours=24)
scheduler.start()

def get_atm_id(price, signal):
    """Instant lookup from cached memory"""
    try:
        # SAFETY CHECK: Prevent 'Strike None' error
        if price is None or str(price).strip() == "" or str(price).lower() == "none":
            log_now("INPUT ERROR: Price received is None or empty.")
            return None, None, None
            
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty:
            log_now("CACHE ERROR: Data not loaded.")
            return None, None, None
        
        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        cols = SCRIP_MASTER_DATA.columns
        strike_col = next((c for c in cols if 'STRIKE' in c.upper()), None)
        type_col = next((c for c in cols if 'OPTION_TYPE' in c.upper()), None)
        exp_col = next((c for c in cols if 'EXPIRY_DATE' in c.upper()), None)
        lot_col = next((c for c in cols if 'LOT' in c.upper() or 'SEM_LOT_UNIT' in c.upper()), None)
        id_col = next((c for c in cols if 'SMST_SECURITY_ID' in c.upper()), 
                 next((c for c in cols if 'TOKEN' in c.upper()), None))

        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & 
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ].copy()
        
        if not match.empty:
            # DYNAMIC SORT: Always pick nearest expiry
            if exp_col:
                match = match.dropna(subset=[exp_col]).sort_values(by=exp_col, ascending=True)
            
            row = match.iloc[0]
            final_id = str(int(row[id_col]))
            dynamic_qty = int(row[lot_col]) if lot_col else 35
            
            log_now(f"MATCH: {row.get('SEM_TRADING_SYMBOL', 'Contract')} -> ID {final_id}, Qty {dynamic_qty}")
            return final_id, strike, dynamic_qty
            
        return None, strike, 35
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None, 35

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    raw_data = request.get_data(as_text=True)
    log_now(f"SIGNAL: {raw_data}")
    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({"status": "error", "message": "No JSON"}), 400

        sec_id, strike, qty = get_atm_id(data.get("price"), data.get("message", ""))
        
        if not sec_id:
            log_now(f"FAILED: Strike {strike} not found.")
            return jsonify({"status": "not_found"}), 404

        log_now(f"EXECUTE: Sending Order for SecurityId {sec_id} with Qty {qty}")

        order = dhan.place_order(
            security_id=sec_id,
            exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.BUY, 
            quantity=qty, 
            order_type=dhan.MARKET,
            product_type=dhan.MARGIN,
            price=0,
            validity='DAY'
        )

        log_now(f"DHAN RESPONSE: {order}")
        return jsonify(order), 200

    except Exception as e:
        log_now(f"RUNTIME ERROR: {str(e)}")
        return jsonify({"status": "error", "reason": str(e)}), 500

@app.route('/')
def health():
    return "BRIDGE_ACTIVE", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
