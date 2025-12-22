import os
import sys
import pandas as pd
from flask import Flask, request, jsonify
from dhanhq import dhanhq

# 1. INITIALIZE APP
app = Flask(__name__)

# 2. CONFIGURATION
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None

def log_now(msg):
    """Force logs to show in Render immediately"""
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def load_scrip_master():
    """Robust CSV loader that won't crash on missing columns"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Loading CSV...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Filtering for Index Options (OPTIDX) where underlying is Bank Nifty (25)
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        und_col = next((c for c in df.columns if 'UNDERLYING_SECURITY_ID' in c.upper()), None)
        
        if inst_col and und_col:
            SCRIP_MASTER_DATA = df[
                (df[inst_col].str.contains('OPTIDX', na=False)) & 
                (df[und_col] == 25)
            ].copy()
            log_now(f"BOOT: Filtered {len(SCRIP_MASTER_DATA)} Bank Nifty contracts.")
        else:
            log_now("BOOT WARNING: Headers not found. Using raw data.")
            SCRIP_MASTER_DATA = df
            
        log_now("BOOT: CSV Loaded successfully.")
    except Exception as e:
        log_now(f"CRITICAL BOOT ERROR: {e}")

# Load the cache on start
load_scrip_master()

def get_atm_id(price, signal):
    """Finds nearest ATM strike ID and handles DH-905 SecurityId errors"""
    try:
        if SCRIP_MASTER_DATA is None: return None, None
        
        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        cols = SCRIP_MASTER_DATA.columns
        strike_col = next((c for c in cols if 'STRIKE' in c.upper()), None)
        type_col = next((c for c in cols if 'OPTION_TYPE' in c.upper()), None)
        # DH-905 FIX: Searching for the exact ID column Dhan expects for orders
        id_col = next((c for c in cols if 'SMST_SECURITY_ID' in c.upper()), None)
        exp_col = next((c for c in cols if 'EXPIRY' in c.upper()), None)

        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & 
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ]
        
        if not match.empty:
            # Sort by expiry to get the nearest (current week) contract
            if exp_col:
                match = match.sort_values(by=exp_col)
            
            # Ensure we get the ID as a clean integer string
            final_id = str(int(match.iloc[0][id_col]))
            return final_id, strike
            
        return None, strike
    except Exception as e:
        log_now(f"RUNTIME LOOKUP ERROR: {e}")
        return None, None

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    log_now(f"SIGNAL RECEIVED: {request.get_data(as_text=True)}")
    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({"status": "error", "message": "No JSON"}), 400

        sec_id, strike = get_atm_id(data.get("price"), data.get("message", ""))
        
        if not sec_id:
            log_now(f"FAILED: Strike {strike} not found.")
            return jsonify({"status": "not_found"}), 404

        log_now(f"EXECUTE: Sending Order for SecurityId {sec_id} (ATM {strike})")

        # PLACE ORDER - FIXED QTY 35
        order = dhan.place_order(
            security_id=sec_id,
            exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.BUY, 
            quantity=35, 
            order_type=dhan.MARKET,
            product_type=dhan.MARGIN,
            price=0,
            validity='DAY'
        )

        log_now(f"DHAN RESPONSE: {order}")
        return jsonify(order), 200

    except Exception as e:
        log_now(f"CRITICAL RUNTIME ERROR: {str(e)}")
        return jsonify({"status": "error", "reason": str(e)}), 500

@app.route('/')
def health():
    return "BRIDGE_ACTIVE", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
