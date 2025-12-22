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

# Dhan Scrip Master URL
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# GLOBAL RAM CACHE
SCRIP_MASTER_DATA = None

def log_now(msg):
    """Force logs to show in Render immediately"""
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def load_scrip_master():
    """Robust CSV loader for Bank Nifty"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Loading CSV...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Use partial matches to find columns safely
        inst_col = [c for c in df.columns if 'INSTRUMENT_NAME' in c][0]
        und_col = [c for c in df.columns if 'UNDERLYING_SECURITY_ID' in c][0]
        
        # Filter for Index Options (OPTIDX) where Underlying is Bank Nifty (25)
        SCRIP_MASTER_DATA = df[
            (df[inst_col] == 'OPTIDX') & 
            (df[und_col] == 25)
        ].copy()
        
        log_now(f"BOOT: CSV Loaded successfully. {len(SCRIP_MASTER_DATA)} contracts in cache.")
    except Exception as e:
        log_now(f"BOOT ERROR: {e}")

# Load the cache on start
load_scrip_master()

def get_atm_id(price, signal):
    """Finds nearest ATM strike ID using keyword column matching"""
    try:
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty:
            return None, None
            
        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        # Finding columns by partial names
        strike_col = [c for c in SCRIP_MASTER_DATA.columns if 'STRIKE' in c][0]
        type_col = [c for c in SCRIP_MASTER_DATA.columns if 'OPTION_TYPE' in c][0]
        id_col = [c for c in SCRIP_MASTER_DATA.columns if 'SMST_SECURITY_ID' in c][0]
        exp_col = [c for c in SCRIP_MASTER_DATA.columns if 'EXPIRY_DATE' in c][0]
        
        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & 
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ]
        
        if not match.empty:
            match = match.sort_values(by=exp_col)
            return str(int(match.iloc[0][id_col])), strike
            
        return None, strike
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    # Force log of incoming signal
    raw_body = request.get_data(as_text=True)
    log_now(f"SIGNAL RECEIVED: {raw_body}")

    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({"status": "error", "message": "Invalid JSON"}), 400

        tv_price = data.get("price")
        signal = data.get("message", "")

        sec_id, strike = get_atm_id(tv_price, signal)
        
        if not sec_id:
            log_now(f"ERROR: Strike {strike} not found in CSV.")
            return jsonify({"status": "not_found"}), 404

        log_now(f"EXECUTE: Sending Order for ID {sec_id} (ATM {strike})")

        # PLACE ORDER
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
        log_now(f"RUNTIME ERROR: {str(e)}")
        return jsonify({"status": "error", "reason": str(e)}), 500

@app.route('/')
def health():
    return "BRIDGE_ACTIVE", 200

if __name__ == "__main__":
    # RESTORED: Uses Render's PORT or defaults to 5000
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
