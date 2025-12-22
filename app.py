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
    """Robust CSV loader that strictly filters for Bank Nifty Index Options"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Loading CSV...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Identify columns using keywords
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        und_col = next((c for c in df.columns if 'UNDERLYING_SECURITY_ID' in c.upper()), None)
        sym_col = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)
        
        if inst_col and und_col:
            # STRICT FILTER: Index Options (OPTIDX) + Bank Nifty (25) + Symbol check
            # This ensures PAGEIND and other stocks are removed from memory
            SCRIP_MASTER_DATA = df[
                (df[inst_col].str.contains('OPTIDX', na=False)) & 
                (df[und_col] == 25) &
                (df[sym_col].str.contains('BANKNIFTY', na=False))
            ].copy()
            
            # DYNAMIC DATE CONVERSION
            exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)
            if exp_col:
                SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
            
            log_now(f"BOOT: Filtered {len(SCRIP_MASTER_DATA)} Bank Nifty contracts.")
        else:
            log_now("BOOT WARNING: Filter columns not found. Using full list.")
            SCRIP_MASTER_DATA = df
            
        log_now("BOOT: CSV Loaded successfully.")
    except Exception as e:
        log_now(f"CRITICAL BOOT ERROR: {e}")

# Load the cache on start
load_scrip_master()

def get_atm_id(price, signal):
    """Finds nearest ATM strike ID with 35 qty default"""
    try:
        if SCRIP_MASTER_DATA is None: return None, None, 35
        
        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        cols = SCRIP_MASTER_DATA.columns
        strike_col = next((c for c in cols if 'STRIKE' in c.upper()), None)
        type_col = next((c for c in cols if 'OPTION_TYPE' in c.upper()), None)
        exp_col = next((c for c in cols if 'EXPIRY_DATE' in c.upper()), None)
        lot_col = next((c for c in cols if 'LOT' in c.upper() or 'SEM_LOT_UNIT' in c.upper()), None)
        id_col = next((c for c in cols if 'SMST_SECURITY_ID' in c.upper()), 
                     next((c for c in cols if 'TOKEN' in c.upper()), None))

        # Filter by Strike and Type
        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & 
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ].copy()
        
        if not match.empty:
            # DYNAMIC SORT: Always pick the record with the earliest expiry date
            if exp_col:
                match = match.dropna(subset=[exp_col])
                match = match.sort_values(by=exp_col, ascending=True)
            
            row = match.iloc[0]
            final_id = str(int(row[id_col]))
            # DEFAULT QTY FIXED TO 35
            dynamic_qty = int(row[lot_col]) if lot_col else 35
            
            log_now(f"MATCH FOUND: {row.get('SEM_TRADING_SYMBOL', 'Contract')} -> ID {final_id}, Qty {dynamic_qty}")
            return final_id, strike, dynamic_qty
            
        return None, strike, 35
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None, 35

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    log_now(f"SIGNAL RECEIVED: {request.get_data(as_text=True)}")
    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({"status": "error", "message": "No JSON"}), 400

        sec_id, strike, qty = get_atm_id(data.get("price"), data.get("message", ""))
        
        if not sec_id:
            log_now(f"FAILED: Strike {strike} not found.")
            return jsonify({"status": "not_found"}), 404

        log_now(f"EXECUTE: Sending Order for SecurityId {sec_id} with Qty {qty}")
        
        # Note: You can add the dhan.place_order() call here to complete the execution logic.
        return jsonify({"status": "success", "security_id": sec_id, "quantity": qty})

    except Exception as e:
        log_now(f"HANDLER ERROR: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
