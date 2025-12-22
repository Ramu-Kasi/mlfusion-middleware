import os
import sys
import pandas as pd
from flask import Flask, request, jsonify
from dhanhq import dhanhq
from datetime import datetime

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
    """STRICT FILTER: Bank Nifty Options only (Excludes BANKEX and non-NSE)"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Loading CSV and applying STRICT Bank Nifty filters...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Identify columns
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        sym_col = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)
        exch_col = next((c for c in df.columns if 'EXCHANGE' in c.upper()), None)
        exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)
        
        if inst_col and sym_col:
            # THE "ONLY BANK NIFTY" FILTER:
            # Rejects BANKEX and ensures NSE Index Options
            mask = (
                (df[inst_col].str.contains('OPTIDX', na=False)) & 
                (df[sym_col].str.contains('BANKNIFTY', case=False, na=False)) &
                (~df[sym_col].str.contains('BANKEX', case=False, na=False))
            )
            
            if exch_col:
                mask = mask & (df[exch_col].str.contains('NSE', case=False, na=False))

            SCRIP_MASTER_DATA = df[mask].copy()
            
            # NEAREST EXPIRY VALIDATION: Convert dates for sorting
            if exp_col:
                SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
                SCRIP_MASTER_DATA = SCRIP_MASTER_DATA.dropna(subset=[exp_col])
            
            log_now(f"BOOT: Success! {len(SCRIP_MASTER_DATA)} Bank Nifty contracts loaded.")
        else:
            log_now("BOOT ERROR: Essential columns missing.")
            
    except Exception as e:
        log_now(f"CRITICAL BOOT ERROR: {e}")

# Initial load
load_scrip_master()

def get_atm_id(price, signal):
    """Retrieves the Security ID for the NEAREST EXPIRY Bank Nifty contract with Qty 35"""
    try:
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty: 
            return None, None, 35
        
        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        cols = SCRIP_MASTER_DATA.columns
        strike_col = next((c for c in cols if 'STRIKE' in c.upper()), None)
        type_col = next((c for c in cols if 'OPTION_TYPE' in c.upper()), None)
        exp_col = next((c for c in cols if 'EXPIRY_DATE' in c.upper()), None)
        id_col = next((c for c in cols if 'SMST_SECURITY_ID' in c.upper()), 
                     next((c for c in cols if 'TOKEN' in c.upper()), None))

        # Filter for the specific Strike and Option Type
        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & 
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ].copy()
        
        if not match.empty:
            # --- NEAREST EXPIRY VALIDATION ---
            today = pd.Timestamp(datetime.now().date())
            match = match[match[exp_col] >= today]
            match = match.sort_values(by=exp_col, ascending=True)
            
            if not match.empty:
                row = match.iloc[0]
                final_id = str(int(row[id_col]))
                expiry_str = row[exp_col].strftime('%Y-%m-%d')
                log_now(f"MATCH FOUND: {row.get('SEM_TRADING_SYMBOL', 'BN')} | Expiry: {expiry_str} -> ID {final_id}")
                return final_id, strike, 35 
            
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
            return jsonify({"status": "error", "message": "No JSON payload"}), 400

        sec_id, strike, qty = get_atm_id(data.get("price"), data.get("message", ""))
        
        if not sec_id:
            log_now(f"FAILED: No Bank Nifty contract found for strike {strike}")
            return jsonify({"status": "not_found"}), 404

        # Fixed line 115 from previous error
        log_now(f"EXECUTE: Sending Order for SecurityId {sec_id} with Qty {qty}")
        return jsonify({"status": "success", "security_id": sec_id, "quantity": qty, "strike": strike})

    except Exception as e:
        log_now(f"HANDLER ERROR: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- FIX FOR RENDER DEPLOYMENT ---
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
