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
        
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        sym_col = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)
        exch_col = next((c for c in df.columns if 'EXCHANGE' in c.upper()), None)
        exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)
        
        if inst_col and sym_col:
            mask = (
                (df[inst_col].str.contains('OPTIDX', na=False)) & 
                (df[sym_col].str.contains('BANKNIFTY', case=False, na=False)) &
                (~df[sym_col].str.contains('BANKEX', case=False, na=False))
            )
            
            if exch_col:
                mask = mask & (df[exch_col].str.contains('NSE', case=False, na=False))

            SCRIP_MASTER_DATA = df[mask].copy()
            
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

        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & 
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ].copy()
        
        if not match.empty:
            today = pd.Timestamp(datetime.now().date())
            match = match[match[exp_col] >= today]
            match = match.sort_values(by=exp_col, ascending=True)
            
            if not match.empty:
                row = match.iloc[0]
                final_id = str(int(row[id_col]))
                log_now(f"MATCH FOUND: {row.get('SEM_TRADING_SYMBOL', 'BN')} -> ID {final_id}")
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
            return jsonify({"status": "not_found"}), 404

        # --- MODIFIED: ACTUAL ORDER PLACEMENT ---
        transaction_type = dhan.BUY if "BUY" in data.get("message", "").upper() else dhan.SELL
        
        order = dhan.place_order(
            tag='MLFusion_BN',
            transaction_type=transaction_type,
            exchange_segment=dhan.NSE_FNO,
            product_type=dhan.INTRA,      # Can change to dhan.MARGIN for carry-forward
            order_type=dhan.MARKET,
            validity=dhan.DAY,
            security_id=sec_id,
            quantity=qty,
            price=0                       # Market orders use 0
        )

        log_now(f"EXECUTE: Dhan API Response: {order}")
        return jsonify({"status": "success", "order_data": order})

    except Exception as e:
        log_now(f"HANDLER ERROR: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    # Keeps the app running on Render
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
