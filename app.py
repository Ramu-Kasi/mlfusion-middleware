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
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def load_scrip_master():
    global SCRIP_MASTER_DATA
    log_now("BOOT: Loading CSV...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # FAIL-SAFE: If keyword search fails, don't crash the whole app
        try:
            inst_col = [c for c in df.columns if 'INSTRUMENT' in c.upper()][0]
            # Some Dhan files use 'UNDERLYING_ID', others 'UNDERLYING_SYMBOL'
            # We filter for Index Options first
            SCRIP_MASTER_DATA = df[df[inst_col].str.contains('OPTIDX', na=False)].copy()
            log_now(f"BOOT: Filtered {len(SCRIP_MASTER_DATA)} option contracts.")
        except Exception as inner_e:
            log_now(f"FILTER ERROR: {inner_e}. Using raw data.")
            SCRIP_MASTER_DATA = df
            
        log_now("BOOT: CSV Loaded successfully.")
    except Exception as e:
        log_now(f"CRITICAL BOOT ERROR: {e}")

load_scrip_master()

def get_atm_id(price, signal):
    try:
        if SCRIP_MASTER_DATA is None: return None, None
        
        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        # Dynamic column finding to prevent 'list index out of range'
        cols = SCRIP_MASTER_DATA.columns
        strike_col = next((c for c in cols if 'STRIKE' in c.upper()), None)
        type_col = next((c for c in cols if 'OPTION_TYPE' in c.upper()), None)
        id_col = next((c for c in cols if 'SECURITY_ID' in c.upper()), None)

        if not all([strike_col, type_col, id_col]):
            log_now(f"MISSING COLUMNS: Strike={strike_col}, Type={type_col}, ID={id_col}")
            return None, strike

        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & 
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ]
        
        if not match.empty:
            return str(int(match.iloc[0][id_col])), strike
        return None, strike
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    log_now(f"SIGNAL: {request.get_data(as_text=True)}")
    try:
        data = request.get_json(force=True, silent=True)
        if not data: return jsonify({"error": "No JSON"}), 400

        sec_id, strike = get_atm_id(data.get("price"), data.get("message", ""))
        
        if not sec_id:
            log_now(f"NOT FOUND: ATM {strike}")
            return jsonify({"status": "not_found"}), 404

        order = dhan.place_order(
            security_id=sec_id, exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.BUY, quantity=35, 
            order_type=dhan.MARKET, product_type=dhan.MARGIN,
            price=0, validity='DAY'
        )
        log_now(f"RESPONSE: {order}")
        return jsonify(order), 200
    except Exception as e:
        log_now(f"RUN ERROR: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/')
def health(): return "ACTIVE", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
