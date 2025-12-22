import os
import sys
import pandas as pd
from flask import Flask, request, jsonify
from dhanhq import dhanhq

app = Flask(__name__)

# CONFIGURATION
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None

def log_now(msg):
    print(f"!!! [ALGO_ENGINE]: {msg}", file=sys.stderr, flush=True)

def load_scrip_master():
    global SCRIP_MASTER_DATA
    try:
        log_now("BOOT: Loading CSV...")
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        # Using Keyword Search for maximum safety against header changes
        inst_col = [c for c in df.columns if 'INSTRUMENT_NAME' in c][0]
        # Filter for Index Options
        SCRIP_MASTER_DATA = df[df[inst_col] == 'OPTIDX'].copy()
        log_now("BOOT: CSV Loaded successfully.")
    except Exception as e:
        log_now(f"BOOT ERROR (Non-Fatal): {e}. Server will still start.")

load_scrip_master()

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    try:
        data = request.get_json(force=True, silent=True)
        if not data: return jsonify({"error": "No JSON"}), 400
        
        tv_price = data.get("price")
        signal = data.get("message", "BUY")
        strike = round(float(tv_price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"

        # Search the cache
        strike_col = [c for c in SCRIP_MASTER_DATA.columns if 'STRIKE' in c][0]
        type_col = [c for c in SCRIP_MASTER_DATA.columns if 'OPTION_TYPE' in c][0]
        id_col = [c for c in SCRIP_MASTER_DATA.columns if 'SMST_SECURITY_ID' in c][0]
        
        match = SCRIP_MASTER_DATA[(SCRIP_MASTER_DATA[strike_col] == strike) & 
                                  (SCRIP_MASTER_DATA[type_col] == opt_type)]
        
        if not match.empty:
            sec_id = str(int(match.iloc[0][id_col]))
            order = dhan.place_order(security_id=sec_id, exchange_segment=dhan.NSE_FNO,
                                    transaction_type=dhan.BUY, quantity=35,
                                    order_type=dhan.MARKET, product_type=dhan.MARGIN,
                                    price=0, validity='DAY')
            return jsonify(order), 200
        return jsonify({"error": "Strike not found"}), 404
    except Exception as e:
        log_now(f"RUNTIME ERROR: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/')
def health_check():
    return "ALGO_LIVE", 200

if __name__ == "__main__":
    # CRITICAL: Bind to Render's port
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
