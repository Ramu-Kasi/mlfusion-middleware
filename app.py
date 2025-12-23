import os
import sys
import time
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime

# 1. INITIALIZE APP
app = Flask(__name__)

# 2. CONFIGURATION & STATE
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None
TRADE_HISTORY = [] 

def log_now(msg):
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def load_scrip_master():
    """OPTIMIZED BOOT: Downloads and prunes data to 1% of original size for speed"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Fetching Master CSV...")
    try:
        # Load only necessary columns to save RAM
        # Common Dhan Columns: SEM_INSTRUMENT_NAME, SEM_SYMBOL_NAME, SEM_EXPIRY_DATE, SEM_STRIKE_PRICE, SEM_SMST_SECURITY_ID
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Identify columns dynamically
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        sym_col = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)
        exch_col = next((c for c in df.columns if 'EXCHANGE' in c.upper()), None)
        exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)
        strike_col = next((c for c in df.columns if 'STRIKE' in c.upper()), None)
        id_col = next((c for c in df.columns if 'SECURITY_ID' in c.upper() or 'SMST' in c.upper()), None)
        type_col = next((c for c in df.columns if 'OPTION_TYPE' in c.upper()), None)

        # STRICT FILTER: Bank Nifty Options only
        mask = (
            (df[inst_col].str.contains('OPTIDX', na=False)) & 
            (df[sym_col].str.contains('BANKNIFTY', case=False, na=False)) &
            (~df[sym_col].str.contains('BANKEX', case=False, na=False))
        )
        if exch_col:
            mask = mask & (df[exch_col].str.contains('NSE', case=False, na=False))

        # Keep ONLY the columns we need for trading
        needed_cols = [id_col, strike_col, type_col, exp_col]
        SCRIP_MASTER_DATA = df[mask][needed_cols].copy()
        
        # Convert dates once at boot
        SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
        SCRIP_MASTER_DATA = SCRIP_MASTER_DATA.dropna(subset=[exp_col])
        
        log_now(f"BOOT: Success! Memory cleared. {len(SCRIP_MASTER_DATA)} BN contracts active.")
    except Exception as e:
        log_now(f"CRITICAL BOOT ERROR: {e}")

# Run once on deployment
load_scrip_master()

def close_opposite_position(type_to_close):
    """Execution: 100ms Reversal"""
    try:
        positions = dhan.get_positions()
        if positions.get('status') == 'success' and positions.get('data'):
            for pos in positions['data']:
                symbol = pos.get('tradingSymbol', '')
                qty = int(pos.get('netQty', 0))
                if "BANKNIFTY" in symbol and symbol.endswith(type_to_close) and qty != 0:
                    dhan.place_order(
                        tag='MLFusion_Exit', transaction_type=dhan.SELL if qty > 0 else dhan.BUY,
                        exchange_segment=dhan.NSE_FNO, product_type=dhan.INTRA,
                        order_type=dhan.MARKET, validity=dhan.DAY,
                        security_id=pos['securityId'], quantity=abs(qty), price=0
                    )
                    time.sleep(0.1) # 100ms Speed Delay
        return True
    except Exception as e:
        log_now(f"REVERSAL ERROR: {e}")
        return False

def get_itm_id(price, signal):
    """Memory-lookup for 1-Step ITM"""
    try:
        if SCRIP_MASTER_DATA is None: return None, None
        atm_strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        strike = (atm_strike - 100) if opt_type == "CE" else (atm_strike + 100)
        
        id_col, strike_col, type_col, exp_col = SCRIP_MASTER_DATA.columns
        
        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & (SCRIP_MASTER_DATA[type_col] == opt_type)
        ].copy()
        
        if not match.empty:
            today = pd.Timestamp(datetime.now().date())
            match = match[match[exp_col] >= today].sort_values(by=exp_col)
            row = match.iloc[0]
            return str(int(row[id_col])), strike
        return None, strike
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None

@app.route('/')
def dashboard():
    html = """
    <html><head><title>Dashboard</title><meta http-equiv="refresh" content="30">
    <style>body{font-family:sans-serif;background:#f4f4f9;padding:20px;} table{width:100%;background:white;border-collapse:collapse;} th,td{padding:10px;border:1px solid #ddd;}</style>
    </head><body><h2>Live Trades</h2><table><tr><th>Time</th><th>Price</th><th>Strike</th><th>Status</th></tr>
    {% for t in trades %}<tr><td>{{t.time}}</td><td>{{t.price}}</td><td>{{t.strike}}</td><td>{{t.status}}</td></tr>{% endfor %}
    </table></body></html>
    """
    return render_template_string(html, trades=reversed(TRADE_HISTORY))

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    trade_info = {"time": datetime.now().strftime("%H:%M:%S"), "price": "N/A", "strike": "N/A", "status": "Pending"}
    try:
        data = request.get_json(force=True)
        signal, price = data.get("message", "").upper(), data.get("price")
        trade_info["price"] = price
        
        # 1. Reversal
        opp = "PE" if "BUY" in signal else "CE"
        close_opposite_position(opp)

        # 2. Buy ITM
        sec_id, strike = get_itm_id(price, signal)
        trade_info["strike"] = strike
        
        if sec_id:
            order = dhan.place_order(
                tag='MLFusion', transaction_type=dhan.BUY, exchange_segment=dhan.NSE_FNO,
                product_type=dhan.INTRA, order_type=dhan.MARKET, validity=dhan.DAY,
                security_id=sec_id, quantity=35, price=0
            )
            trade_info["status"] = order.get('status', 'failure')
        
        TRADE_HISTORY.append(trade_info)
        return jsonify(trade_info)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
