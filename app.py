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
    """Force logs to show in Render immediately"""
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def load_scrip_master():
    """BOOT: Aggressive column identification for SecurityID to avoid DH-905"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Loading Master CSV and filtering Bank Nifty...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Identify columns dynamically
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        sym_col = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)
        exch_col = next((c for c in df.columns if 'EXCHANGE' in c.upper()), None)
        exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)
        strike_col = next((c for c in df.columns if 'STRIKE' in c.upper()), None)
        type_col = next((c for c in df.columns if 'OPTION_TYPE' in c.upper()), None)
        id_col = next((c for c in df.columns if 'SMST_SECURITY_ID' in c.upper() or 'SECURITY_ID' in c.upper()), None)

        # Filter: Bank Nifty NSE Index Options
        mask = (
            (df[inst_col].str.contains('OPTIDX', na=False)) & 
            (df[sym_col].str.contains('BANKNIFTY', case=False, na=False)) &
            (~df[sym_col].str.contains('BANKEX', case=False, na=False))
        )
        if exch_col:
            mask = mask & (df[exch_col].str.contains('NSE', case=False, na=False))

        # Memory pruning
        needed_cols = [id_col, strike_col, type_col, exp_col]
        SCRIP_MASTER_DATA = df[mask][needed_cols].copy()
        
        # Format IDs and Dates properly
        SCRIP_MASTER_DATA[id_col] = SCRIP_MASTER_DATA[id_col].astype(str).str.split('.').str[0]
        SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
        SCRIP_MASTER_DATA = SCRIP_MASTER_DATA.dropna(subset=[exp_col])
        
        log_now(f"BOOT: Success! {len(SCRIP_MASTER_DATA)} Bank Nifty contracts loaded.")
    except Exception as e:
        log_now(f"CRITICAL BOOT ERROR: {e}")

# Run the master load during the Render build/deploy phase
load_scrip_master()

def close_opposite_position(type_to_close):
    """Reversal closer with 100ms delay"""
    try:
        positions = dhan.get_positions()
        if positions.get('status') == 'success' and positions.get('data'):
            for pos in positions['data']:
                symbol = pos.get('tradingSymbol', '')
                qty = int(pos.get('netQty', 0))
                
                if "BANKNIFTY" in symbol and symbol.endswith(type_to_close) and qty != 0:
                    log_now(f"REVERSAL: Closing {symbol}")
                    dhan.place_order(
                        tag='MLFusion_Exit',
                        transaction_type=dhan.SELL if qty > 0 else dhan.BUY,
                        exchange_segment=dhan.NSE_FNO,
                        product_type=dhan.INTRA,
                        order_type=dhan.MARKET,
                        validity=dhan.DAY,
                        security_id=pos['securityId'],
                        quantity=abs(qty),
                        price=0
                    )
                    time.sleep(0.1) 
        return True
    except Exception as e:
        log_now(f"REVERSAL ERROR: {e}")
        return False

def get_itm_id(price, signal):
    """NEAREST EXPIRY LOOKUP: FIXED TRY/EXCEPT BLOCK STRUCTURE"""
    try:
        if SCRIP_MASTER_DATA is None: 
            return None, None
            
        atm_strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        strike = (atm_strike - 100) if opt_type == "CE" else (atm_strike + 100)
        
        id_col, strike_col, type_col, exp_col = SCRIP_MASTER_DATA.columns
        
        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & 
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ].copy()
        
        if not match.empty:
            # FIXED LINE 112: Ensure today calculation and sorting is inside the try block
            today = pd.Timestamp(datetime.now().date())
            match = match[match[exp_col] >= today]
            match = match.sort_values(by=exp_col, ascending=True)
            
            if not match.empty:
                row = match.iloc[0]
                return str(row[id_col]), strike
        return None, strike
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None

@app.route('/')
def dashboard():
    """Summary Page remains intact"""
    html = """
    <html>
        <head>
            <title>MLFusion Live Dashboard</title>
            <meta http-equiv="refresh" content="30">
            <style>
                body { font-family: sans-serif; margin: 40px; background: #f4f4f9; }
                table { width: 100%; border-collapse: collapse; background: white; }
                th, td { padding: 12px; border: 1px solid #ddd; text-align: left; }
                th { background: #333; color: white; }
                .CE { color: green; font-weight: bold; }
                .PE { color: red; font-weight: bold; }
            </style>
        </head>
        <body>
            <h2>Bank Nifty Live Summary</h2>
            <p><b>Total Signal Count:</b> {{ count }}</p>
            <table>
                <tr><th>Time</th><th>Price</th><th>Strike</th><th>Type</th><th>Dhan Status</th><th>Remarks</th></tr>
                {% for t in trades %}
                <tr>
                    <td>{{ t.time }}</td>
                    <td>{{ t.price }}</td>
                    <td>{{ t.strike }}</td>
                    <td class="{{ t.type }}">{{ t.type }}</td>
                    <td>{{ t.status }}</td>
                    <td>{{ t.remarks }}</td>
                </tr>
                {% endfor %}
            </table>
        </body>
    </html>
    """
    return render_template_string(html, trades=reversed(TRADE_HISTORY), count=len(TRADE_HISTORY))

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    """Signal handler with verified DH-905 fix logic"""
    log_now(f"SIGNAL RECEIVED: {request.get_data(as_text=True)}")
    trade_info = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "price": "N/
