import os
import sys
import time
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime, timedelta

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
    """BOOT: Loading Master CSV and filtering strictly for Monthly Bank Nifty"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Loading Master CSV (NSE Tuesday Expiry Cycle)...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        sym_col = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)
        exch_col = next((c for c in df.columns if 'EXCHANGE' in c.upper()), None)
        exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)
        strike_col = next((c for c in df.columns if 'STRIKE' in c.upper()), None)
        type_col = next((c for c in df.columns if 'OPTION_TYPE' in c.upper()), None)
        id_col = next((c for c in df.columns if 'SMST_SECURITY_ID' in c.upper() or 'SECURITY_ID' in c.upper()), None)

        mask = (
            (df[inst_col].str.contains('OPTIDX', na=False)) & 
            (df[sym_col].str.contains('BANKNIFTY', case=False, na=False))
        )
        if exch_col:
            mask = mask & (df[exch_col].str.contains('NSE', case=False, na=False))

        needed_cols = [id_col, strike_col, type_col, exp_col]
        SCRIP_MASTER_DATA = df[mask][needed_cols].copy()
        SCRIP_MASTER_DATA[id_col] = SCRIP_MASTER_DATA[id_col].astype(str).str.split('.').str[0]
        SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
        SCRIP_MASTER_DATA = SCRIP_MASTER_DATA.dropna(subset=[exp_col])
        
        log_now(f"BOOT: Success! {len(SCRIP_MASTER_DATA)} Monthly BN contracts loaded.")
    except Exception as e:
        log_now(f"CRITICAL BOOT ERROR: {e}")

load_scrip_master()

def close_opposite_position(type_to_close):
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
                        product_type=dhan.MARGIN, 
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
    """LOOKUP: Tuesday Expiry Aware + Friday Holiday Early Exit"""
    try:
        if SCRIP_MASTER_DATA is None: return None, None, "N/A"
            
        atm_strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        strike = (atm_strike - 100) if opt_type == "CE" else (atm_strike + 100)
        
        id_col, strike_col, type_col, exp_col = SCRIP_MASTER_DATA.columns
        match = SCRIP_MASTER_DATA[(SCRIP_MASTER_DATA[strike_col] == strike) & (SCRIP_MASTER_DATA[type_col] == opt_type)].copy()
        
        if not match.empty:
            now = datetime.now()
            today = pd.Timestamp(now.date())
            match = match[match[exp_col] >= today].sort_values(by=exp_col)
            unique_expiries = sorted(match[exp_col].unique())
            
            if unique_expiries:
                # Target Expiry (Tuesday at 3:30 PM)
                expiry_time = unique_expiries[0].replace(hour=15, minute=30)
                hours_left = (expiry_time - now).total_seconds() / 3600
                
                # Logic: Monday Expiry Eve OR Friday Afternoon with Holiday Monday (approx < 100hrs)
                is_fri_afternoon = (now.weekday() == 4 and now.hour >= 14)
                
                if (hours_left < 48) or (is_fri_afternoon and hours_left < 100):
                    log_now(f"EXPIRY PROTECTION: Moving to Next Monthly Expiry.")
                    target_expiry = unique_expiries[1] if len(unique_expiries) >= 2 else unique_expiries[0]
                else:
                    target_expiry = unique_expiries[0]
            else:
                return None, strike, "N/A"
                
            final_match = match[match[exp_col] == target_expiry]
            if not final_match.empty:
                row = final_match.iloc[0]
                return str(row[id_col]), strike, target_expiry.strftime('%Y-%m-%d')
        return None, strike, "N/A"
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None, "N/A"

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    log_now(f"SIGNAL RECEIVED: {request.get_data(as_text=True)}")
    trade_info = {"time": datetime.now().strftime("%H:%M:%S"), "price": "N/A", "strike": "N/A", "type": "N/A", "expiry": "N/A", "status": "Pending", "remarks": ""}
    try:
        data = request.get_json(force=True, silent=True)
        if not data: return jsonify({"status": "error"}), 400

        signal, current_price = data.get("message", "").upper(), data.get("price")
        trade_info["price"] = current_price
        current_type, opposite_type = ("CE", "PE") if "BUY" in signal else ("PE", "CE")
        trade_info["type"] = current_type

        close_opposite_position(opposite_type)

        sec_id, strike, exp_date = get_itm_id(current_price, signal)
        trade_info["strike"], trade_info["expiry"] = strike, exp_date
        
        if not sec_id:
            trade_info["status"], trade_info["remarks"] = "Failure", "ID Lookup Error"
            TRADE_HISTORY.append(trade_info)
            return jsonify({"status": "not_found"}), 404

        order = dhan.place_order(tag='MLFusion_BN', transaction_type=dhan.BUY, exchange_segment=dhan.NSE_FNO, product_type=dhan.MARGIN, order_type=dhan.MARKET, validity=dhan.DAY, security_id=sec_id, quantity=35, price=0)

        trade_info["status"] = order.get('status', 'failure')
        trade_info["remarks"] = f"OrderID: {order.get('data', {}).get('orderId', 'N/A')}" if trade_info["status"] == "success" else str(order.get('remarks', 'API Error'))
        TRADE_HISTORY.append(trade_info)
        return jsonify({"status": trade_info["status"], "order_data": order})
    except Exception as e:
        trade_info["status"], trade_info["remarks"] = "Error", str(e)
        TRADE_HISTORY.append(trade_info)
        return jsonify({"status": "error"}), 500

@app.route('/')
def dashboard():
    html = """<html><head><title>MLFusion Live Dashboard</title><meta http-equiv="refresh" content="30"><style>body{font-family:sans-serif;margin:40px;background:#f4f4f9;}table{width:100%;border-collapse:collapse;background:white;}th,td{padding:12px;border:1px solid #ddd;text-align:left;}th{background:#333;color:white;}.CE{color:green;font-weight:bold;}.PE{color:red;font-weight:bold;}</style></head><body><h2>Bank Nifty Monthly Summary (NSE Tuesday Cycle)</h2><p><b>Total Signal Count:</b> {{ count }}</p><table><tr><th>Time</th><th>Price</th><th>Strike</th><th>Type</th><th>Expiry</th><th>Dhan Status</th><th>Remarks</th></tr>{% for t in trades %}<tr><td>{{ t.time }}</td><td>{{ t.price }}</td><td>{{ t.strike }}</td><td class="{{ t.type }}">{{ t.type }}</td><td>{{ t.expiry }}</td><td>{{ t.status }}</td><td>{{ t.remarks }}</td></tr>{% endfor %}</table></body></html>"""
    return render_template_string(html, trades=reversed(TRADE_HISTORY), count=len(TRADE_HISTORY))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
