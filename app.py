import os
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime
import pytz

app = Flask(__name__)

# --- CONFIGURATION ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# STATE SETTINGS
TRADE_HISTORY = [] 

def get_api_status():
    try:
        profile = dhan.get_fund_limits()
        if profile.get('status') == 'success':
            return "Active"
        return "Expired"
    except Exception:
        return "Inactive"

# --- 1. DASHBOARD TEMPLATE (UNTOUCHED) ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>MLFusion Trading Bot</title>
    <meta http-equiv="refresh" content="60">
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f0f2f5; margin: 0; padding: 20px; }
        .status-bar { background: white; padding: 15px; border-radius: 8px; display: flex; align-items: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; gap: 20px; }
        .dot { height: 12px; width: 12px; border-radius: 50%; display: inline-block; margin-right: 5px; }
        .status-active { background-color: #28a745; color: #fff; padding: 2px 10px; border-radius: 10px; font-size: 0.9em; font-weight: bold; }
        .status-expired { background-color: #dc3545; color: #fff; padding: 2px 10px; border-radius: 10px; font-size: 0.9em; font-weight: bold; }
        .refresh-text { color: #666; font-size: 0.9em; margin-left: auto; }
        table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        th { background: #333; color: white; padding: 12px; text-align: left; font-size: 0.9em; }
        td { padding: 12px; border-bottom: 1px solid #eee; font-size: 0.9em; }
        .type-ce { color: #28a745; font-weight: bold; }
        .type-pe { color: #dc3545; font-weight: bold; }
    </style>
</head>
<body>
    <div class="status-bar">
        <b>Dhan API Status:</b>
        {% set api_state = get_status() %}
        <span class="{{ 'status-active' if api_state == 'Active' else 'status-expired' }}">
            {{ api_state }}
        </span>
        <span class="refresh-text">Refreshes every 60s | Last Check: {{ last_run }} (IST)</span>
    </div>

    <h3>Trade History</h3>
    <table>
        <thead>
            <tr>
                <th>Time (IST)</th>
                <th>Price</th>
                <th>Strike</th>
                <th>Type</th>
                <th>Expiry</th>
                <th>Status</th>
                <th>Remarks</th>
            </tr>
        </thead>
        <tbody>
            {% for trade in history %}
            <tr>
                <td>{{ trade.time }}</td>
                <td>{{ trade.price }}</td>
                <td>{{ trade.strike }}</td>
                <td class="type-{{ trade.type|lower }}">{{ trade.type }}</td>
                <td>{{ trade.expiry }}</td>
                <td>{{ trade.status }}</td>
                <td>{{ trade.remarks }}</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</body>
</html>
"""

@app.route('/')
def dashboard():
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist).strftime("%H:%M:%S")
    return render_template_string(
        DASHBOARD_HTML, 
        history=TRADE_HISTORY, 
        get_status=get_api_status,
        last_run=now_ist
    )

# --- 2. SURGICAL REVERSAL ---
def surgical_reversal(signal_type):
    try:
        positions_resp = dhan.get_positions()
        if positions_resp.get('status') == 'success':
            for pos in positions_resp.get('data', []):
                symbol = pos.get('tradingSymbol', '').upper()
                net_qty = int(pos.get('netQty', 0))
                if "BANKNIFTY" in symbol and net_qty != 0:
                    is_call = "CE" in symbol
                    is_put = "PE" in symbol
                    if (signal_type == "BUY" and is_put) or (signal_type == "SELL" and is_call):
                        exit_side = dhan.SELL if net_qty > 0 else dhan.BUY
                        dhan.place_order(
                            security_id=pos['securityId'],
                            exchange_segment=pos['exchangeSegment'],
                            transaction_type=exit_side,
                            quantity=abs(net_qty),
                            order_type=dhan.MARKET,
                            product_type=dhan.MARGIN,
                            price=0
                        )
        return True
    except Exception:
        return False

# --- 3. WEBHOOK ENDPOINT ---
@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json(force=True, silent=True)
    if not data: return jsonify({"status": "no data"}), 400
    
    # Payload Logic: Matches 'message' from your TradingView image
    signal = data.get('message', data.get('signal', '')).upper()
    price = float(data.get('price',
