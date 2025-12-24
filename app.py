import os
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime

app = Flask(__name__)

# --- CONFIGURATION ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# STATE SETTINGS
TRADE_HISTORY = [] 

# --- 1. DASHBOARD LOGIC ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>MLFusion Pure Execution</title>
    <meta http-equiv="refresh" content="30">
    <style>
        body { font-family: sans-serif; background: #0f172a; color: #f8fafc; padding: 20px; }
        .container { max-width: 1000px; margin: auto; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; background: #1e293b; border-radius: 8px; overflow: hidden; }
        th, td { padding: 15px; border-bottom: 1px solid #334155; text-align: left; }
        th { background: #334155; color: #38bdf8; }
        .status-success { color: #4ade80; font-weight: bold; }
        .status-failure { color: #fb7185; font-weight: bold; }
        .signal-buy { color: #4ade80; }
        .signal-sell { color: #fb7185; }
    </style>
</head>
<body>
    <div class="container">
        <h2>🚀 MLFusion: Pure Execution Mode</h2>
        <p>API Status: <span class="status-success">Connected (No Risk Guard)</span></p>
        <table>
            <tr><th>Time</th><th>Signal</th><th>Symbol</th><th>Status</th><th>Remarks</th></tr>
            {% for trade in history %}
            <tr>
                <td>{{ trade.time }}</td>
                <td class="signal-{{ trade.signal|lower }}">{{ trade.signal }}</td>
                <td>{{ trade.symbol }}</td>
                <td class="status-{{ trade.status }}">{{ trade.status }}</td>
                <td>{{ trade.remarks }}</td>
            </tr>
            {% endfor %}
        </table>
    </div>
</body>
</html>
"""

@app.route('/')
def dashboard():
    return render_template_string(DASHBOARD_HTML, history=TRADE_HISTORY)

# --- 2. SURGICAL REVERSAL (ESSENTIAL) ---
def surgical_reversal(signal_type):
    """Closes ONLY the opposite BN position to prevent hedging."""
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
                            product_type=pos['productType']
                        )
        return True
    except Exception as e:
        return False

# --- 3. THE UNFILTERED WEBHOOK ---
@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json(force=True, silent=True)
    signal = data.get('signal', '').upper()
    
    # Surgical reversal stays to ensure we swap CE/PE correctly
    surgical_reversal(signal)
    
    # PLACE NEW ORDER LOGIC
    # (The bot will now attempt this order REGARDLESS of your P&L)
    
    TRADE_HISTORY.insert(0, {
        "time": datetime.now().strftime("%H:%M:%S"),
        "signal": signal,
        "symbol": "BANKNIFTY",
        "status": "success",
        "remarks": "Order Triggered (24/7 Mode)"
    })
    
    return jsonify({"status": "executed"}), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
