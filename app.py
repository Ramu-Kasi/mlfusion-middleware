import os
from flask import Flask, request

app = Flask(__name__)

def display_trade_log(trades):
    # Standard 2026 Lot Size
    LOT_SIZE = 30 
    header = (f"{'Symbol':<10} | {'Signal Time':<18} | {'Type':<5} | {'Lots':<4} | "
              f"{'Strike':<7} | {'Entry':<8} | {'Exit':<8} | {'PnL':<9} | {'Status':<8} | {'Remarks':<15}")
    print("-" * len(header))
    print(header)
    print("-" * len(header))

    for trade in trades:
        pnl = (trade['exit_price'] - trade['entry_price']) * (trade['lots'] * LOT_SIZE) if trade['exit_price'] > 0 else 0.0
        row = (f"{trade['symbol']:<10} | {trade['signal_time']:<18} | {trade['type']:<5} | "
               f"{trade['lots']:<4} | {trade.get('strike_price', 'N/A'):<7} | "
               f"{trade['entry_price']:<8.2f} | {trade['exit_price']:<8.2f} | "
               f"{pnl:<9.2f} | {trade.get('status', 'OPEN'):<8} | {trade.get('remarks', ''):<15}")
        print(row)

@app.route('/')
def health_check():
    return "Bot is Live", 200

@app.route('/webhook', methods=['POST'])
def webhook():
    print(f"Signal Received: {request.json}")
    return "OK", 200

if __name__ == "__main__":
    # This line solves the "No open ports" error from your screenshot
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
