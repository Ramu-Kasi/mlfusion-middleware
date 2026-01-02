import os
import datetime
from flask import Flask, request

app = Flask(__name__)

def display_trade_log(trades):
    """
    Stays 100% true to your automated logic.
    Strike, PnL, and Status now pull directly from your trade data.
    """
    LOT_SIZE = 30 # Bank Nifty 2026
    
    # Strictly maintaining your UI layout and spacing
    header = (f"{'Symbol':<10} | {'Signal Time':<18} | {'Type':<5} | {'Lots':<4} | "
              f"{'Strike':<7} | {'Entry':<8} | {'Exit':<8} | {'PnL':<9} | {'Status':<8} | {'Remarks':<15}")
    
    print("-" * len(header))
    print(header)
    print("-" * len(header))

    for trade in trades:
        # 1. PnL: Automatically calculated from your Entry/Exit
        pnl = (trade['exit_price'] - trade['entry_price']) * (trade['lots'] * LOT_SIZE) if trade['exit_price'] > 0 else 0.0
        
        # 2. Strike: Now properly mapped to your internal 'strike_price' logic
        strike_display = trade.get('strike_price', 'Calculating...')
        
        row = (f"{trade['symbol']:<10} | "
               f"{trade['signal_time']:<18} | "
               f"{trade['type']:<5} | "
               f"{trade['lots']:<4} | "
               f"{strike_display:<7} | "
               f"{trade['entry_price']:<8.2f} | "
               f"{trade['exit_price']:<8.2f} | "
               f"{pnl:<9.2f} | "
               f"{trade.get('status', 'OPEN'):<8} | "
               f"{trade.get('remarks', ''):<15}")
        print(row)
    print("-" * len(header))

@app.route('/webhook', methods=['POST'])
def webhook_receiver():
    # Minimal code to keep Render active without touching your core
    return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
