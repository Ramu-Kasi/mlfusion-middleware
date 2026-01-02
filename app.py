import os
import datetime
from flask import Flask, request

app = Flask(__name__)

def display_trade_log(trades):
    """
    Displays the trade log with all requested columns.
    Built on base version template.
    """
    LOT_SIZE = 30 # Bank Nifty 2026 Lot Size
    
    # Header: Precisely aligned for your UI layout
    header = (f"{'Symbol':<10} | {'Signal Time':<18} | {'Type':<5} | {'Lots':<4} | "
              f"{'Strike':<7} | {'Entry':<8} | {'Exit':<8} | {'PnL':<9} | {'Status':<8} | {'Remarks':<15}")
    
    print("-" * len(header))
    print(header)
    print("-" * len(header))

    for trade in trades:
        # PnL Calculation
        pnl = (trade['exit_price'] - trade['entry_price']) * (trade['lots'] * LOT_SIZE) if trade['exit_price'] > 0 else 0.0
        
        # Mapping Strike Price from your logic
        strike_display = trade.get('strike_price', 'N/A')
        
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

@app.route('/', methods=['GET'])
def home():
    """FIX for 404 Error: Tells Render the bot is alive when visiting the URL"""
    return "Bot is Online and Ready for Monday Morning!", 200

@app.route('/webhook', methods=['POST'])
def webhook_receiver():
    """Receives alerts from TradingView 15m/1h Signals"""
    data
