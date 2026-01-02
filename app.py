import datetime

# --- UI CONSTANTS (STRICTLY MAINTAINED) ---
# No changes to colors, fonts, or layout settings as per user rules.

def display_trade_log(trades):
    """
    Displays the trade log with new columns added.
    1. No. of Lots
    2. PnL (Profit and Loss)
    3. Signal Time (To track 15m bar consistency)
    """
    
    # Updated Header with additional columns
    header = f"{'Symbol':<15} | {'Signal Time':<20} | {'Type':<8} | {'Lots':<6} | {'Entry':<10} | {'Exit':<10} | {'PnL':<10}"
    print("-" * len(header))
    print(header)
    print("-" * len(header))

    for trade in trades:
        # Standard calculation: (Exit - Entry) * (Lots * LotSize)
        # Note: BN Lot Size is 30 for Jan 2026
        lot_size = 30
        pnl = (trade['exit_price'] - trade['entry_price']) * (trade['lots'] * lot_size) if trade['exit_price'] > 0 else 0.0
        
        # Color/Style remains inherited from your base version
        # Only the data columns are added/populated
        row = (f"{trade['symbol']:<15} | "
               f"{trade['signal_time']:<20} | "
               f"{trade['type']:<8} | "
               f"{trade['lots']:<6} | "
               f"{trade['entry_price']:<10.2f} | "
               f"{trade['exit_price']:<10.2f} | "
               f"{pnl:<10.2f}")
        print(row)
    print("-" * len(header))

# Example usage for verification
sample_trades = [
    {
        'symbol': 'BANKNIFTY',
        'signal_time': '2026-01-02 10:15:00',
        'type': 'BUY',
        'lots': 1,
        'entry_price': 1000.00,
        'exit_price': 1050.00
    }
]

# display_trade_log(sample_trades)
