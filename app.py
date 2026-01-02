# ==============================================================================
# PROJECT: Auto-Logger (Bank Nifty Focus)
# LAST STABLE VERSION: January 2, 2026
# CHANGE LOG: Updated BANKNIFTY_LOT_SIZE to 30 as per Jan 2026 NSE revision.
# ==============================================================================

from dhanhq import dhanhq

# --- 1. Configuration & Authentication ---
client_id = "YOUR_CLIENT_ID"
access_token = "YOUR_ACCESS_TOKEN"
dhan = dhanhq(client_id, access_token)

# --- 2. Fixed Parameters (Revised for Jan 2026) ---
# BN lot size changed from 35 to 30 effective this series
BANKNIFTY_LOT_SIZE = 30  
target_lots = 1
order_quantity = target_lots * BANKNIFTY_LOT_SIZE

# --- 3. Core Execution Logic ---
def place_banknifty_order(security_id, transaction_type):
    """
    Places a Market Order for Bank Nifty.
    - transaction_type: dhan.BUY or dhan.SELL
    - quantity: fixed at 30 units (1 lot)
    """
    try:
        # Placing order with exact lot multiplier to avoid silent rejection
        response = dhan.place_order(
            tag='Auto-Logger',
            transaction_type=transaction_type,
            exchange_segment=dhan.NFO,
            product_type=dhan.INTRA,
            order_type=dhan.MARKET,
            validity='DAY',
            security_id=security_id,
            quantity=order_quantity,  # Triple-checked: results in 30
            price=0
        )
        
        # --- Simple Log Handling ---
        # If status is 'success', it means Dhan accepted the request.
        # If 'remarks' contains an error, it explains why the Exchange rejected it.
        if response.get('status') == 'success':
            order_id = response.get('data', {}).get('orderId')
            print(f"SUCCESS: Order {order_id} placed for {order_quantity} units.")
        else:
            error_msg = response.get('remarks', 'Unknown Exchange Error')
            print(f"REJECTED: {error_msg}")
            
        return response

    except Exception as e:
        print(f"SCRIPT ERROR: {str(e)}")

# --- 4. Example Usage ---
# To test: uncomment the line below with a valid Bank Nifty Security ID
# place_banknifty_order(security_id='45678', transaction_type=dhan.BUY)
