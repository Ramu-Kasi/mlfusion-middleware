import os
from flask import Flask, jsonify
from dhanhq import dhanhq

app = Flask(__name__)

# Basic Connection Setup
def get_dhan_connection():
    # Pulling credentials safely from Render Environment Variables
    client_id = os.environ.get('DHAN_CLIENT_ID')
    access_token = os.environ.get('DHAN_ACCESS_TOKEN')
    
    # Initializing the Dhan library
    return dhanhq(client_id, access_token)

@app.route('/')
def test_connection():
    dhan = get_dhan_connection()
    
    # The "Baby Step" test: Fetch fund limits
    funds = dhan.get_fund_limits()
    
    if funds.get('status') == 'success':
        balance = funds['data']['availabelBalance']
        return f"✅ Connection Successful! Available Balance: {balance}"
    else:
        return f"❌ Connection Failed: {funds.get('remarks', 'Unknown Error')}", 401

if __name__ == "__main__":
    # Render requires binding to 0.0.0.0
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
