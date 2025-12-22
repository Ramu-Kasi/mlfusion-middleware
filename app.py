import os
from flask import Flask, request, jsonify
from dhanhq import dhanhq

app = Flask(__name__)

# Basic Connection Setup
def get_dhan_connection():
    client_id = os.environ.get('DHAN_CLIENT_ID')
    access_token = os.environ.get('DHAN_ACCESS_TOKEN')
    return dhanhq(client_id, access_token)

@app.route('/')
def home():
    return "✅ Bridge is Online and Connected to Dhan."

# BABY STEP: Listen for the signal
@app.route('/mlfusion', methods=['POST'])
def receive_signal():
    try:
        # 1. Capture the incoming TradingView JSON
        data = request.get_json()
        
        # 2. Print it so you can see it in Render logs
        print(f"DEBUG: Signal Received -> {data}")
        
        # 3. Respond back to TradingView so it knows we got it
        return jsonify({"status": "received", "message": "Signal logged in bridge"}), 200

    except Exception as e:
        print(f"ERROR: {str(e)}")
        return jsonify({"error": "Invalid JSON"}), 400

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
