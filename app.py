import os
import logging
import sys
from flask import Flask, request, jsonify

# 1. Initialize the app first
app = Flask(__name__)

# 2. Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json()
    
    # Check if 'message' exists to prevent errors
    if not data or 'message' not in data:
        logging.warning("Received request with no message key")
        return jsonify({"status": "ERROR", "reason": "No message provided"}), 400

    # Clean the message and get the strike if provided
    message = data.get("message", "").replace(" ", "").upper()
    strike = data.get("strike", "N/A")

    if "BUY" in message:
        logging.info(f"PROCESS SUCCESS: BUY signal at Strike: {strike}")
        return jsonify({"status": "BUY", "strike": strike})
    
    elif "SELL" in message:
        logging.info(f"PROCESS SUCCESS: SELL signal at Strike: {strike}")
        return jsonify({"status": "SELL", "strike": strike})
    
    else:
        logging.info(f"PROCESS UNKNOWN: Received {message}")
        return jsonify({"status": "UNKNOWN"})

# 3. The Run logic must be at the very bottom
if __name__ == '__main__':
    # This specifically looks for your Render Environment Variable 'PORT'
    # If not found, it defaults to 5000
    port = int(os.environ.get("PORT", 5000))
    
    # host='0.0.0.0' is required for Render to connect to the internet
    app.run(host='0.0.0.0', port=port)
