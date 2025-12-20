import os # Add this at the top

if __name__ == '__main__':
    # This gets the PORT variable from Render, or defaults to 5000 if not found
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
    
from flask import Flask, request, jsonify
import logging
import sys

app = Flask(__name__)

# Configure logging to output to stdout (Standard Output)
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

    message = data.get("message", "").replace(" ", "").upper()

    if "BUY" in message:
        logging.info("PROCESS SUCCESS: The message is BUY")
        return jsonify({"status": "BUY"})
    
    elif "SELL" in message:
        logging.info("PROCESS SUCCESS: The message is SELL")
        return jsonify({"status": "SELL"})
    
    else:
        logging.info(f"PROCESS UNKNOWN: Received {message}")
        return jsonify({"status": "UNKNOWN"})

if __name__ == '__main__':
    app.run(debug=True)
