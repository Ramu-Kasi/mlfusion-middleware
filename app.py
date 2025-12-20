from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/mlfusion", methods=["POST"])
def mlfusion():
    data = request.get_json()
    message = data.get("message", "")
    print(f"Received message: {message}")  # Check logs

    # Normalize message to handle case and spaces
    if message.strip().upper() == "BUY":
        return jsonify({"status": "BUY"}), 200
    elif message.strip().upper() == "SELL":
        return jsonify({"status": "SELL"}), 200
    else:
        return jsonify({"status": "UNKNOWN"}), 200

if __name__ == "__main__":
   app.run(host="0.0.0.0", port=5000)
