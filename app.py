from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/mlfusion", methods=["POST"])
def mlfusion():
    data = request.get_json()
    message = data.get("message")
    print(f"Received message: {message}")  # <-- This will show in your logs
    return jsonify({"status": "ok"})

if __name__ == "__main__":
   app.run(host="0.0.0.0", port=5000)
