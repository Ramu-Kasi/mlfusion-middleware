from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/mlfusion", methods=["POST"])
def mlfusion():
    signal = request.json.get("message")
    print("Signal received:", signal)
    return jsonify({"status": "ok"})

app.run(host="0.0.0.0", port=5000)
