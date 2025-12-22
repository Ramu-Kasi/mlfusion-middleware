@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    # Force immediate log of the signal
    raw_body = request.get_data(as_text=True)
    sys.stderr.write(f"!!! [ALGO_ENGINE]: RAW SIGNAL -> {raw_body}\n")
    sys.stderr.flush()

    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({"status": "error", "message": "Invalid JSON"}), 400

        tv_price = data.get("price")
        signal = data.get("message", "")

        sec_id, strike = get_atm_id(tv_price, signal)
        
        if not sec_id:
            sys.stderr.write(f"!!! [ALGO_ENGINE]: Strike {strike} not found in CSV\n")
            sys.stderr.flush()
            return jsonify({"status": "not_found"}), 404

        sys.stderr.write(f"!!! [ALGO_ENGINE]: PLACING ORDER for {sec_id}\n")
        sys.stderr.flush()

        order = dhan.place_order(
            security_id=sec_id,
            exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.BUY, 
            quantity=35, 
            order_type=dhan.MARKET,
            product_type=dhan.MARGIN,
            price=0,
            validity='DAY'
        )

        # Log the actual response from Dhan
        sys.stderr.write(f"!!! [ALGO_ENGINE]: DHAN RESPONSE -> {order}\n")
        sys.stderr.flush()

        return jsonify(order), 200

    except Exception as e:
        sys.stderr.write(f"!!! [ALGO_ENGINE]: RUNTIME ERROR -> {str(e)}\n")
        sys.stderr.flush()
        return jsonify({"status": "error", "reason": str(e)}), 500
