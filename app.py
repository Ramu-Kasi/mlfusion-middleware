@app.route('/')
def dashboard():
    status_text, status_color = check_token_status()
    # Using triple quotes (""") ensures the multi-line HTML string is never unterminated
    html = """
    <html>
    <head>
        <title>MLFusion Live Dashboard</title>
        <meta http-equiv="refresh" content="60">
        <style>
            body { font-family: sans-serif; margin: 40px; background: #f4f4f9; }
            .status-bar { 
                background: white; 
                padding: 15px; 
                border-radius: 8px; 
                margin-bottom: 20px; 
                display: flex; 
                justify-content: space-between; 
                align-items: center; 
                box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
            }
            .status-pill { 
                padding: 5px 15px; 
                border-radius: 20px; 
                color: white; 
                font-weight: bold; 
            }
            table { width: 100%; border-collapse: collapse; background: white; }
            th, td { padding: 12px; border: 1px solid #ddd; text-align: left; }
            th { background: #333; color: white; }
            .CE { color: green; font-weight: bold; }
            .PE { color: red; font-weight: bold; }
        </style>
    </head>
    <body>
        <div class="status-bar">
            <div>
                <strong>Dhan API Status:</strong> 
                <span class="status-pill" style="background-color: {{ color }};">{{ status }}</span>
            </div>
            {% if status != 'Active' %}
            <a href="https://web.dhan.co/" target="_blank" style="color:#dc3545; font-weight:bold;">Refresh Token Now</a>
            {% endif %}
            <div style="font-size: 12px; color: #666;">Auto-refreshing in 60s</div>
        </div>

        <h2>Bank Nifty Trades</h2>
        <table>
            <tr><th>Time</th><th>Price</th><th>Strike</th><th>Type</th><th>Expiry</th><th>Status</th><th>Remarks</th></tr>
            {% for t in trades %}
            <tr>
                <td>{{ t.time }}</td>
                <td>{{ t.price }}</td>
                <td>{{ t.strike }}</td>
                <td class="{{ t.type }}">{{ t.type }}</td>
                <td>{{ t.expiry }}</td>
                <td>{{ t.status }}</td>
                <td>{{ t.remarks }}</td>
            </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    """
    return render_template_string(html, trades=reversed(TRADE_HISTORY), status=status_text, color=status_color)
