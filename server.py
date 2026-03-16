"""
Flask web server serving the SaiyanBot dashboard
Run: python server.py
"""

import json
import threading
import time
from flask import Flask, jsonify, request, Response
from bot import bot, BotConfig, REGIME_PARAMS

app = Flask(__name__, static_folder=".", static_url_path="")
app.secret_key = "saiyan_secret_2024"


# ── SSE helper ────────────────────────────────────────────────────────────────

def sse_stream():
    """Server-Sent Events stream for real-time dashboard updates"""
    while True:
        try:
            state = bot.get_state()
            data = json.dumps(state)
            yield f"data: {data}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        time.sleep(2)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return app.send_static_file("dashboard.html")


@app.route("/api/stream")
def stream():
    return Response(sse_stream(),
                    content_type="text/event-stream",
                    headers={
                        "Cache-Control": "no-cache",
                        "X-Accel-Buffering": "no",
                    })


@app.route("/api/state")
def state():
    return jsonify(bot.get_state())


@app.route("/api/start", methods=["POST"])
def start_bot():
    if not bot._running:
        bot.start()
        return jsonify({"status": "started"})
    return jsonify({"status": "already_running"})


@app.route("/api/stop", methods=["POST"])
def stop_bot():
    bot.stop()
    return jsonify({"status": "stopped"})


@app.route("/api/config", methods=["GET", "POST"])
def config():
    if request.method == "GET":
        from dataclasses import asdict
        return jsonify(asdict(bot.config))
    data = request.json or {}
    # Type coercion
    typed = {}
    defaults = BotConfig()
    for k, v in data.items():
        if not hasattr(defaults, k):
            continue
        default_val = getattr(defaults, k)
        try:
            if isinstance(default_val, bool):
                if isinstance(v, bool):
                    typed[k] = v
                elif isinstance(v, str):
                    typed[k] = v.lower() not in ("false", "0", "no", "off")
                else:
                    typed[k] = bool(v)
            elif isinstance(default_val, int):
                typed[k] = int(v)
            elif isinstance(default_val, float):
                typed[k] = float(v)
            else:
                typed[k] = v
        except (ValueError, TypeError):
            pass
    bot.update_config(typed)
    return jsonify({"status": "ok", "updated": list(typed.keys())})


@app.route("/api/close_trade", methods=["POST"])
def close_trade():
    data = request.json or {}
    trade_id = data.get("trade_id")
    if not trade_id:
        return jsonify({"error": "trade_id required"}), 400
    bot.close_trade(trade_id)
    return jsonify({"status": "closed", "trade_id": trade_id})


@app.route("/api/regime_params")
def regime_params():
    return jsonify(REGIME_PARAMS)


@app.route("/api/reset_data", methods=["POST"])
def reset_data():
    try:
        bot.reset_data()
        return jsonify({"status": "success", "message": "Database and state reset successfully."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Auto-start bot on server launch
    bot.start()
    print("\n" + "=" * 60)
    print("  🚀 SAIYAN OCC Trading Bot Dashboard")
    print("  📊 Open your browser: http://localhost:5000")
    print("  📄 Paper mode: ENABLED (safe by default)")
    print("  ⚙️  To use live trading: set API keys in Settings tab")
    print("=" * 60 + "\n")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
