from flask import Flask, render_template, request, Response, stream_with_context
import requests
import os

# NOTE: Pandas and Plotly are no longer needed on the Flask server side 
# because we moved the charting logic to the Frontend (JavaScript) 
# to enable the "Live Logging" feature.
# import pandas as pd
# import json
# import plotly
# import plotly.express as px

app = Flask(__name__)

# Configuration
BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000")

@app.route('/', methods=['GET'])
def dashboard():
    """
    Serves the dashboard HTML page.
    The logic for fetching data and updating the chart has moved to 
    the JavaScript in 'dashboard.html' to support live streaming.
    """
    return render_template('dashboard.html')

@app.route('/api/run_simulation', methods=['GET'])
def proxy_simulation():
    """
    Relays streaming NDJSON from the backend to the client.
    Handles the cross-service handshake and ensures the stream context is preserved.
    """
    start_date = request.args.get('start_date')
    if not start_date:
        return {"error": "start_date parameter required"}, 400

    def generate():
        try:
            # Stream=True prevents loading the entire simulation result into memory
            with requests.get(
                f"{BACKEND_URL}/simulate", 
                params={"start_date": start_date, "horizon_days": 7}, 
                stream=True,
                timeout=120
            ) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=None):
                    if chunk:
                        yield chunk
        except Exception as e:
            # Format error as NDJSON so the frontend parser can catch it
            yield f'{{"step": "error", "message": "{str(e)}"}}\n'.encode()

    return Response(
        stream_with_context(generate()), 
        content_type='application/x-ndjson'
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)