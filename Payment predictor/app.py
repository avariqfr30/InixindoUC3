from flask import Flask, jsonify, request, render_template
import core

app = Flask(__name__)

@app.route('/')
def index():
    # Flask automatically looks for index.html inside the /templates folder
    return render_template('index.html')

@app.route('/api/summary')
def api_summary():
    df = core.get_data()
    if df.empty:
        return jsonify({"stats": {"total_outstanding": 0, "client_count": 0}, "data": []})

    clients = df['Client'].unique()
    total_outstanding = df[df['Status'] == 'Outstanding']['Amount'].sum()
    
    results = []
    for client in clients:
        stats = core.calculate_risk(client)
        if stats:
            results.append(stats)

    # Priority sorting: High risk bubbles to the top, then sorts by money owed
    results.sort(key=lambda x: (x['risk'] == 'High', x['outstanding']), reverse=True)

    return jsonify({
        "stats": {
            "total_outstanding": int(total_outstanding),
            "client_count": len(clients)
        },
        "data": results
    })

@app.route('/api/generate-reminder', methods=['POST'])
def generate_reminder():
    req_data = request.json
    client_name = req_data.get('client')
    
    if not client_name:
        return jsonify({"error": "Missing client parameter"}), 400

    msg = core.generate_ai_reminder(client_name)
    return jsonify({"message": msg})

if __name__ == '__main__':
    app.run(port=5500, debug=True)