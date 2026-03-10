# app.py
import io
import logging
from flask import Flask, send_file, request, jsonify, render_template
from flask_cors import CORS

from core import ReportGenerator, KnowledgeBase
from config import DB_URI, SMART_SUGGESTIONS

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

kb = KnowledgeBase(DB_URI)
generator = ReportGenerator(kb)

@app.route('/')
def home(): 
    return render_template('index.html')

@app.route('/get-config')
def get_config():
    if kb.df is None or kb.df.empty: 
        return jsonify({"error": "File db.csv tidak ditemukan di dalam folder 'data/db.csv'."})
        
    try:
        # Mengambil Kuartal / Periode dari database finansial
        timeframes = kb.df['Periode Laporan'].dropna().unique().tolist()
    except KeyError:
        return jsonify({"error": "Struktur CSV salah. Pastikan terdapat kolom 'Periode Laporan'."})
            
    return jsonify({
        "timeframes": timeframes, 
        "suggestions": SMART_SUGGESTIONS
    })

@app.route('/generate', methods=['POST'])
def generate_doc():
    data = request.json
    
    timeframe = data.get('timeframe')
    notes = data.get('notes', '')
    
    doc, filename = generator.run(timeframe, notes)
    
    out = io.BytesIO()
    doc.save(out)
    out.seek(0)
    
    return send_file(
        out, 
        as_attachment=True, 
        download_name=f"{filename}.docx", 
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )

@app.route('/refresh-knowledge', methods=['POST'])
def refresh():
    success = kb.refresh_data()
    return jsonify({"status": "success" if success else "error"})

if __name__ == '__main__':
    app.run(port=5000, debug=True, threaded=True)