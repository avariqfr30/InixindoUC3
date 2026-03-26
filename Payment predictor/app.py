import io
import logging

from flask import Flask, jsonify, render_template, request, send_file
from flask_cors import CORS

from config import DB_URI, SMART_SUGGESTIONS
from core import KnowledgeBase, ReportGenerator

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

knowledge_base = KnowledgeBase(DB_URI)
report_generator = ReportGenerator(knowledge_base)


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/get-config")
def get_config():
    if knowledge_base.df is None or knowledge_base.df.empty:
        return jsonify(
            {
                "error": "File db.csv tidak ditemukan di dalam folder 'data/db.csv'."
            }
        )

    return jsonify({"suggestions": SMART_SUGGESTIONS})


@app.route("/generate", methods=["POST"])
def generate_doc():
    payload = request.get_json(silent=True) or {}
    notes = payload.get("notes", "")

    try:
        document, file_name = report_generator.run(notes)
    except Exception as exc:
        logger.exception("Gagal generate dokumen: %s", exc)
        return jsonify({"error": "Gagal generate dokumen. Pastikan Ollama aktif."}), 500

    output_stream = io.BytesIO()
    document.save(output_stream)
    output_stream.seek(0)

    return send_file(
        output_stream,
        as_attachment=True,
        download_name=f"{file_name}.docx",
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


@app.route("/refresh-knowledge", methods=["POST"])
def refresh_knowledge():
    success = knowledge_base.refresh_data()
    return jsonify({"status": "success" if success else "error"})


if __name__ == "__main__":
    app.run(port=5000, debug=True, threaded=True)
