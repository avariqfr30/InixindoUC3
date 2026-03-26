import argparse
import io
import logging
import os

from flask import Flask, current_app, jsonify, render_template, request, send_file
from flask_cors import CORS

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_app():
    from config import DB_URI, SMART_SUGGESTIONS
    from core import KnowledgeBase, ReportGenerator

    app = Flask(__name__)
    CORS(app)

    knowledge_base = KnowledgeBase(DB_URI)
    report_generator = ReportGenerator(knowledge_base)
    app.config["knowledge_base"] = knowledge_base
    app.config["report_generator"] = report_generator

    @app.route("/")
    def home():
        return render_template("index.html")

    @app.route("/get-config")
    def get_config():
        active_knowledge_base = current_app.config["knowledge_base"]
        if active_knowledge_base.df is None or active_knowledge_base.df.empty:
            return jsonify({"error": "Financial data is currently unavailable."})

        return jsonify({"suggestions": SMART_SUGGESTIONS})

    @app.route("/generate", methods=["POST"])
    def generate_doc():
        payload = request.get_json(silent=True) or {}
        notes = payload.get("notes", "")
        active_report_generator = current_app.config["report_generator"]

        try:
            document, file_name = active_report_generator.run(notes)
        except Exception as exc:
            logger.exception("Document generation failed: %s", exc)
            return jsonify({"error": "Document generation failed. Please verify the service configuration."}), 500

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
        active_knowledge_base = current_app.config["knowledge_base"]
        success = active_knowledge_base.refresh_data()
        return jsonify({"status": "success" if success else "error"})

    return app


def parse_args():
    parser = argparse.ArgumentParser(description="Run the financial reporting app.")
    parser.add_argument(
        "--data-mode",
        choices=("demo", "internal_api"),
        help="Select the internal data acquisition mode for this process.",
    )
    parser.add_argument(
        "--internal-api-base-url",
        help="Optional override for the internal API base URL.",
    )
    return parser.parse_args()


def apply_runtime_overrides(args):
    if args.data_mode:
        os.environ["DATA_ACQUISITION_MODE"] = args.data_mode
    if args.internal_api_base_url:
        os.environ["INTERNAL_API_BASE_URL"] = args.internal_api_base_url
        os.environ.setdefault("DATA_ACQUISITION_MODE", "internal_api")


app = create_app() if __name__ != "__main__" else None


if __name__ == "__main__":
    runtime_args = parse_args()
    apply_runtime_overrides(runtime_args)
    app = create_app()
    app.run(port=5000, debug=True, threaded=True)
