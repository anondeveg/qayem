import os
import shutil
import threading
from pathlib import Path
from flask import Flask, request, jsonify, render_template, send_from_directory
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

# Ensure dotenv is loaded
load_dotenv()

from extractor import extract_highlights

# Global thread-safe progress store for active tasks
PROGRESS_STORE = {}
PROGRESS_LOCK = threading.Lock()

app = Flask(__name__, static_folder="static", template_folder="templates")

# Configure upload folder
UPLOAD_FOLDER = Path("./uploads")
UPLOAD_FOLDER.mkdir(exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# Configure highlights output folder
HIGHLIGHTS_FOLDER = Path("./highlights")
HIGHLIGHTS_FOLDER.mkdir(exist_ok=True)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/progress", methods=["GET"])
def get_progress():
    task_id = request.args.get("task_id")
    if not task_id:
        return jsonify({"error": "Missing task_id"}), 400
    with PROGRESS_LOCK:
        progress = PROGRESS_STORE.get(task_id, {"current": 0, "total": 0})
    return jsonify(progress)

@app.route("/api/extract", methods=["POST"])
def extract():
    if "pdf" not in request.files:
        return jsonify({"error": "No PDF file uploaded"}), 400
        
    pdf_file = request.files["pdf"]
    if pdf_file.filename == "":
        return jsonify({"error": "Empty filename"}), 400
        
    # Get parameters
    task_id = request.form.get("task_id")
    context = request.form.get("context", "false").lower() == "true"
    olmocr = request.form.get("olmocr", "false").lower() == "true"
    olmocr_server = request.form.get("olmocr_server", "").strip() or None
    olmocr_api_key = request.form.get("olmocr_api_key", "").strip() or None
    olmocr_model = request.form.get("olmocr_model", "").strip() or None
    
    try:
        context_margin = float(request.form.get("context_margin", "80.0"))
        merge_threshold = float(request.form.get("merge_threshold", "20.0"))
    except ValueError:
        return jsonify({"error": "Invalid numeric arguments"}), 400
        
    # Save the file securely
    filename = secure_filename(pdf_file.filename)
    if not filename or filename.endswith(".pdf") is False:
        filename = "temp_uploaded_file.pdf"
        
    pdf_path = UPLOAD_FOLDER / filename
    pdf_file.save(pdf_path)
    
    # Initialize progress store entry
    if task_id:
        with PROGRESS_LOCK:
            if len(PROGRESS_STORE) > 100:
                oldest_keys = list(PROGRESS_STORE.keys())[:50]
                for k in oldest_keys:
                    PROGRESS_STORE.pop(k, None)
            PROGRESS_STORE[task_id] = {"current": 0, "total": 0}
 
    def progress_cb(current, total, phase="parsing", percent=None):
        if task_id:
            with PROGRESS_LOCK:
                PROGRESS_STORE[task_id] = {
                    "current": current,
                    "total": total,
                    "phase": phase,
                    "percent": percent
                }
 
    output_json_path = HIGHLIGHTS_FOLDER / f"{pdf_path.stem}_highlights.json"
    
    try:
        # Perform extraction
        highlights = extract_highlights(
            pdf_path=str(pdf_path),
            merge_threshold=merge_threshold,
            save_images=True,
            save_dir=str(HIGHLIGHTS_FOLDER),
            context=context,
            context_margin=context_margin,
            progress_callback=progress_cb,
            output_json_path=str(output_json_path),
            olmocr=olmocr,
            olmocr_server=olmocr_server,
            olmocr_api_key=olmocr_api_key,
            olmocr_model=olmocr_model
        )
        
        # Clean up the uploaded PDF file to conserve space
        if pdf_path.exists():
            pdf_path.unlink()
            
        compiled_pdf_path = None
        compiled_pdf = HIGHLIGHTS_FOLDER / pdf_path.stem / "compiled_highlights.pdf"
        if compiled_pdf.exists():
            compiled_pdf_path = f"highlights/{pdf_path.stem}/compiled_highlights.pdf"
            
        return jsonify({
            "success": True, 
            "highlights": highlights,
            "compiled_pdf_path": compiled_pdf_path
        })
        
    except Exception as e:
        if pdf_path.exists():
            pdf_path.unlink()
        return jsonify({"error": f"Extraction failed: {str(e)}"}), 500

@app.route("/highlights/<path:filename>")
def serve_highlight_image(filename):
    return send_from_directory(HIGHLIGHTS_FOLDER, filename)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
