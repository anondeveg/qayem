import os
import shutil
import threading
from pathlib import Path
from flask import Flask, request, jsonify, render_template, send_from_directory
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

# Ensure dotenv is loaded
load_dotenv()

from extractor import extract_highlights, ocr_full_pdf

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
    full_ocr = request.form.get("full_ocr", "false").lower() == "true"
    
    # Parse OCR Engine
    ocr_engine = request.form.get("ocr_engine", "auto").lower().strip()
    if "ocr_engine" not in request.form and request.form.get("olmocr", "true").lower() == "false":
        ocr_engine = "native"

    olmocr_server = request.form.get("olmocr_server", "").strip() or "http://localhost:11434/v1"
    olmocr_api_key = request.form.get("olmocr_api_key", "").strip() or None
    olmocr_model = request.form.get("olmocr_model", "").strip() or "richardyoung/olmocr2:7b-q8"
    mistral_api_key = request.form.get("mistral_api_key", "").strip() or None
    
    # Validate Mistral API key if mistralocr engine is selected
    if ocr_engine == "mistralocr" and not mistral_api_key:
        return jsonify({"error": "Mistral AI OCR requires an API key. Please enter your Mistral API key."}), 400
    
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
        highlights = []
        if not full_ocr:
            highlights = extract_highlights(
                pdf_path=str(pdf_path),
                merge_threshold=merge_threshold,
                save_images=True,
                save_dir=str(HIGHLIGHTS_FOLDER),
                context=context,
                context_margin=context_margin,
                progress_callback=progress_cb,
                output_json_path=str(output_json_path),
                olmocr=None,
                olmocr_server=olmocr_server,
                olmocr_api_key=olmocr_api_key,
                olmocr_model=olmocr_model,
                ocr_engine=ocr_engine,
                mistral_api_key=mistral_api_key
            )
        
        # Run full OCR if requested, before deleting the uploaded PDF
        full_ocr_txt_path = None
        if full_ocr:
            full_ocr_text = ocr_full_pdf(
                pdf_path=str(pdf_path),
                ocr_engine=ocr_engine,
                olmocr_server=olmocr_server,
                olmocr_api_key=olmocr_api_key,
                olmocr_model=olmocr_model,
                mistral_api_key=mistral_api_key,
                progress_callback=progress_cb
            )
            # Save collated text file
            full_ocr_file = HIGHLIGHTS_FOLDER / f"{pdf_path.stem}_full_ocr.txt"
            with open(full_ocr_file, "w", encoding="utf-8") as f:
                f.write(full_ocr_text)
            full_ocr_txt_path = f"highlights/{pdf_path.stem}_full_ocr.txt"

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
            "compiled_pdf_path": compiled_pdf_path,
            "full_ocr_txt_path": full_ocr_txt_path
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
