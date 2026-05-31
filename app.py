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

def update_env_file(key: str, value: str):
    """Update or insert a key-value pair in the .env file and update the current environment."""
    env_path = Path(".env")
    lines = []
    key_found = False
    
    if env_path.exists():
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            
    new_lines = []
    for line in lines:
        if line.strip().startswith(f"{key}="):
            new_lines.append(f"{key}={value}\n")
            key_found = True
        else:
            new_lines.append(line)
            
    if not key_found:
        new_lines.append(f"{key}={value}\n")
        
    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)
        
    # Update current process environment
    os.environ[key] = value

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/settings", methods=["GET", "POST"])
def settings():
    if request.method == "POST":
        data = request.json or {}
        raw_key = data.get("ocr_space_key", "").strip()
        if not raw_key:
            return jsonify({"error": "API Key cannot be empty"}), 400
        try:
            import re
            # Split by commas or newlines/carriage returns
            keys = [k.strip() for k in re.split(r'[\n\r,]+', raw_key) if k.strip()]
            normalized_key = ",".join(keys)
            if not normalized_key:
                return jsonify({"error": "API Key cannot be empty"}), 400
            update_env_file("OCR_SPACE_KEY", normalized_key)
            return jsonify({"success": True, "ocr_space_key": normalized_key})
        except Exception as e:
            return jsonify({"error": f"Failed to save settings: {str(e)}"}), 500
    else:
        current_key = os.getenv("OCR_SPACE_KEY", "K89878519788957")
        return jsonify({"ocr_space_key": current_key})


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
    ocr = request.form.get("ocr", "false").lower() == "true"
    ocr_engine = request.form.get("ocr_engine", "ocr_space")
    ocr_space_engine = int(request.form.get("ocr_space_engine", "3"))
    lang = request.form.get("lang", "ara+eng")
    context = request.form.get("context", "false").lower() == "true"
    
    try:
        context_margin = float(request.form.get("context_margin", "80.0"))
        merge_threshold = float(request.form.get("merge_threshold", "20.0"))
    except ValueError:
        return jsonify({"error": "Invalid numeric arguments"}), 400
        
    # Save the file securely
    filename = secure_filename(pdf_file.filename)
    # Ensure filename doesn't contain spaces that secure_filename strips or modifies too heavily
    if not filename or filename.endswith(".pdf") is False:
        filename = "temp_uploaded_file.pdf"
        
    pdf_path = UPLOAD_FOLDER / filename
    pdf_file.save(pdf_path)
    
    # Initialize progress store entry
    if task_id:
        with PROGRESS_LOCK:
            # Clean up progress store if it gets too large
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

    warnings_list = []
    output_json_path = HIGHLIGHTS_FOLDER / f"{pdf_path.stem}_highlights.json"
    
    try:
        # Load the latest environment key just in case it was updated
        api_key = os.getenv("OCR_SPACE_KEY", "K89878519788957")
        
        # Tesseract setup requires system lang paths or setup checks
        tessdata_dir = None
        if ocr:
            # We import setup_ocr_languages from main
            from main import setup_ocr_languages
            try:
                tessdata_dir = setup_ocr_languages(lang)
            except Exception as e:
                # Fallback to system dir, but warn
                app.logger.warning(f"Tesseract language setup warning: {e}")
                
        # Perform extraction
        highlights = extract_highlights(
            pdf_path=str(pdf_path),
            ocr=ocr,
            ocr_engine=ocr_engine,
            ocr_space_key=api_key,
            ocr_space_engine=ocr_space_engine,
            lang=lang,
            tessdata_dir=tessdata_dir,
            merge_threshold=merge_threshold,
            save_images=True,
            save_dir=str(HIGHLIGHTS_FOLDER),
            context=context,
            context_margin=context_margin,
            warnings=warnings_list,
            progress_callback=progress_cb,
            output_json_path=str(output_json_path)
        )
        
        # We can clean up the uploaded PDF file to conserve space
        if pdf_path.exists():
            pdf_path.unlink()
            
        compiled_pdf_path = None
        compiled_pdf = HIGHLIGHTS_FOLDER / pdf_path.stem / "compiled_highlights.pdf"
        if compiled_pdf.exists():
            compiled_pdf_path = f"highlights/{pdf_path.stem}/compiled_highlights.pdf"
            
        return jsonify({
            "success": True, 
            "highlights": highlights,
            "warnings": warnings_list,
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
