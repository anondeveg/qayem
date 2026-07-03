import os
import sys
import json
import shutil
import logging
import subprocess
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
import fitz  # PyMuPDF
from PIL import Image

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

_EASYOCR_READER = None

class TokenBucketRateLimiter:
    def __init__(self, limit_per_minute: int = 500):
        import time
        import threading
        self.limit = limit_per_minute
        self.history = []  # list of (timestamp, page_count)
        self.lock = threading.Lock()

    def acquire(self, page_count: int):
        import time
        if page_count > self.limit:
            page_count = self.limit

        while True:
            with self.lock:
                now = time.time()
                # Clear entries older than 60 seconds
                self.history = [entry for entry in self.history if now - entry[0] < 60]
                
                current_pages = sum(entry[1] for entry in self.history)
                if current_pages + page_count <= self.limit:
                    self.history.append((now, page_count))
                    return
                
                # Find how long we need to wait
                target_remaining = self.limit - page_count
                temp_sum = 0
                wait_until = now
                for ts, count in reversed(self.history):
                    if temp_sum + count > target_remaining:
                        wait_until = ts + 60
                        break
                    temp_sum += count
                
                sleep_time = max(0.1, wait_until - now)
                
            logger.info(f"Mistral OCR Rate limit reached. Sleeping for {sleep_time:.2f} seconds before processing {page_count} pages.")
            time.sleep(sleep_time)

mistral_rate_limiter = TokenBucketRateLimiter(500)

def get_easyocr_reader():
    global _EASYOCR_READER
    if _EASYOCR_READER is None:
        import easyocr
        logger.info("Initializing EasyOCR reader for ['ar', 'en']...")
        _EASYOCR_READER = easyocr.Reader(['ar', 'en'])
    return _EASYOCR_READER

def has_arabic(text: str) -> bool:
    """Check if the text contains any Arabic characters."""
    if not text:
        return False
    return any(
        '\u0600' <= char <= '\u06FF' or 
        '\u0750' <= char <= '\u077F' or 
        '\u08A0' <= char <= '\u08FF' or 
        '\uFB50' <= char <= '\uFDFF' or 
        '\uFE70' <= char <= '\uFEFF'
        for char in text
    )

def get_easyocr_blocks(results: list) -> list:
    """Extract standard block format from EasyOCR results list."""
    if not results:
        return []

    processed = []
    for bbox, text, conf in results:
        xs = [pt[0] for pt in bbox]
        ys = [pt[1] for pt in bbox]
        x_min, x_max = min(xs), max(xs)
        y_min, y_max = min(ys), max(ys)
        h = y_max - y_min
        y_center = y_min + h / 2.0
        processed.append({
            "text": text,
            "x_min": x_min,
            "x_max": x_max,
            "y_min": y_min,
            "y_max": y_max,
            "y_center": y_center,
            "h": h
        })
    return processed

def sort_and_format_ocr_blocks(blocks: list) -> str:
    if not blocks:
        return ""

    sorted_blocks = sorted(blocks, key=lambda item: item["y_center"])

    lines = []
    current_line = [sorted_blocks[0]]
    for item in sorted_blocks[1:]:
        prev = current_line[-1]
        avg_h = (prev["h"] + item["h"]) / 2.0
        if abs(item["y_center"] - prev["y_center"]) < avg_h * 0.6:
            current_line.append(item)
        else:
            lines.append(current_line)
            current_line = [item]
    lines.append(current_line)

    sorted_text_lines = []
    for line in lines:
        line_has_arabic = any(has_arabic(item["text"]) for item in line)
        if line_has_arabic:
            sorted_line = sorted(line, key=lambda item: item["x_min"], reverse=True)
        else:
            sorted_line = sorted(line, key=lambda item: item["x_min"], reverse=False)
        line_text = " ".join([item["text"] for item in sorted_line])
        sorted_text_lines.append(line_text)

    return "\n".join(sorted_text_lines)

def extract_text_via_easyocr(img_annots, page_rect, rect, context_margin=None) -> str:
    """Crops the rendered page image (at zoom 4.0), runs EasyOCR on it, and returns sorted text."""
    try:
        import numpy as np
        reader = get_easyocr_reader()
        
        if context_margin is not None:
            crop_rect = fitz.Rect(
                page_rect.x0,
                max(page_rect.y0, rect.y0 - context_margin),
                page_rect.x1,
                min(page_rect.y1, rect.y1 + context_margin)
            )
        else:
            crop_rect = rect

        crop_box = (
            (crop_rect.x0 - page_rect.x0) * 4.0,
            (crop_rect.y0 - page_rect.y0) * 4.0,
            (crop_rect.x1 - page_rect.x0) * 4.0,
            (crop_rect.y1 - page_rect.y0) * 4.0
        )
        cropped_img = img_annots.crop(crop_box)
        
        target_size = (int(cropped_img.width * 0.5), int(cropped_img.height * 0.5))
        resized_img = cropped_img.resize(target_size, Image.Resampling.BILINEAR)

        img_np = np.array(resized_img)
        ocr_res = reader.readtext(img_np)
        blocks = get_easyocr_blocks(ocr_res)
        text = sort_and_format_ocr_blocks(blocks)
        return text.strip()
    except Exception as e:
        logger.error(f"EasyOCR extraction failed: {e}")
        return "[EasyOCR Extraction Failed]"

def run_olmocr_ocr(compiled_pdf_path: str, task_id: str, server: str = "http://localhost:11434/v1", api_key: str = None, model: str = "richardyoung/olmocr2:7b-q8", progress_callback = None) -> dict:
    """
    Runs the olmocr pipeline on the compiled highlights PDF.
    Returns a dictionary mapping page numbers (1-indexed) to their parsed OCR text.
    """
    compiled_pdf = Path(compiled_pdf_path)
    if not compiled_pdf.exists():
        logger.error(f"Compiled highlights PDF not found for OCR: {compiled_pdf}")
        return {}

    # Read pages count to know total items
    try:
        doc = fitz.open(compiled_pdf)
        num_pages = len(doc)
        doc.close()
    except Exception as e:
        logger.error(f"Failed to read compiled highlights PDF page count: {e}")
        num_pages = 0

    if num_pages == 0:
        return {}

    # Define unique temporary workspace folder to prevent conflicts
    workspace_dir = Path("uploads") / f"olmocr_workspace_{task_id}"
    if workspace_dir.exists():
        try:
            shutil.rmtree(workspace_dir)
        except Exception as e:
            logger.warning(f"Could not remove existing workspace dir {workspace_dir}: {e}")
    workspace_dir.mkdir(parents=True, exist_ok=True)

    # Build the olmocr.pipeline subprocess command
    cmd = [
        sys.executable,
        "-u",  # Unbuffered output to read progress in real time
        "-m", "olmocr.pipeline",
        str(workspace_dir),
        "--pdfs", str(compiled_pdf)
    ]
    if server:
        cmd.extend(["--server", server])
    if api_key:
        cmd.extend(["--api_key", api_key])
    if model:
        cmd.extend(["--model", model])

    # If running on a local server, limit concurrency to prevent system/Ollama overload
    is_local = False
    if server:
        is_local = "localhost" in server or "127.0.0.1" in server
    else:
        is_local = True

    if is_local:
        cmd.extend(["--workers", "1", "--max_concurrent_requests", "1", "--target_longest_image_dim", "600"])
        logger.info("Local server detected: Limiting olmocr to 1 worker, 1 concurrent request, and 600px image dimension.")

    logger.info(f"Running olmocr pipeline command: {' '.join(cmd)}")
    
    if progress_callback:
        progress_callback(0, num_pages, phase="ocr", percent=0)

    try:
        # Start the pipeline subprocess and capture output
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        # Read output in real-time and parse progress
        completed_pages = 0
        failed_pages = 0
        finished_col_idx = -1
        errored_col_idx = -1
        worker_finished = {}
        worker_errored = {}

        while True:
            line = proc.stdout.readline()
            if not line and proc.poll() is not None:
                break
            if line:
                # Log pipeline output locally so it is visible in the systemd journal
                logger.info(f"[olmocr pipeline] {line.strip()}")
                
                # Check for WorkerTracker table headers and data rows to parse progress live
                if "|" in line:
                    parts = [p.strip() for p in line.split("|")]
                    if parts and parts[0] == "Worker ID":
                        if "finished" in parts:
                            finished_col_idx = parts.index("finished")
                        if "errored" in parts:
                            errored_col_idx = parts.index("errored")
                        continue
                    
                    if finished_col_idx != -1 or errored_col_idx != -1:
                        if parts[0].isdigit():
                            try:
                                w_id = int(parts[0])
                                if finished_col_idx != -1 and len(parts) > finished_col_idx:
                                    worker_finished[w_id] = int(parts[finished_col_idx])
                                if errored_col_idx != -1 and len(parts) > errored_col_idx:
                                    worker_errored[w_id] = int(parts[errored_col_idx])
                                
                                completed_pages = sum(worker_finished.values())
                                failed_pages = sum(worker_errored.values())
                                
                                total_done = min(num_pages, completed_pages + failed_pages)
                                pct = int((total_done / num_pages) * 100) if num_pages > 0 else 0
                                if progress_callback:
                                    progress_callback(total_done, num_pages, phase="ocr", percent=pct)
                            except Exception as e:
                                logger.warning(f"Error parsing WorkerTracker row: {e}")

                # Fallback to end-of-run summary metrics if printed
                if "Completed pages:" in line:
                    try:
                        parts = line.split("Completed pages:")
                        count = int(parts[1].strip().split()[0].replace(",", ""))
                        completed_pages = max(completed_pages, count)
                        total_done = min(num_pages, completed_pages + failed_pages)
                        pct = int((total_done / num_pages) * 100) if num_pages > 0 else 0
                        if progress_callback:
                            progress_callback(total_done, num_pages, phase="ocr", percent=pct)
                    except Exception as e:
                        logger.warning(f"Error parsing Completed pages count: {e}")
                elif "Failed pages:" in line:
                    try:
                        parts = line.split("Failed pages:")
                        count = int(parts[1].strip().split()[0].replace(",", ""))
                        failed_pages = max(failed_pages, count)
                        total_done = min(num_pages, completed_pages + failed_pages)
                        pct = int((total_done / num_pages) * 100) if num_pages > 0 else 0
                        if progress_callback:
                            progress_callback(total_done, num_pages, phase="ocr", percent=pct)
                    except Exception as e:
                        logger.warning(f"Error parsing Failed pages count: {e}")
                
        proc.wait()
        logger.info(f"olmocr pipeline finished with exit code {proc.returncode}")
        
    except Exception as e:
        logger.error(f"Error running olmocr subprocess: {e}")
        if workspace_dir.exists():
            shutil.rmtree(workspace_dir)
        return {}

    # Find the resulting JSONL file
    results_dir = workspace_dir / "results"
    jsonl_files = list(results_dir.glob("*.jsonl"))
    
    ocr_results = {}
    if jsonl_files:
        jsonl_file = jsonl_files[0]
        try:
            with open(jsonl_file, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    doc_data = json.loads(line)
                    text = doc_data.get("text", "")
                    spans = doc_data.get("attributes", {}).get("pdf_page_numbers", [])
                    
                    for start, end, page_num in spans:
                        page_text = text[start:end].strip()
                        ocr_results[page_num] = page_text
        except Exception as e:
            logger.error(f"Failed to read/parse olmocr JSONL result file {jsonl_file}: {e}")
    else:
        logger.error(f"No JSONL output file found in workspace: {results_dir}")

    # Clean up workspace directory
    try:
        if workspace_dir.exists():
            shutil.rmtree(workspace_dir)
    except Exception as e:
        logger.warning(f"Failed to clean up olmocr workspace {workspace_dir}: {e}")

    if progress_callback:
        progress_callback(num_pages, num_pages, phase="ocr", percent=100)

    return ocr_results

def run_mistral_ocr(compiled_pdf_path: str, api_key: str, progress_callback = None) -> dict:
    """
    Runs Mistral OCR on the compiled highlights PDF, splitting into chunks if necessary
    to handle files gracefully and obey the 500 pages per minute rate limit.
    """
    import fitz
    import requests
    import time
    import traceback
    
    if not api_key:
        logger.error("Mistral OCR called without an API key!")
        return {}
    
    compiled_pdf = Path(compiled_pdf_path)
    if not compiled_pdf.exists():
        logger.error(f"Compiled highlights PDF not found for Mistral OCR: {compiled_pdf}")
        return {}

    # Open PDF to get page count
    try:
        doc = fitz.open(compiled_pdf)
        num_pages = len(doc)
    except Exception as e:
        logger.error(f"Failed to read compiled highlights PDF page count: {e}")
        return {}

    if num_pages == 0:
        logger.warning("Mistral OCR: PDF has 0 pages, nothing to process.")
        doc.close()
        return {}

    logger.info(f"Mistral OCR: Processing {num_pages} pages from {compiled_pdf}")

    # Mistral rate limit of 500 pages per minute.
    # We will chunk the document into pieces of at most 100 pages each,
    # and acquire rate limit tickets before sending each chunk.
    chunk_size = 100
    ocr_results = {}
    
    if progress_callback:
        progress_callback(0, num_pages, phase="ocr", percent=0)

    # Temporary directory for chunk PDFs
    temp_dir = compiled_pdf.parent / "mistral_chunks"
    temp_dir.mkdir(exist_ok=True)
    
    try:
        for chunk_start in range(0, num_pages, chunk_size):
            chunk_end = min(chunk_start + chunk_size, num_pages)
            chunk_len = chunk_end - chunk_start
            
            # Acquire rate limit for chunk_len pages
            mistral_rate_limiter.acquire(chunk_len)
            
            # Save the chunk to a separate PDF
            chunk_doc = fitz.open()
            chunk_doc.insert_pdf(doc, from_page=chunk_start, to_page=chunk_end - 1)
            chunk_pdf_path = temp_dir / f"chunk_{chunk_start}_{chunk_end}.pdf"
            chunk_doc.save(str(chunk_pdf_path))
            chunk_doc.close()
            
            file_id = None
            try:
                # 1. Upload chunk file to Mistral
                upload_url = "https://api.mistral.ai/v1/files"
                headers = {"Authorization": f"Bearer {api_key}"}
                
                with open(chunk_pdf_path, "rb") as f:
                    files = {
                        "file": (chunk_pdf_path.name, f, "application/pdf")
                    }
                    logger.info(f"Uploading PDF chunk ({chunk_len} pages, {chunk_start} to {chunk_end}) to Mistral Files API...")
                    resp = requests.post(upload_url, headers=headers, files=files, data={"purpose": "ocr"})
                
                logger.info(f"Upload response status: {resp.status_code}")
                if resp.status_code != 200:
                    logger.error(f"Mistral file upload failed! Status: {resp.status_code}, Body: {resp.text[:1000]}")
                    continue
                
                upload_json = resp.json()
                logger.info(f"Upload response keys: {list(upload_json.keys())}")
                file_id = upload_json.get("id")
                if not file_id:
                    logger.error(f"No 'id' in Mistral upload response! Full response: {upload_json}")
                    continue
                
                logger.info(f"Uploaded successfully, file_id: {file_id}")
                
                # 1.5 Get Signed URL for the file
                signed_url_req = f"https://api.mistral.ai/v1/files/{file_id}/url"
                logger.info(f"Fetching signed URL for file_id {file_id}...")
                url_resp = requests.get(signed_url_req, headers={"Authorization": f"Bearer {api_key}"})
                
                if url_resp.status_code != 200:
                    logger.error(f"Failed to get signed URL! Status: {url_resp.status_code}, Body: {url_resp.text[:1000]}")
                    continue
                    
                signed_url = url_resp.json().get("url")
                if not signed_url:
                    logger.error(f"No 'url' in signed URL response! Full response: {url_resp.json()}")
                    continue
                
                # 2. Run OCR on uploaded file using the signed URL
                ocr_url = "https://api.mistral.ai/v1/ocr"
                ocr_headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                ocr_payload = {
                    "model": "mistral-ocr-latest",
                    "document": {
                        "type": "document_url",
                        "document_url": signed_url
                    }
                }
                logger.info(f"Triggering Mistral OCR for file_id {file_id} with signed URL...")
                resp = requests.post(ocr_url, headers=ocr_headers, json=ocr_payload)
                
                logger.info(f"OCR response status: {resp.status_code}")
                if resp.status_code != 200:
                    logger.error(f"Mistral OCR request failed! Status: {resp.status_code}, Body: {resp.text[:2000]}")
                    continue
                
                ocr_json = resp.json()
                ocr_pages = ocr_json.get("pages", [])
                logger.info(f"OCR returned {len(ocr_pages)} pages. Response top-level keys: {list(ocr_json.keys())}")
                
                if not ocr_pages:
                    # Log the full response for debugging
                    logger.warning(f"Mistral OCR returned 0 pages! Full response (truncated): {str(ocr_json)[:2000]}")
                
                # Parse pages
                for page in ocr_pages:
                    page_index_in_chunk = page.get("index", 0)
                    markdown_text = page.get("markdown", "")
                    absolute_page = chunk_start + page_index_in_chunk + 1
                    ocr_results[absolute_page] = markdown_text
                    if markdown_text:
                        logger.debug(f"Page {absolute_page}: got {len(markdown_text)} chars of text")
                    else:
                        logger.warning(f"Page {absolute_page}: OCR returned empty markdown")
                    
                # Clean up chunk file locally
                if chunk_pdf_path.exists():
                    chunk_pdf_path.unlink()
                    
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error processing Mistral OCR chunk {chunk_start}-{chunk_end}: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"Response body: {e.response.text[:2000]}")
            except Exception as e:
                logger.error(f"Error processing Mistral OCR chunk {chunk_start}-{chunk_end}: {e}")
                logger.error(traceback.format_exc())
            finally:
                # 3. Always attempt to delete the file from Mistral Files API
                if file_id:
                    try:
                        delete_url = f"https://api.mistral.ai/v1/files/{file_id}"
                        logger.info(f"Deleting remote file {file_id} from Mistral Files API...")
                        del_resp = requests.delete(delete_url, headers={"Authorization": f"Bearer {api_key}"})
                        logger.info(f"Delete response status: {del_resp.status_code}")
                    except Exception as ex:
                        logger.warning(f"Failed to delete remote file {file_id}: {ex}")
            
            # Progress callback update
            if progress_callback:
                completed = min(chunk_end, num_pages)
                pct = int((completed / num_pages) * 100)
                progress_callback(completed, num_pages, phase="ocr", percent=pct)
                
    finally:
        doc.close()
        # Clean up temp_dir if empty or exists
        if temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                logger.warning(f"Could not remove temp chunks dir {temp_dir}: {e}")
    
    logger.info(f"Mistral OCR completed. Got results for {len(ocr_results)} pages out of {num_pages}.")
    return ocr_results

# Supported annotation types
# 2: FreeText, 4: Square, 5: Circle, 8: Highlight, 9: Underline, 10: StrikeOut, 11: Squiggly, 15: Ink
SUPPORTED_ANNOT_TYPES = {2, 4, 5, 8, 9, 10, 11, 15}

def get_annot_type_id(annot) -> int:
    """Helper to extract integer type ID of an annotation in a backward-compatible way."""
    annot_type = annot.type
    if isinstance(annot_type, tuple):
        return annot_type[0]
    return annot_type

def merge_rects(rects: list, threshold: float = 20.0) -> list:
    """
    Merge rectangles that are vertically close (within threshold) and on the same page.
    This groups multi-line highlights into single cohesive blocks.
    """
    if not rects:
        return []
    
    # Sort rects primarily by top coordinate (y0)
    sorted_rects = sorted(rects, key=lambda r: (r.y0, r.x0))
    merged = []
    current = sorted_rects[0]
    
    for next_rect in sorted_rects[1:]:
        # Calculate vertical gap
        vertical_gap = next_rect.y0 - current.y1
        # Calculate vertical overlap
        vertical_overlap = max(0.0, min(current.y1, next_rect.y1) - max(current.y0, next_rect.y0))
        
        # Merge if they are close vertically or overlap
        if vertical_gap <= threshold or vertical_overlap > 0.0:
            current = fitz.Rect(
                min(current.x0, next_rect.x0),
                min(current.y0, next_rect.y0),
                max(current.x1, next_rect.x1),
                max(current.y1, next_rect.y1)
            )
        else:
            merged.append(current)
            current = next_rect
            
    merged.append(current)
    return merged

def compile_highlights_pdf(extracted_data, save_images, pdf_save_dir):
    """Compile all highlight styled crop images into a single PDF document."""
    if save_images and pdf_save_dir:
        img_paths = []
        for item in extracted_data:
            if item.get("image_path"):
                p = Path(item["image_path"])
                if p.exists():
                    img_paths.append(p)
                    
        if img_paths:
            try:
                images = []
                for p in img_paths:
                    try:
                        im = Image.open(p).convert("RGB")
                        images.append(im)
                    except Exception as e:
                        logger.error(f"Failed to open image for PDF compilation: {p}, error: {e}")
                        
                if images:
                    pdf_output_path = pdf_save_dir / "compiled_highlights.pdf"
                    images[0].save(pdf_output_path, save_all=True, append_images=images[1:])
                    for im in images:
                        im.close()
                    logger.info(f"Compiled all {len(images)} images into a single PDF: {pdf_output_path}")
            except Exception as e:
                logger.error(f"Failed to compile highlight images to PDF: {e}")

def extract_highlights(
    pdf_path: str,
    merge_threshold: float = 20.0,
    save_images: bool = True,
    save_dir: str = "highlights",
    context: bool = False,
    context_margin: float = 80.0,
    progress_callback = None,
    output_json_path: str = None,
    olmocr: bool = None,
    olmocr_server: str = "http://localhost:11434/v1",
    olmocr_api_key: str = None,
    olmocr_model: str = "richardyoung/olmocr2:7b-q8",
    ocr_engine: str = "auto",
    mistral_api_key: str = None
) -> list:
    """
    Core function to process the PDF and extract highlights with auto-detection.
    """
    # For backward compatibility, handle `olmocr` parameter
    if olmocr is True:
        ocr_engine = "olmocr"
    elif olmocr is False and ocr_engine == "olmocr":
        ocr_engine = "auto"

    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        logger.error(f"Failed to open PDF file {pdf_path}: {e}")
        raise
        
    # Prepare folder to save cropped highlight images if required
    pdf_save_dir = None
    if save_images:
        pdf_save_dir = Path(save_dir) / pdf_path.stem
        
    import json
    extracted_data = []
    total_pages = len(doc)
    
    for page_num in range(total_pages):
        if progress_callback:
            try:
                pct = int((page_num + 1) / total_pages * 100)
                progress_callback(page_num + 1, total_pages, phase="parsing", percent=pct)
            except Exception as e:
                logger.warning(f"Progress callback failed: {e}")
                
        page = doc.load_page(page_num)
        
        # Get all annotation rects that are of supported types
        rects = []
        for annot in page.annots():
            type_id = get_annot_type_id(annot)
            if type_id in SUPPORTED_ANNOT_TYPES:
                rect = annot.rect
                if rect.width > 0 and rect.height > 0:
                    rects.append(rect)
                    
        if not rects:
            continue
            
        # Merge close annotation rects into single blocks
        merged_rects = merge_rects(rects, threshold=merge_threshold)
        logger.info(f"Page {page_num + 1}: Found {len(rects)} highlights, merged into {len(merged_rects)} blocks")
        
        if save_images and pdf_save_dir:
            pdf_save_dir.mkdir(parents=True, exist_ok=True)
            
        page_rect = page.rect
        
        # Render page at zoom 4.0 if save_images is True OR we might need OCR.
        need_rendering = save_images or ocr_engine in ("auto", "easyocr", "olmocr", "mistralocr")
        img_annots = None
        if need_rendering:
            try:
                pix_annots = page.get_pixmap(matrix=fitz.Matrix(4.0, 4.0), annots=True)
                img_annots = Image.frombytes("RGB", [pix_annots.width, pix_annots.height], pix_annots.samples)
            except Exception as e:
                logger.error(f"Failed to render page {page_num + 1}: {e}")
            
        for idx, rect in enumerate(merged_rects):
            highlight_id = idx + 1
            image_path = None
            
            # Save visual image (with highlights) if save_images is enabled
            if save_images and img_annots:
                try:
                    vertical_margin = 15.0  # PDF points of extra padding top/bottom
                    crop_y0 = max(page_rect.y0, rect.y0 - vertical_margin)
                    crop_y1 = min(page_rect.y1, rect.y1 + vertical_margin)
                    
                    crop_box = (
                        0.0,  # Full page width starting from left
                        (crop_y0 - page_rect.y0) * 4.0,
                        img_annots.width,  # Full page width ending at right
                        (crop_y1 - page_rect.y0) * 4.0
                    )
                    styled_img = img_annots.crop(crop_box)
                    image_filename = f"page_{page_num + 1}_highlight_{highlight_id}.png"
                    img_path = pdf_save_dir / image_filename
                    styled_img.save(img_path, format="PNG")
                    image_path = str(img_path)
                except Exception as e:
                    logger.error(f"Error saving image for page {page_num + 1}, highlight {highlight_id}: {e}")
            
            # Extract native text
            native_text = ""
            try:
                native_text = page.get_text("text", clip=rect).strip()
            except Exception as e:
                logger.error(f"Native text extraction failed for page {page_num + 1}: {e}")
            
            native_context = ""
            if context:
                try:
                    c_rect = fitz.Rect(
                        page_rect.x0,
                        max(page_rect.y0, rect.y0 - context_margin),
                        page_rect.x1,
                        min(page_rect.y1, rect.y1 + context_margin)
                    )
                    native_context = page.get_text("text", clip=c_rect).strip()
                except Exception as e:
                    logger.error(f"Native context failed for page {page_num + 1}: {e}")

            # Determine text content using fallback logic
            extracted_text = native_text or "[No selectable text found]"
            context_text = native_context or "[No selectable context found]" if context else None
            current_engine = "native"

            run_easyocr_for_quote = False
            run_easyocr_for_context = False

            if ocr_engine == "easyocr":
                run_easyocr_for_quote = True
                if context:
                    run_easyocr_for_context = True
            elif ocr_engine == "auto":
                if not native_text or native_text == "[No selectable text found]" or has_arabic(native_text):
                    run_easyocr_for_quote = True
                if context and (not native_context or native_context == "[No selectable context found]" or has_arabic(native_context)):
                    run_easyocr_for_context = True
            elif ocr_engine == "olmocr":
                if context and (not native_context or native_context == "[No selectable context found]" or has_arabic(native_context)):
                    run_easyocr_for_context = True

            if run_easyocr_for_quote and img_annots:
                extracted_text = extract_text_via_easyocr(img_annots, page_rect, rect)
                current_engine = "easyocr"
                
            if run_easyocr_for_context and img_annots:
                context_text = extract_text_via_easyocr(img_annots, page_rect, rect, context_margin=context_margin)

            result_item = {
                "page": page_num + 1,
                "rect": [rect.x0, rect.y0, rect.x1, rect.y1],
                "image_path": image_path,
                "text": extracted_text,
                "ocr_engine": current_engine
            }
            if context:
                result_item["context"] = context_text
            extracted_data.append(result_item)
            
            # Save repeatedly to disk after each processed highlight block
            if output_json_path:
                try:
                    with open(output_json_path, "w", encoding="utf-8") as f:
                        json.dump(extracted_data, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    logger.error(f"Failed to save repeated JSON update: {e}")
                        
    doc.close()
    
    # Compile images to a single PDF if save_images is enabled
    compile_highlights_pdf(extracted_data, save_images, pdf_save_dir)
    
    if ocr_engine == "olmocr" and save_images and pdf_save_dir:
        compiled_pdf_path = pdf_save_dir / "compiled_highlights.pdf"
        if compiled_pdf_path.exists():
            import uuid
            task_id = f"{pdf_path.stem}_{uuid.uuid4().hex[:8]}"
            
            # Run olmocr OCR
            ocr_texts = run_olmocr_ocr(
                compiled_pdf_path=str(compiled_pdf_path),
                task_id=task_id,
                server=olmocr_server,
                api_key=olmocr_api_key,
                model=olmocr_model,
                progress_callback=progress_callback
            )
            
            # Update the text properties of highlights with OCR results
            for idx, item in enumerate(extracted_data):
                page_num_in_compiled = idx + 1
                if page_num_in_compiled in ocr_texts:
                    ocr_text = ocr_texts[page_num_in_compiled]
                    if ocr_text:
                        item["text"] = ocr_text
                        item["ocr_engine"] = "olmocr"

            # Re-save the final JSON results with updated OCR texts
            if output_json_path:
                try:
                    with open(output_json_path, "w", encoding="utf-8") as f:
                        json.dump(extracted_data, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    logger.error(f"Failed to save final JSON update after olmocr: {e}")
    elif ocr_engine == "mistralocr" and save_images and pdf_save_dir:
        compiled_pdf_path = pdf_save_dir / "compiled_highlights.pdf"
        if compiled_pdf_path.exists():
            # Run Mistral OCR
            ocr_texts = run_mistral_ocr(
                compiled_pdf_path=str(compiled_pdf_path),
                api_key=mistral_api_key,
                progress_callback=progress_callback
            )
            
            # Update the text properties of highlights with OCR results
            for idx, item in enumerate(extracted_data):
                page_num_in_compiled = idx + 1
                if page_num_in_compiled in ocr_texts:
                    ocr_text = ocr_texts[page_num_in_compiled]
                    if ocr_text:
                        item["text"] = ocr_text
                        item["ocr_engine"] = "mistralocr"

            # Re-save the final JSON results with updated OCR texts
            if output_json_path:
                try:
                    with open(output_json_path, "w", encoding="utf-8") as f:
                        json.dump(extracted_data, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    logger.error(f"Failed to save final JSON update after mistralocr: {e}")
    
    return extracted_data

def run_easyocr_on_full_page(page, page_num: int, total_pages: int, progress_callback=None) -> str:
    """Renders the full page image at zoom 4.0, runs EasyOCR on it, and returns sorted text."""
    try:
        import numpy as np
        pix = page.get_pixmap(matrix=fitz.Matrix(4.0, 4.0))
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        
        target_size = (int(img.width * 0.5), int(img.height * 0.5))
        resized_img = img.resize(target_size, Image.Resampling.BILINEAR)

        img_np = np.array(resized_img)
        reader = get_easyocr_reader()
        ocr_res = reader.readtext(img_np)
        blocks = get_easyocr_blocks(ocr_res)
        text = sort_and_format_ocr_blocks(blocks)
        return text.strip()
    except Exception as e:
        logger.error(f"EasyOCR full page extraction failed for page {page_num}: {e}")
        return f"[EasyOCR Extraction Failed on Page {page_num}]"

def ocr_full_pdf(
    pdf_path: str,
    ocr_engine: str = "auto",
    olmocr_server: str = "http://localhost:11434/v1",
    olmocr_api_key: str = None,
    olmocr_model: str = "richardyoung/olmocr2:7b-q8",
    mistral_api_key: str = None,
    progress_callback = None
) -> str:
    """
    Runs OCR on all pages of the PDF, returning the full collated text.
    """
    pdf_path = Path(pdf_path)
    try:
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
    except Exception as e:
        logger.error(f"Failed to open PDF for full OCR: {e}")
        return ""

    if total_pages == 0:
        return ""

    # If it is olmocr, run the pipeline on the original PDF path
    if ocr_engine == "olmocr":
        import uuid
        task_id = f"{pdf_path.stem}_full_{uuid.uuid4().hex[:8]}"
        ocr_texts = run_olmocr_ocr(
            compiled_pdf_path=str(pdf_path),
            task_id=task_id,
            server=olmocr_server,
            api_key=olmocr_api_key,
            model=olmocr_model,
            progress_callback=progress_callback
        )
        full_text_list = []
        for p in range(1, total_pages + 1):
            full_text_list.append(f"--- Page {p} ---\n" + ocr_texts.get(p, "[No OCR text found for this page]"))
        doc.close()
        return "\n\n".join(full_text_list)

    # If it is mistralocr, run mistral OCR on the original PDF path
    if ocr_engine == "mistralocr":
        ocr_texts = run_mistral_ocr(
            compiled_pdf_path=str(pdf_path),
            api_key=mistral_api_key,
            progress_callback=progress_callback
        )
        full_text_list = []
        for p in range(1, total_pages + 1):
            full_text_list.append(f"--- Page {p} ---\n" + ocr_texts.get(p, "[No OCR text found for this page]"))
        doc.close()
        return "\n\n".join(full_text_list)

    # Otherwise (native, easyocr, auto), process page-by-page
    full_text_list = []
    for page_num in range(total_pages):
        page = doc.load_page(page_num)
        
        # Report progress
        if progress_callback:
            pct = int((page_num + 1) / total_pages * 100)
            progress_callback(page_num + 1, total_pages, phase="full_ocr", percent=pct)

        native_text = ""
        try:
            native_text = page.get_text("text").strip()
        except Exception as e:
            logger.error(f"Native text extraction failed for page {page_num + 1}: {e}")
            
        use_ocr = False
        if ocr_engine == "easyocr":
            use_ocr = True
        elif ocr_engine == "auto":
            if not native_text or has_arabic(native_text):
                use_ocr = True
        
        if use_ocr:
            page_text = run_easyocr_on_full_page(page, page_num + 1, total_pages, progress_callback)
        else:
            page_text = native_text or "[No text found on this page]"

        full_text_list.append(f"--- Page {page_num + 1} ---\n" + page_text)

    doc.close()
    return "\n\n".join(full_text_list)

