import os
os.environ['PADDLE_PDX_ENABLE_MKLDNN_BYDEFAULT'] = '0'
os.environ['OMP_NUM_THREADS'] = '2'
os.environ['MKL_NUM_THREADS'] = '2'
os.environ['CPU_NUM'] = '2'
import io
import logging
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()
import fitz  # PyMuPDF
from PIL import Image, ImageEnhance
import pytesseract
import numpy as np
import requests

def preprocess_image_for_ocr(img: Image.Image) -> Image.Image:
    """Apply grayscale, 2x upscaling, contrast enhancement, and binarization to optimize for OCR."""
    # Convert to grayscale
    gray = img.convert('L')
    # Resize 2x (upscaling) using Lanczos interpolation
    scaled = gray.resize((gray.width * 2, gray.height * 2), Image.Resampling.LANCZOS)
    # Enhance Contrast (factor of 2.0)
    enhancer = ImageEnhance.Contrast(scaled)
    contrast = enhancer.enhance(2.0)
    # Binarize with threshold of 180 (values > 180 become white, others black)
    binarized = contrast.point(lambda p: 255 if p > 180 else 0)
    return binarized

def sort_easyocr_results(results: list) -> str:
    """Sort EasyOCR results right-to-left within lines to preserve Arabic reading order."""
    if not results:
        return ""
        
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
        
    # Sort primarily by vertical position (y_center)
    processed = sorted(processed, key=lambda item: item["y_center"])
    
    # Group into lines
    lines = []
    if processed:
        current_line = [processed[0]]
        for item in processed[1:]:
            prev = current_line[-1]
            avg_h = (prev["h"] + item["h"]) / 2.0
            # If vertical distance between centers is less than 60% of average height, same line
            if abs(item["y_center"] - prev["y_center"]) < avg_h * 0.6:
                current_line.append(item)
            else:
                lines.append(current_line)
                current_line = [item]
        lines.append(current_line)
        
    # For each line, sort from right to left (descending order of x_min)
    sorted_text_lines = []
    for line in lines:
        sorted_line = sorted(line, key=lambda item: item["x_min"], reverse=True)
        line_text = " ".join([item["text"] for item in sorted_line])
        sorted_text_lines.append(line_text)
        
    return "\n".join(sorted_text_lines)

def sort_paddleocr_results(result_dict: dict) -> str:
    """Sort PaddleOCR results right-to-left within lines to preserve Arabic reading order."""
    texts = result_dict.get("rec_texts", [])
    polys = result_dict.get("rec_polys", [])
    
    if not texts:
        return ""
        
    processed = []
    for i in range(len(texts)):
        text = texts[i]
        bbox = polys[i]
        
        # Convert numpy array to list if needed
        if hasattr(bbox, "tolist"):
            bbox = bbox.tolist()
            
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
        
    # Sort primarily by vertical position (y_center)
    processed = sorted(processed, key=lambda item: item["y_center"])
    
    # Group into lines
    lines = []
    if processed:
        current_line = [processed[0]]
        for item in processed[1:]:
            prev = current_line[-1]
            avg_h = (prev["h"] + item["h"]) / 2.0
            # If vertical distance between centers is less than 60% of average height, same line
            if abs(item["y_center"] - prev["y_center"]) < avg_h * 0.6:
                current_line.append(item)
            else:
                lines.append(current_line)
                current_line = [item]
        lines.append(current_line)
        
    # For each line, sort from right to left (descending order of x_min)
    sorted_text_lines = []
    for line in lines:
        sorted_line = sorted(line, key=lambda item: item["x_min"], reverse=True)
        line_text = " ".join([item["text"] for item in sorted_line])
        sorted_text_lines.append(line_text)
        
    return "\n".join(sorted_text_lines)


def get_paddleocr_blocks(result_dict: dict) -> list:
    """Extract standard block format from PaddleOCR results dict."""
    texts = result_dict.get("rec_texts", [])
    polys = result_dict.get("rec_polys", [])
    if not texts:
        return []
        
    processed = []
    for i in range(len(texts)):
        text = texts[i]
        bbox = polys[i]
        
        # Convert numpy array to list if needed
        if hasattr(bbox, "tolist"):
            bbox = bbox.tolist()
            
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


def get_easyocr_blocks(results: list) -> list:
    """Extract standard block format from EasyOCR results list."""
    if not results:
        return []
        
    processed = []
    for bbox, text, conf in results:
        # Convert bbox corners to coords
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




def ocr_space_extract(img: Image.Image, lang: str = "ara", api_key: str = "helloworld", ocr_space_engine: int = 3) -> str:
    """
    Perform OCR using OCR.space API.
    Raises ValueError if image exceeds 1MB.
    Raises Exception if API call fails or times out.
    """
    # Convert PIL Image to PNG bytes
    img_buffer = io.BytesIO()
    img.save(img_buffer, format="PNG")
    img_bytes = img_buffer.getvalue()
    
    # Check size (1MB = 1,048,576 bytes)
    if len(img_bytes) > 1048576:
        raise ValueError(f"Image size ({len(img_bytes) / 1048576:.2f} MB) exceeds 1MB limit for OCR.space free tier")
        
    url = 'https://api.ocr.space/parse/image'
    
    # Map language code
    ocr_lang = "eng"
    if "ara" in lang:
        ocr_lang = "ara"
        
    payload = {
        'isOverlayRequired': False,
        'apikey': api_key,
        'language': ocr_lang,
        'OCREngine': ocr_space_engine,
    }
    
    # Make POST request with a timeout of 8 seconds
    r = requests.post(
        url,
        files={'image.png': img_bytes},
        data=payload,
        timeout=8
    )
    
    if r.status_code != 200:
        raise Exception(f"OCR.space API returned HTTP status {r.status_code}")
        
    res_json = r.json()
    if res_json.get("IsErroredOnProcessing"):
        error_msg = res_json.get("ErrorMessage")
        if isinstance(error_msg, list):
            error_msg = ", ".join(error_msg)
        raise Exception(f"OCR.space API Error: {error_msg}")
        
    parsed_results = res_json.get("ParsedResults")
    if not parsed_results:
        raise Exception("OCR.space API returned empty results")
        
    text = parsed_results[0].get("ParsedText", "")
    return text.strip()

def is_native_text_valid(text: str) -> bool:
    if not text:
        return False
    stripped = text.strip()
    if len(stripped) < 3:
        return False
    import re
    if not re.search(r'[\w\u0600-\u06FF]', stripped):
        return False
    return True

def extract_from_blocks(blocks, rel_x0=None, rel_y0=None, rel_x1=None, rel_y1=None, filter_quote=False):
    filtered = []
    for b in blocks:
        if filter_quote:
            is_in_quote = (b["y_center"] >= rel_y0 - 4) and (b["y_center"] <= rel_y1 + 4) and \
                          (b["x_max"] >= rel_x0 - 15) and (b["x_min"] <= rel_x1 + 15)
            if not is_in_quote:
                continue
        filtered.append(b)
        
    if not filtered:
        return ""
        
    # Sort vertically by y_center
    filtered = sorted(filtered, key=lambda item: item["y_center"])
    
    # Group into lines
    lines = []
    current_line = [filtered[0]]
    for item in filtered[1:]:
        prev = current_line[-1]
        avg_h = (prev["h"] + item["h"]) / 2.0
        if abs(item["y_center"] - prev["y_center"]) < avg_h * 0.6:
            current_line.append(item)
        else:
            lines.append(current_line)
            current_line = [item]
    lines.append(current_line)
    
    # Sort each line right-to-left
    sorted_text_lines = []
    for line in lines:
        sorted_line = sorted(line, key=lambda item: item["x_min"], reverse=True)
        line_text = " ".join([item["text"] for item in sorted_line])
        sorted_text_lines.append(line_text)
        
    return "\n".join(sorted_text_lines)

def process_tesseract_data(data, ocr_zoom, rel_x0=None, rel_y0=None, rel_x1=None, rel_y1=None, filter_quote=False):
    n_words = len(data['text'])
    words_by_line = {}
    
    for i in range(n_words):
        text = data['text'][i].strip()
        try:
            conf = float(data['conf'][i])
        except (ValueError, TypeError):
            conf = -1
        if not text or conf == -1:
            continue
            
        left = data['left'][i] / ocr_zoom
        top = data['top'][i] / ocr_zoom
        width = data['width'][i] / ocr_zoom
        height = data['height'][i] / ocr_zoom
        
        x_min = left
        x_max = left + width
        y_min = top
        y_max = top + height
        y_center = y_min + height / 2.0
        
        if filter_quote:
            is_in_quote = (y_center >= rel_y0 - 4) and (y_center <= rel_y1 + 4) and \
                          (x_max >= rel_x0 - 15) and (x_min <= rel_x1 + 15)
            if not is_in_quote:
                continue
                
        line_key = (data['block_num'][i], data['par_num'][i], data['line_num'][i])
        if line_key not in words_by_line:
            words_by_line[line_key] = []
            
        words_by_line[line_key].append({
            "text": text,
            "x_min": x_min,
            "y_center": y_center
        })
        
    sorted_line_keys = sorted(
        words_by_line.keys(),
        key=lambda k: sum(w["y_center"] for w in words_by_line[k]) / len(words_by_line[k]) if words_by_line[k] else 0
    )
    
    sorted_lines_text = []
    for k in sorted_line_keys:
        sorted_words = sorted(words_by_line[k], key=lambda w: w["x_min"])
        sorted_lines_text.append(" ".join(w["text"] for w in sorted_words))
        
    return "\n".join(sorted_lines_text)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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

def crop_region(page, rect: fitz.Rect, zoom: float = 4.0, include_annots: bool = True) -> Image.Image:
    """Render and crop a specific region of a PDF page with high resolution (zoom)."""
    matrix = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=matrix, clip=rect, annots=include_annots)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    return img

def extract_highlights(
    pdf_path: str,
    ocr: bool = False,
    ocr_engine: str = "ocr_space",
    ocr_space_key: str = "K89878519788957",
    ocr_space_engine: int = 3,
    lang: str = "ara+eng",
    tessdata_dir: str = None,
    merge_threshold: float = 20.0,
    save_images: bool = True,
    save_dir: str = "highlights",
    context: bool = False,
    context_margin: float = 80.0,
    warnings: list = None,
    progress_callback = None,
    output_json_path: str = None
) -> list:
    """
    Core function to process the PDF and extract highlights.
    
    Returns a list of dictionaries:
    [
        {
            "page": page_number,
            "rect": [x0, y0, x1, y1],
            "image_path": str or None,
            "text": extracted_text,
            "context": context_text (optional)
        }
    ]
    """
    # Fallback to env key if default key is passed
    if ocr_space_key == "K89878519788957":
        ocr_space_key = os.getenv("OCR_SPACE_KEY", ocr_space_key)

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
        
    # Initialize EasyOCR if chosen
    easyocr_reader = None
    if ocr and ocr_engine == "easyocr":
        try:
            import easyocr
            easyocr_langs = []
            if "ara" in lang:
                easyocr_langs.append("ar")
            if "eng" in lang or "en" in lang:
                easyocr_langs.append("en")
            if not easyocr_langs:
                easyocr_langs = ["ar", "en"]
            logger.info(f"Initializing EasyOCR reader for languages: {easyocr_langs}...")
            easyocr_reader = easyocr.Reader(easyocr_langs)
        except Exception as e:
            logger.error(f"EasyOCR initialization failed: {e}. Falling back to Tesseract.")
            ocr_engine = "tesseract"

    # Initialize PaddleOCR if chosen or as a fallback
    paddle_reader = None
    if ocr and (ocr_engine == "paddleocr" or ocr_engine == "ocr_space"):
        try:
            from paddleocr import PaddleOCR
            paddle_lang = "en"
            if "ara" in lang:
                paddle_lang = "ar"
            logger.info(f"Initializing PaddleOCR reader for language: {paddle_lang}...")
            paddle_reader = PaddleOCR(use_textline_orientation=True, lang=paddle_lang)
        except Exception as e:
            logger.error(f"PaddleOCR initialization failed: {e}. Falling back to Tesseract.")
            if ocr_engine == "paddleocr":
                ocr_engine = "tesseract"

    import json
    extracted_data = []
    tasks = []
    total_pages = len(doc)
    
    # Step 1: Collect highlight regions & crop images (and perform native text extraction if ocr is False)
    for page_num in range(total_pages):
        if progress_callback:
            try:
                if ocr:
                    pct = int((page_num + 1) / total_pages * 50)
                    progress_callback(page_num + 1, total_pages, phase="parsing", percent=pct)
                else:
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
        
        # Render clean page at zoom 4.0 once (without annotations)
        pix_clean = page.get_pixmap(matrix=fitz.Matrix(4.0, 4.0), annots=False)
        img_clean = Image.frombytes("RGB", [pix_clean.width, pix_clean.height], pix_clean.samples)
        
        # Render annotated page at zoom 4.0 if save_images is True
        img_annots = None
        if save_images:
            pix_annots = page.get_pixmap(matrix=fitz.Matrix(4.0, 4.0), annots=True)
            img_annots = Image.frombytes("RGB", [pix_annots.width, pix_annots.height], pix_annots.samples)
            
        for idx, rect in enumerate(merged_rects):
            highlight_id = idx + 1
            image_path = None
            
            # Save visual image (with highlights) if save_images is enabled
            if save_images and img_annots:
                try:
                    crop_box = (
                        (rect.x0 - page_rect.x0) * 4.0,
                        (rect.y0 - page_rect.y0) * 4.0,
                        (rect.x1 - page_rect.x0) * 4.0,
                        (rect.y1 - page_rect.y0) * 4.0
                    )
                    styled_img = img_annots.crop(crop_box)
                    image_filename = f"page_{page_num + 1}_highlight_{highlight_id}.png"
                    img_path = pdf_save_dir / image_filename
                    styled_img.save(img_path, format="PNG")
                    image_path = str(img_path)
                except Exception as e:
                    logger.error(f"Error saving image for page {page_num + 1}, highlight {highlight_id}: {e}")
            
            # Extract native text in highlight region
            native_quote = ""
            try:
                native_quote = page.get_text("text", clip=rect).strip()
            except Exception as e:
                logger.error(f"Native text extraction failed for page {page_num + 1}: {e}")
            
            # Smart Native Text Bypass check
            if ocr and is_native_text_valid(native_quote):
                item = {
                    "page": page_num + 1,
                    "rect": [rect.x0, rect.y0, rect.x1, rect.y1],
                    "image_path": image_path,
                    "text": native_quote,
                    "ocr_engine": "native"
                }
                if context:
                    try:
                        c_rect = fitz.Rect(
                            page_rect.x0,
                            max(page_rect.y0, rect.y0 - context_margin),
                            page_rect.x1,
                            min(page_rect.y1, rect.y1 + context_margin)
                        )
                        item["context"] = page.get_text("text", clip=c_rect).strip()
                    except Exception as e:
                        logger.error(f"Native context failed: {e}")
                        item["context"] = ""
                extracted_data.append(item)
                
                if output_json_path:
                    try:
                        with open(output_json_path, "w", encoding="utf-8") as f:
                            json.dump(extracted_data, f, ensure_ascii=False, indent=2)
                    except Exception as e:
                        logger.error(f"Failed to save repeated JSON update: {e}")
                continue
            
            # If OCR is False, just extract native text
            if not ocr:
                item = {
                    "page": page_num + 1,
                    "rect": [rect.x0, rect.y0, rect.x1, rect.y1],
                    "image_path": image_path,
                    "text": native_quote or "[No native text found. Try running with --ocr]",
                    "ocr_engine": "native"
                }
                if context:
                    try:
                        c_rect = fitz.Rect(
                            page_rect.x0,
                            max(page_rect.y0, rect.y0 - context_margin),
                            page_rect.x1,
                            min(page_rect.y1, rect.y1 + context_margin)
                        )
                        item["context"] = page.get_text("text", clip=c_rect).strip() or "[No native context found. Try running with --ocr]"
                    except Exception as e:
                        logger.error(f"Native context failed: {e}")
                        item["context"] = "[Context Extraction Failed]"
                extracted_data.append(item)
                
                if output_json_path:
                    try:
                        with open(output_json_path, "w", encoding="utf-8") as f:
                            json.dump(extracted_data, f, ensure_ascii=False, indent=2)
                    except Exception as e:
                        logger.error(f"Failed to save repeated JSON update: {e}")
                continue
            
            # OCR is True and native text is not valid -> Schedule OCR task
            item_idx = len(extracted_data)
            item = {
                "page": page_num + 1,
                "rect": [rect.x0, rect.y0, rect.x1, rect.y1],
                "image_path": image_path,
                "text": "",
                "ocr_engine": ocr_engine
            }
            if context:
                item["context"] = ""
            extracted_data.append(item)
            
            # Crop OCR images from cached img_clean
            clean_img = None
            clean_c_img = None
            c_rect = None
            ocr_rect = None
            ocr_zoom = 1.0 if ocr_engine in ("paddleocr", "easyocr") else 4.0
            
            if context:
                c_rect = fitz.Rect(
                    page_rect.x0,
                    max(page_rect.y0, rect.y0 - context_margin),
                    page_rect.x1,
                    min(page_rect.y1, rect.y1 + context_margin)
                )
                crop_box_c = (
                    (c_rect.x0 - page_rect.x0) * 4.0,
                    (c_rect.y0 - page_rect.y0) * 4.0,
                    (c_rect.x1 - page_rect.x0) * 4.0,
                    (c_rect.y1 - page_rect.y0) * 4.0
                )
                clean_c_img = img_clean.crop(crop_box_c)
                if ocr_zoom == 1.0:
                    clean_c_img = clean_c_img.resize((int(clean_c_img.width / 4.0), int(clean_c_img.height / 4.0)), Image.Resampling.BILINEAR)
                if ocr_engine not in ("paddleocr", "easyocr"):
                    clean_c_img = preprocess_image_for_ocr(clean_c_img)
                
                # For ocr_space we need clean_img too since it doesn't support de-duplication
                if ocr_engine == "ocr_space":
                    ocr_rect = fitz.Rect(page_rect.x0, rect.y0, page_rect.x1, rect.y1)
                    crop_box_quote = (
                        (ocr_rect.x0 - page_rect.x0) * 4.0,
                        (ocr_rect.y0 - page_rect.y0) * 4.0,
                        (ocr_rect.x1 - page_rect.x0) * 4.0,
                        (ocr_rect.y1 - page_rect.y0) * 4.0
                    )
                    clean_img = img_clean.crop(crop_box_quote)
                    if ocr_zoom == 1.0:
                        clean_img = clean_img.resize((int(clean_img.width / 4.0), int(clean_img.height / 4.0)), Image.Resampling.BILINEAR)
                    clean_img = preprocess_image_for_ocr(clean_img)
            else:
                ocr_rect = fitz.Rect(page_rect.x0, rect.y0, page_rect.x1, rect.y1)
                crop_box_quote = (
                    (ocr_rect.x0 - page_rect.x0) * 4.0,
                    (ocr_rect.y0 - page_rect.y0) * 4.0,
                    (ocr_rect.x1 - page_rect.x0) * 4.0,
                    (ocr_rect.y1 - page_rect.y0) * 4.0
                )
                clean_img = img_clean.crop(crop_box_quote)
                if ocr_zoom == 1.0:
                    clean_img = clean_img.resize((int(clean_img.width / 4.0), int(clean_img.height / 4.0)), Image.Resampling.BILINEAR)
                if ocr_engine not in ("paddleocr", "easyocr"):
                    clean_img = preprocess_image_for_ocr(clean_img)
                    
            tasks.append({
                "item_idx": item_idx,
                "page_num": page_num + 1,
                "rect": rect,
                "c_rect": c_rect,
                "ocr_rect": ocr_rect,
                "highlight_id": highlight_id,
                "image_path": image_path,
                "clean_img": clean_img,
                "clean_c_img": clean_c_img,
                "ocr_zoom": ocr_zoom
            })
            
    doc.close()
    
    if not ocr or not tasks:
        return extracted_data

    # Helper function to save repeated updates
    def save_incremental():
        if output_json_path:
            try:
                with open(output_json_path, "w", encoding="utf-8") as f:
                    json.dump(extracted_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.error(f"Failed to save repeated JSON update: {e}")

    # Step 3: Run OCR (Batch PaddleOCR vs ThreadPoolExecutor for others)
    if ocr_engine == "paddleocr" and paddle_reader is not None:
        images_to_ocr = []
        image_mapping = []  # stores (task_idx, type)
        
        for idx, t in enumerate(tasks):
            if t["clean_img"]:
                images_to_ocr.append(np.array(t["clean_img"].convert('RGB')))
                image_mapping.append((idx, "quote_only"))
            if t["clean_c_img"]:
                images_to_ocr.append(np.array(t["clean_c_img"].convert('RGB')))
                image_mapping.append((idx, "context_and_quote"))
                
        if images_to_ocr:
            batch_size = 4
            logger.info(f"Performing batch PaddleOCR on {len(images_to_ocr)} images in chunks of {batch_size}...")
            total_ocr = len(images_to_ocr)
            completed_ocr = 0
            
            for i in range(0, total_ocr, batch_size):
                chunk = images_to_ocr[i:i + batch_size]
                chunk_mapping = image_mapping[i:i + batch_size]
                
                try:
                    ocr_results = paddle_reader.ocr(chunk)
                except Exception as e:
                    logger.error(f"Batch PaddleOCR failed: {e}")
                    raise RuntimeError(f"فشلت عملية التعرف الضوئي PaddleOCR: {str(e)}")
                    
                for res_idx, ocr_res in enumerate(ocr_results):
                    task_idx, res_type = chunk_mapping[res_idx]
                    t = tasks[task_idx]
                    item_idx = t["item_idx"]
                    
                    if ocr_res:
                        blocks = get_paddleocr_blocks(ocr_res)
                    else:
                        blocks = []
                        
                    if res_type == "quote_only":
                        extracted_data[item_idx]["text"] = extract_from_blocks(blocks, filter_quote=False)
                    elif res_type == "context_and_quote":
                        # Context text
                        context_text = extract_from_blocks(blocks, filter_quote=False)
                        extracted_data[item_idx]["context"] = context_text
                        
                        # Filter quote text from context OCR
                        rect = t["rect"]
                        c_rect = t["c_rect"]
                        rel_x0 = rect.x0 - c_rect.x0
                        rel_y0 = rect.y0 - c_rect.y0
                        rel_x1 = rect.x1 - c_rect.x0
                        rel_y1 = rect.y1 - c_rect.y0
                        
                        quote_text = extract_from_blocks(
                            blocks, 
                            rel_x0=rel_x0, 
                            rel_y0=rel_y0, 
                            rel_x1=rel_x1, 
                            rel_y1=rel_y1, 
                            filter_quote=True
                        )
                        extracted_data[item_idx]["text"] = quote_text
                        
                completed_ocr += len(chunk)
                if progress_callback:
                    try:
                        pct = 50 + int((completed_ocr / total_ocr) * 50)
                        progress_callback(completed_ocr, total_ocr, phase="ocr", percent=pct)
                    except Exception as e:
                        pass
                save_incremental()

    else:
        # EasyOCR, Tesseract, OCR.space
        ocr_tasks = []
        for idx, t in enumerate(tasks):
            if ocr_engine == "ocr_space":
                if t["clean_img"]:
                    ocr_tasks.append({"task_idx": idx, "img": t["clean_img"], "type": "quote_only"})
                if t["clean_c_img"]:
                    ocr_tasks.append({"task_idx": idx, "img": t["clean_c_img"], "type": "context_only"})
            else:
                if t["clean_img"]:
                    ocr_tasks.append({"task_idx": idx, "img": t["clean_img"], "type": "quote_only"})
                if t["clean_c_img"]:
                    ocr_tasks.append({"task_idx": idx, "img": t["clean_c_img"], "type": "context_and_quote"})
                    
        if ocr_tasks:
            # Set thread workers count
            max_workers = 4
            if ocr_engine == "ocr_space":
                max_workers = 8
            elif ocr_engine == "tesseract":
                max_workers = min(6, os.cpu_count() or 4)
                
            def process_single_task(t_info):
                t_idx = t_info["task_idx"]
                img = t_info["img"]
                t_type = t_info["type"]
                t = tasks[t_idx]
                item_idx = t["item_idx"]
                
                try:
                    if ocr_engine == "tesseract":
                        config = "--psm 6"
                        if tessdata_dir:
                            config = f'--tessdata-dir "{tessdata_dir}" --psm 6'
                        
                        data = pytesseract.image_to_data(img, lang=lang, config=config, output_type=pytesseract.Output.DICT)
                        
                        if t_type == "quote_only":
                            text = process_tesseract_data(data, t["ocr_zoom"], filter_quote=False)
                            return item_idx, "text", text, None
                        elif t_type == "context_and_quote":
                            context_text = process_tesseract_data(data, t["ocr_zoom"], filter_quote=False)
                            
                            # Filter quote
                            rect = t["rect"]
                            c_rect = t["c_rect"]
                            rel_x0 = rect.x0 - c_rect.x0
                            rel_y0 = rect.y0 - c_rect.y0
                            rel_x1 = rect.x1 - c_rect.x0
                            rel_y1 = rect.y1 - c_rect.y0
                            
                            quote_text = process_tesseract_data(
                                data, 
                                t["ocr_zoom"], 
                                rel_x0=rel_x0, 
                                rel_y0=rel_y0, 
                                rel_x1=rel_x1, 
                                rel_y1=rel_y1, 
                                filter_quote=True
                            )
                            return item_idx, "context_and_quote", (context_text, quote_text), None
                            
                    elif ocr_engine == "easyocr" and easyocr_reader is not None:
                        img_np = np.array(img)
                        ocr_res = easyocr_reader.readtext(img_np)
                        blocks = get_easyocr_blocks(ocr_res)
                        
                        if t_type == "quote_only":
                            text = extract_from_blocks(blocks, filter_quote=False)
                            return item_idx, "text", text, None
                        elif t_type == "context_and_quote":
                            context_text = extract_from_blocks(blocks, filter_quote=False)
                            
                            rect = t["rect"]
                            c_rect = t["c_rect"]
                            rel_x0 = rect.x0 - c_rect.x0
                            rel_y0 = rect.y0 - c_rect.y0
                            rel_x1 = rect.x1 - c_rect.x0
                            rel_y1 = rect.y1 - c_rect.y0
                            
                            quote_text = extract_from_blocks(
                                blocks, 
                                rel_x0=rel_x0, 
                                rel_y0=rel_y0, 
                                rel_x1=rel_x1, 
                                rel_y1=rel_y1, 
                                filter_quote=True
                            )
                            return item_idx, "context_and_quote", (context_text, quote_text), None
                            
                    elif ocr_engine == "ocr_space":
                        api_keys = [k.strip() for k in ocr_space_key.split(",") if k.strip()]
                        if not api_keys:
                            api_keys = ["K89878519788957"]
                        ocr_success = False
                        last_error = ""
                        for api_k in api_keys:
                            try:
                                text = ocr_space_extract(img, lang=lang, api_key=api_k, ocr_space_engine=ocr_space_engine)
                                ocr_success = True
                                break
                            except Exception as e:
                                last_error = str(e)
                        if not ocr_success:
                            raise RuntimeError("فشلت جميع مفاتيح API لـ OCR.space! يرجى اختيار محرك محلي مثل PaddleOCR أو Tesseract والبدء من جديد.")
                        
                        if t_type == "quote_only":
                            return item_idx, "text", text, None
                        elif t_type == "context_only":
                            return item_idx, "context", text, None
                            
                    return item_idx, "text", "", None
                except Exception as e:
                    return item_idx, "text", "", e

            logger.info(f"Running parallel OCR using ThreadPoolExecutor with {max_workers} workers...")
            total_ocr = len(ocr_tasks)
            completed_ocr = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_single_task, t_info) for t_info in ocr_tasks]
                for future in as_completed(futures):
                    item_idx, res_type, result, err = future.result()
                    if err:
                        raise err
                    
                    if res_type == "text":
                        extracted_data[item_idx]["text"] = result
                    elif res_type == "context":
                        extracted_data[item_idx]["context"] = result
                    elif res_type == "context_and_quote":
                        context_text, quote_text = result
                        extracted_data[item_idx]["context"] = context_text
                        extracted_data[item_idx]["text"] = quote_text
                        
                    completed_ocr += 1
                    if progress_callback:
                        try:
                            pct = 50 + int((completed_ocr / total_ocr) * 50)
                            progress_callback(completed_ocr, total_ocr, phase="ocr", percent=pct)
                        except Exception as e:
                            pass
                    save_incremental()

    return extracted_data
