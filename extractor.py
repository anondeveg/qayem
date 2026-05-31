import os
os.environ['PADDLE_PDX_ENABLE_MKLDNN_BYDEFAULT'] = '0'
os.environ['OMP_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'
os.environ['CPU_NUM'] = '1'
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
                    progress_callback(page_num + 1, total_pages * 2)
                else:
                    progress_callback(page_num + 1, total_pages)
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
            
        for idx, rect in enumerate(merged_rects):
            highlight_id = idx + 1
            image_path = None
            
            # Save visual image (with highlights) if save_images is enabled
            if save_images:
                try:
                    styled_img = crop_region(page, rect, zoom=4.0, include_annots=True)
                    image_filename = f"page_{page_num + 1}_highlight_{highlight_id}.png"
                    img_path = pdf_save_dir / image_filename
                    styled_img.save(img_path, format="PNG")
                    image_path = str(img_path)
                except Exception as e:
                    logger.error(f"Error saving image for page {page_num + 1}, highlight {highlight_id}: {e}")
            
            if ocr:
                page_rect = page.rect
                ocr_rect = fitz.Rect(page_rect.x0, rect.y0, page_rect.x1, rect.y1)
                
                ocr_zoom = 1.5 if ocr_engine in ("paddleocr", "easyocr") else 4.0
                clean_img = crop_region(page, ocr_rect, zoom=ocr_zoom, include_annots=False)
                
                # Preprocess image for legacy OCR engines (like Tesseract), but pass raw color image for deep learning engines
                if ocr_engine not in ("paddleocr", "easyocr"):
                    clean_img = preprocess_image_for_ocr(clean_img)
                
                clean_c_img = None
                if context:
                    c_rect = fitz.Rect(
                        page_rect.x0,
                        max(page_rect.y0, rect.y0 - context_margin),
                        page_rect.x1,
                        min(page_rect.y1, rect.y1 + context_margin)
                    )
                    ocr_c_zoom = 1.5 if ocr_engine in ("paddleocr", "easyocr") else 4.0
                    clean_c_img = crop_region(page, c_rect, zoom=ocr_c_zoom, include_annots=False)
                    if ocr_engine not in ("paddleocr", "easyocr"):
                        clean_c_img = preprocess_image_for_ocr(clean_c_img)
                        
                tasks.append({
                    "page_num": page_num + 1,
                    "rect": rect,
                    "highlight_id": highlight_id,
                    "image_path": image_path,
                    "clean_img": clean_img,
                    "clean_c_img": clean_c_img
                })
            else:
                # Native text extraction
                extracted_text = ""
                try:
                    native_text = page.get_text("text", clip=rect)
                    extracted_text = native_text.strip()
                    if not extracted_text:
                        extracted_text = "[No native text found. Try running with --ocr]"
                except Exception as e:
                    logger.error(f"Native text extraction failed for page {page_num + 1}: {e}")
                    extracted_text = "[Extraction Failed]"
                
                context_text = None
                if context:
                    try:
                        page_rect = page.rect
                        c_rect = fitz.Rect(
                            page_rect.x0,
                            max(page_rect.y0, rect.y0 - context_margin),
                            page_rect.x1,
                            min(page_rect.y1, rect.y1 + context_margin)
                        )
                        native_context = page.get_text("text", clip=c_rect)
                        context_text = native_context.strip()
                        if not context_text:
                            context_text = "[No native context found. Try running with --ocr]"
                    except Exception as e:
                        logger.error(f"Native context failed for page {page_num + 1}: {e}")
                        context_text = "[Context Extraction Failed]"
                
                result_item = {
                    "page": page_num + 1,
                    "rect": [rect.x0, rect.y0, rect.x1, rect.y1],
                    "image_path": image_path,
                    "text": extracted_text,
                    "ocr_engine": "native"
                }
                if context:
                    result_item["context"] = context_text
                extracted_data.append(result_item)
                
                # Save repeatedly to disk after each processed native highlight block
                if output_json_path:
                    try:
                        with open(output_json_path, "w", encoding="utf-8") as f:
                            json.dump(extracted_data, f, ensure_ascii=False, indent=2)
                    except Exception as e:
                        logger.error(f"Failed to save repeated JSON update: {e}")
                        
    doc.close()
    
    if not ocr:
        return extracted_data

    # Step 2: Pre-populate extracted_data with task placeholders
    extracted_data = [None] * len(tasks)
    for i, t in enumerate(tasks):
        r = t["rect"]
        item = {
            "page": t["page_num"],
            "rect": [r.x0, r.y0, r.x1, r.y1],
            "image_path": t["image_path"],
            "text": "",
            "ocr_engine": ocr_engine
        }
        if context:
            item["context"] = ""
        extracted_data[i] = item

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
                image_mapping.append((idx, "text"))
            if t["clean_c_img"]:
                images_to_ocr.append(np.array(t["clean_c_img"].convert('RGB')))
                image_mapping.append((idx, "context"))
                
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
                    if ocr_res:
                        text_out = sort_paddleocr_results(ocr_res)
                    else:
                        text_out = ""
                        
                    if res_type == "text":
                        extracted_data[task_idx]["text"] = text_out
                    else:
                        extracted_data[task_idx]["context"] = text_out
                        
                completed_ocr += len(chunk)
                if progress_callback:
                    try:
                        ocr_prog = total_pages + int((completed_ocr / total_ocr) * total_pages)
                        progress_callback(ocr_prog, total_pages * 2)
                    except Exception as e:
                        pass
                save_incremental()

    else:
        # EasyOCR, Tesseract, OCR.space
        ocr_tasks = []
        for idx, t in enumerate(tasks):
            if t["clean_img"]:
                ocr_tasks.append({"task_idx": idx, "img": t["clean_img"], "type": "text"})
            if t["clean_c_img"]:
                ocr_tasks.append({"task_idx": idx, "img": t["clean_c_img"], "type": "context"})
                
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
                
                try:
                    if ocr_engine == "tesseract":
                        config = "--psm 6"
                        if tessdata_dir:
                            config = f'--tessdata-dir "{tessdata_dir}" --psm 6'
                        text = pytesseract.image_to_string(img, lang=lang, config=config).strip()
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
                    elif ocr_engine == "easyocr" and easyocr_reader is not None:
                        img_np = np.array(img)
                        ocr_res = easyocr_reader.readtext(img_np)
                        text = sort_easyocr_results(ocr_res)
                    else:
                        text = ""
                    return t_idx, t_type, text, None
                except Exception as e:
                    return t_idx, t_type, "", e

            logger.info(f"Running parallel OCR using ThreadPoolExecutor with {max_workers} workers...")
            total_ocr = len(ocr_tasks)
            completed_ocr = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_single_task, t_info) for t_info in ocr_tasks]
                for future in as_completed(futures):
                    t_idx, t_type, text, err = future.result()
                    if err:
                        raise err
                    if t_type == "text":
                        extracted_data[t_idx]["text"] = text
                    else:
                        extracted_data[t_idx]["context"] = text
                        
                    completed_ocr += 1
                    if progress_callback:
                        try:
                            ocr_prog = total_pages + int((completed_ocr / total_ocr) * total_pages)
                            progress_callback(ocr_prog, total_pages * 2)
                        except Exception as e:
                            pass
                    save_incremental()

    return extracted_data
