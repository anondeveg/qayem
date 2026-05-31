import os
os.environ['PADDLE_PDX_ENABLE_MKLDNN_BYDEFAULT'] = '0'
os.environ['OMP_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'
os.environ['CPU_NUM'] = '1'
import io
import logging
from pathlib import Path
from dotenv import load_dotenv

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
    
    for page_num in range(len(doc)):
        if progress_callback:
            try:
                progress_callback(page_num + 1, len(doc))
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
            
            # Extract text
            extracted_text = ""
            current_engine = "native"
            if ocr:
                try:
                    # For OCR, render a clean image without annotations for maximum accuracy
                    # Expand the crop box to the full width of the page
                    page_rect = page.rect
                    ocr_rect = fitz.Rect(page_rect.x0, rect.y0, page_rect.x1, rect.y1)
                    
                    ocr_zoom = 1.5 if ocr_engine in ("paddleocr", "easyocr") else 4.0
                    clean_img = crop_region(page, ocr_rect, zoom=ocr_zoom, include_annots=False)
                    
                    # Preprocess image for legacy OCR engines (like Tesseract), but pass raw color image for deep learning engines
                    if ocr_engine in ("paddleocr", "easyocr"):
                        optimized_img = clean_img
                    else:
                        optimized_img = preprocess_image_for_ocr(clean_img)
                    
                    current_engine = ocr_engine
                    ocr_text = ""
                    
                    if current_engine == "ocr_space":
                        api_keys = [k.strip() for k in ocr_space_key.split(",") if k.strip()]
                        if not api_keys:
                            api_keys = ["K89878519788957"]
                            
                        ocr_success = False
                        last_error = ""
                        for api_k in api_keys:
                            try:
                                ocr_text = ocr_space_extract(optimized_img, lang=lang, api_key=api_k, ocr_space_engine=ocr_space_engine)
                                ocr_success = True
                                break
                            except Exception as e:
                                last_error = str(e)
                                masked_key = api_k[:4] + "..." + api_k[-4:] if len(api_k) > 8 else "..."
                                logger.warning(f"OCR.space failed with key {masked_key} (reason: {e}). Trying next key if available.")
                                
                        if not ocr_success:
                            raise RuntimeError("فشلت جميع مفاتيح API لـ OCR.space! يرجى اختيار محرك محلي مثل PaddleOCR أو Tesseract والبدء من جديد.")
                            
                    if current_engine == "easyocr" and easyocr_reader is not None:
                        img_np = np.array(optimized_img)
                        ocr_results = easyocr_reader.readtext(img_np)
                        extracted_text = sort_easyocr_results(ocr_results)
                    elif current_engine == "paddleocr" and paddle_reader is not None:
                        img_np = np.array(optimized_img.convert('RGB'))
                        ocr_results = paddle_reader.ocr(img_np)
                        if ocr_results and ocr_results[0]:
                            extracted_text = sort_paddleocr_results(ocr_results[0])
                        else:
                            extracted_text = ""
                    elif current_engine == "tesseract":
                        config = "--psm 6"
                        if tessdata_dir:
                            config = f'--tessdata-dir "{tessdata_dir}" --psm 6'
                        # Run OCR
                        ocr_text_raw = pytesseract.image_to_string(optimized_img, lang=lang, config=config)
                        extracted_text = ocr_text_raw.strip()
                    else:
                        extracted_text = ocr_text.strip()
                except RuntimeError as e:
                    raise
                except Exception as e:
                    logger.error(f"OCR failed for page {page_num + 1}, highlight {highlight_id}: {e}")
                    extracted_text = "[OCR Failed]"
                
                # Extract surrounding context if requested
                context_text = None
                if context:
                    try:
                        c_rect = fitz.Rect(
                            page_rect.x0,
                            max(page_rect.y0, rect.y0 - context_margin),
                            page_rect.x1,
                            min(page_rect.y1, rect.y1 + context_margin)
                        )
                        ocr_c_zoom = 1.5 if ocr_engine in ("paddleocr", "easyocr") else 4.0
                        clean_c_img = crop_region(page, c_rect, zoom=ocr_c_zoom, include_annots=False)
                        
                        if ocr_engine in ("paddleocr", "easyocr"):
                            optimized_c_img = clean_c_img
                        else:
                            optimized_c_img = preprocess_image_for_ocr(clean_c_img)
                        
                        current_c_engine = ocr_engine
                        ocr_c_text = ""
                        
                        if current_c_engine == "ocr_space":
                            api_keys = [k.strip() for k in ocr_space_key.split(",") if k.strip()]
                            if not api_keys:
                                api_keys = ["K89878519788957"]
                                
                            ocr_success = False
                            last_error = ""
                            for api_k in api_keys:
                                try:
                                    ocr_c_text = ocr_space_extract(optimized_c_img, lang=lang, api_key=api_k, ocr_space_engine=ocr_space_engine)
                                    ocr_success = True
                                    break
                                except Exception as e:
                                    last_error = str(e)
                                    masked_key = api_k[:4] + "..." + api_k[-4:] if len(api_k) > 8 else "..."
                                    logger.warning(f"OCR.space context failed with key {masked_key} (reason: {e}). Trying next key if available.")
                                    
                            if not ocr_success:
                                raise RuntimeError("فشلت جميع مفاتيح API لـ OCR.space! يرجى اختيار محرك محلي مثل PaddleOCR أو Tesseract والبدء من جديد.")
                                
                        if current_c_engine == "easyocr" and easyocr_reader is not None:
                            img_np = np.array(optimized_c_img)
                            ocr_results = easyocr_reader.readtext(img_np)
                            context_text = sort_easyocr_results(ocr_results)
                        elif current_c_engine == "paddleocr" and paddle_reader is not None:
                            img_np = np.array(optimized_c_img.convert('RGB'))
                            ocr_results = paddle_reader.ocr(img_np)
                            if ocr_results and ocr_results[0]:
                                context_text = sort_paddleocr_results(ocr_results[0])
                            else:
                                context_text = ""
                        elif current_c_engine == "tesseract":
                            config = "--psm 6"
                            if tessdata_dir:
                                config = f'--tessdata-dir "{tessdata_dir}" --psm 6'
                            ocr_text_raw = pytesseract.image_to_string(optimized_c_img, lang=lang, config=config)
                            context_text = ocr_text_raw.strip()
                        else:
                            context_text = ocr_c_text.strip()
                    except RuntimeError as e:
                        raise
                    except Exception as e:
                        logger.error(f"OCR context failed for page {page_num + 1}, highlight {highlight_id}: {e}")
                        context_text = "[OCR Context Failed]"
            else:
                try:
                    # Native text extraction from PDF
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
    return extracted_data
