import os
import logging
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
        
        # Render annotated page at zoom 4.0 only if save_images is True
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
            
            # Extract native text
            extracted_text = ""
            try:
                native_text = page.get_text("text", clip=rect)
                extracted_text = native_text.strip()
                if not extracted_text:
                    extracted_text = "[No selectable text found]"
            except Exception as e:
                logger.error(f"Native text extraction failed for page {page_num + 1}: {e}")
                extracted_text = "[Extraction Failed]"
            
            context_text = None
            if context:
                try:
                    c_rect = fitz.Rect(
                        page_rect.x0,
                        max(page_rect.y0, rect.y0 - context_margin),
                        page_rect.x1,
                        min(page_rect.y1, rect.y1 + context_margin)
                    )
                    native_context = page.get_text("text", clip=c_rect)
                    context_text = native_context.strip()
                    if not context_text:
                        context_text = "[No selectable context found]"
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
    
    # Compile images to a single PDF if save_images is enabled
    compile_highlights_pdf(extracted_data, save_images, pdf_save_dir)
    
    return extracted_data
