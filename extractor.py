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

def run_olmocr_ocr(compiled_pdf_path: str, task_id: str, server: str = None, api_key: str = None, model: str = None, progress_callback = None) -> dict:
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

    logger.info(f"Running olmocr pipeline command: {' '.join(cmd)}")
    
    if progress_callback:
        progress_callback(0, num_pages, phase="ocr", percent=0)

    try:
        # Start the pipeline subprocess and capture output
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        # Read output in real-time
        # (Though we won't do fragile log parsing, reading the output ensures it finishes and logs are piped)
        while True:
            line = proc.stdout.readline()
            if not line and proc.poll() is not None:
                break
            if line:
                # Log pipeline output locally for debugging if needed
                logger.debug(f"[olmocr pipeline] {line.strip()}")
                
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
    olmocr: bool = False,
    olmocr_server: str = None,
    olmocr_api_key: str = None,
    olmocr_model: str = None
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
    
    if olmocr and save_images and pdf_save_dir:
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
            # Each highlight i corresponds to page i+1 in compiled_highlights.pdf (1-indexed)
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
    
    return extracted_data
