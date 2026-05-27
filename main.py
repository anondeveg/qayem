import os
import sys
import json
import argparse
import urllib.request
from pathlib import Path
import pytesseract
from dotenv import load_dotenv
from extractor import extract_highlights

load_dotenv()

def download_traineddata(lang: str, target_dir: Path) -> bool:
    """Download the Tesseract traineddata file for a language from GitHub."""
    target_dir.mkdir(parents=True, exist_ok=True)
    dest_path = target_dir / f"{lang}.traineddata"
    
    url = f"https://github.com/tesseract-ocr/tessdata_best/raw/main/{lang}.traineddata"
    
    print(f"\n[OCR setup] Downloading Tesseract language data for '{lang}'...")
    print(f"Source: {url}")
    print(f"Destination: {dest_path}")
    
    try:
        # Simple progress reporter
        def report(count, block_size, total_size):
            if total_size > 0:
                percent = int(count * block_size * 100 / total_size)
                percent = min(100, percent)
                sys.stdout.write(f"\rDownloading... {percent}%")
                sys.stdout.flush()
        
        urllib.request.urlretrieve(url, dest_path, reporthook=report)
        sys.stdout.write("\nDone!\n")
        sys.stdout.flush()
        return True
    except Exception as e:
        print(f"\nError downloading {lang}.traineddata: {e}")
        return False

def setup_ocr_languages(lang_str: str) -> str:
    """
    Ensure all requested OCR languages are available.
    If any language is missing from the system, it downloads ALL requested
    languages to a local 'tessdata' folder and returns the path to that folder.
    Returns None if system-wide languages are used.
    """
    req_langs = [l.strip() for l in lang_str.split('+') if l.strip()]
    if not req_langs:
        req_langs = ["eng"]
        lang_str = "eng"
        
    try:
        sys_langs = pytesseract.get_languages()
    except Exception:
        sys_langs = []
        
    all_sys_available = all(l in sys_langs for l in req_langs)
    
    if all_sys_available:
        # All languages are already installed on the system
        return None
        
    # Set up local tessdata directory
    local_dir = Path("tessdata")
    local_dir.mkdir(parents=True, exist_ok=True)
    
    # Download any missing languages into the local folder
    for l in req_langs:
        local_file = local_dir / f"{l}.traineddata"
        if not local_file.exists():
            success = download_traineddata(l, local_dir)
            if not success:
                print(f"Warning: Could not obtain '{l}' language pack. Tesseract may throw an error.")
                
    return str(local_dir.absolute())

def main():
    parser = argparse.ArgumentParser(
        description="Extract highlights from a PDF file and save them as a JSON file, with optional OCR support."
    )
    parser.add_argument(
        "pdf_path",
        nargs="?",
        default=None,
        help="Path to the input PDF file. If omitted, the script runs in interactive mode."
    )
    parser.add_argument(
        "--ocr",
        action="store_true",
        help="Enable OCR to extract text from scanned PDFs or images."
    )
    parser.add_argument(
        "--lang",
        default="ara+eng",
        help="Tesseract language code(s), e.g., 'eng', 'ara', or 'ara+eng' (default: 'ara+eng')."
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path to the output JSON file. Defaults to <pdf_name>_highlights.json."
    )
    parser.add_argument(
        "--merge-threshold",
        type=float,
        default=20.0,
        help="Vertical distance threshold in points for merging consecutive lines/highlights (default: 20.0)."
    )
    parser.add_argument(
        "--no-images",
        action="store_true",
        help="Disable saving cropped images of the highlighted areas."
    )
    parser.add_argument(
        "--save-dir",
        default="highlights",
        help="Directory to save cropped highlight images (default: 'highlights')."
    )
    parser.add_argument(
        "--context",
        action="store_true",
        help="Extract surrounding context (recommended, as tight quotes can be inaccurate due to cropping slices)."
    )
    parser.add_argument(
        "--context-margin",
        type=float,
        default=80.0,
        help="Vertical margin in points to extract context above and below the highlight (default: 80.0)."
    )
    parser.add_argument(
        "--ocr-engine",
        default="ocr_space",
        choices=["ocr_space", "paddleocr", "tesseract", "easyocr"],
        help="OCR engine to use: 'ocr_space' (API-based, default), 'paddleocr' (high-quality local, recommended), 'tesseract' (lightweight local), 'easyocr' (deep learning local) (default: 'ocr_space')."
    )
    parser.add_argument(
        "--ocr-space-key",
        default=os.getenv("OCR_SPACE_KEY", "K89878519788957"),
        help="API Key for OCR.space (default: loaded from .env or 'K89878519788957')."
    )
    parser.add_argument(
        "--ocr-space-engine",
        type=int,
        default=3,
        choices=[1, 2, 3],
        help="OCR.space Engine version (1, 2, or 3). Engine 3 is recommended for high accuracy and Arabic (default: 3)."
    )

    args = parser.parse_args()

    # Interactive Mode
    if args.pdf_path is None:
        print("=== PDF Highlight Extractor ===")
        pdf_input = input("Enter the path to the PDF file: ").strip()
        if not pdf_input:
            print("Error: No PDF path provided.")
            sys.exit(1)
        args.pdf_path = pdf_input
        
        ocr_input = input("Do you want to enable OCR for scanned parts? (y/N): ").strip().lower()
        args.ocr = ocr_input in ('y', 'yes')
        
        if args.ocr:
            engine_input = input("Choose OCR engine (1. OCR.space, 2. PaddleOCR, 3. Tesseract, 4. EasyOCR) [1]: ").strip()
            if engine_input == '2':
                args.ocr_engine = 'paddleocr'
            elif engine_input == '3':
                args.ocr_engine = 'tesseract'
            elif engine_input == '4':
                args.ocr_engine = 'easyocr'
            else:
                args.ocr_engine = 'ocr_space'
                
            if args.ocr_engine == 'ocr_space':
                default_key = args.ocr_space_key
                key_input = input(f"Enter OCR.space API Key [{default_key}]: ").strip()
                if key_input:
                    args.ocr_space_key = key_input
                
            lang_input = input("Enter OCR language code(s) (e.g. 'ara', 'eng', 'ara+eng') [ara+eng]: ").strip()
            if lang_input:
                args.lang = lang_input
                
        context_input = input("Do you want to extract surrounding context? (Tight quotes can be inaccurate) (y/N): ").strip().lower()
        args.context = context_input in ('y', 'yes')
        if args.context:
            margin_input = input("Enter vertical margin for context in points [80.0]: ").strip()
            if margin_input:
                try:
                    args.context_margin = float(margin_input)
                except ValueError:
                    print("Invalid margin. Using default: 80.0")
                
        output_input = input("Enter output JSON path (Press Enter for default): ").strip()
        if output_input:
            args.output = output_input

    # Validate PDF path
    pdf_file = Path(args.pdf_path)
    if not pdf_file.exists():
        print(f"Error: File not found at '{args.pdf_path}'")
        sys.exit(1)

    # Set default output JSON filename
    if args.output is None:
        args.output = f"{pdf_file.stem}_highlights.json"

    # Setup Tesseract OCR languages if OCR is enabled (for primary or fallback use)
    tessdata_dir = None
    if args.ocr:
        try:
            tessdata_dir = setup_ocr_languages(args.lang)
        except Exception as e:
            if args.ocr_engine == "tesseract":
                print(f"Error initializing Tesseract: {e}")
                print("Make sure Tesseract OCR is installed on your system.")
                sys.exit(1)
            else:
                print(f"Warning: Tesseract initialization failed: {e}. Fallback to Tesseract might fail if needed.")

    # Run extraction
    print(f"\nProcessing '{pdf_file.name}'...")
    print(f"OCR Enabled: {args.ocr} (Engine: '{args.ocr_engine}', Language: '{args.lang}')")
    if args.ocr and args.ocr_engine == "ocr_space":
        masked_key = args.ocr_space_key[:4] + "..." + args.ocr_space_key[-4:] if len(args.ocr_space_key) > 8 else "..."
        print(f"OCR.space API Key: {masked_key} (Engine Version: {args.ocr_space_engine})")
    
    print(f"Context Extraction Enabled: {args.context}")
    if args.context:
        print(f"OCR Context Margin: {args.context_margin} points")
    elif args.ocr:
        print("Warning: Running OCR without --context. Tight highlight quotes can be less accurate due to text slicing at crop boundaries.")
        
    print(f"Save Cropped Images: {not args.no_images}")
    
    warnings_list = []
    try:
        highlights_data = extract_highlights(
            pdf_path=str(pdf_file),
            ocr=args.ocr,
            ocr_engine=args.ocr_engine,
            ocr_space_key=args.ocr_space_key,
            ocr_space_engine=args.ocr_space_engine,
            lang=args.lang,
            tessdata_dir=tessdata_dir,
            merge_threshold=args.merge_threshold,
            save_images=not args.no_images,
            save_dir=args.save_dir,
            context=args.context,
            context_margin=args.context_margin,
            warnings=warnings_list,
            output_json_path=args.output
        )
    except Exception as e:
        print(f"Extraction failed: {e}")
        sys.exit(1)

    if warnings_list:
        print("\n[معلومات المعالجة / Warnings]:")
        for w in warnings_list:
            print(f" - {w}")

    # Save to JSON file
    if highlights_data:
        try:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(highlights_data, f, ensure_ascii=False, indent=2)
            print(f"\nSuccess! Extracted {len(highlights_data)} highlight blocks.")
            print(f"JSON output saved to: {Path(args.output).resolve()}")
            if not args.no_images:
                print(f"Cropped images saved in: {Path(args.save_dir).resolve() / pdf_file.stem}")
        except Exception as e:
            print(f"Error saving JSON file: {e}")
            sys.exit(1)
    else:
        print("\nNo highlights found in the PDF file.")

if __name__ == "__main__":
    main()
