import os
import sys
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv
from extractor import extract_highlights

load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description="Extract highlights from a PDF file and save them as a JSON file."
    )
    parser.add_argument(
        "pdf_path",
        nargs="?",
        default=None,
        help="Path to the input PDF file. If omitted, the script runs in interactive mode.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path to the output JSON file. Defaults to <pdf_name>_highlights.json.",
    )
    parser.add_argument(
        "--merge-threshold",
        type=float,
        default=20.0,
        help="Vertical distance threshold in points for merging consecutive highlights (default: 20.0).",
    )
    parser.add_argument(
        "--no-images",
        action="store_true",
        help="Disable saving cropped images of the highlighted areas.",
    )
    parser.add_argument(
        "--save-dir",
        default="highlights",
        help="Directory to save cropped highlight images (default: 'highlights').",
    )
    parser.add_argument(
        "--context",
        action="store_true",
        help="Extract surrounding context paragraph (recommended for accurate quotes).",
    )
    parser.add_argument(
        "--context-margin",
        type=float,
        default=80.0,
        help="Vertical margin in points for context extraction (default: 80.0).",
    )
    parser.add_argument(
        "--olmocr",
        action="store_true",
        default=True,
        help="Enable olmOCR Vision-Language Model OCR on compiled highlights PDF (enabled by default).",
    )
    parser.add_argument(
        "--no-olmocr",
        action="store_false",
        dest="olmocr",
        help="Disable olmOCR Vision-Language Model OCR on compiled highlights PDF.",
    )
    parser.add_argument(
        "--ocr-engine",
        choices=["auto", "olmocr", "easyocr", "native"],
        default="auto",
        help="OCR engine to use (default: auto).",
    )
    parser.add_argument(
        "--olmocr-server",
        default="http://localhost:11434/v1",
        help="URL of OpenAI-compatible server for olmOCR (default: http://localhost:11434/v1).",
    )
    parser.add_argument(
        "--olmocr-api-key",
        default=None,
        help="API key for the olmOCR server (optional).",
    )
    parser.add_argument(
        "--olmocr-model",
        default="richardyoung/olmocr2:7b-q8",
        help="Model name to use for olmOCR (default: richardyoung/olmocr2:7b-q8).",
    )

    args = parser.parse_args()

    # Interactive Mode
    if args.pdf_path is None:
        print("=== قيم - PDF Highlight Extractor ===")
        pdf_input = input("Enter the path to the PDF file: ").strip()
        if not pdf_input:
            print("Error: No PDF path provided.")
            sys.exit(1)
        args.pdf_path = pdf_input

        context_input = input(
            "Enable context extraction? (Recommended — tight quotes can be inaccurate) [Y/n]: "
        ).strip().lower()
        args.context = context_input not in ("n", "no")

        # Backwards compatibility check
        if not args.olmocr:
            if args.ocr_engine == "auto":
                args.ocr_engine = "native"

        print("Select OCR Engine:")
        print("  1. Auto (Native for English, EasyOCR fallback for Arabic/scanned) [Default]")
        print("  2. olmOCR (Vision-Language Model OCR — slow but smart)")
        print("  3. EasyOCR (Fast local OCR for Arabic/English)")
        print("  4. Native (No OCR, selectable text only)")
        engine_choice = input("Enter choice [1-4, default: 1]: ").strip()
        
        if engine_choice == "2":
            args.ocr_engine = "olmocr"
        elif engine_choice == "3":
            args.ocr_engine = "easyocr"
        elif engine_choice == "4":
            args.ocr_engine = "native"
        else:
            args.ocr_engine = "auto"

        if args.ocr_engine == "olmocr":
            server_input = input(
                "olmOCR server URL (e.g. http://localhost:11434/v1) [leave blank for local Ollama]: "
            ).strip()
            if server_input:
                args.olmocr_server = server_input
            else:
                args.olmocr_server = "http://localhost:11434/v1"

            model_input = input(
                "olmOCR Model name [leave blank for richardyoung/olmocr2:7b-q8]: "
            ).strip()
            if model_input:
                args.olmocr_model = model_input
            else:
                args.olmocr_model = "richardyoung/olmocr2:7b-q8"

            key_input = input("olmOCR API key [optional, press Enter to skip]: ").strip()
            if key_input:
                args.olmocr_api_key = key_input

        output_input = input(
            "Output JSON path [press Enter for default]: "
        ).strip()
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

    # For backward compatibility check when arguments are passed via CLI
    if not args.olmocr:
        if args.ocr_engine == "auto":
            args.ocr_engine = "native"

    # Run extraction
    print(f"\nProcessing '{pdf_file.name}'...")
    print(f"Context Extraction: {args.context}")
    print(f"OCR Engine: {args.ocr_engine.upper()}")
    if args.ocr_engine == "olmocr":
        server_display = args.olmocr_server or "(local Ollama)"
        print(f"  olmOCR Server: {server_display}")
        print(f"  olmOCR Model: {args.olmocr_model}")
    print(f"Save Cropped Images: {not args.no_images}")
    print()

    def progress_cb(current, total, phase="parsing", percent=None):
        pct = percent if percent is not None else (int(current / total * 100) if total > 0 else 0)
        label = "Parsing pages" if phase == "parsing" else "olmOCR"
        sys.stdout.write(f"\r[{label}] {current}/{total} ({pct}%)")
        sys.stdout.flush()

    try:
        highlights_data = extract_highlights(
            pdf_path=str(pdf_file),
            merge_threshold=args.merge_threshold,
            save_images=not args.no_images,
            save_dir=args.save_dir,
            context=args.context,
            context_margin=args.context_margin,
            progress_callback=progress_cb,
            output_json_path=args.output,
            olmocr=None,
            olmocr_server=args.olmocr_server,
            olmocr_api_key=args.olmocr_api_key,
            olmocr_model=args.olmocr_model,
            ocr_engine=args.ocr_engine,
        )
    except Exception as e:
        print(f"\nExtraction failed: {e}")
        sys.exit(1)

    print()  # Newline after progress

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
