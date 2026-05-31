import sys
from pathlib import Path

# Add parent dir to path
sys.path.append(str(Path(__file__).parent.parent))

from extractor import extract_highlights

pdf_path = "/home/anondev/ai-lab/qayem/test_highlight.pdf"

print("Starting highlight extraction with PaddleOCR...")
try:
    results = extract_highlights(
        pdf_path=pdf_path,
        ocr=True,
        ocr_engine="paddleocr",
        lang="eng",
        save_images=False,
        context=True,
        context_margin=80.0
    )
    
    print(f"Success! Extracted {len(results)} highlight blocks.")
    if results:
        print("\nFirst Highlight Preview:")
        print(f"Page: {results[0]['page']}")
        print(f"OCR Engine Used: {results[0]['ocr_engine']}")
        print(f"Text: {results[0]['text']}")
        print(f"Context: {results[0]['context']}")
    else:
        print("No highlights found.")
except Exception as e:
    print(f"PaddleOCR extraction test failed: {e}")
    sys.exit(1)
