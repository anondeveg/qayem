import sys
from pathlib import Path
from unittest.mock import patch

# Add parent dir to path
sys.path.append(str(Path(__file__).parent.parent))

from extractor import extract_highlights

pdf_path = "/home/anondev/ai-lab/qayem/test_highlight.pdf"

print("Starting highlight extraction with PaddleOCR (Forced)...")
try:
    with patch("extractor.is_native_text_valid", return_value=False):
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
        for idx, res in enumerate(results):
            print(f"\nHighlight {idx + 1} Preview:")
            print(f"Page: {res['page']}")
            print(f"OCR Engine Used: {res['ocr_engine']}")
            print(f"Text: {repr(res['text'])}")
            print(f"Context: {repr(res['context'])}")
    else:
        print("No highlights found.")
except Exception as e:
    print(f"PaddleOCR extraction test failed: {e}")
    sys.exit(1)
