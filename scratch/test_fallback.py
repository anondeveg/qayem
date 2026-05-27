import sys
import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

# Add project directory to path
sys.path.append(str(Path(__file__).parent.parent))

from extractor import extract_highlights
import fitz

class TestOCRSpaceFallback(unittest.TestCase):
    def setUp(self):
        self.pdf_path = "/home/anondev/ai-lab/qayem/al-hayat_al-jensya_fi_mesr_al-qadima.pdf"
        
    @patch('requests.post')
    def test_network_failure_fallback_to_tesseract(self, mock_post):
        import requests
        mock_post.side_effect = requests.exceptions.ConnectionError("Simulated offline state")
        
        print("\n--- Running Network Failure Fallback Test (context=True) ---")
        results = extract_highlights(
            pdf_path=self.pdf_path,
            ocr=True,
            ocr_engine="ocr_space",
            ocr_space_key="K89878519788957",
            lang="ara",
            tessdata_dir="/home/anondev/ai-lab/qayem/tessdata",
            save_images=False,
            context=True
        )
        self.assertTrue(len(results) > 0)
        self.assertIn("context", results[0])
        print(f"Fallback quote text: {results[0]['text']}")
        print(f"Fallback context text: {results[0]['context']}")
        self.assertNotEqual(results[0]['text'], "[OCR Failed]")
        self.assertNotEqual(results[0]['context'], "[OCR Context Failed]")

        print("\n--- Running Network Failure Fallback Test (context=False) ---")
        results_no_ctx = extract_highlights(
            pdf_path=self.pdf_path,
            ocr=True,
            ocr_engine="ocr_space",
            ocr_space_key="K89878519788957",
            lang="ara",
            tessdata_dir="/home/anondev/ai-lab/qayem/tessdata",
            save_images=False,
            context=False
        )
        self.assertTrue(len(results_no_ctx) > 0)
        self.assertNotIn("context", results_no_ctx[0])
        print(f"Fallback quote text (no context): {results_no_ctx[0]['text']}")
        self.assertNotEqual(results_no_ctx[0]['text'], "[OCR Failed]")

    @patch('requests.post')
    def test_image_size_exceeded_fallback_to_tesseract(self, mock_post):
        mock_post.return_value = MagicMock()
        with patch('extractor.ocr_space_extract') as mock_ocr_space:
            mock_ocr_space.side_effect = ValueError("Simulated Image size exceeds 1MB limit")
            
            print("\n--- Running Image Size Exceeded Fallback Test (context=True) ---")
            results = extract_highlights(
                pdf_path=self.pdf_path,
                ocr=True,
                ocr_engine="ocr_space",
                ocr_space_key="K89878519788957",
                lang="ara",
                tessdata_dir="/home/anondev/ai-lab/qayem/tessdata",
                save_images=False,
                context=True
            )
            self.assertTrue(len(results) > 0)
            self.assertIn("context", results[0])
            print(f"Fallback quote text: {results[0]['text']}")
            print(f"Fallback context text: {results[0]['context']}")
            self.assertNotEqual(results[0]['text'], "[OCR Failed]")
            self.assertNotEqual(results[0]['context'], "[OCR Context Failed]")

if __name__ == "__main__":
    unittest.main()
