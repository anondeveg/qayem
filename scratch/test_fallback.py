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
        self.pdf_path = "/home/anondev/ai-lab/qayem/test_highlight.pdf"
        
    @patch('requests.post')
    @patch('extractor.is_native_text_valid', return_value=False)
    def test_network_failure_raises_error(self, mock_bypass, mock_post):
        import requests
        mock_post.side_effect = requests.exceptions.ConnectionError("Simulated offline state")
        
        print("\n--- Running Network Failure Raises Error Test ---")
        with self.assertRaises(RuntimeError) as ctx:
            extract_highlights(
                pdf_path=self.pdf_path,
                ocr=True,
                ocr_engine="ocr_space",
                ocr_space_key="K89878519788957",
                lang="ara",
                tessdata_dir="/home/anondev/ai-lab/qayem/tessdata",
                save_images=False,
                context=True
            )
        self.assertIn("فشلت جميع مفاتيح API", str(ctx.exception))
        print("Success! RuntimeError correctly raised on network failure.")

    @patch('requests.post')
    @patch('extractor.is_native_text_valid', return_value=False)
    def test_image_size_exceeded_raises_error(self, mock_bypass, mock_post):
        mock_post.return_value = MagicMock()
        with patch('extractor.ocr_space_extract') as mock_ocr_space:
            mock_ocr_space.side_effect = ValueError("Simulated Image size exceeds 1MB limit")
            
            print("\n--- Running Image Size Exceeded Raises Error Test ---")
            with self.assertRaises(RuntimeError) as ctx:
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
            self.assertIn("فشلت جميع مفاتيح API", str(ctx.exception))
            print("Success! RuntimeError correctly raised on image size limit exceeded.")

if __name__ == "__main__":
    unittest.main()
