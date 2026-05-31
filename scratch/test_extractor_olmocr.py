import os
import sys
import json
import shutil
import threading
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

# Setup mock OpenAI server
class MockOpenAIServer(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Suppress logging to keep console clean
        pass

    def do_GET(self):
        if self.path == "/v1/models":
            mock_models = {
                "data": [
                    {
                        "id": "mock-model",
                        "object": "model",
                        "created": 1678888888,
                        "owned_by": "organization"
                    }
                ]
            }
            response_bytes = json.dumps(mock_models).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response_bytes)))
            self.end_headers()
            self.wfile.write(response_bytes)
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/v1/chat/completions":
            # For testing, we return a mock markdown content with yaml front matter
            mock_response = {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "---\nprimary_language: ar\nis_rotation_valid: true\nrotation_correction: 0\nis_table: false\nis_diagram: false\n---\nهذا النص مستخرج بواسطة نموذج olmOCR الافتراضي."
                        },
                        "finish_reason": "stop"
                    }
                ],
                "usage": {
                    "prompt_tokens": 12,
                    "completion_tokens": 24,
                    "total_tokens": 36
                }
            }
            
            response_bytes = json.dumps(mock_response).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response_bytes)))
            self.end_headers()
            self.wfile.write(response_bytes)
        else:
            self.send_response(404)
            self.end_headers()

def start_mock_server():
    server = HTTPServer(('localhost', 8000), MockOpenAIServer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server

def test_extractor_olmocr():
    print("Starting mock OpenAI-compatible server...")
    server = start_mock_server()
    
    pdf_path = "test_highlight.pdf"
    if not os.path.exists(pdf_path):
        print(f"Error: test PDF not found at {pdf_path}")
        server.shutdown()
        return

    # Clear highlights folder for test_highlight to reset state
    highlights_dir = Path("highlights/test_highlight")
    if highlights_dir.exists():
        shutil.rmtree(highlights_dir)
        
    print("Calling extract_highlights with olmocr=True...")
    from extractor import extract_highlights
    
    def progress_cb(current, total, phase="parsing", percent=None):
        print(f"[Progress] page {current}/{total}, phase: {phase}, percent: {percent}%")

    output_json = "highlights/test_highlight_highlights.json"
    if os.path.exists(output_json):
        os.remove(output_json)
        
    highlights = extract_highlights(
        pdf_path=pdf_path,
        merge_threshold=20.0,
        save_images=True,
        save_dir="highlights",
        context=True,
        context_margin=80.0,
        progress_callback=progress_cb,
        output_json_path=output_json,
        olmocr=True,
        olmocr_server="http://localhost:8000/v1",
        olmocr_api_key="mock-key",
        olmocr_model="mock-model"
    )
    
    print("\nExtraction finished!")
    print(f"Number of highlights: {len(highlights)}")
    
    for idx, item in enumerate(highlights):
        print(f"\nHighlight {idx + 1}:")
        print(f"  Page: {item['page']}")
        print(f"  Text: {item['text']}")
        print(f"  OCR Engine: {item.get('ocr_engine')}")
        print(f"  Image Path: {item.get('image_path')}")
        
    # Check assertions
    assert len(highlights) == 2, "Should have extracted exactly 2 highlights."
    for item in highlights:
        assert item.get('ocr_engine') == 'olmocr', "OCR engine should be olmocr."
        assert "olmOCR" in item['text'], "OCR text should contain mock server response."
        
    print("\nSUCCESS: Extractor integration with olmOCR is verified and working perfectly!")
    
    # Clean up test output json
    if os.path.exists(output_json):
        os.remove(output_json)
    server.shutdown()

if __name__ == "__main__":
    test_extractor_olmocr()
