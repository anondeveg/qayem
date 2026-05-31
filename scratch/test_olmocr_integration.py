import os
import sys
import json
import shutil
import asyncio
import threading
import subprocess
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

# Setup mock OpenAI server
class MockOpenAIServer(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(f"[MOCK SERVER LOG] {format % args}", flush=True)

    def do_GET(self):
        print(f"[MOCK SERVER GET] Request path: {self.path}", flush=True)
        if self.path == "/v1/models":
            # Return status 200 and some dummy models info
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
        print(f"[MOCK SERVER POST] Request path: {self.path}", flush=True)
        if self.path == "/v1/chat/completions":
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            req = json.loads(post_data.decode('utf-8'))
            print(f"[MOCK SERVER POST] Request body prompt length: {len(req['messages'][0]['content'])}", flush=True)
            
            # Extract page number or metadata from prompt if needed
            # For testing, we return a mock markdown content with yaml front matter
            mock_response = {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "---\nprimary_language: ar\nis_rotation_valid: true\nrotation_correction: 0\nis_table: false\nis_diagram: false\n---\nهذا النص المستخرج بواسطة olmOCR لقصاصة الهايلايت."
                        },
                        "finish_reason": "stop"
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 20,
                    "total_tokens": 30
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

def test_integration():
    print("Starting mock OpenAI-compatible server...", flush=True)
    server = start_mock_server()
    
    # Paths
    pdf_path = "highlights/test_highlight/compiled_highlights.pdf"
    if not os.path.exists(pdf_path):
        print(f"Error: test compiled PDF not found at {pdf_path}. Run extract_highlights first.", flush=True)
        server.shutdown()
        return
        
    workspace_dir = Path("uploads/test_olmocr_workspace")
    if workspace_dir.exists():
        shutil.rmtree(workspace_dir)
    workspace_dir.mkdir(parents=True, exist_ok=True)
    
    # Build olmocr command
    cmd = [
        sys.executable,
        "-u", # Unbuffered python
        "-m", "olmocr.pipeline",
        str(workspace_dir),
        "--pdfs", pdf_path,
        "--server", "http://localhost:8000/v1",
        "--model", "mock-model",
        "--api_key", "mock-key"
    ]
    
    print(f"Running command: {' '.join(cmd)}", flush=True)
    
    # Stream the output
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    while True:
        line = proc.stdout.readline()
        if not line and proc.poll() is not None:
            break
        if line:
            print(f"[PIPELINE OUT] {line.strip()}", flush=True)
            
    print(f"Pipeline finished with exit code {proc.returncode}", flush=True)
    
    # Check results folder
    results_dir = workspace_dir / "results"
    jsonl_files = list(results_dir.glob("*.jsonl"))
    print(f"Found JSONL files: {jsonl_files}", flush=True)
    
    if jsonl_files:
        jsonl_file = jsonl_files[0]
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                doc = json.loads(line)
                text = doc.get("text", "")
                spans = doc.get("attributes", {}).get("pdf_page_numbers", [])
                print(f"\nDocument text length: {len(text)}", flush=True)
                print(f"Spans: {spans}", flush=True)
                
                for start, end, page_num in spans:
                    page_text = text[start:end].strip()
                    print(f"Page {page_num} OCR text: {page_text}", flush=True)
    else:
        print("Fail: No JSONL file generated.", flush=True)
        
    # Clean up workspace
    shutil.rmtree(workspace_dir)
    server.shutdown()

if __name__ == "__main__":
    test_integration()
