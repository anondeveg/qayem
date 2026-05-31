import requests
import json
from pathlib import Path

url = "http://127.0.0.1:5000/api/extract"
pdf_path = "/home/anondev/ai-lab/qayem/test_highlight.pdf"

if not Path(pdf_path).exists():
    print(f"Error: Test PDF not found at {pdf_path}")
    exit(1)

print("Sending PDF to Flask extraction endpoint...")
files = {
    "pdf": open(pdf_path, "rb")
}
data = {
    "context": "true",
    "context_margin": "80.0",
    "merge_threshold": "20.0"
}

try:
    r = requests.post(url, files=files, data=data, timeout=30)
    print(f"Status Code: {r.status_code}")
    res = r.json()
    if res.get("success"):
        print(f"Success! Extracted {len(res['highlights'])} highlights.")
        print("\nFirst Highlight Output Preview:")
        print(json.dumps(res['highlights'][0], ensure_ascii=False, indent=2))
    else:
        print("API Error:", res.get("error"))
except Exception as e:
    print("Request failed:", e)
