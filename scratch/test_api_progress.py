import time
import threading
import requests
import json
from pathlib import Path

pdf_path = "/home/anondev/ai-lab/qayem/al-hayat_al-jensya_fi_mesr_al-qadima.pdf"
task_id = "test_task_unique_123"

def poll_progress():
    print("[Poller] Starting progress polling...")
    # Poll for 30 seconds max
    for _ in range(30):
        try:
            r = requests.get(f"http://127.0.0.1:5000/api/progress?task_id={task_id}")
            if r.status_code == 200:
                data = r.json()
                print(f"[Poller] Progress: {data.get('current')} / {data.get('total')}")
            else:
                print(f"[Poller] HTTP {r.status_code}: {r.text}")
        except Exception as e:
            print(f"[Poller] Error: {e}")
        time.sleep(1.0)

# Set keys to bad keys first to trigger fallback
print("Setting invalid API keys to trigger Tesseract fallback warning...")
requests.post("http://127.0.0.1:5000/api/settings", json={"ocr_space_key": "badkey1,badkey2"})

# Start polling thread
t = threading.Thread(target=poll_progress, daemon=True)
t.start()

# Make the POST request
print("Sending PDF highlight extraction request with task_id...")
files = {
    "pdf": open(pdf_path, "rb")
}
data = {
    "task_id": task_id,
    "ocr": "true",
    "ocr_engine": "ocr_space",
    "ocr_space_engine": "3",
    "lang": "ara",
    "context": "true",
    "context_margin": "80.0",
    "merge_threshold": "20.0"
}

try:
    r = requests.post("http://127.0.0.1:5000/api/extract", files=files, data=data, timeout=60)
    print(f"\n[Extraction] Status Code: {r.status_code}")
    res = r.json()
    if res.get("success"):
        print(f"[Extraction] Success! Extracted {len(res['highlights'])} highlights.")
        print(f"[Extraction] Warnings: {res.get('warnings')}")
        print("\nFirst Highlight Output Preview:")
        print(json.dumps(res['highlights'][0], ensure_ascii=False, indent=2))
    else:
        print("[Extraction] API Error:", res.get("error"))
except Exception as e:
    print("[Extraction] Request failed:", e)

# Restore default API key
print("\nRestoring default API key...")
requests.post("http://127.0.0.1:5000/api/settings", json={"ocr_space_key": "K89878519788957"})
