import subprocess
import time
import urllib.request
import urllib.parse
import json
from pathlib import Path

BASE_DIR = Path(r"C:\Users\alexander\.gemini\antigravity-ide\scratch\walpulse")
JAR_PATH = BASE_DIR / "target" / "walpulse-1.0.0.jar"
DOCS_IMAGES = BASE_DIR / "docs" / "images"
EDGE_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

def wait_for_server(timeout=25):
    start = time.time()
    while time.time() - start < timeout:
        try:
            req = urllib.request.urlopen("http://localhost:8080/api/v1/status", timeout=1)
            if req.status == 200:
                print("WalPulse server is UP and responding!")
                return True
        except Exception:
            time.sleep(0.5)
    return False

def post_json(url, data=None):
    try:
        req = urllib.request.Request(url, method="POST")
        if data:
            req.add_header("Content-Type", "application/json")
            body = json.dumps(data).encode("utf-8")
            urllib.request.urlopen(req, data=body, timeout=5)
        else:
            urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"Error posting to {url}: {e}")

def take_screenshot(url, output_png):
    cmd = [
        EDGE_PATH,
        "--headless=new",
        "--window-size=1380,880",
        f"--screenshot={output_png.resolve()}",
        url
    ]
    subprocess.run(cmd, check=True)
    print(f"Captured screenshot: {output_png.name}")

def main():
    # Start Spring Boot process
    print("Launching WalPulse Spring Boot application...")
    proc = subprocess.Popen(["java", "-jar", str(JAR_PATH)], cwd=str(BASE_DIR))

    try:
        if not wait_for_server():
            print("Failed to start server in time!")
            return

        print("Populating simulation data...")
        # Populate orders and payments
        for _ in range(5):
            post_json("http://localhost:8080/api/v1/simulate/order")
            time.sleep(0.1)

        post_json("http://localhost:8080/api/v1/simulate/payment?orderId=10001")
        post_json("http://localhost:8080/api/v1/simulate/delete?customerId=42")
        post_json("http://localhost:8080/api/v1/simulate/outbox")
        post_json("http://localhost:8080/api/v1/simulate/schema?column=loyalty_tier")
        post_json("http://localhost:8080/api/v1/simulate/schema?column=express_shipping")
        post_json("http://localhost:8080/api/v1/simulate/dlq-sample")

        # Give SSE and background events time to settle
        time.sleep(2)

        # 1. Capture Live Stream tab
        take_screenshot("http://localhost:8080/dashboard?tab=stream", DOCS_IMAGES / "web-live-stream.png")

        # 2. Capture Schema Catalog tab
        take_screenshot("http://localhost:8080/dashboard?tab=schemas", DOCS_IMAGES / "web-schema-catalog.png")

        # 3. Capture DLQ Studio tab
        take_screenshot("http://localhost:8080/dashboard?tab=dlq", DOCS_IMAGES / "web-dlq-studio.png")

        # 4. Capture Rule Sandbox tab
        take_screenshot("http://localhost:8080/dashboard?tab=sandbox", DOCS_IMAGES / "web-rule-sandbox.png")

        print("All screenshots successfully captured!")

    finally:
        print("Terminating server process...")
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()

if __name__ == "__main__":
    main()
