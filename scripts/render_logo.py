import subprocess
from pathlib import Path

BASE_DIR = Path(r"C:\Users\alexander\.gemini\antigravity-ide\scratch\walpulse")
SVG_FILE = BASE_DIR / "docs" / "images" / "logo.svg"
PNG_FILE = BASE_DIR / "docs" / "images" / "logo.png"
EDGE_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

html_temp = BASE_DIR / "docs" / "images" / "temp_logo.html"
svg_content = SVG_FILE.read_text(encoding="utf-8")

html_content = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ background: transparent; display: flex; justify-content: center; align-items: center; width: 512px; height: 512px; overflow: hidden; }}
    svg {{ width: 512px; height: 512px; }}
  </style>
</head>
<body>
  {svg_content}
</body>
</html>
"""
html_temp.write_text(html_content, encoding="utf-8")

cmd = [
    EDGE_PATH,
    "--headless=new",
    "--window-size=512,512",
    "--default-background-color=00000000",
    f"--screenshot={PNG_FILE.resolve()}",
    html_temp.resolve().as_uri(),
]
subprocess.run(cmd, check=True)
if html_temp.exists():
    html_temp.unlink()
print("Rendered logo.png successfully!")
