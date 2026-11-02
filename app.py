import os
import subprocess
import sys
import urllib.request
from pathlib import Path

try:
    import uvicorn
    import fastapi  # noqa: F401
except ImportError:
    req_file = Path(__file__).resolve().parent / "requirements.txt"
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "--version"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        get_pip = Path("/tmp/get-pip.py")
        urllib.request.urlretrieve("https://bootstrap.pypa.io/get-pip.py", get_pip)
        subprocess.run(
            [sys.executable, str(get_pip), "--break-system-packages", "-q"],
            check=True,
        )
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--break-system-packages", "-q", "-r", str(req_file)],
        check=True,
    )
    import uvicorn

from astra_clone_backend import app, PORT

if __name__ == "__main__":
    port = int(os.environ.get("ASTRA_PORT", os.environ.get("PORT", PORT)))
    host = os.environ.get("ASTRA_HOST", "0.0.0.0")
    uvicorn.run(app, host=host, port=port, loop="asyncio")
