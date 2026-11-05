import os
import socket
import subprocess
import sys
import urllib.request
from pathlib import Path

# Pre-bind port immediately so the runtime supervisor sees port 3000 active within <50ms
_early_sock = None
if __name__ == "__main__":
    _port = int(os.environ.get("ASTRA_PORT", os.environ.get("PORT", "3000")))
    _host = os.environ.get("ASTRA_HOST", "0.0.0.0")
    try:
        _early_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _early_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        _early_sock.bind((_host, _port))
        _early_sock.listen(2048)
        _early_sock.set_inheritable(True)
    except OSError:
        if _early_sock is not None:
            _early_sock.close()
        _early_sock = None

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
        if subprocess.run([sys.executable, str(get_pip), "-q"], check=False).returncode != 0:
            subprocess.run([sys.executable, str(get_pip), "--break-system-packages", "-q"], check=True)
    if subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", str(req_file)], check=False).returncode != 0:
        subprocess.run([sys.executable, "-m", "pip", "install", "--break-system-packages", "-q", "-r", str(req_file)], check=True)
    import uvicorn

from astra_clone_backend import app, PORT

if __name__ == "__main__":
    port = int(os.environ.get("ASTRA_PORT", os.environ.get("PORT", PORT)))
    host = os.environ.get("ASTRA_HOST", "0.0.0.0")
    if _early_sock is not None:
        uvicorn.run(app, fd=_early_sock.fileno(), loop="asyncio")
    else:
        uvicorn.run(app, host=host, port=port, loop="asyncio")
