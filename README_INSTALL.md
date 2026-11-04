# AstraG — High-Performance IPTV & MPEG-TS / HLS Broadcast Server (Ubuntu 22.04 / 24.04)

**AstraG** combines the Astra Control Panel web interface with a native, low-memory asyncio MPEG-TS & HLS transport engine (`stream_engine.py`) and FastAPI control/streaming server (`astra_clone_backend.py` / `astrag_backend.py`).

## Screenshots

### Login Screen (`admin` / `admin`)
![AstraG Login](docs/screenshots/astrag-login.png)

### Main Dashboard Window
![AstraG Main Window](docs/screenshots/astrag-main.png)

## One-Command Installation on Ubuntu 22.04 / 24.04

```bash
rm -rf /tmp/astrag && git clone https://github.com/onlyoktv/astrag.git /tmp/astrag && sudo bash /tmp/astrag/install.sh --start --port 8000 && rm -rf /tmp/astrag
```

Or directly via `curl`:

```bash
curl -fsSL https://raw.githubusercontent.com/onlyoktv/astrag/main/install.sh | sudo bash -s -- --start --port 8000
```

## Complete Uninstall Command

```bash
sudo bash install.sh --uninstall
```

| Path | Purpose / Ownership |
| --- | --- |
| `/opt/astrag/` | Application code (`astrag_backend.py`, `astra_clone_backend.py`, `stream_engine.py`), `venv` |
| `/opt/astrag/www/` | Static web UI (`index.html`, `mod.js`, `account.html`) |
| `/etc/astrag/astrag.conf` | Systemd `EnvironmentFile` (`ASTRA_HOST`, `ASTRA_PORT`, etc.) |
| `/var/lib/astrag/state.json` | Persistent server state and streams configuration |
| `/usr/bin/astrag` | CLI launcher |
| `astrag.service` | Systemd service unit |
