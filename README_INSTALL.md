# AstraG — High-Performance Astra-Compatible IPTV & MPEG-TS/HLS Server

AstraG combines the embedded Astra Control Panel web interface with a native, low-memory asyncio MPEG-TS & HLS transport engine (`stream_engine.py`) and FastAPI control/streaming server (`astra_clone_backend.py` / `astrag_backend.py`).

## Key Capabilities
- **MPEG-TS (`.ts`) & HLS (`.m3u8`) Streaming**:
  - Instant VLC `.ts` playback via `/play/{id}`, `/play/{id}.ts`, `/live/{id}`, `/live/{id}.ts` with automatic `PAT + PMT` and pre-roll keyframe burst on connect plus packet coalescing (`32 KB` chunks).
  - Low-memory sliding-window HLS via `/play/{id}.m3u8` and `/play/{id}/index.m3u8`.
  - Public M3U / M3U8 / XSPF playlists at `/playlist.m3u8`, `/playlist.m3u8?ts`, `/playlist.m3u8?hls`, and `/playlist.xspf`.
- **Full Cesbo Astra-Compatible Stream Engine**:
  - Inputs: `http://`, `https://` (direct MPEG-TS and HLS `.m3u8` master/media playlists), `udp://` (unicast & multicast), `rtp://`.
  - Outputs: `udp://` (with `7 * 188 = 1316` byte MTU batching), `http://` (multi-port listeners).
  - Full PSI/SI demuxing (`PAT`, `PMT`, `SDT`, `NIT`), real-time Stream Analyzer (`Analyze` button in UI), PES header error detection, and scrambling detection.
  - Default administrator account `admin` / `admin` and full user management in `Settings -> Users`.

## Ubuntu 22.04 / 24.04 Installation & Update (`AstraG`)

```bash
sudo bash install.sh --update --port 8000
sudo systemctl status astrag.service
sudo journalctl -u astrag.service -f
```

| Path | Purpose / Ownership |
| --- | --- |
| `/opt/astrag/` | Application code (`astrag_backend.py`, `astra_clone_backend.py`, `stream_engine.py`), `venv` |
| `/opt/astrag/www/` | Static web UI (`index.html`, `mod.js`, `account.html`) |
| `/etc/astrag/astrag.conf` | Systemd `EnvironmentFile` (`ASTRA_HOST`, `ASTRA_PORT`, etc.) |
| `/var/lib/astrag/state.json` | Persistent server state and streams configuration (auto-migrated from `/var/lib/astra-clone/state.json` if present) |
| `/usr/bin/astrag` | CLI launcher |
| `astrag.service` | Systemd service unit |
