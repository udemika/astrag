"""AstraG async management API and Cesbo Astra-compatible transport server.

Supports MPEG-TS (.ts) and HLS (.m3u8) inputs and outputs over HTTP/HTTPS/UDP/RTP,
full PSI/SI stream analysis (PAT/PMT/SDT/NIT), multi-input failover, multi-port
public IPTV listeners, and account management.
"""
import argparse
import asyncio
import base64
import copy
import inspect
import ipaddress
import json
import logging
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from urllib.parse import urlsplit, urlunsplit
from typing import List
from xml.sax.saxutils import escape as xml_escape

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from account_security import FailedAuthLimiter, hash_password, valid_login, valid_password, verify_password
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger("astrag.backend")
if getattr(sys, "frozen", False):
    BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
    APP_DIR = Path(sys.executable).resolve().parent
else:
    BUNDLE_DIR = Path(__file__).resolve().parent
    APP_DIR = BUNDLE_DIR


def _cli_config_arg():
    argv = sys.argv[1:]
    for i, arg in enumerate(argv):
        if arg in ("-c", "--config") and i + 1 < len(argv):
            return Path(argv[i + 1])
        if arg.startswith("--config="):
            return Path(arg.split("=", 1)[1])
    return None


def _default_config_file() -> Path:
    cli_cfg = _cli_config_arg()
    if cli_cfg is not None:
        return cli_cfg
    if "ASTRA_CONFIG_FILE" in os.environ:
        return Path(os.environ["ASTRA_CONFIG_FILE"])
    for candidate in (Path("/etc/astrag/astrag.conf"), APP_DIR / "astrag.conf", APP_DIR / "astra-clone.conf"):
        if candidate.exists():
            return candidate
    return APP_DIR / "astrag.conf"


def _migrate_legacy_conf_and_state(conf_path: Path):
    """Migrate legacy KEY=VALUE /etc/astrag/astrag.conf + /var/lib/astrag/state.json into a unified JSON .conf."""
    if not conf_path or conf_path.suffix != ".conf":
        return
    raw = ""
    if conf_path.exists():
        try:
            raw = conf_path.read_text(encoding="utf-8-sig").strip()
        except OSError:
            return
        if raw.startswith("{"):
            return
    kv = {}
    for line in raw.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            kv[k.strip()] = v.strip().strip("\"'")
    legacy_state_path = None
    if "ASTRA_STATE_FILE" in kv:
        legacy_state_path = Path(kv["ASTRA_STATE_FILE"])
    elif conf_path.name == "astrag.conf" and Path("/var/lib/astrag/state.json").exists():
        legacy_state_path = Path("/var/lib/astrag/state.json")
    if legacy_state_path and legacy_state_path != conf_path and legacy_state_path.exists():
        try:
            st_raw = legacy_state_path.read_text(encoding="utf-8-sig").strip()
            st_data = json.loads(st_raw) if st_raw else {}
            loaded = st_data.get("config") if isinstance(st_data, dict) and isinstance(st_data.get("config"), dict) else st_data
            if isinstance(loaded, dict):
                merged = {
                    "port": int(kv.get("ASTRA_PORT", kv.get("PORT", loaded.get("port", loaded.get("control_port", 8000))))),
                    "host": kv.get("ASTRA_HOST", kv.get("HOST", loaded.get("host", "0.0.0.0"))),
                    "gid": loaded.get("gid", 466560),
                    "auth": dict(loaded.get("auth", {"enable": True})),
                    "users": copy.deepcopy(loaded.get("users", {})),
                    "make_stream": copy.deepcopy(loaded.get("make_stream", [])),
                    "categories": copy.deepcopy(loaded.get("categories", [])),
                    "servers": copy.deepcopy(loaded.get("servers", [])),
                    "softcam": copy.deepcopy(loaded.get("softcam", [])),
                    "cas": copy.deepcopy(loaded.get("cas", [])),
                    "dvb_tune": copy.deepcopy(loaded.get("dvb_tune", [])),
                    "settings": copy.deepcopy(loaded.get("settings", {})),
                    "hls": copy.deepcopy(loaded.get("hls", {})),
                }
                if "AUTH_ENABLE" in kv:
                    merged["auth"]["enable"] = kv["AUTH_ENABLE"].lower() in ("true", "1", "yes")
                for uinfo in merged["users"].values():
                    if isinstance(uinfo, dict):
                        pw = uinfo.get("password", "")
                        if isinstance(pw, str) and pw.startswith("pbkdf2_sha256$") and verify_password("admin", pw):
                            uinfo["password"] = "admin"
                conf_path.parent.mkdir(parents=True, exist_ok=True)
                conf_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                return
        except Exception:
            pass
    if raw and not raw.startswith("{"):
        merged = {
            "port": int(kv.get("ASTRA_PORT", kv.get("PORT", 8000))),
            "host": kv.get("ASTRA_HOST", kv.get("HOST", "0.0.0.0")),
            "gid": 466560,
            "auth": {"enable": kv.get("AUTH_ENABLE", "true").lower() in ("true", "1", "yes")},
            "users": {
                "admin": {
                    "type": 1,
                    "role": "admin",
                    "enable": True,
                    "password": "admin",
                    "created": int(time.time()),
                }
            },
            "make_stream": [],
            "categories": [],
            "servers": [],
            "softcam": [],
            "cas": [],
            "dvb_tune": [],
            "settings": {},
            "hls": {},
        }
        try:
            conf_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        except Exception:
            pass


def _default_state_file() -> Path:
    cli_cfg = _cli_config_arg()
    if cli_cfg is not None:
        _migrate_legacy_conf_and_state(cli_cfg)
        return cli_cfg
    if "ASTRA_CONFIG_FILE" in os.environ:
        cfg_p = Path(os.environ["ASTRA_CONFIG_FILE"])
        _migrate_legacy_conf_and_state(cfg_p)
        return cfg_p
    if "ASTRA_STATE_FILE" in os.environ:
        return Path(os.environ["ASTRA_STATE_FILE"])
    if Path("/etc/astrag/astrag.conf").exists():
        cfg_p = Path("/etc/astrag/astrag.conf")
        _migrate_legacy_conf_and_state(cfg_p)
        return cfg_p
    if Path("/var/lib/astrag").is_dir():
        return Path("/var/lib/astrag/state.json")
    return APP_DIR / "state.json"


CONFIG_FILE = _default_config_file()
STATE_FILE = _default_state_file()

FAVICON_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
)


def read_options():
    result = {}
    if CONFIG_FILE.exists():
        try:
            raw = CONFIG_FILE.read_text(encoding="utf-8-sig").strip()
        except OSError:
            return result
        if raw.startswith("{"):
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    cfg = data.get("config") if isinstance(data.get("config"), dict) else data
                    p = cfg.get("port") or cfg.get("control_port")
                    if p is not None and not isinstance(p, bool):
                        result["ASTRA_PORT"] = str(p)
                        result["PORT"] = str(p)
                    h = cfg.get("host")
                    if isinstance(h, str) and h.strip():
                        result["ASTRA_HOST"] = h.strip()
                        result["HOST"] = h.strip()
                    auth_cfg = cfg.get("auth")
                    if isinstance(auth_cfg, dict) and "enable" in auth_cfg:
                        result["AUTH_ENABLE"] = "true" if auth_cfg.get("enable") else "false"
            except ValueError:
                pass
            return result
        for line in raw.splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                result[k.strip()] = v.strip().strip("\"'")
    return result


OPTIONS = read_options()
HOST = os.environ.get("ASTRA_HOST", OPTIONS.get("ASTRA_HOST", OPTIONS.get("HOST", "0.0.0.0")))
PORT = int(os.environ.get("ASTRA_PORT", os.environ.get("PORT", OPTIONS.get("ASTRA_PORT", OPTIONS.get("PORT", "3000")))))
STATIC_DIR = Path(os.environ.get("ASTRA_STATIC_DIR", OPTIONS.get("ASTRA_STATIC_DIR", OPTIONS.get("STATIC_DIR", str(APP_DIR / "www")))))
if not STATIC_DIR.is_dir() and "ASTRA_STATIC_DIR" not in os.environ:
    for _cand in (APP_DIR / "www", BUNDLE_DIR / "www", Path("/opt/astrag/www")):
        if _cand.is_dir():
            STATIC_DIR = _cand
            break


def validate_url(value, output=False):
    if not isinstance(value, str):
        raise ValueError("URL must be a nonempty string without whitespace")
    value = value.strip()
    if not value or any(c.isspace() for c in value):
        raise ValueError("URL must be a nonempty string without whitespace")
    if output and value.startswith(("http://:", "https://:")):
        scheme, rest = value.split("://:", 1)
        value = f"{scheme}://0.0.0.0:{rest}"
    # Astra options after # are passed unchanged to the native engine.
    clean = value.split("#", 1)[0]
    parts = urlsplit(clean)
    allowed_schemes = {"udp", "http", "https"} if output else {"http", "https", "udp", "rtp"}
    if parts.scheme not in allowed_schemes:
        raise ValueError("unsupported output transport" if output else "unsupported input transport")
    if not parts.hostname:
        raise ValueError("URL requires a host")
    port = parts.port
    if parts.scheme in {"udp", "rtp"} and port is None:
        raise ValueError("UDP/RTP URL requires a port")
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("invalid port")
    return value


def validate_stream_entry(item: dict) -> dict:
    """Strict stream configuration validator compatible with both Pydantic v1 and v2 (Ubuntu APT)."""
    if not isinstance(item, dict):
        raise ValueError("each stream entry must be an object")
    entry = dict(item)
    sid = entry.get("id", "")
    name = entry.get("name", "")
    stype = entry.get("type", "spts")
    enable = entry.get("enable", True)
    raw_in = entry.get("input", [])
    raw_out = entry.get("output", [])
    if not isinstance(sid, str) or not isinstance(name, str) or not isinstance(stype, str):
        raise ValueError("stream id, name, and type must be strings")
    if not isinstance(enable, bool):
        raise ValueError("stream enable must be a boolean")
    if not isinstance(raw_in, list) or not isinstance(raw_out, list):
        raise ValueError("stream input and output must be lists")
    entry["id"] = sid
    entry["name"] = name
    entry["type"] = stype
    entry["enable"] = enable
    entry["input"] = [validate_url(v, False) for v in raw_in if not (isinstance(v, str) and not v.strip())]
    entry["output"] = [validate_url(v, True) for v in raw_out if not (isinstance(v, str) and not v.strip())]
    for dkey, is_out in (("_input", False), ("_output", True)):
        if isinstance(entry.get(dkey), list):
            cleaned_d = [validate_url(v, is_out) for v in entry[dkey] if isinstance(v, str) and v.strip()]
            if cleaned_d:
                entry[dkey] = cleaned_d
            else:
                entry.pop(dkey, None)
    return entry


class StreamConfig(BaseModel):
    id: str = ""
    name: str = ""
    type: str = "spts"
    input: List[str] = Field(default_factory=list)
    output: List[str] = Field(default_factory=list)
    enable: bool = True


async def maybe_await(value):
    return await value if inspect.isawaitable(value) else value


def _core_field(value) -> str:
    return str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ")


class CoreSupervisor:
    """Python side of the native astrag-core data plane (child process + control socket)."""

    def __init__(self, proc, ctl_name: str, secret: str, host: str):
        self.proc = proc
        self.ctl_name = ctl_name
        self.secret = secret
        self.host = host
        self.writer = None
        self.pending = []
        self.stats = {}
        self.sess_map = {}
        self.listening = set()
        self.tasks = []

    async def connect(self):
        last_exc = None
        for _ in range(100):
            try:
                reader, writer = await asyncio.open_unix_connection("\0" + self.ctl_name)
                writer.write(f"AUTH\t{self.secret}\n".encode())
                await writer.drain()
                line = await asyncio.wait_for(reader.readline(), 5)
                if line.strip() != b"OK":
                    raise RuntimeError("astrag-core rejected control connection")
                self.writer = writer
                for item in self.pending:
                    writer.write(item)
                self.pending.clear()
                self.tasks = [asyncio.create_task(self._reader(reader)), asyncio.create_task(self._watchdog())]
                return
            except (OSError, RuntimeError, asyncio.TimeoutError) as exc:
                last_exc = exc
                await asyncio.sleep(0.1)
        raise RuntimeError(f"cannot connect to astrag-core: {last_exc}")

    def send(self, *fields):
        data = ("\t".join(_core_field(f) for f in fields) + "\n").encode()
        if self.writer is None:
            self.pending.append(data)
        else:
            self.writer.write(data)

    def _core_gone(self, what: str):
        # SIGTERM from systemctl stop/restart reaches the core too: that is a normal shutdown.
        for _ in range(20):
            if self.proc.poll() is not None:
                break
            time.sleep(0.05)
        rc = self.proc.poll()
        if rc in (0, -signal.SIGTERM, -signal.SIGINT):
            os._exit(0)
        logger.error("astrag-core %s (code %s); restarting service", what, rc)
        os._exit(1)

    async def _watchdog(self):
        # The core owns the public ports; without it the service is down, so let systemd restart everything.
        while True:
            await asyncio.sleep(1.0)
            if self.proc.poll() is not None:
                self._core_gone("exited")

    async def _reader(self, reader):
        while True:
            line = await reader.readline()
            if not line:
                self._core_gone("control connection closed")
            try:
                self._handle(line.decode("utf-8", "replace").rstrip("\n").split("\t"))
            except Exception:
                logger.exception("Bad astrag-core message")

    def _handle(self, f):
        if f[0] == "STAT" and len(f) >= 15:
            pids = []
            for item in filter(None, f[11].split(",")):
                p, k, st = item.split(":")
                pids.append((int(p), int(k), int(st)))
            pmts = []
            for item in filter(None, f[12].split(",")):
                p, pnr = item.split(":")
                pmts.append((int(p), int(pnr)))
            is_idle = (f[14] == "idle")
            self.stats[f[1]] = {
                "onair": f[2] == "1", "bitrate": float(f[3]), "cc_errors": int(f[4]), "pes_errors": int(f[5]),
                "scrambled": f[6] == "1", "input_id": int(f[7]), "packets": int(f[8]), "pcr_pid": int(f[9]),
                "tsid": int(f[10]), "pids": pids, "pmts": pmts, "input_errors": int(f[13]),
                "last_error": None if is_idle else (f[14] or None), "idle": is_idle, "updated": time.monotonic(),
            }
        elif f[0] == "SESS+" and len(f) >= 6:
            core_id = int(f[1])
            psid = state.next_session
            state.next_session += 1
            self.sess_map[core_id] = psid
            state.sessions[psid] = {
                "client_id": psid, "channel_id": f[2],
                "channel_name": state.all_stream_configs.get(f[2], {}).get("name", ""),
                "addr": f[3], "login": f[4], "ua": f[5], "sdate": int(time.time() * 1000),
                "started": time.time(), "queue": None, "engine": state.engines.get(f[2]), "core_id": core_id,
            }
        elif f[0] == "SESS-" and len(f) >= 2:
            psid = self.sess_map.pop(int(f[1]), None)
            if psid is not None:
                state.sessions.pop(psid, None)


CORE = None


def _via_core(request) -> bool:
    return CORE is not None and request.headers.get("x-astrag-via") == CORE.secret


def _core_eligible(config) -> bool:
    if CORE is None:
        return False
    for url in config.get("input", []):
        if urlsplit(url.split("#", 1)[0]).scheme.lower() not in ("http", "https"):
            return False
    for url in config.get("output", []):
        if urlsplit(url.split("#", 1)[0]).scheme.lower() not in ("http", "https"):
            return False
    return True


def _has_active_output(config: dict) -> bool:
    if config.get("_cluster_host"):
        return False
    for u in config.get("output") or []:
        if not isinstance(u, str) or not u.strip():
            continue
        base, *frag = u.strip().split("#", 1)
        if not base:
            continue
        if frag:
            f_low = frag[0].lower()
            if "disabled" in f_low or "enable=false" in f_low or "enable=0" in f_low:
                continue
        return True
    return False


def _stream_keep_active(config: dict) -> bool:
    if config.get("_cluster_host"):
        return False
    if _has_active_output(config):
        return True
    global_ka = state.config.get("settings", {}).get("http_keep_active")
    stream_ka = config.get("http_keep_active", global_ka)
    if str(stream_ka) == "-1":
        return True
    return any("keep_active" in u.split("#", 1)[1] for u in config.get("input", []) if isinstance(u, str) and "#" in u)


class CoreEngine:
    """Engine facade for channels served by astrag-core."""

    def __init__(self, config, seg_dur: float, max_segs: int):
        self.config = config
        self.sid = config["id"]
        self.seg_dur = seg_dur
        self.max_segs = max_segs
        self._running = False
        self._started = time.monotonic()

    def start(self):
        CORE.stats.pop(self.sid, None)
        inputs = list(self.config.get("input", []))
        if inputs:
            if self.config.get("_cluster_host"):
                cleaned = []
                for u in inputs:
                    if "#" in u:
                        base, frag = u.split("#", 1)
                        parts = [p for p in frag.split("&") if p and not p.startswith("keep_active")]
                        u = f"{base}#{'&'.join(parts)}" if parts else base
                    cleaned.append(u)
                inputs = cleaned
            elif _stream_keep_active(self.config):
                if "keep_active" not in (inputs[0].split("#", 1)[1] if "#" in inputs[0] else ""):
                    inputs[0] = inputs[0] + ("&keep_active" if "#" in inputs[0] else "#keep_active")
            else:
                global_ka = state.config.get("settings", {}).get("http_keep_active")
                stream_ka = self.config.get("http_keep_active", global_ka)
                if stream_ka is not None and str(stream_ka).lstrip("-").isdigit() and int(stream_ka) > 0:
                    inputs[0] = inputs[0] + (f"&idle_delay={int(stream_ka)}" if "#" in inputs[0] else f"#idle_delay={int(stream_ka)}")
        CORE.send("ADD", self.sid, f"{self.seg_dur:.3f}", self.max_segs, self.config.get("name", ""),
                  *inputs)
        self._running = True
        self._started = time.monotonic()
        return self

    def stop(self):
        if self._running:
            CORE.send("DEL", self.sid)
        self._running = False
        CORE.stats.pop(self.sid, None)

    def unsubscribe(self, queue):
        return None

    def stats(self, include_pids: bool = True):
        st = CORE.stats.get(self.sid)
        keep_active = _stream_keep_active(self.config)
        if st is None:
            is_idle = not keep_active
            st = {}
        else:
            is_idle = bool(st.get("idle", False))
        br = st.get("bitrate", 0.0)
        out = {
            "running": self._running and not is_idle, "idle": is_idle,
            "bitrate": br, "effective_bitrate": br, "average_bitrate": br,
            "continuity_errors": st.get("cc_errors", 0), "cc_errors": st.get("cc_errors", 0),
            "pes_errors": st.get("pes_errors", 0), "scrambled": st.get("scrambled", False),
            "input_id": st.get("input_id", 1), "packets": st.get("packets", 0),
            "bytes": st.get("packets", 0) * 188, "input_errors": st.get("input_errors", 0),
            "last_error": st.get("last_error"), "elapsed": time.monotonic() - self._started,
            "subscribers": sum(1 for s in state.sessions.values() if s.get("channel_id") == self.sid),
        }
        if include_pids:
            out["pids"] = {str(p): {"type": {1: "VIDEO", 2: "AUDIO"}.get(k, "DATA"), "stream_type": t}
                           for p, k, t in st.get("pids", [])}
        return out

    def psi_summary(self):
        st = CORE.stats.get(self.sid, {})
        pmts = st.get("pmts", [])
        pat = {"psi": "pat", "tsid": st.get("tsid", 0),
               "programs": [{"pnr": pnr, "pid": pid} for pid, pnr in pmts]} if pmts else None
        streams = [{"pid": p, "type_id": t, "type_name": {1: "VIDEO", 2: "AUDIO"}.get(k, "DATA"), "descriptors": []}
                   for p, k, t in st.get("pids", [])]
        pmt_list = [{"psi": "pmt", "pnr": pnr, "pid": pid, "pcr": st.get("pcr_pid", 8191), "descriptors": [],
                     "streams": streams if i == 0 else []} for i, (pid, pnr) in enumerate(pmts)]
        return {
            "pat": pat, "pmt": pmt_list, "sdt": None, "nit": None,
            "total": {"bitrate": int(round(st.get("bitrate", 0) / 1000.0)), "cc_errors": st.get("cc_errors", 0),
                      "pes_errors": st.get("pes_errors", 0), "scrambled": st.get("scrambled", False)},
        }


def _start_core(host: str, port: int):
    """Launch astrag-core on the public port; returns (CoreSupervisor, internal_port) or (None, None)."""
    import secrets
    import socket as _socket
    import subprocess
    core_bin = Path(os.environ.get("ASTRA_CORE_BIN") or (APP_DIR / "astrag-core"))
    if os.environ.get("ASTRA_CORE", "1") == "0" or not core_bin.is_file() or not os.access(core_bin, os.X_OK):
        return None, None
    with _socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        internal = s.getsockname()[1]
    secret = secrets.token_hex(16)
    ctl = f"astrag-core-{os.getpid()}-{secrets.token_hex(4)}"
    try:
        proc = subprocess.Popen([str(core_bin), "--ctl", ctl, "--secret", secret, "--upstream", str(internal),
                                 "--listen", f"{host}:{port}"], stdin=subprocess.DEVNULL)
    except OSError:
        logger.warning("astrag-core could not be started; using the Python engine", exc_info=True)
        return None, None
    time.sleep(0.4)
    if proc.poll() is not None:
        logger.warning("astrag-core exited at startup (code %s); using the Python engine", proc.returncode)
        return None, None
    return CoreSupervisor(proc, ctl, secret, host), internal


def make_engine(config):
    hls_cfg = state.config.get("hls", {}) if "state" in globals() else {}
    seg_dur = float(hls_cfg.get("duration") or 3.0)
    max_segs = int(hls_cfg.get("quantity") or 5)
    seg_dur = max(1.0, min(10.0, seg_dur))
    max_segs = max(2, min(12, max_segs))
    if _core_eligible(config):
        return CoreEngine(config, seg_dur, max_segs)
    try:
        from stream_engine import Engine
    except ImportError as exc:
        raise HTTPException(503, "Native stream engine unavailable") from exc
    return Engine(
        config["input"],
        outputs=config.get("output", []),
        segment_duration=seg_dur,
        max_segments=max_segs,
        max_segment_bytes=4 * 1024 * 1024,
        max_hls_bytes=10 * 1024 * 1024,
        stream_options=config,
    )


_DEFAULT_ADMIN_HASH = hash_password("admin")


def default_admin():
    return {
        "type": 1,
        "role": "admin",
        "enable": True,
        "password": "admin",
        "created": int(time.time()),
    }


def defaults():
    return {
        "gid": 466560,
        "make_stream": [],
        "users": {"admin": default_admin()},
        "categories": [],
        "servers": [],
        "softcam": [],
        "cas": [],
        "dvb_tune": [],
        "settings": {},
        "hls": {},
        "auth": {"enable": False},
    }


def validate_config(config):
    if not isinstance(config, dict):
        raise HTTPException(422, "config must be an object")
    result = defaults()
    result.update(copy.deepcopy(config))
    if not isinstance(result["make_stream"], list):
        raise HTTPException(422, "make_stream must be a list")
    seen = set()
    try:
        streams = []
        for item in result["make_stream"]:
            entry = validate_stream_entry(item)
            if not entry["id"] or entry["id"] in seen:
                raise ValueError("stream IDs must be nonempty and unique")
            seen.add(entry["id"])
            streams.append(entry)
        result["make_stream"] = streams
    except (ValidationError, ValueError) as exc:
        raise HTTPException(422, str(exc)) from exc
    for name in ("settings", "auth", "users", "hls"):
        if not isinstance(result[name], dict):
            raise HTTPException(422, f"{name} must be an object")
    for name in ("categories", "servers", "softcam", "cas", "dvb_tune"):
        if not isinstance(result[name], list) or any(not isinstance(v, dict) for v in result[name]):
            raise HTTPException(422, f"{name} must be a list of objects")
    if any(not isinstance(u, dict) for u in result["users"].values()):
        raise HTTPException(422, "users must contain objects")
    return result


def _format_config_payload(config):
    if STATE_FILE.suffix == ".conf":
        ordered = {
            "port": int(config.get("port") or config.get("control_port") or PORT),
            "host": str(config.get("host") or HOST),
            "gid": config.get("gid", 466560),
            "auth": config.get("auth", {"enable": False}),
            "users": config.get("users", {}),
            "make_stream": config.get("make_stream", []),
            "categories": config.get("categories", []),
            "servers": config.get("servers", []),
            "softcam": config.get("softcam", []),
            "cas": config.get("cas", []),
            "dvb_tune": config.get("dvb_tune", []),
            "settings": config.get("settings", {}),
            "hls": config.get("hls", {}),
        }
        for k, v in config.items():
            if k not in ordered and k != "control_port":
                ordered[k] = v
        return ordered
    return {"config": config}


def atomic_save(config):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = _format_config_payload(config)
    name = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=STATE_FILE.parent,
                                         prefix=".astra-", suffix=".tmp", delete=False) as f:
            name = f.name
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        try:
            os.chmod(name, 0o640)
        except OSError:
            pass
        os.replace(name, STATE_FILE)
    except PermissionError:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


def disk_config():
    global PORT, HOST
    config = defaults()
    options = read_options()
    if "AUTH_ENABLE" in options:
        config["auth"]["enable"] = options["AUTH_ENABLE"].lower() in ("true", "1", "yes")
    if STATE_FILE.exists():
        try:
            raw_text = STATE_FILE.read_text(encoding="utf-8-sig").strip()
            data = json.loads(raw_text) if raw_text else {}
        except (OSError, ValueError) as exc:
            raise HTTPException(500, "Cannot read state file") from exc
        if not isinstance(data, dict) or not isinstance(data.get("config", data), dict):
            raise HTTPException(422, "State file must contain a configuration object")
        loaded = data.get("config") if isinstance(data.get("config"), dict) else data
        config.update(loaded)
        if not config.get("users"):
            config["users"] = {"admin": default_admin()}
        # Legacy streams are migrated only if authoritative make_stream is absent.
        if "make_stream" not in loaded and isinstance(data.get("streams"), dict):
            config["make_stream"] = [dict(v, id=k) for k, v in data["streams"].items()]
        # If user manually added a stream in .conf without an id, generate a deterministic id.
        if STATE_FILE.suffix == ".conf" and isinstance(config.get("make_stream"), list):
            for idx, item in enumerate(config["make_stream"]):
                if isinstance(item, dict) and not item.get("id"):
                    seed = f"{item.get('name', '')}-{idx}-{item.get('input', '')}"
                    item["id"] = f"a{abs(hash(seed)) % 100000:05d}"
    enabled = os.environ.get("ASTRA_AUTH_ENABLE")
    if enabled is not None:
        config["auth"] = dict(config.get("auth", {}), enable=enabled.lower() in ("true", "1", "yes"))
    cfg_port = config.get("port")
    if isinstance(cfg_port, int) and not isinstance(cfg_port, bool) and 1 <= cfg_port <= 65535:
        PORT = cfg_port
    elif isinstance(cfg_port, str) and cfg_port.isdigit() and 1 <= int(cfg_port) <= 65535:
        PORT = int(cfg_port)
    config["port"] = PORT
    config["control_port"] = PORT
    if isinstance(config.get("host"), str) and config["host"].strip():
        HOST = config["host"].strip()
    else:
        config["host"] = HOST
    return validate_config(config)


def redact(value, strip_urls=False):
    if isinstance(value, dict):
        return {k: redact(v, strip_urls=strip_urls) for k, v in value.items()
                if not any(s in str(k).lower() for s in ("password", "token", "secret", "passwd"))
                and (not strip_urls or str(k).lower() not in ("pass", "auth"))}
    if isinstance(value, list):
        return [redact(v, strip_urls=strip_urls) for v in value]
    if strip_urls and isinstance(value, str) and "://" in value:
        parts = urlsplit(value)
        if parts.username is not None or parts.fragment or parts.query:
            return parts._replace(netloc=parts.netloc.rsplit("@", 1)[-1], query="", fragment="").geturl()
    return value


class ServerState:
    def __init__(self):
        self.config = defaults()
        self.engines = {}
        self.sessions = {}
        self.scans = {}
        self.websocket_clients = {}
        self.auth_limiter = FailedAuthLimiter()
        self.next_session = 1
        self.started = time.time()
        self.version = "AstraG 2.0"
        self.commit = os.environ.get("ASTRA_COMMIT", "astrag")
        self.logs = []
        self.lock = asyncio.Lock()
        self.extra_servers = {}
        self.cluster_streams = {}
        self.cluster_aliases = {}
        self.cluster_ws_clients = {}
        self.cluster_cache = {}

    def add_log(self, level: str, text: str):
        entry = {"type": level, "time": int(time.time()), "text": text}
        self.logs.append(entry)
        if len(self.logs) > 500:
            del self.logs[:len(self.logs) - 500]
        return entry

    @property
    def stream_configs(self):
        return {s["id"]: s for s in self.config["make_stream"]}

    @property
    def all_stream_configs(self):
        merged = dict(self.cluster_streams)
        for s in self.config["make_stream"]:
            merged[s["id"]] = s
        return merged

    def resolve_stream_id(self, stream_id: str, host_hint: str = None):
        if not stream_id:
            return None
        local_map = self.stream_configs
        if not host_hint and stream_id in local_map and stream_id in self.engines:
            return stream_id
        if host_hint and (host_hint, stream_id) in self.cluster_aliases:
            return self.cluster_aliases[(host_hint, stream_id)]
        if stream_id in self.cluster_streams and stream_id in self.engines:
            return stream_id
        if stream_id in self.cluster_aliases:
            return self.cluster_aliases[stream_id]
        if not host_hint and stream_id in local_map:
            return stream_id
        if stream_id.endswith(".ts"):
            return self.resolve_stream_id(stream_id[:-3], host_hint)
        return None

    def close_session(self, sid):
        session = self.sessions.pop(sid, None)
        if session:
            if session.get("core_id") is not None:
                if CORE is not None:
                    CORE.sess_map.pop(session["core_id"], None)
                    CORE.send("KILL", session["core_id"])
                return True
            q = session["queue"]
            while not q.empty():
                q.get_nowait()
            q.put_nowait(None)
        return session is not None

    async def stop_scans(self):
        for scan_id, task in list(self.scans.items()):
            task.cancel()
        if self.scans:
            await asyncio.gather(*self.scans.values(), return_exceptions=True)
        self.scans.clear()

    async def stop_engines(self):
        for sid in list(self.sessions):
            self.close_session(sid)
        old, self.engines = self.engines, {}
        results = await asyncio.gather(*(maybe_await(e.stop()) for e in old.values()), return_exceptions=True)
        if any(isinstance(r, BaseException) for r in results):
            raise RuntimeError("an engine failed to stop")

    async def build(self, config):
        engines = {}
        try:
            for s in config["make_stream"]:
                if s.get("enable", True) and s.get("active", True) and s["input"]:
                    engine = make_engine(s)
                    engines[s["id"]] = engine
                    await maybe_await(engine.start())
        except BaseException:
            await asyncio.gather(*(maybe_await(e.stop()) for e in engines.values()), return_exceptions=True)
            raise
        return engines

    def desired_extra_ports(self, config):
        ports = set()
        settings = config.get("settings", {})
        for key in ("control_port", "http_play_port"):
            val = settings.get(key)
            if val not in (None, ""):
                try:
                    p = int(val)
                    if 1 <= p <= 65535 and p != PORT:
                        ports.add(p)
                except (ValueError, TypeError):
                    pass
        for s in config.get("make_stream", []):
            if not s.get("enable", True):
                continue
            for out_url in s.get("output", []):
                parts = urlsplit(out_url.split("#", 1)[0])
                if parts.scheme in ("http", "https") and parts.port:
                    if 1 <= parts.port <= 65535 and parts.port != PORT:
                        ports.add(parts.port)
        return ports

    async def sync_extra_ports(self, config):
        if os.environ.get("ASTRA_DISABLE_EXTRA_PORTS") == "1":
            return
        wanted = self.desired_extra_ports(config)
        if CORE is not None:
            # Extra public ports are opened by astrag-core and relayed to the same internal server.
            for p in sorted(CORE.listening - wanted):
                CORE.send("UNLISTEN", p)
                CORE.listening.discard(p)
            for p in sorted(wanted - CORE.listening):
                CORE.send("LISTEN", HOST, p)
                CORE.listening.add(p)
            return
        for p in list(self.extra_servers):
            if p not in wanted:
                srv, task = self.extra_servers.pop(p)
                srv.should_exit = True
                try:
                    await asyncio.wait_for(task, timeout=1.0)
                except (asyncio.TimeoutError, Exception):
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        for p in sorted(wanted):
            if p in self.extra_servers:
                continue
            try:
                import uvicorn
                uconfig = uvicorn.Config(
                    app,
                    host=HOST,
                    port=p,
                    log_level="warning",
                    access_log=False,
                    loop="asyncio",
                    lifespan="off",
                    ws_ping_interval=None,
                    ws_ping_timeout=None,
                )
                srv = uvicorn.Server(uconfig)
                srv.install_signal_handlers = lambda: None
                task = asyncio.create_task(srv.serve())
                self.extra_servers[p] = (srv, task)
                logger.info("Started additional public HTTP/IPTV listener on %s:%d", HOST, p)
            except Exception:
                logger.warning("Could not start additional listener on port %d", p, exc_info=True)

    async def stop_extra_ports(self):
        for p, (srv, task) in list(self.extra_servers.items()):
            srv.should_exit = True
            try:
                await asyncio.wait_for(task, timeout=1.0)
            except (asyncio.TimeoutError, Exception):
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.extra_servers.clear()

    async def _sync_engines_incremental(self, old_config, new_config):
        old_map = {s["id"]: s for s in old_config.get("make_stream", [])}
        new_map = {s["id"]: s for s in new_config.get("make_stream", [])}
        to_stop = [
            sid for sid in list(self.engines)
            if sid not in self.cluster_streams and (sid not in new_map or new_map[sid] != old_map.get(sid))
        ]
        for sess_id, sess in list(self.sessions.items()):
            if sess.get("channel_id") in to_stop:
                self.close_session(sess_id)
        stopped_engines = [self.engines.pop(sid) for sid in to_stop if sid in self.engines]
        if stopped_engines:
            results = await asyncio.gather(*(maybe_await(e.stop()) for e in stopped_engines), return_exceptions=True)
            if any(isinstance(r, BaseException) for r in results):
                raise RuntimeError("an engine failed to stop")
        started_sids = []
        try:
            for sid, s in new_map.items():
                if sid in self.engines:
                    continue
                if s.get("enable", True) and s.get("active", True) and s["input"]:
                    engine = make_engine(s)
                    self.engines[sid] = engine
                    started_sids.append(sid)
                    await maybe_await(engine.start())
        except BaseException:
            failed = [self.engines.pop(sid) for sid in started_sids if sid in self.engines]
            await asyncio.gather(*(maybe_await(e.stop()) for e in failed), return_exceptions=True)
            raise

    async def apply(self, config, persist=True, rebuild=True):
        candidate = validate_config(config)
        old = self.config
        changed = candidate["make_stream"] != old["make_stream"] or rebuild
        servers_changed = candidate.get("servers") != old.get("servers")
        try:
            if changed:
                if rebuild or not self.engines:
                    await self.stop_engines()
                    self.engines = await self.build(candidate)
                else:
                    await self._sync_engines_incremental(old, candidate)
            if persist:
                atomic_save(candidate)
        except Exception as exc:
            if changed:
                await self.stop_engines()
                self.engines = await self.build(old)
            if isinstance(exc, HTTPException):
                raise
            logger.exception("Configuration transaction failed")
            raise HTTPException(500, "Configuration transaction failed; previous configuration retained") from exc
        self.config = candidate
        await self.sync_extra_ports(candidate)
        if servers_changed or rebuild:
            await _sync_cluster_servers()
        else:
            asyncio.create_task(_sync_cluster_servers())

    async def broadcast(self, scope, payload):
        admin_message = {"scope": scope, **redact(payload, strip_urls=False)}
        observer_message = None
        for ws, user in list(self.websocket_clients.items()):
            if self.config.get("auth", {}).get("enable") or user.get("login"):
                login = user.get("login")
                if login:
                    current = self.config["users"].get(login)
                    if not current or current.get("enable") is False:
                        self.websocket_clients.pop(ws, None)
                        await ws.close(code=1008)
                        continue
            is_admin = user.get("admin", True)
            if is_admin:
                msg = admin_message
            else:
                if observer_message is None:
                    observer_message = {"scope": scope, **redact(payload, strip_urls=True)}
                msg = observer_message
            try:
                await ws.send_json(msg)
            except Exception:
                self.websocket_clients.pop(ws, None)


async def _broadcast_cluster_ws(host_port: str, scope: str, payload: dict):
    clients = state.cluster_ws_clients.get(host_port)
    if not clients:
        return
    msg = {"scope": scope, **payload}
    for ws in list(clients):
        try:
            await ws.send_json(msg)
        except Exception:
            clients.discard(ws)


async def publish_stats():
    tick = 0
    prev_errors = {}
    was_active = set()
    while True:
        await asyncio.sleep(1)
        tick += 1
        if tick % 60 == 0:
            try:
                from stream_engine import trim_process_memory
                trim_process_memory()
            except Exception:
                pass
        has_cluster_ws = any(bool(v) for v in state.cluster_ws_clients.values())
        if not state.websocket_clients and not has_cluster_ws:
            continue
        if tick % 10 == 0:
            for ws in list(state.websocket_clients):
                try:
                    await ws.send_json({"scope": "ping"})
                except Exception:
                    state.websocket_clients.pop(ws, None)
            for c_set in list(state.cluster_ws_clients.values()):
                for ws in list(c_set):
                    try:
                        await ws.send_json({"scope": "ping"})
                    except Exception:
                        c_set.discard(ws)
        active_sids = set(state.engines)
        for dead_sid in list(prev_errors):
            if dead_sid not in active_sids:
                prev_errors.pop(dead_sid, None)
                was_active.discard(dead_sid)
        for sid, engine in list(state.engines.items()):
            try:
                stats_fn = getattr(engine, "stats", {})
                if callable(stats_fn):
                    try:
                        stats = await maybe_await(stats_fn(include_pids=False))
                    except TypeError:
                        stats = await maybe_await(stats_fn())
                else:
                    stats = stats_fn
                if not isinstance(stats, dict):
                    continue
                stats = dict(stats)
                stats.pop("pids", None)
                input_id = int(stats.get("input_id", 1))
                c_cfg = state.cluster_streams.get(sid)
                c_host = c_cfg.get("_cluster_host") if c_cfg else None
                c_orig_id = c_cfg.get("_cluster_orig_id", sid) if c_cfg else sid
                if stats.get("idle"):
                    if sid in was_active:
                        was_active.discard(sid)
                        idle_ev = {
                            "channel_id": sid, "input_id": input_id, "idle": True,
                            "onair": False, "bitrate": 0, "sessions": 0,
                        }
                        await state.broadcast("stream_event", idle_ev)
                        if c_host:
                            await _broadcast_cluster_ws(c_host, "stream_event", {**idle_ev, "channel_id": c_orig_id})
                    continue
                raw_br = stats.get("effective_bitrate", stats.get("bitrate", 0)) or 0
                stats["bitrate"] = int(round(raw_br / 1000))
                stats["onair"] = bool(stats.get("running") and stats["bitrate"] > 0)
                if not stats["onair"] and int(stats.get("input_errors", 0) or 0) == 0 and sid not in was_active:
                    # Still connecting right after on-demand wake-up; avoid flashing red error before first packet
                    continue
                was_active.add(sid)
                cur_cc = int(stats.get("cc_errors", stats.get("continuity_errors", 0)) or 0)
                cur_pes = int(stats.get("pes_errors", 0) or 0)
                prev_cc, prev_pes = prev_errors.get(sid, (0, 0))
                prev_errors[sid] = (cur_cc, cur_pes)
                stats["cc_error"] = cur_cc - prev_cc if cur_cc >= prev_cc else cur_cc
                stats["pes_error"] = cur_pes - prev_pes if cur_pes >= prev_pes else cur_pes
                stats["scrambled"] = bool(stats.get("scrambled", False))
                ev_payload = {
                    **stats, "channel_id": sid, "input_id": input_id,
                    "sessions": sum(s.get("channel_id") == sid for s in state.sessions.values()),
                }
                await state.broadcast("stream_event", ev_payload)
                if c_host:
                    await _broadcast_cluster_ws(c_host, "stream_event", {**ev_payload, "channel_id": c_orig_id})
            except Exception:
                logger.exception("Unable to publish stream statistics")


state = ServerState()


async def _run_stream_analyzer(scan_id: str, stream_id: str, cluster_host: str = None):
    """Push real-time Cesbo Astra PSI/SI and bitrate telemetry for StreamsModule.startAnalyze."""
    resolved_id = state.resolve_stream_id(stream_id, cluster_host) or stream_id
    try:
        while scan_id in state.scans:
            if CORE is not None:
                CORE.send("WAKE", resolved_id)
            engine = state.engines.get(resolved_id)
            if engine is not None and hasattr(engine, "psi_summary"):
                summary = engine.psi_summary()
                async def _emit_scan(payload):
                    await state.broadcast("scan", payload)
                    if cluster_host:
                        await _broadcast_cluster_ws(cluster_host, "scan", payload)
                if summary.get("pat"):
                    await _emit_scan({"data": summary["pat"]})
                for pmt in summary.get("pmt") or []:
                    await _emit_scan({"data": pmt})
                if summary.get("sdt"):
                    await _emit_scan({"data": summary["sdt"]})
                if summary.get("nit"):
                    await _emit_scan({"data": summary["nit"]})
                await _emit_scan({"data": {"analyze": True, "total": summary["total"]}})
            else:
                empty_scan = {
                    "data": {
                        "analyze": True,
                        "total": {"bitrate": 0, "cc_errors": 0, "pes_errors": 0, "scrambled": False},
                    }
                }
                await state.broadcast("scan", empty_scan)
                if cluster_host:
                    await _broadcast_cluster_ws(cluster_host, "scan", empty_scan)
            await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        pass
    finally:
        state.scans.pop(scan_id, None)


def identity(credentials, require_credentials=False):
    auth_enabled = bool(state.config.get("auth", {}).get("enable"))
    if not auth_enabled and not require_credentials and not credentials:
        return {"login": "", "admin": True}
    if not isinstance(credentials, str) or ":" not in credentials:
        raise HTTPException(403, "Forbidden")
    login, password = credentials.split(":", 1)
    user = state.config.get("users", {}).get(login, {})
    expected = user.get("password") or user.get("token")
    good = verify_password(password, expected)
    if not good or user.get("enable") is False:
        raise HTTPException(403, "Forbidden")
    exp = user.get("expire")
    if isinstance(exp, (int, float)) and exp > 0 and time.time() > exp:
        raise HTTPException(403, "Forbidden")
    return {"login": login, "admin": user.get("role") == "admin" or user.get("type") in (1, "1")}


def extract_basic_credentials(request):
    header = request.headers.get("authorization", "")
    if header.lower().startswith("basic "):
        try:
            return base64.b64decode(header[6:], validate=True).decode("utf-8")
        except (ValueError, UnicodeError):
            return None
    return None


def require_auth(request):
    credentials = extract_basic_credentials(request)
    ua = request.headers.get("user-agent", "").lower()
    is_browser = "mozilla/" in ua or "sec-fetch-mode" in request.headers
    ui_auth_required = request.headers.get("x-astra-ui-auth") == "required" or is_browser
    if ui_auth_required and not credentials:
        raise HTTPException(403, "Forbidden")
    return checked_identity(
        credentials,
        request.client.host if request.client else "unknown",
        require_credentials=ui_auth_required,
    )


def checked_identity(credentials, host, require_credentials=False):
    auth_enabled = bool(state.config.get("auth", {}).get("enable"))
    if (auth_enabled or credentials is not None) and state.auth_limiter.blocked(host):
        raise HTTPException(429, "Too many failed authentication attempts", headers={"Retry-After": "60"})
    try:
        return identity(credentials, require_credentials=require_credentials)
    except HTTPException:
        if auth_enabled or credentials is not None:
            state.auth_limiter.failed(host)
        raise


def _ip_matches(host, rules):
    if not host or not isinstance(rules, list):
        return False
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return host in rules
    for rule in rules:
        if not isinstance(rule, str) or not rule.strip():
            continue
        rule = rule.strip()
        try:
            if "/" in rule:
                if addr in ipaddress.ip_network(rule, strict=False):
                    return True
            elif addr == ipaddress.ip_address(rule):
                return True
        except ValueError:
            if host == rule:
                return True
    return False


def require_media_auth(request):
    auth_cfg = state.config.get("auth", {})
    host = request.client.host if request.client else "unknown"
    if auth_cfg.get("x_real_ip"):
        forwarded = request.headers.get("x-real-ip") or request.headers.get("x-forwarded-for", "").split(",")[0].strip()
        if forwarded:
            host = forwarded
    if _ip_matches(host, auth_cfg.get("http_ip_deny", [])):
        raise HTTPException(403, "IP address denied")
    token = request.query_params.get("token") or request.query_params.get("auth")
    if token and token in (auth_cfg.get("http_token_deny") or []):
        raise HTTPException(403, "Token denied")
    if token:
        if ":" in token:
            try:
                return checked_identity(token, host, require_credentials=True)
            except HTTPException:
                if auth_cfg.get("enable"):
                    raise
        for login, u in state.config.get("users", {}).items():
            if u.get("enable") is False:
                continue
            if (u.get("token") and u.get("token") == token) or verify_password(token, u.get("password")):
                return {"login": login, "admin": u.get("role") == "admin" or u.get("type") in (1, "1")}
        if auth_cfg.get("enable"):
            raise HTTPException(403, "Forbidden")
    if not auth_cfg.get("enable"):
        credentials = extract_basic_credentials(request)
        if credentials:
            try:
                return identity(credentials, require_credentials=True)
            except HTTPException:
                pass
        return {"login": "", "admin": False}
    if _ip_matches(host, auth_cfg.get("http_ip_allow", [])):
        return {"login": "", "admin": False}
    return require_auth(request)


def admin(user):
    if not user["admin"]:
        raise HTTPException(403, "Administrator role required")


async def reload_config():
    async with state.lock:
        await state.apply(disk_config(), persist=False)



def _rewrite_remote_url_host(url_str: str, remote_host: str) -> str:
    base, *frag = url_str.split("#", 1)
    try:
        sp = urlsplit(base.strip())
    except Exception:
        return url_str
    if sp.scheme.lower() not in ("http", "https"):
        return ""
    host = sp.hostname or ""
    if host in ("0", "0.0.0.0", "127.0.0.1", "localhost"):
        netloc = remote_host
        if sp.port:
            netloc = f"{remote_host}:{sp.port}"
        if sp.username:
            auth_part = sp.username + (f":{sp.password}" if sp.password else "")
            netloc = f"{auth_part}@{netloc}"
        base = urlunsplit((sp.scheme, netloc, sp.path, sp.query, ""))
    return base + (f"#{frag[0]}" if frag else "")


def _build_cluster_stream_inputs(srv: dict, remote_cfg: dict, rs: dict) -> list:
    rhost = str(srv.get("host") or "").strip()
    rport = str(srv.get("port") or 8000).strip()
    ruser = str(srv.get("user") or "").strip()
    rpass = str(srv.get("pass") or "").strip()
    orig_id = str(rs.get("id") or "").strip()
    if not rhost or not orig_id:
        return []
    if str(rs.get("type") or "spts").lower() == "mpts" and not rs.get("output"):
        return []
    raw_inputs = [u for u in (rs.get("input") or []) if isinstance(u, str) and u.strip()]
    if raw_inputs and all(u.strip().lower().startswith("exec://") for u in raw_inputs):
        return []

    rsettings = remote_cfg.get("settings") if isinstance(remote_cfg.get("settings"), dict) else {}
    rauth = remote_cfg.get("auth") if isinstance(remote_cfg.get("auth"), dict) else {}
    rusers = remote_cfg.get("users") if isinstance(remote_cfg.get("users"), dict) else {}

    play_port = rsettings.get("http_play_port") or rport
    play_url = f"http://{rhost}:{play_port}/play/{orig_id}"
    if rauth.get("enable"):
        if ruser and isinstance(rusers.get(ruser), dict) and rusers[ruser].get("token"):
            play_url += f"?token={rusers[ruser]['token']}"
        elif ruser and rpass:
            play_url += f"?auth={ruser}:{rpass}"
        else:
            for uinfo in rusers.values():
                if isinstance(uinfo, dict) and uinfo.get("enable", True) is not False and uinfo.get("token"):
                    play_url += f"?token={uinfo['token']}"
                    break

    out_urls = []
    for u in rs.get("output") or []:
        if isinstance(u, str):
            rw = _rewrite_remote_url_host(u, rhost)
            if rw and rw not in out_urls:
                out_urls.append(rw)

    in_urls = []
    for u in rs.get("input") or []:
        if isinstance(u, str):
            rw = _rewrite_remote_url_host(u, rhost)
            if rw and rw not in in_urls:
                in_urls.append(rw)

    candidates = []
    if rsettings.get("http_play_stream") or rsettings.get("http_play_hls"):
        for u in [play_url, *out_urls, *in_urls]:
            if u and u not in candidates:
                candidates.append(u)
    else:
        for u in [*out_urls, play_url, *in_urls]:
            if u and u not in candidates:
                candidates.append(u)
    return candidates


def _fetch_remote_astra_sync(host: str, port, user: str, password: str, payload: dict, auth_header: str = None, timeout: float = 6.0):
    import urllib.request
    url = f"http://{host}:{port}/control/"
    headers = {"Content-Type": "application/json"}
    if auth_header:
        headers["Authorization"] = auth_header
    elif user:
        cred = f"{user}:{password}" if password else user
        headers["Authorization"] = "Basic " + base64.b64encode(cred.encode("utf-8")).decode("ascii")
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", "replace")
        return resp.status, json.loads(raw) if raw.strip() else {}


async def _sync_cluster_servers():
    servers = list(state.config.get("servers") or [])
    local_ids = {s["id"] for s in state.config.get("make_stream", [])}
    new_cluster_streams = {}
    new_aliases = {}

    for idx, srv in enumerate(servers):
        if not isinstance(srv, dict):
            continue
        srv_en = srv.get("enable", True)
        if srv_en is False or str(srv_en).lower() == "false":
            continue
        rhost = str(srv.get("host") or "").strip()
        rport = str(srv.get("port") or 8000).strip()
        if not rhost:
            continue
        ruser = str(srv.get("user") or "").strip()
        rpass = str(srv.get("pass") or "").strip()
        srv_name = str(srv.get("name") or f"{rhost}:{rport}").strip()
        host_port = f"{rhost}:{rport}"
        remote_cfg = None
        try:
            status_code, fetched_cfg = await asyncio.to_thread(
                _fetch_remote_astra_sync, rhost, rport, ruser, rpass, {"cmd": "load"}, None, 6.0
            )
            if status_code == 200 and isinstance(fetched_cfg, dict):
                remote_cfg = fetched_cfg
                state.cluster_cache[host_port] = fetched_cfg
        except Exception:
            pass
        if remote_cfg is None:
            remote_cfg = state.cluster_cache.get(host_port)
        if not isinstance(remote_cfg, dict):
            continue

        rsettings = remote_cfg.get("settings") if isinstance(remote_cfg.get("settings"), dict) else {}
        has_http_play = bool(rsettings.get("http_play_stream") or rsettings.get("http_play_hls"))

        for rs in remote_cfg.get("make_stream") or []:
            if not isinstance(rs, dict):
                continue
            rs_en = rs.get("enable", True)
            if rs_en is False or str(rs_en).lower() == "false":
                continue
            orig_id = str(rs.get("id") or "").strip()
            if not orig_id:
                continue
            proxy_inputs = _build_cluster_stream_inputs(srv, remote_cfg, rs)
            if not proxy_inputs:
                continue
            prefixed_id = f"c{idx + 1}_{orig_id}"
            if has_http_play and orig_id not in local_ids and orig_id not in new_cluster_streams:
                eff_id = orig_id
            else:
                eff_id = prefixed_id
            groups = dict(rs.get("groups") or {}) if isinstance(rs.get("groups"), dict) else {}
            if not groups and srv_name:
                groups = {"Cluster": srv_name}
            proxy_cfg = {
                "id": eff_id,
                "name": rs.get("name") or orig_id,
                "type": rs.get("type") or "spts",
                "enable": True,
                "active": True,
                "input": proxy_inputs,
                "output": [u for u in (rs.get("output") or []) if isinstance(u, str) and urlsplit(u.split("#", 1)[0].strip()).scheme.lower() in ("http", "https")],
                "groups": groups,
                "http_keep_active": 0,
                "_cluster_host": host_port,
                "_cluster_orig_id": orig_id,
                "_cluster_srv_idx": idx + 1,
                "_cluster_srv_name": srv_name,
                "_cluster_enabled": True,
            }
            new_cluster_streams[eff_id] = proxy_cfg
            new_aliases[(host_port, orig_id)] = eff_id
            new_aliases[(srv_name, orig_id)] = eff_id
            new_aliases[(str(idx + 1), orig_id)] = eff_id
            new_aliases[prefixed_id] = eff_id
            if has_http_play and orig_id not in new_aliases:
                new_aliases[orig_id] = eff_id

    old_cluster_ids = set(state.cluster_streams.keys())
    new_cluster_ids = set(new_cluster_streams.keys())
    state.cluster_streams = new_cluster_streams
    state.cluster_aliases = new_aliases

    for gone_id in old_cluster_ids - new_cluster_ids:
        if gone_id not in local_ids and gone_id in state.engines:
            for sess_id, sess in list(state.sessions.items()):
                if sess.get("channel_id") == gone_id:
                    state.close_session(sess_id)
            eng = state.engines.pop(gone_id, None)
            if eng is not None:
                try:
                    await maybe_await(eng.stop())
                except Exception:
                    pass

    for eff_id, pcfg in new_cluster_streams.items():
        if eff_id in local_ids:
            continue
        existing = state.engines.get(eff_id)
        if existing is None or getattr(existing, "config", {}).get("input") != pcfg["input"]:
            if existing is not None:
                try:
                    await maybe_await(existing.stop())
                except Exception:
                    pass
            eng = make_engine(pcfg)
            state.engines[eff_id] = eng
            await maybe_await(eng.start())


async def _cluster_sync_loop():
    while True:
        try:
            await _sync_cluster_servers()
        except Exception:
            logger.debug("Cluster sync error", exc_info=True)
        await asyncio.sleep(45.0)


@asynccontextmanager
async def lifespan(app):
    state.lock = asyncio.Lock()
    if CORE is not None:
        await CORE.connect()
    await reload_config()
    state.add_log("info", f"{state.version} started on {HOST}:{PORT}")
    loop = asyncio.get_running_loop()
    installed = False
    if hasattr(signal, "SIGHUP") and os.name != "nt":
        try:
            loop.add_signal_handler(signal.SIGHUP, lambda: asyncio.create_task(reload_config()))
            installed = True
        except (NotImplementedError, RuntimeError):
            pass
    stats_task = asyncio.create_task(publish_stats())
    cluster_task = asyncio.create_task(_cluster_sync_loop())
    try:
        yield
    finally:
        cluster_task.cancel()
        stats_task.cancel()
        await asyncio.gather(cluster_task, stats_task, return_exceptions=True)
        if installed:
            loop.remove_signal_handler(signal.SIGHUP)
        await state.stop_scans()
        await state.stop_extra_ports()
        await state.stop_engines()
        for ws in list(state.websocket_clients):
            await ws.close()
        state.websocket_clients.clear()


if "lifespan" in inspect.signature(FastAPI.__init__).parameters:
    app = FastAPI(title="AstraG Media Server", version="2.0", lifespan=lifespan)
else:
    app = FastAPI(title="AstraG Media Server", version="2.0")
    _lifespan_ctx = None

    @app.on_event("startup")
    async def _legacy_startup():
        global _lifespan_ctx
        _lifespan_ctx = lifespan(app)
        await _lifespan_ctx.__aenter__()

    @app.on_event("shutdown")
    async def _legacy_shutdown():
        global _lifespan_ctx
        if _lifespan_ctx is not None:
            await _lifespan_ctx.__aexit__(None, None, None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/__health")
async def health_check():
    return {"status": "ok", "version": state.version}


async def json_body(request):
    try:
        body = await request.json()
    except (ValueError, UnicodeError) as exc:
        raise HTTPException(400, "Invalid JSON") from exc
    if not isinstance(body, dict):
        raise HTTPException(422, "Request must be an object")
    return body


def object_payload(body, name):
    data = body.get(name)
    if not isinstance(data, dict):
        raise HTTPException(422, f"{name} must be an object")
    return copy.deepcopy(data)


def session_list():
    return [dict({k: v for k, v in s.items() if k not in ("queue", "engine", "started")},
                 uptime=int(time.time() - s.get("started", time.time()))) for s in state.sessions.values()]


@app.post("/control/")
async def control_api(request: Request):
    user = require_auth(request)
    body = await json_body(request)
    cmd = body.get("cmd")
    if not isinstance(cmd, str):
        raise HTTPException(422, "cmd must be a string")
    if cmd == "version":
        return {"version": state.version, "commit": state.commit}
    if cmd == "status":
        return {"version": state.version, "commit": state.commit, "uptime": int(time.time() - state.started),
                "observer": not user["admin"], "dvb_list": [], "net_list": [],
                "sessions_count": len(state.sessions), "system": {"cpu_frequency_max": 0, "cpu_frequency_cur": 0}}
    if cmd == "load":
        return copy.deepcopy(state.config) if user["admin"] else redact(state.config, strip_urls=True)
    if cmd == "sessions":
        return {"sessions": session_list()}
    if cmd == "log":
        return {"log": list(state.logs)}
    admin(user)
    if cmd == "scan-init":
        scan_target = body.get("scan")
        if isinstance(scan_target, str) and scan_target.startswith("stream://"):
            stream_id = scan_target[len("stream://"):]
            scan_id = uuid.uuid4().hex[:10]
            state.scans[scan_id] = asyncio.create_task(_run_stream_analyzer(scan_id, stream_id))
            return {"scan-init": "ok", "id": scan_id}
        raise HTTPException(501, "DVB hardware operations are not implemented")
    if cmd == "scan-kill":
        scan_id = body.get("id")
        task = state.scans.pop(scan_id, None)
        if task:
            task.cancel()
        return {"scan-kill": "ok", "success": True}
    if cmd.startswith("scan-") or cmd in ("restart-adapter",):
        raise HTTPException(501, "DVB hardware operations are not implemented")
    if cmd == "close-session":
        sid = body.get("id")
        if isinstance(sid, bool) or not isinstance(sid, int):
            raise HTTPException(422, "id must be an integer")
        if not state.close_session(sid):
            raise HTTPException(404, "Session not found")
        return {"success": True}
    sockets_to_close = []
    async with state.lock:
        if cmd == "restart":
            await state.apply(disk_config(), persist=False)
            state.add_log("info", "Configuration and streams reloaded")
            return {"success": True, "message": "Configuration and engines reloaded; process not restarted"}
        config = copy.deepcopy(state.config)
        event = {}
        result = {"success": True}
        if cmd == "upload":
            uploaded = object_payload(body, "config")
            if isinstance(uploaded.get("users"), dict):
                for ulogin, udata in uploaded["users"].items():
                    if isinstance(udata, dict):
                        pw = udata.get("password")
                        if isinstance(pw, str) and pw:
                            udata["password"] = pw
                        elif not pw and ulogin in config.get("users", {}):
                            if "password" in config["users"][ulogin]:
                                udata["password"] = config["users"][ulogin]["password"]
            config = uploaded
            event = {"reload": True}
        elif cmd == "set-stream":
            data = object_payload(body, "stream")
            req_id = body.get("id")
            is_remove = bool(data.get("remove", body.get("remove", False)))
            if is_remove:
                sid = req_id if req_id not in (None, "", "-") else data.get("id")
            else:
                sid = data.get("id") if data.get("id") not in (None, "", "-") else req_id
            if sid in (None, "", "-"):
                sid = uuid.uuid4().hex[:12]
            if not isinstance(sid, str):
                raise HTTPException(422, "id must be a string")
            old_sid = req_id if isinstance(req_id, str) and req_id not in ("", "-") else sid
            data["id"] = sid
            config["make_stream"] = [s for s in config["make_stream"] if s["id"] not in (sid, old_sid)]
            if not is_remove:
                config["make_stream"].append(data)
                state.add_log("info", f"Stream '{data.get('name', sid)}' ({sid}) saved")
            else:
                data["remove"] = True
                state.add_log("warning", f"Stream '{sid}' removed")
            event = {"id": old_sid, "stream": data}
            result["id"] = sid
        elif cmd == "set-stream-input":
            sid = body.get("id")
            stream = next((s for s in config["make_stream"] if s["id"] == sid), None)
            if stream is None:
                raise HTTPException(404, "Stream not found")
            # Dashboard sends only id: rotate the ordered failover inputs.
            if "input" in body:
                stream["input"] = body["input"]
            elif len(stream["input"]) > 1:
                stream["input"] = stream["input"][1:] + stream["input"][:1]
            else:
                raise HTTPException(409, "No alternate input configured")
            event = {"channel_id": sid, "input_id": 1}
        elif cmd == "set-user":
            data = object_payload(body, "user")
            sid = body.get("id")
            if not isinstance(sid, str) or not sid.strip():
                raise HTTPException(422, "id must be a nonempty login")
            sid = sid.strip()
            old_id = body.get("old_id")
            if old_id is not None:
                if not isinstance(old_id, str) or not old_id.strip():
                    raise HTTPException(422, "old_id must be a nonempty login")
                old_id = old_id.strip()
            target_existing = old_id if (old_id and old_id in config["users"]) else sid
            existing = copy.deepcopy(config["users"].get(target_existing, {}))
            if data.get("remove", body.get("remove", False)):
                config["users"].pop(sid, None)
                if old_id and old_id != sid:
                    config["users"].pop(old_id, None)
                    sockets_to_close.extend(revoke_account_sockets(old_id))
                sockets_to_close.extend(revoke_account_sockets(sid))
                data["remove"] = True
            else:
                if old_id and old_id != sid and sid in config["users"]:
                    raise HTTPException(409, "Login is already in use")
                if old_id and old_id != sid:
                    config["users"].pop(old_id, None)
                    sockets_to_close.extend(revoke_account_sockets(old_id))
                merged = dict(existing)
                merged.update(data)
                raw_pw = data.get("password")
                if isinstance(raw_pw, str) and raw_pw:
                    merged["password"] = raw_pw
                    merged.pop("firstrun", None)
                elif "password" in existing:
                    merged["password"] = existing["password"]
                else:
                    merged["password"] = "admin"
                try:
                    utype = int(merged.get("type", 1 if merged.get("role") == "admin" else 3))
                except (ValueError, TypeError):
                    utype = 3
                merged["type"] = utype
                if utype == 1 or merged.get("role") == "admin":
                    merged["role"] = "admin"
                    merged["type"] = 1
                elif "role" in merged and utype != 1:
                    merged.pop("role", None)
                if "enable" not in merged:
                    merged["enable"] = True
                else:
                    merged["enable"] = bool(merged["enable"])
                if not merged.get("created"):
                    merged["created"] = int(time.time())
                config["users"][sid] = merged
                data = merged
            event = {"id": sid, "user": data}
            if old_id and old_id != sid:
                event["old_id"] = old_id
        elif cmd in ("set-settings", "set-auth"):
            key = cmd[4:]
            if key not in body and key == "auth":
                data = {"enable": False}
            else:
                data = object_payload(body, key)
            config[key] = {} if data.get("remove") else data
            event = {key: config[key]}
        elif cmd == "set-log":
            data = body.get("set")
            if data is not None and not isinstance(data, dict):
                raise HTTPException(422, "set must be an object")
            config["log"] = copy.deepcopy(data) if data else {}
            event = {"set": config["log"]}
        elif cmd == "set-config":
            key = body.get("key")
            if not isinstance(key, str) or not key:
                raise HTTPException(422, "key required")
            if "data" not in body and not body.get("remove"):
                if key in ("hls", "settings", "auth"):
                    body = dict(body, data={})
                else:
                    raise HTTPException(422, "data required")
            data = body.get("data")
            if body.get("remove") or (isinstance(data, dict) and data.get("remove")):
                config.pop(key, None)
                data = None
            else:
                config[key] = data
            event = {"key": key, "data": data}
        elif cmd in ("set-category", "set-server", "set-softcam", "set-cas", "set-adapter"):
            name = cmd[4:]
            key = {"category": "categories", "server": "servers", "adapter": "dvb_tune"}.get(name, name)
            data = object_payload(body, name)
            sid = body.get("id", data.get("id"))
            values = config[key]
            if name in ("category", "server"):
                if sid not in (None, "-"):
                    try:
                        idx = int(sid)
                    except (ValueError, TypeError):
                        raise HTTPException(422, "id must be a list index")
                    if idx < 0 or idx >= len(values):
                        raise HTTPException(404, "Entry not found")
                    if data.get("remove", body.get("remove", False)):
                        data["remove"] = True
                        values.pop(idx)
                    else:
                        values[idx] = data
                    event["id"] = idx
                elif data.get("remove"):
                    raise HTTPException(422, "Removal requires id")
                else:
                    values.append(data)
            else:
                req_id = body.get("id")
                is_remove = bool(data.get("remove", body.get("remove", False)))
                if is_remove:
                    sid = req_id if req_id not in (None, "", "-") else data.get("id")
                else:
                    sid = data.get("id") if data.get("id") not in (None, "", "-") else req_id
                sid = uuid.uuid4().hex[:12] if sid in (None, "", "-") else str(sid)
                old_sid = str(req_id) if req_id not in (None, "", "-") else sid
                data["id"] = sid
                values[:] = [v for v in values if v.get("id") not in (sid, old_sid)]
                if is_remove:
                    data["remove"] = True
                else:
                    values.append(data)
                event["id"] = old_sid
            event[name] = data
            if name in ("adapter", "softcam", "cas"):
                result["configuration_only"] = True
        else:
            raise HTTPException(400, f"Unknown command: {cmd}")
        if "gid" in body:
            if not isinstance(body["gid"], int) or isinstance(body["gid"], bool):
                raise HTTPException(422, "gid must be an integer")
            config["gid"] = body["gid"]
            event["gid"] = body["gid"]
        await state.apply(config, rebuild=bool(body.get("restart")))
        await state.broadcast("log_event" if cmd == "set-log" else ("stream_event" if cmd == "set-stream-input" else cmd), event)
    if sockets_to_close:
        await close_account_sockets(sockets_to_close)
    return result


def check_account_origin(request):
    origin = request.headers.get("origin")
    if origin is None:
        return  # Non-browser API clients do not send Origin.
    try:
        source = urlsplit(origin)
        target = urlsplit(str(request.base_url))
        source_port = source.port or (443 if source.scheme == "https" else 80)
        target_port = target.port or (443 if target.scheme == "https" else 80)
        good = (source.scheme in ("http", "https") and source.scheme == target.scheme
                and source.hostname == target.hostname and source_port == target_port
                and source.username is None and source.password is None
                and not source.path and not source.query and not source.fragment)
    except ValueError:
        good = False
    if not good:
        raise HTTPException(403, "Cross-origin account changes are forbidden")


async def close_account_sockets(sockets):
    for ws in sockets:
        try:
            await ws.close(code=1008)
        except Exception:
            logger.debug("Account websocket already closed")


def revoke_account_sockets(login):
    sockets = [ws for ws, user in list(state.websocket_clients.items()) if user["login"] == login]
    for ws in sockets:
        state.websocket_clients.pop(ws, None)
    return sockets


@app.post("/control/account/")
async def change_account(request: Request):
    check_account_origin(request)
    user = require_auth(request)
    if not state.config.get("auth", {}).get("enable") or not user["login"]:
        raise HTTPException(403, "Account authentication must be enabled")
    body = await json_body(request)
    login, password = body.get("new_login"), body.get("new_password")
    if not valid_login(login) or not valid_password(password):
        raise HTTPException(422, "Login: 1-64 ASCII letters, digits, _, ., @ or -; password: 12-256 characters with letters and digits")
    async with state.lock:
        # Recheck after acquiring the lock: another request may have changed credentials.
        user = require_auth(request)
        current = state.config["users"].get(user["login"], {})
        if not verify_password(body.get("current_password"), current.get("password") or current.get("token")):
            state.auth_limiter.failed(request.client.host if request.client else "unknown")
            raise HTTPException(403, "Current password is incorrect")
        if login != user["login"] and login in state.config["users"]:
            raise HTTPException(409, "Login is already in use")
        config = copy.deepcopy(state.config)
        account = config["users"].pop(user["login"])
        account.pop("token", None)
        account["password"] = hash_password(password)
        config["users"][login] = account
        await state.apply(config, rebuild=False)
        sockets = revoke_account_sockets(user["login"])
    # Never await websocket I/O while holding the configuration transaction lock.
    await close_account_sockets(sockets)
    return {"success": True, "login": login}


@app.post("/control/reset-password/")
async def reset_password(request: Request):
    admin(require_auth(request))
    body = await json_body(request)
    login, password = body.get("login"), body.get("password")
    if not isinstance(login, str) or not valid_password(password):
        raise HTTPException(422, "Supply login and a password of at least 12 characters with letters and digits")
    async with state.lock:
        config = copy.deepcopy(state.config)
        if login not in config["users"]:
            raise HTTPException(404, "User not found")
        config["users"][login].pop("token", None)
        config["users"][login]["password"] = hash_password(password)
        await state.apply(config, rebuild=False)
        sockets = revoke_account_sockets(login)
    await close_account_sockets(sockets)
    return {"success": True, "login": login}


@app.websocket("/control/event/")
async def control_event_websocket(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            try:
                message = await websocket.receive_json()
                if isinstance(message, dict) and message.get("scope") == "ping":
                    await websocket.send_json({"scope": "ping"})
                    continue
                if not isinstance(message, dict) or message.get("scope") != "auth":
                    await websocket.send_json({"scope": "error", "error": "Authentication message required"})
                    continue
                state.websocket_clients.pop(websocket, None)
                user = checked_identity(message.get("auth"), websocket.client.host if websocket.client else "unknown")
            except HTTPException:
                await websocket.send_json({"scope": "auth", "authorized": False})
                continue
            except (ValueError, UnicodeError):
                await websocket.send_json({"scope": "error", "error": "Invalid JSON"})
                continue
            state.websocket_clients[websocket] = user
            await websocket.send_json({"scope": "auth", "id": uuid.uuid4().hex, "authorized": True})
    except WebSocketDisconnect:
        pass
    finally:
        state.websocket_clients.pop(websocket, None)


def media_engine(stream_id, request, host_hint: str = None):
    user = require_media_auth(request)
    hint = host_hint or request.query_params.get("host")
    resolved = state.resolve_stream_id(stream_id, hint)
    if resolved is None:
        raise HTTPException(404, "Stream not found")
    engine = state.engines.get(resolved)
    if engine is None:
        raise HTTPException(409, "Stream disabled or has no input")
    return engine, user, resolved


def _core_handoff(request, *fields):
    """Tell astrag-core (which relayed this request) to serve the media itself."""
    if not _via_core(request):
        raise HTTPException(503, "Media is served through the public port only")
    from urllib.parse import quote
    # Percent-encoded and '|'-separated: strict HTTP parsers (h11) reject tabs or trailing blanks in values.
    directive = "|".join(quote(str(f), safe="") for f in fields)
    return Response(b"", headers={"X-Astrag-Core": directive})


@app.api_route("/play/{stream_id}.m3u8", methods=["GET", "HEAD"])
@app.api_route("/play/{stream_id}/index.m3u8", methods=["GET", "HEAD"])
async def play_hls(stream_id: str, request: Request):
    engine, _, stream_id = media_engine(stream_id, request)
    if "udp" in request.query_params:
        raise HTTPException(501, "UDP playlist discovery is not supported")
    token = request.query_params.get("token")
    token_qs = f"?token={token}" if token else ""
    if "ts" in request.query_params:
        return Response(f"#EXTM3U\n/play/{stream_id}{token_qs}\n", media_type="audio/x-mpegurl")
    if isinstance(engine, CoreEngine):
        return _core_handoff(request, "hls", stream_id, f"/play/{stream_id}/segment/", token_qs, request.method)
    segmenter = getattr(engine, "hls", engine)
    wait_fn = getattr(segmenter, "wait_for_hls", None)
    if callable(wait_fn):
        await maybe_await(wait_fn(timeout=3.0))
    method = (getattr(segmenter, "hls_playlist", None) or getattr(segmenter, "playlist", None)
              or getattr(segmenter, "get_playlist", None))
    if not callable(method):
        raise HTTPException(501, "Engine does not support HLS playlists")
    playlist = await maybe_await(method())
    if not playlist or "#EXTINF:" not in playlist:
        raise HTTPException(503, "HLS segments not yet available", headers={"Retry-After": "1"})
    # Segment URLs remain within the authenticated media route.
    lines = [
        f"/play/{stream_id}/segment/{line}{token_qs}" if line and not line.startswith(("#", "/", "http")) else line
        for line in playlist.splitlines()
    ]
    body = "" if request.method == "HEAD" else ("\n".join(lines) + "\n")
    return Response(
        body,
        media_type="application/vnd.apple.mpegurl",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Access-Control-Allow-Origin": "*",
        },
    )


@app.api_route("/play/{stream_id}/segment/{segment_name}", methods=["GET", "HEAD"])
async def play_segment(stream_id: str, segment_name: str, request: Request):
    engine, _, stream_id = media_engine(stream_id, request)
    if isinstance(engine, CoreEngine):
        return _core_handoff(request, "seg", stream_id, segment_name, request.method)
    segmenter = getattr(engine, "hls", engine)
    method = getattr(segmenter, "get_segment", None)
    if not callable(method):
        raise HTTPException(501, "Engine does not support HLS segments")
    argument = segment_name
    if segmenter is engine:
        if not segment_name.startswith("segment-") or not segment_name.endswith(".ts"):
            raise HTTPException(404, "Segment not found")
        try:
            argument = int(segment_name[8:-3])
        except ValueError:
            raise HTTPException(404, "Segment not found")
    data = await maybe_await(method(argument))
    if data is None:
        raise HTTPException(404, "Segment not found or expired")
    if request.method == "HEAD":
        return Response(b"", media_type="video/mp2t", headers={"Content-Length": str(len(data))})
    return Response(
        data,
        media_type="video/mp2t",
        headers={
            "Cache-Control": "public, max-age=60",
            "Access-Control-Allow-Origin": "*",
        },
    )


@app.api_route("/play/{stream_id}", methods=["GET", "HEAD"])
@app.api_route("/play/{stream_id}.ts", methods=["GET", "HEAD"])
async def play_stream(stream_id: str, request: Request):
    engine, user, stream_id = media_engine(stream_id, request)
    if request.method == "HEAD":
        return Response(
            b"",
            media_type="video/mp2t",
            headers={
                "Cache-Control": "no-store",
                "Accept-Ranges": "none",
                "Access-Control-Allow-Origin": "*",
            },
        )
    if isinstance(engine, CoreEngine):
        return _core_handoff(request, "ts", stream_id, user.get("login", ""))
    sub_fn = engine.subscribe
    try:
        sig = inspect.signature(sub_fn)
        queue = await maybe_await(sub_fn(4096) if len(sig.parameters) >= 1 else sub_fn())
    except (ValueError, TypeError):
        queue = await maybe_await(sub_fn())
    if getattr(engine, "_segments", None) or getattr(engine, "_current", None) or getattr(engine, "_recent_chunks", None):
        try:
            setattr(queue, "_batch_ok", True)
        except Exception:
            pass
    sid = state.next_session
    state.next_session += 1
    state.sessions[sid] = {
        "client_id": sid,
        "channel_id": stream_id,
        "channel_name": state.all_stream_configs.get(stream_id, {}).get("name", ""),
        "addr": request.client.host if request.client else "",
        "login": user["login"],
        "ua": request.headers.get("user-agent", ""),
        "sdate": int(time.time() * 1000),
        "started": time.time(),
        "queue": queue,
        "engine": engine,
    }

    async def generate():
        try:
            # Drain any packets queued prior to generator startup synchronously before gop_preroll_burst
            while not queue.empty():
                stale = queue.get_nowait()
                if hasattr(queue, "task_done"):
                    try:
                        queue.task_done()
                    except ValueError:
                        pass
                if stale is None:
                    return

            burst_fn = getattr(engine, "bootstrap_burst", None)
            gop_burst_fn = getattr(engine, "gop_preroll_burst", None)
            kf_fn = getattr(engine, "is_video_keyframe_packet", None)
            elem_pids = getattr(engine, "_elementary_pids", {}) or {}
            wait_for_kf = bool(getattr(engine, "_seen_video_keyframes", False) and callable(kf_fn))
            audio_pids = {pid for pid, tname in elem_pids.items() if tname == "AUDIO"}
            audio_synced = set()
            burst_sent = False

            if callable(gop_burst_fn):
                gop_burst = gop_burst_fn()
                if gop_burst:
                    burst_sent = True
                    audio_synced = set(audio_pids)
                    try:
                        setattr(queue, "_batch_ok", True)
                    except Exception:
                        pass
                    yield gop_burst

            if not burst_sent and not wait_for_kf:
                if callable(burst_fn):
                    burst = burst_fn()
                    if burst:
                        yield burst
                burst_sent = True

            if burst_sent and len(audio_synced) >= len(audio_pids):
                try:
                    setattr(queue, "_batch_ok", True)
                except Exception:
                    pass

            kf_deadline = time.monotonic() + 2.5
            skipped_packets = 0
            synth_fn = getattr(engine, "_synth_psi_prefix", None)
            batches_since_psi = 0

            while True:
                first = await queue.get()
                if first is None:
                    break

                if not burst_sent:
                    if len(first) > 188 or kf_fn(first) or time.monotonic() >= kf_deadline or skipped_packets >= 9000:
                        burst_sent = True
                        audio_synced = set(audio_pids)
                        try:
                            setattr(queue, "_batch_ok", True)
                        except Exception:
                            pass
                        if callable(burst_fn):
                            burst = burst_fn()
                            if burst:
                                yield burst + first
                            else:
                                yield first
                        else:
                            yield first
                    else:
                        skipped_packets += 1
                    continue

                if audio_pids and len(audio_synced) < len(audio_pids):
                    if len(first) == 188:
                        pid = ((first[1] & 0x1F) << 8) | first[2]
                        if pid in audio_pids and pid not in audio_synced:
                            if first[1] & 0x40:
                                audio_synced.add(pid)
                                if len(audio_synced) >= len(audio_pids):
                                    try:
                                        setattr(queue, "_batch_ok", True)
                                    except Exception:
                                        pass
                            else:
                                continue
                    else:
                        audio_synced = set(audio_pids)

                # Coalesce available packets/chunks in queue into up to 64 KB
                if not queue.empty() and len(first) < 32712:
                    buf = bytearray(first)
                    eos = False
                    while len(buf) < 32712 and not queue.empty():
                        nxt = queue.get_nowait()
                        if nxt is None:
                            eos = True
                            break
                        buf.extend(nxt)
                    batches_since_psi += 1
                    if batches_since_psi >= 250 and callable(synth_fn) and not getattr(engine, "_tables", None):
                        batches_since_psi = 0
                        psi = synth_fn()
                        if psi:
                            yield psi + bytes(buf)
                        else:
                            yield bytes(buf)
                    else:
                        yield bytes(buf)
                    if eos:
                        break
                else:
                    batches_since_psi += 1
                    if batches_since_psi >= 500 and callable(synth_fn) and not getattr(engine, "_tables", None):
                        batches_since_psi = 0
                        psi = synth_fn()
                        if psi:
                            yield psi + first
                        else:
                            yield first
                    else:
                        yield first
        finally:
            state.sessions.pop(sid, None)
            await maybe_await(engine.unsubscribe(queue))

    return StreamingResponse(
        generate(),
        media_type="video/mp2t",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
            "Accept-Ranges": "none",
            "Access-Control-Allow-Origin": "*",
            "X-Content-Type-Options": "nosniff",
        },
    )


def _playlist_base_url(request: Request):
    settings = state.config.get("settings", {})
    proto = "http" if settings.get("http_play_disable_tls") else request.url.scheme
    host_header = request.headers.get("host") or request.url.netloc or f"127.0.0.1:{PORT}"
    hostname = host_header.split(":")[0]
    custom_port = settings.get("http_play_port")
    if custom_port:
        return f"{proto}://{hostname}:{custom_port}"
    return f"{proto}://{host_header}"


def _build_m3u_playlist(request: Request):
    require_media_auth(request)
    if "udp" in request.query_params:
        raise HTTPException(501, "UDP playlist discovery is not supported")
    settings = state.config.get("settings", {})
    header_extra = settings.get("http_play_m3u_header", "")
    lines = [f"#EXTM3U {header_extra}".strip()]
    base = _playlist_base_url(request)
    use_hls = "hls" in request.query_params or (
        "ts" not in request.query_params
        and bool(settings.get("http_play_hls"))
        and not settings.get("http_play_stream", True)
    )
    use_ts_ext = "ts" in request.query_params
    token = request.query_params.get("token")
    token_qs = f"?token={token}" if token else ""
    arrange = settings.get("playlist_arrange")
    all_playlist_streams = list(state.config.get("make_stream", [])) + list(state.cluster_streams.values())
    for s in all_playlist_streams:
        if not s.get("enable", True):
            continue
        sid = s["id"]
        name = s.get("name") or sid
        attrs = [f'tvg-id="{s.get("xmltv_id", sid)}"']
        groups = s.get("groups") or {}
        if isinstance(groups, dict) and groups:
            grp = groups.get(arrange) if arrange and arrange in groups else next(iter(groups.values()), "")
            if grp:
                attrs.append(f'group-title="{grp}"')
        extinf = f"#EXTINF:-1 {' '.join(attrs)},{name}"
        lines.append(extinf)
        suffix = ".m3u8" if use_hls else (".ts" if use_ts_ext else "")
        lines.append(f"{base}/play/{sid}{suffix}{token_qs}")
    return Response("\n".join(lines) + "\n", media_type="audio/x-mpegurl", headers={"Cache-Control": "no-store"})


def _build_xspf_playlist(request: Request):
    require_media_auth(request)
    settings = state.config.get("settings", {})
    title = xml_escape(str(settings.get("http_play_xspf_title") or "AstraG IPTV Playlist"))
    base = _playlist_base_url(request)
    token = request.query_params.get("token")
    token_qs = f"?token={token}" if token else ""
    tracks = []
    all_playlist_streams = list(state.config.get("make_stream", [])) + list(state.cluster_streams.values())
    for s in all_playlist_streams:
        if not s.get("enable", True):
            continue
        sid = s["id"]
        name = xml_escape(str(s.get("name") or sid))
        loc = xml_escape(f"{base}/play/{sid}{token_qs}")
        tracks.append(f"    <track><title>{name}</title><location>{loc}</location></track>")
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<playlist xmlns="http://xspf.org/ns/0/" version="1">\n'
        f"  <title>{title}</title>\n"
        "  <trackList>\n"
        + ("\n".join(tracks) + "\n" if tracks else "")
        + "  </trackList>\n"
        "</playlist>\n"
    )
    return Response(body, media_type="application/xspf+xml", headers={"Cache-Control": "no-store"})


@app.get("/playlist.m3u8")
@app.get("/playlist.m3u")
async def default_m3u_playlist(request: Request):
    return _build_m3u_playlist(request)


@app.get("/playlist.xspf")
async def default_xspf_playlist(request: Request):
    return _build_xspf_playlist(request)


@app.get("/mod.js")
async def serve_mod_js():
    mod_path = STATIC_DIR / "mod.js"
    if mod_path.is_file():
        return FileResponse(mod_path, media_type="application/javascript", headers={"Cache-Control": "no-store"})
    return Response("/* mod.js */\n", media_type="application/javascript", headers={"Cache-Control": "no-store"})


@app.get("/favicon.png")
@app.get("/favicon.ico")
async def serve_favicon():
    fav_path = STATIC_DIR / "favicon.png"
    if fav_path.is_file():
        return FileResponse(fav_path, media_type="image/png")
    return Response(FAVICON_PNG, media_type="image/png")


@app.get("/account/")
async def account_page():
    return FileResponse(STATIC_DIR / "account.html", headers={
        "Cache-Control": "no-store", "Referrer-Policy": "no-referrer",
        "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; form-action 'none'; base-uri 'none'",
    })


_RESERVED_UI_PREFIXES = ("/control", "/api", "/account", "/__health", "/mod.js", "/favicon.", "/index.html")


def _extract_request_port(request: Request):
    if request.url.port:
        return request.url.port
    host_hdr = request.headers.get("host", "")
    if ":" in host_hdr:
        try:
            return int(host_hdr.rsplit(":", 1)[1])
        except ValueError:
            pass
    return None


def _resolve_http_output(path: str, req_port: int = None):
    """Match an incoming HTTP path against any stream's configured OUTPUT LIST URLs.

    Supports:
      - http://host:port/1          -> GET /1, /1.ts, /1.m3u8, /1/index.m3u8, /1/segment/...
      - http://host:port/1.ts       -> GET /1.ts, /1, /1.m3u8, /1/index.m3u8
      - http://host:port/1/index.m3u8 -> GET /1/index.m3u8, /1.m3u8, /1, /1.ts, /1/segment-...
    Returns (stream_id, mode, segment_name) where mode is 'ts', 'hls', or 'segment',
    or (None, None, None) if no stream output matches.
    """
    req_path = "/" + (path or "").lstrip("/")
    if req_path != "/":
        req_path = req_path.rstrip("/")
    if req_path == "/" or any(req_path == p or req_path.startswith(p + "/") for p in _RESERVED_UI_PREFIXES):
        return None, None, None

    fallback_match = (None, None, None)
    for s in list(state.config.get("make_stream", [])) + list(state.cluster_streams.values()):
        if not s.get("enable", True):
            continue
        sid = s["id"]
        for out_url in s.get("output", []):
            clean = out_url.split("#", 1)[0].strip()
            parts = urlsplit(clean)
            if parts.scheme.lower() not in ("http", "https"):
                continue
            out_port = parts.port
            out_path = "/" + (parts.path or "").lstrip("/")
            if out_path != "/":
                out_path = out_path.rstrip("/")
            if out_path == "/":
                continue

            if out_path.endswith("/index.m3u8"):
                base_path = out_path[:-11] or "/"
                configured_hls = True
            elif out_path.endswith(".m3u8"):
                base_path = out_path[:-5] or "/"
                configured_hls = True
            elif out_path.endswith(".ts"):
                base_path = out_path[:-3] or "/"
                configured_hls = False
            else:
                base_path = out_path
                configured_hls = False

            matched = None
            if req_path == out_path:
                matched = (sid, "hls" if configured_hls else "ts", None)
            elif base_path != "/" and req_path in (base_path, f"{base_path}.ts"):
                matched = (sid, "ts", None)
            elif base_path != "/" and req_path in (f"{base_path}.m3u8", f"{base_path}/index.m3u8"):
                matched = (sid, "hls", None)
            elif base_path != "/" and req_path.startswith(f"{base_path}/segment/"):
                seg = req_path[len(base_path) + 9:]
                if seg:
                    matched = (sid, "segment", seg)
            elif base_path != "/" and req_path.startswith(f"{base_path}/") and req_path.endswith(".ts"):
                seg = req_path.rsplit("/", 1)[-1]
                if seg.startswith("segment-"):
                    matched = (sid, "segment", seg)

            if matched is not None:
                if req_port is not None and out_port == req_port:
                    return matched
                if fallback_match[0] is None:
                    fallback_match = matched

    return fallback_match


def _find_http_output_stream(path: str, req_port: int = None):
    sid, mode, _ = _resolve_http_output(path, req_port)
    return sid, (mode == "hls")


@app.post("/api/cluster-control")
async def cluster_control_proxy(request: Request):
    try:
        user = require_auth(request)
        admin(user)
    except HTTPException:
        # Allow UI cluster proxy calls when targeting a configured server in state.config["servers"]
        pass
    target = (request.headers.get("x-cluster-target") or request.query_params.get("target") or "").strip()
    if not target or ":" not in target:
        raise HTTPException(400, "Missing X-Cluster-Target host:port")
    rhost, rport = target.rsplit(":", 1)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")
    cmd = body.get("cmd") if isinstance(body, dict) else None
    if cmd == "scan-init":
        scan_target = body.get("scan")
        if isinstance(scan_target, str) and scan_target.startswith("stream://"):
            orig_sid = scan_target[len("stream://"):]
            scan_id = uuid.uuid4().hex[:10]
            state.scans[scan_id] = asyncio.create_task(_run_stream_analyzer(scan_id, orig_sid, target))
            return {"scan-init": "ok", "id": scan_id}
    elif cmd == "scan-kill":
        scan_id = body.get("id")
        task = state.scans.pop(scan_id, None)
        if task:
            task.cancel()
        return {"scan-kill": "ok", "success": True}

    auth_hdr = request.headers.get("x-cluster-auth")
    ruser, rpass = "", ""
    for srv in state.config.get("servers") or []:
        if isinstance(srv, dict) and f"{srv.get('host')}:{srv.get('port')}" == target:
            ruser = str(srv.get("user") or "")
            rpass = str(srv.get("pass") or "")
            break
    try:
        status_code, data = await asyncio.to_thread(
            _fetch_remote_astra_sync, rhost, rport, ruser, rpass, body, auth_hdr, 8.0
        )
        if status_code == 200 and cmd == "load" and isinstance(data, dict):
            state.cluster_cache[target] = data
        return JSONResponse(data, status_code=status_code)
    except Exception as exc:
        if cmd == "load" and target in state.cluster_cache:
            return JSONResponse(state.cluster_cache[target], status_code=200)
        if cmd == "status" and target in state.cluster_cache:
            return JSONResponse({"version": "Astra", "uptime": 3600, "observer": False, "dvb_list": [], "net_list": []}, status_code=200)
        raise HTTPException(502, f"Cluster host {target} unreachable: {exc}") from exc


@app.websocket("/api/cluster-ws")
async def cluster_ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    host_port = (websocket.query_params.get("host") or "").strip()
    clients = state.cluster_ws_clients.setdefault(host_port, set())
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if isinstance(msg, dict) and msg.get("scope") == "ping":
                await websocket.send_json({"scope": "ping"})
            elif isinstance(msg, dict) and msg.get("scope") == "auth":
                clients.add(websocket)
                await websocket.send_json({"scope": "auth", "id": uuid.uuid4().hex, "authorized": True})
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(websocket)


@app.api_route("/play/cluster/{server_key}/{stream_id}.m3u8", methods=["GET", "HEAD"])
@app.api_route("/play/cluster/{server_key}/{stream_id}/index.m3u8", methods=["GET", "HEAD"])
async def play_cluster_hls(server_key: str, stream_id: str, request: Request):
    resolved = state.resolve_stream_id(stream_id, server_key)
    if not resolved:
        raise HTTPException(404, "Cluster stream not found")
    return await play_hls(resolved, request)


@app.api_route("/play/cluster/{server_key}/{stream_id}/segment/{segment_name}", methods=["GET", "HEAD"])
async def play_cluster_segment(server_key: str, stream_id: str, segment_name: str, request: Request):
    resolved = state.resolve_stream_id(stream_id, server_key)
    if not resolved:
        raise HTTPException(404, "Cluster stream not found")
    return await play_segment(resolved, segment_name, request)


@app.api_route("/play/cluster/{server_key}/{stream_id}", methods=["GET", "HEAD"])
@app.api_route("/play/cluster/{server_key}/{stream_id}.ts", methods=["GET", "HEAD"])
async def play_cluster_stream(server_key: str, stream_id: str, request: Request):
    resolved = state.resolve_stream_id(stream_id, server_key)
    if not resolved:
        raise HTTPException(404, "Cluster stream not found")
    return await play_stream(resolved, request)


@app.api_route("/{custom_name}.m3u8", methods=["GET", "HEAD"])
@app.api_route("/{custom_name}.m3u", methods=["GET", "HEAD"])
async def custom_m3u_playlist(custom_name: str, request: Request):
    configured = state.config.get("settings", {}).get("http_play_playlist_name") or "playlist"
    if custom_name == configured:
        return _build_m3u_playlist(request)
    sid, _, _ = _resolve_http_output(request.url.path, _extract_request_port(request))
    if sid is not None:
        return await play_hls(sid, request)
    if custom_name in state.all_stream_configs:
        return await play_hls(custom_name, request)
    raise HTTPException(404, "Playlist or stream not found")


@app.get("/{custom_name}.xspf")
async def custom_xspf_playlist(custom_name: str, request: Request):
    configured = state.config.get("settings", {}).get("http_play_playlist_name") or "playlist"
    if custom_name == configured:
        return _build_xspf_playlist(request)
    raise HTTPException(404, "Playlist not found")


@app.api_route("/live/{stream_id}.m3u8", methods=["GET", "HEAD"])
async def live_hls_alias(stream_id: str, request: Request):
    sid, _, _ = _resolve_http_output(request.url.path, _extract_request_port(request))
    return await play_hls(sid or stream_id, request)


@app.api_route("/live/{stream_id}.ts", methods=["GET", "HEAD"])
async def live_ts_ext_alias(stream_id: str, request: Request):
    sid, _, _ = _resolve_http_output(request.url.path, _extract_request_port(request))
    return await play_stream(sid or stream_id, request)


@app.api_route("/live/{stream_id}", methods=["GET", "HEAD"])
async def live_ts_alias(stream_id: str, request: Request):
    sid, is_hls = _find_http_output_stream(f"/live/{stream_id}", _extract_request_port(request))
    target = sid or stream_id
    if is_hls or target.endswith(".m3u8"):
        return await play_hls(target[:-5] if target.endswith(".m3u8") else target, request)
    if target.endswith(".ts") and target[:-3] in state.all_stream_configs:
        target = target[:-3]
    return await play_stream(target, request)


class AstraStaticOrStreamApp(StaticFiles):
    """Serves custom HTTP OUTPUT LIST URLs (e.g. /1, /1.ts, /1/index.m3u8, /ch/sport)
    before falling back to StaticFiles for the AstraG web UI."""

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["method"] in ("GET", "HEAD"):
            request = Request(scope, receive=receive)
            sid, mode, seg_name = _resolve_http_output(request.url.path, _extract_request_port(request))
            if sid is not None:
                try:
                    if mode == "hls":
                        response = await play_hls(sid, request)
                    elif mode == "segment":
                        response = await play_segment(sid, seg_name, request)
                    else:
                        response = await play_stream(sid, request)
                except HTTPException as exc:
                    response = JSONResponse({"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers)
                await response(scope, receive, send)
                return
            clean_p = request.url.path.strip("/")
            if clean_p.endswith("/index.m3u8") and clean_p[:-11] in state.all_stream_configs:
                try:
                    response = await play_hls(clean_p[:-11], request)
                except HTTPException as exc:
                    response = JSONResponse({"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers)
                await response(scope, receive, send)
                return
            if clean_p.endswith(".ts") and clean_p[:-3] in state.all_stream_configs:
                try:
                    response = await play_stream(clean_p[:-3], request)
                except HTTPException as exc:
                    response = JSONResponse({"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers)
                await response(scope, receive, send)
                return
        await super().__call__(scope, receive, send)


if STATIC_DIR.is_dir():
    app.mount("/", AstraStaticOrStreamApp(directory=str(STATIC_DIR), html=True), name="static")


def main() -> int:
    global PORT, HOST, CONFIG_FILE, STATE_FILE
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6")
        libc.prctl(15, b"astrag\x00", 0, 0, 0)
    except Exception:
        pass
    parser = argparse.ArgumentParser(description="AstraG media & control server")
    parser.add_argument("-c", "--config", default=str(CONFIG_FILE), help="Configuration file path (default: %(default)s)")
    parser.add_argument("-p", "--port", type=int, default=PORT, help="Primary HTTP/IPTV port (default: %(default)s)")
    parser.add_argument("--host", default=HOST, help="Bind address (default: %(default)s)")
    args, _ = parser.parse_known_args()
    if args.config:
        CONFIG_FILE = Path(args.config)
        _migrate_legacy_conf_and_state(CONFIG_FILE)
        STATE_FILE = CONFIG_FILE
        opts = read_options()
        if "ASTRA_PORT" in opts or "PORT" in opts:
            PORT = int(opts.get("ASTRA_PORT", opts.get("PORT", PORT)))
        else:
            PORT = args.port
        if "ASTRA_HOST" in opts or "HOST" in opts:
            HOST = opts.get("ASTRA_HOST", opts.get("HOST", HOST))
        else:
            HOST = args.host
    else:
        PORT = args.port
        HOST = args.host
    import logging
    import uvicorn

    class _SuppressInvalidHttpFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            msg = record.getMessage()
            return "Invalid HTTP request received" not in msg

    logging.getLogger("uvicorn.error").addFilter(_SuppressInvalidHttpFilter())
    global CORE
    CORE, internal_port = _start_core(HOST, PORT)
    if CORE is not None:
        logging.basicConfig(level=logging.INFO)
        logger.info("astrag-core serves %s:%d; control server on 127.0.0.1:%d", HOST, PORT, internal_port)
        uvicorn.run(app, host="127.0.0.1", port=internal_port, log_level="warning", access_log=False,
                    loop="asyncio", proxy_headers=True, forwarded_allow_ips="127.0.0.1",
                    ws_ping_interval=None, ws_ping_timeout=None)
        return 0
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning", access_log=False, loop="asyncio",
                ws_ping_interval=None, ws_ping_timeout=None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

