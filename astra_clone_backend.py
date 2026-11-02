"""Async management API and native transport integration for the local media server.

HLS is an in-memory MPEG-TS remux, not a transcoder; codec/keyframe compatibility
is the caller's responsibility. DVB hardware and conditional access execution are
not implemented (their configuration can be stored).
"""
import asyncio
import base64
import copy
import inspect
import json
import logging
import os
from pathlib import Path
import signal
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from urllib.parse import urlsplit
from typing import List

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response, StreamingResponse
from account_security import FailedAuthLimiter, hash_password, valid_login, valid_password, verify_password
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

logger = logging.getLogger("astra.backend")
APP_DIR = Path(__file__).resolve().parent
CONFIG_FILE = Path(os.environ.get("ASTRA_CONFIG_FILE", APP_DIR / "astra-clone.conf"))
STATE_FILE = Path(os.environ.get("ASTRA_STATE_FILE", APP_DIR / "state.json"))


def read_options():
    result = {}
    if CONFIG_FILE.exists():
        for line in CONFIG_FILE.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                result[k.strip()] = v.strip().strip("\"'")
    return result


OPTIONS = read_options()
PORT = int(os.environ.get("ASTRA_PORT", os.environ.get("PORT", OPTIONS.get("PORT", "3000"))))
STATIC_DIR = Path(os.environ.get("ASTRA_STATIC_DIR", OPTIONS.get("STATIC_DIR", str(APP_DIR / "www"))))
if not STATIC_DIR.is_dir() and "ASTRA_STATIC_DIR" not in os.environ:
    STATIC_DIR = APP_DIR / "www"


def validate_url(value, output=False):
    if not isinstance(value, str) or not value or any(c.isspace() for c in value):
        raise ValueError("URL must be a nonempty string without whitespace")
    # Astra options after # are passed unchanged to the native engine.
    parts = urlsplit(value)
    if parts.scheme not in ({"udp"} if output else {"http", "https", "udp", "rtp"}):
        raise ValueError("unsupported output transport" if output else "unsupported input transport")
    if not parts.hostname:
        raise ValueError("URL requires a host")
    port = parts.port
    if parts.scheme in {"udp", "rtp"} and port is None:
        raise ValueError("UDP/RTP URL requires a port")
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("invalid port")
    return value


class StreamConfig(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True)
    id: str = ""
    name: str = ""
    type: str = "spts"
    input: List[str] = Field(default_factory=list)
    output: List[str] = Field(default_factory=list)
    enable: bool = True

    @field_validator("input")
    @classmethod
    def inputs(cls, values):
        return [validate_url(v) for v in values]

    @field_validator("output")
    @classmethod
    def outputs(cls, values):
        return [validate_url(v, True) for v in values]


async def maybe_await(value):
    return await value if inspect.isawaitable(value) else value


def make_engine(config):
    try:
        from stream_engine import Engine
    except ImportError as exc:
        raise HTTPException(503, "Native stream engine unavailable") from exc
    return Engine(config["input"], outputs=config.get("output", []))


def defaults():
    return {"gid": 466560, "make_stream": [], "users": {}, "categories": [],
            "servers": [], "softcam": [], "cas": [], "dvb_tune": [],
            "settings": {}, "hls": {}, "auth": {"enable": False}}


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
            entry = StreamConfig.model_validate(item).model_dump()
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


def atomic_save(config):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    name = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=STATE_FILE.parent,
                                         prefix=".astra-", suffix=".tmp", delete=False) as f:
            name = f.name
            json.dump({"config": config}, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, STATE_FILE)
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


def disk_config():
    config = defaults()
    options = read_options()
    if "AUTH_ENABLE" in options:
        config["auth"]["enable"] = options["AUTH_ENABLE"].lower() in ("true", "1", "yes")
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError) as exc:
            raise HTTPException(500, "Cannot read state file") from exc
        if not isinstance(data, dict) or not isinstance(data.get("config", data), dict):
            raise HTTPException(422, "State file must contain a configuration object")
        config.update(data.get("config", data))
        # Legacy streams are migrated only if authoritative make_stream is absent.
        if "make_stream" not in data.get("config", data) and isinstance(data.get("streams"), dict):
            config["make_stream"] = [dict(v, id=k) for k, v in data["streams"].items()]
    enabled = os.environ.get("ASTRA_AUTH_ENABLE")
    if enabled is not None:
        config["auth"] = dict(config.get("auth", {}), enable=enabled.lower() in ("true", "1", "yes"))
    config["control_port"] = PORT
    return validate_config(config)


def redact(value):
    if isinstance(value, dict):
        return {k: redact(v) for k, v in value.items()
                if not any(s in str(k).lower() for s in ("password", "token", "secret", "passwd"))
                and str(k).lower() not in ("pass", "auth")}
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, str) and "://" in value:
        parts = urlsplit(value)
        if parts.username is not None or parts.fragment or parts.query:
            return parts._replace(netloc=parts.netloc.rsplit("@", 1)[-1], query="", fragment="").geturl()
    return value


class ServerState:
    def __init__(self):
        self.config = defaults()
        self.engines = {}
        self.sessions = {}
        self.websocket_clients = {}
        self.auth_limiter = FailedAuthLimiter()
        self.next_session = 1
        self.started = time.time()
        self.version = "native-python 2.0"
        self.commit = os.environ.get("ASTRA_COMMIT", "local")
        self.logs = []
        self.lock = asyncio.Lock()

    @property
    def stream_configs(self):
        return {s["id"]: s for s in self.config["make_stream"]}

    def close_session(self, sid):
        session = self.sessions.pop(sid, None)
        if session:
            q = session["queue"]
            while not q.empty():
                q.get_nowait()
            q.put_nowait(None)
        return session is not None

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

    async def apply(self, config, persist=True, rebuild=True):
        candidate = validate_config(config)
        old = self.config
        changed = candidate["make_stream"] != old["make_stream"] or rebuild
        try:
            if changed:
                await self.stop_engines()
                self.engines = await self.build(candidate)
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

    async def broadcast(self, scope, payload):
        message = {"scope": scope, **redact(payload)}
        for ws, user in list(self.websocket_clients.items()):
            if self.config.get("auth", {}).get("enable"):
                current = self.config["users"].get(user["login"])
                if not current or current.get("enable") is False:
                    self.websocket_clients.pop(ws, None)
                    await ws.close(code=1008)
                    continue
            try:
                await ws.send_json(message)
            except Exception:
                self.websocket_clients.pop(ws, None)


async def publish_stats():
    while True:
        await asyncio.sleep(1)
        for sid, engine in list(state.engines.items()):
            try:
                stats = getattr(engine, "stats", {})
                stats = await maybe_await(stats() if callable(stats) else stats)
                if not isinstance(stats, dict):
                    continue
                stats = dict(stats)
                stats["bitrate"] = stats.get("bitrate", 0) / 1000
                stats["onair"] = bool(stats.get("running") and stats["bitrate"])
                stats["cc_error"] = stats.get("cc_errors", 0)
                stats["pes_error"] = stats.get("pes_errors", 0)
                stats["scrambled"] = bool(stats.get("scrambled", False))
                await state.broadcast("stream_event", {
                    **stats, "channel_id": sid, "input_id": 1,
                    "sessions": sum(s["channel_id"] == sid for s in state.sessions.values()),
                })
            except Exception:
                logger.exception("Unable to publish stream statistics")


state = ServerState()


def identity(credentials):
    if not state.config.get("auth", {}).get("enable"):
        return {"login": "", "admin": True}
    if not isinstance(credentials, str) or ":" not in credentials:
        raise HTTPException(403, "Forbidden")
    login, password = credentials.split(":", 1)
    user = state.config.get("users", {}).get(login, {})
    expected = user.get("password") or user.get("token")
    good = verify_password(password, expected)
    if not good or user.get("enable") is False:
        raise HTTPException(403, "Forbidden")
    return {"login": login, "admin": user.get("role") == "admin" or user.get("type") in (1, "1")}


def require_auth(request):
    credentials = None
    header = request.headers.get("authorization", "")
    if header.lower().startswith("basic "):
        try:
            credentials = base64.b64decode(header[6:], validate=True).decode("utf-8")
        except (ValueError, UnicodeError):
            pass
    return checked_identity(credentials, request.client.host if request.client else "unknown")


def checked_identity(credentials, host):
    if state.config.get("auth", {}).get("enable") and state.auth_limiter.blocked(host):
        raise HTTPException(429, "Too many failed authentication attempts", headers={"Retry-After": "60"})
    try:
        return identity(credentials)
    except HTTPException:
        state.auth_limiter.failed(host)
        raise


def admin(user):
    if not user["admin"]:
        raise HTTPException(403, "Administrator role required")


async def reload_config():
    async with state.lock:
        await state.apply(disk_config(), persist=False)


@asynccontextmanager
async def lifespan(app):
    state.lock = asyncio.Lock()
    await reload_config()
    loop = asyncio.get_running_loop()
    installed = False
    if hasattr(signal, "SIGHUP") and os.name != "nt":
        try:
            loop.add_signal_handler(signal.SIGHUP, lambda: asyncio.create_task(reload_config()))
            installed = True
        except (NotImplementedError, RuntimeError):
            pass
    stats_task = asyncio.create_task(publish_stats())
    try:
        yield
    finally:
        stats_task.cancel()
        await asyncio.gather(stats_task, return_exceptions=True)
        if installed:
            loop.remove_signal_handler(signal.SIGHUP)
        await state.stop_engines()
        for ws in list(state.websocket_clients):
            await ws.close()
        state.websocket_clients.clear()


app = FastAPI(title="Native Media Server", version="2.0", lifespan=lifespan)


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
                 uptime=int(time.time() - s["started"])) for s in state.sessions.values()]


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
        return copy.deepcopy(state.config) if user["admin"] else redact(state.config)
    if cmd == "sessions":
        return {"sessions": session_list()}
    if cmd == "log":
        return {"log": list(state.logs)}
    admin(user)
    if cmd.startswith("scan-") or cmd in ("restart-adapter",):
        raise HTTPException(501, "DVB hardware operations are not implemented")
    if cmd == "close-session":
        sid = body.get("id")
        if isinstance(sid, bool) or not isinstance(sid, int):
            raise HTTPException(422, "id must be an integer")
        if not state.close_session(sid):
            raise HTTPException(404, "Session not found")
        return {"success": True}
    async with state.lock:
        if cmd == "restart":
            await state.apply(disk_config(), persist=False)
            return {"success": True, "message": "Configuration and engines reloaded; process not restarted"}
        config = copy.deepcopy(state.config)
        event = {}
        result = {"success": True}
        if cmd == "upload":
            config = object_payload(body, "config")
            event = {"reload": True}
        elif cmd == "set-stream":
            data = object_payload(body, "stream")
            sid = body.get("id", data.get("id"))
            if sid in (None, "", "-"):
                sid = uuid.uuid4().hex[:12]
            if not isinstance(sid, str):
                raise HTTPException(422, "id must be a string")
            data["id"] = sid
            config["make_stream"] = [s for s in config["make_stream"] if s["id"] != sid]
            if not data.get("remove", body.get("remove", False)):
                config["make_stream"].append(data)
            else:
                data["remove"] = True
            event = {"id": sid, "stream": data}
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
            if not isinstance(sid, str) or not sid:
                raise HTTPException(422, "id must be a nonempty login")
            if data.get("remove", body.get("remove", False)):
                config["users"].pop(sid, None)
                data["remove"] = True
            else:
                config["users"][sid] = data
            event = {"id": sid, "user": data}
        elif cmd in ("set-settings", "set-auth"):
            key = cmd[4:]
            data = object_payload(body, key)
            config[key] = {} if data.get("remove") else data
            event = {key: config[key]}
        elif cmd == "set-config":
            key = body.get("key")
            if not isinstance(key, str) or not key:
                raise HTTPException(422, "key required")
            if "data" not in body:
                raise HTTPException(422, "data required")
            data = body["data"]
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
                sid = uuid.uuid4().hex[:12] if sid in (None, "-") else str(sid)
                data["id"] = sid
                values[:] = [v for v in values if v.get("id") != sid]
                if data.get("remove", body.get("remove", False)):
                    data["remove"] = True
                else:
                    values.append(data)
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
        await state.broadcast("stream_event" if cmd == "set-stream-input" else cmd, event)
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


def media_engine(stream_id, request):
    user = require_auth(request)
    if stream_id not in state.stream_configs:
        raise HTTPException(404, "Stream not found")
    engine = state.engines.get(stream_id)
    if engine is None:
        raise HTTPException(409, "Stream disabled or has no input")
    return engine, user


@app.get("/play/{stream_id}.m3u8")
async def play_hls(stream_id: str, request: Request):
    engine, _ = media_engine(stream_id, request)
    if "udp" in request.query_params:
        raise HTTPException(501, "UDP playlist discovery is not supported")
    if "ts" in request.query_params:
        return Response(f"#EXTM3U\n/play/{stream_id}\n", media_type="audio/x-mpegurl")
    segmenter = getattr(engine, "hls", engine)
    method = (getattr(segmenter, "hls_playlist", None) or getattr(segmenter, "playlist", None)
              or getattr(segmenter, "get_playlist", None))
    if not callable(method):
        raise HTTPException(501, "Engine does not support HLS playlists")
    playlist = await maybe_await(method())
    if not playlist or "#EXTINF:" not in playlist:
        raise HTTPException(503, "HLS segments not yet available", headers={"Retry-After": "2"})
    # Segment URLs remain within the authenticated media route.
    lines = [f"/play/{stream_id}/segment/{line}" if line and not line.startswith(("#", "/", "http")) else line
             for line in playlist.splitlines()]
    return Response("\n".join(lines) + "\n", media_type="application/vnd.apple.mpegurl",
                    headers={"Cache-Control": "no-store"})


@app.get("/play/{stream_id}/segment/{segment_name}")
async def play_segment(stream_id: str, segment_name: str, request: Request):
    engine, _ = media_engine(stream_id, request)
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
    return Response(data, media_type="video/mp2t")


@app.get("/play/{stream_id}")
async def play_stream(stream_id: str, request: Request):
    engine, user = media_engine(stream_id, request)
    queue = await maybe_await(engine.subscribe())
    sid = state.next_session
    state.next_session += 1
    state.sessions[sid] = {"client_id": sid, "channel_id": stream_id,
                           "channel_name": state.stream_configs[stream_id].get("name", ""),
                           "addr": request.client.host if request.client else "", "login": user["login"],
                           "started": time.time(), "queue": queue, "engine": engine}

    async def generate():
        try:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    break
                yield chunk
        finally:
            state.sessions.pop(sid, None)
            await maybe_await(engine.unsubscribe(queue))
    return StreamingResponse(generate(), media_type="video/mp2t", headers={"Cache-Control": "no-store"})


@app.get("/account/")
async def account_page():
    return FileResponse(STATIC_DIR / "account.html", headers={
        "Cache-Control": "no-store", "Referrer-Policy": "no-referrer",
        "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; form-action 'none'; base-uri 'none'",
    })


if STATIC_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.environ.get("ASTRA_HOST", "0.0.0.0"), port=PORT, loop="asyncio")
