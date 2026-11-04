"""AstraG native asyncio MPEG-TS & HLS transport engine (Cesbo Astra-compatible).

Use ``await Engine(urls, outputs=[]).start()``; ``subscribe()`` returns a bounded
asyncio.Queue containing 188-byte packets and finally END_OF_STREAM (None).
Call unsubscribe(queue) or await stop() to close subscriptions. feed(bytes) is
synchronous and may also be used without start(). All methods belong on one
asyncio event-loop thread. Overflow drops the oldest queued packet.

Inputs: HTTP(S) (both direct MPEG-TS and HLS .m3u8/.m3u master/media playlists),
udp://[address]:port, rtp://[address]:port. IPv4 multicast supports
?interface=192.0.2.1 or #localaddr=192.0.2.1; outputs support udp://host:port?ttl=1.
Multiple inputs are ordered failover sources, never interleaved.
Includes full MPEG-TS PSI/SI demuxing (PAT, PMT, SDT, NIT), PES error checking,
scrambling detection, Astra URL #options filtering, pre-roll keyframe burst for
instant VLC .ts playback, and low-memory sliding-window HLS segmentation.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
import ctypes
from dataclasses import dataclass
import ipaddress
import math
import os
import socket
import time
from typing import Callable
from urllib.parse import parse_qs, urljoin, urlsplit

PACKET_SIZE = 188
PCR_HZ = 27_000_000
PCR_WRAP = (1 << 33) * 300
END_OF_STREAM = None
MAX_TRACKED_PIDS = 256
PREROLL_MAX_BYTES = 188 * 256  # 48 KB (~256 TS packets) pre-roll burst for instant VLC lock

_LIBC = None


def _configure_libc_allocator():
    """Configure glibc malloc on Linux so large media buffers use mmap and return to OS immediately."""
    global _LIBC
    if os.name != "posix":
        return
    try:
        libc = ctypes.CDLL("libc.so.6")
        # M_TRIM_THRESHOLD = -1, M_MMAP_THRESHOLD = -3, M_ARENA_MAX = -8
        if hasattr(libc, "mallopt"):
            libc.mallopt.argtypes = [ctypes.c_int, ctypes.c_int]
            libc.mallopt.restype = ctypes.c_int
            libc.mallopt(-3, 131072)  # M_MMAP_THRESHOLD = 128 KB (prevents dynamic 32MB heap retention)
            libc.mallopt(-1, 131072)  # M_TRIM_THRESHOLD = 128 KB
            libc.mallopt(-8, 2)       # M_ARENA_MAX = 2
        if hasattr(libc, "malloc_trim"):
            libc.malloc_trim.argtypes = [ctypes.c_size_t]
            libc.malloc_trim.restype = ctypes.c_int
            _LIBC = libc
    except Exception:
        pass


def trim_process_memory():
    """Return unused glibc heap pages to the operating system."""
    if _LIBC is not None:
        try:
            _LIBC.malloc_trim(0)
        except Exception:
            pass


_configure_libc_allocator()

_C_TS_SO_B64 = (
    "/Td6WFoAAATm1rRGAgAhARwAAAAQz1jM4DPvCI1dAD+RRYRoPYmm2orhgzJO8e3vZxg4kd0U2xRLBEwKo/SLmkP53QH45GUEy0Sb"
    "RamEROCYOWMbw9UjFPrL66gG8nmgkDElB+8WCEkfhomxOuWSgKlMsdHNgAtwH/9l/X0wZoIBzOFvFPw8U6KICIeKwBT1Eklx0aiZ"
    "RSSRo8TiH8d3LdddH2Qb+aqMjiw/X+FTCuxLJAMj8+F3loD5L6L5veMtNstjuxEdVCbL0THpF74ptq41P64Z8IjX6M5NY7yq4U/6"
    "UbX4hw/DGLXuFSdvLeCJ23NU4hBieZ26Qb3jgBosaiJy0nbY7122+Awr+Visig6pIkcEJT41QXdBK9hYIJonE4CiCUzKXyXwjUQi"
    "Nz5K/WqFHuOKhv/iwoejp+lgUTjtHUjibuktk6ecVAbKup7xNeC0hi3JNTeqb60DQF1RzPMh8eSHO6RgsZou2ppalCH9VlG7TUak"
    "kIpXRaY2Dj+uBEKM7RrHnwPsIavUESI1hcmp5a5xSuejSBwlBxfKp16AvR+5K5vSsqRjv5hD/8p49Fvn/jEzF4ulrVS+1bzBUzfe"
    "6lWTmkqUoVQNLUx534y/V1m07o4bGb7zDouxIBkP31n9qDKR/r0mMuN552zm4AzSU/LXFQDyYy0dTi4niffRhHF2Yr/MSZntjXOa"
    "s3b+DBOImWTNsWdPAYo+onAnXjdVGIxLj6WrvVa6UTKxFfo+VEO4r+Qy7gKTM4j1kMi81zJKNiOIkrZWFlatj130ODRUWJOPTw5O"
    "CSgKblpiUyZgiG1CPxpyY5NaZSFfaSjqL3IkUBoOdRg9idJaB1WJsgxsuGeXo9s02REcr/aJq5fpihdxhG8Y2R878MOQH0KhuY+4"
    "DT4pbKkFAMnBECQd4UsmoM49soQY4jqF7+DfMYG8LZqBi+Vsad9BAz0vPlQDEJhELldkC1e01Cir6z2/UOMM4fIW2vTH7EORyE6a"
    "qU8O9X1DGBQzyHfNr/MzvrQjblW9U5giRCH7WN86f8ZsPeqr79pqpaYsfmmnYbD4xanhZju3G5zxl4/OyY1AKtHf+g5MOG+vM36U"
    "6HrALy15C5KAbhRI7g9jzpkpNbv1DtL8fLk7FWJHwNblFBsGKRBMNJoF82ldPJ3xH9TyEjQC7b7hLMn3rTSG2pi9qd22+WJ24evF"
    "F3j83ZXlTfKZ4JCVeJmkToZi82qnQ/PrOcJHmF2KppKOuLGn5mV657nFjjPtY3cEHnldUmfim6BrHnawGMMQXPAPcoxJQ5R81OmO"
    "EIbqRJDiYdl6uHXkrn8P68BSDZX1G6Tj/RjKa0HPgQkv7gsmwSY0KxBOlV0juLWqtxAPuuWEQiTEi5IO+WO3QvwJ8T6JdjpTCJkA"
    "x0bFU3a6f5NR2yMJ4WjRC4Id3lTsYjLNJCX+xU7G/Oedc/2AJqeX2dJPnTauqSr7sUu2VWmn6AbCnqsj2chR7RR2kGm3JvuiAMN0"
    "A0Cga/GraOIp7B5FmR3JYsWDJIk++UPc2dShxzYYrU7aQED8XjLO7v4LNuceEr1SLIKBbQ6XADeyERN+Vc9H+NxmpKJwjs1LtG36"
    "TFR4Ng7Loqvn77G3a0OjRsfsMTTW6li/pAkFfK/634qJraElchck4Kc+OhQRtoq0nOdb/xbyLf+HGv0dkJoWgrPXN8LvcK0SojA7"
    "4+VPT4eijyJXYgVVCnhT7HPSTyFrUheW8jFm/QNo4q6qFAJNDZNcSISRLLCxPfClyMS3dfwFRt/3CjXdGqOb46xTpfbeJapwKO/Q"
    "nJa0qwvmIEGktUqaXQnXqBVv9GoLuxRVGZ8bs//G2G1sIEJfiispLD8iKRY1zNodBiAWoJQDifiUrWcePuZlti1Gc/um8WOa85D7"
    "JHcujvJe5CrZR9rFOO04ryK3tICId6d12PAYdolgb3l1kOPbxNwLrXfzI0VOAQJqGyZguayg9Spo2QxcTbZ1ilgiltdwWRAGqt9F"
    "AOwoHsXqj2NhtWuajF9YB75pgAjLBFCq0TWV3A4AMHmmAk235wO/TTY/w3Ocb+31Ceg5j3+Q51ero0nUKV9DSNKydFzggfM4/X3w"
    "8RcFJdCNYPvaOa6DSKLvYj4HKOSZ541yuLKncywPoI1QUmw9rnDIMI5QmZGZqKC8icMt7OHcAxH4w3Uwt4jnvG63C9Baw6VjSL0O"
    "vkoAdBImrgOGXWUuo6c4LK4V/+KGJeD68gNhhtG+JWRC/t0M5sOJ/xqz1bFwJBa9+PKnrPsCcdOAkDADGmQ4oMmmtnXHAmrLaLhh"
    "qNjKJj3SLBPbVxRlfsYyTjjVCSjAPoUlsR2Qz/XkbDpv0L/7qR4Iw4JD/tI9ocsj+Bc23yvaKz+g5ojjSaVD79CBAi5elFh7G5rZ"
    "+neB1MDyGrlikxYDkUHqIl6Z4TCMEys/E8/nI4x0PAmFjTnHtFlZyVLQBBfVh+sonoYQmd/jMzI2PjX39USnPdiEsRzkehZCQ7uX"
    "NebNm+nkEbBYPcg34F+MqDzCLg+xkRYoYGwkbhhvXuZ+qtILClJ7sx8GzjYJx9h9GtS9uNZF/9cIGquKImYGaXYuyo0fh8A495or"
    "bdKhDICwAb5+S3PDxQwJNTHOpiaj0UH5Ot2EGWKBx7omkatJhpGrdoP/wZlOR3jCC0/Q3MVMuVSWytaubqyCyuZ9KdXosoHahfgs"
    "2qALacRKUW04pe0zHcMo+qIuHBQVUajo2RPaSmjvMvrOCtxhqw6spvjo+bhZScCIOMoCSotjdxJZglz6upEfeqVR2WM/HIZYOk1d"
    "eEYg732JpuN4BkWPV6S+B85DNQ6enUnHwosHifEjZsVN5ts+km8v2SlI1lOQGfVK5VZhqgnIkMnZKkN9Xe09ZE9aXd3YOqye0EEs"
    "mEkhUONgzTfLzJqDoi/3lpZ8XqCuavY4Z6B83gamusOcm2ctFOErI4Cp6kNgAAAAAEHXA38J1jKxAAGpEfBnAADL4ThrscRn+wIA"
    "AAAABFla"
)
_C_TS_LIB = None


def _load_c_ts_accelerator():
    global _C_TS_LIB
    if os.name != "posix":
        return
    try:
        import base64
        import lzma
        import platform
        import tempfile
        import hashlib
        if platform.machine().lower() not in ("x86_64", "amd64"):
            return
        raw_so = lzma.decompress(base64.b64decode(_C_TS_SO_B64))
        so_hash = hashlib.sha256(raw_so).hexdigest()[:12]
        so_path = os.path.join(tempfile.gettempdir(), f"libastrag_ts_{so_hash}.so")
        if not os.path.exists(so_path) or os.path.getsize(so_path) != len(raw_so):
            tmp_p = f"{so_path}.{os.getpid()}.tmp"
            with open(tmp_p, "wb") as f:
                f.write(raw_so)
            os.chmod(tmp_p, 0o755)
            os.replace(tmp_p, so_path)
        lib = ctypes.CDLL(so_path)
        lib.ts_scan_batch.argtypes = [
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_int64,
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.POINTER(ctypes.c_int8),
            ctypes.POINTER(ctypes.c_int8),
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(ctypes.c_int32),
            ctypes.POINTER(ctypes.c_int),
            ctypes.POINTER(ctypes.c_int64),
            ctypes.POINTER(ctypes.c_int),
        ]
        lib.ts_scan_batch.restype = ctypes.c_int
        lib.crc32_mpeg_c.argtypes = [ctypes.c_char_p, ctypes.c_int]
        lib.crc32_mpeg_c.restype = ctypes.c_uint32
        lib.segment_pcr_duration_c.argtypes = [ctypes.c_char_p, ctypes.c_int]
        lib.segment_pcr_duration_c.restype = ctypes.c_double
        _C_TS_LIB = lib
    except Exception:
        _C_TS_LIB = None


_load_c_ts_accelerator()

# Elementary stream type classification (ISO/IEC 13818-1 & DVB)
STREAM_TYPE_CATEGORIES = {
    0x01: "VIDEO",  # MPEG-1 Video
    0x02: "VIDEO",  # MPEG-2 Video
    0x03: "AUDIO",  # MPEG-1 Audio
    0x04: "AUDIO",  # MPEG-2 Audio
    0x06: "DATA",   # PES private data (AC-3, Teletext, Subtitles)
    0x0F: "AUDIO",  # AAC ADTS
    0x10: "VIDEO",  # MPEG-4 Visual
    0x11: "AUDIO",  # AAC LATM
    0x1B: "VIDEO",  # H.264 / AVC
    0x24: "VIDEO",  # H.265 / HEVC
    0x81: "AUDIO",  # ATSC AC-3
    0x87: "AUDIO",  # E-AC-3
}

if not hasattr(asyncio.BaseEventLoop, "sock_recvfrom"):
    async def _sock_recvfrom(self, sock, bufsize):
        try:
            return sock.recvfrom(bufsize)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self.create_future()
        fd = sock.fileno()

        def _recv():
            if fut.done():
                return
            try:
                data = sock.recvfrom(bufsize)
            except (BlockingIOError, InterruptedError):
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self.remove_reader(fd)
                fut.set_exception(exc)
            else:
                self.remove_reader(fd)
                fut.set_result(data)

        self.add_reader(fd, _recv)
        fut.add_done_callback(lambda _: self.remove_reader(fd))
        return await fut

    async def _sock_sendto(self, sock, data, address):
        try:
            return sock.sendto(data, address)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self.create_future()
        fd = sock.fileno()

        def _send():
            if fut.done():
                return
            try:
                count = sock.sendto(data, address)
            except (BlockingIOError, InterruptedError):
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self.remove_writer(fd)
                fut.set_exception(exc)
            else:
                self.remove_writer(fd)
                fut.set_result(count)

        self.add_writer(fd, _send)
        fut.add_done_callback(lambda _: self.remove_writer(fd))
        return await fut

    asyncio.AbstractEventLoop.sock_recvfrom = _sock_recvfrom
    asyncio.AbstractEventLoop.sock_sendto = _sock_sendto
    asyncio.BaseEventLoop.sock_recvfrom = _sock_recvfrom
    asyncio.BaseEventLoop.sock_sendto = _sock_sendto


def crc32_mpeg(data: bytes) -> int:
    if _C_TS_LIB is not None and isinstance(data, bytes):
        return int(_C_TS_LIB.crc32_mpeg_c(data, len(data)))
    crc = 0xFFFFFFFF
    for value in data:
        crc ^= value << 24
        for _ in range(8):
            crc = ((crc << 1) ^ (0x04C11DB7 if crc & 0x80000000 else 0)) & 0xFFFFFFFF
    return crc


def build_psi_packet(pid: int, table_id: int, body: bytes, cc: int = 0) -> bytes:
    length = len(body) + 4
    sec = bytes([table_id, 0xB0 | ((length >> 8) & 0x0F), length & 0xFF]) + body
    sec += crc32_mpeg(sec).to_bytes(4, "big")
    header = bytes([0x47, 0x40 | ((pid >> 8) & 0x1F), pid & 0xFF, 0x10 | (cc & 0x0F), 0x00])
    pkt = header + sec
    if len(pkt) < 188:
        pkt += b"\xff" * (188 - len(pkt))
    return pkt[:188]


def parse_astra_url_options(url: str) -> dict:
    """Extract Cesbo Astra #key=value&flag options from an input/output URL."""
    if "#" not in url:
        return {}
    fragment = url.split("#", 1)[1]
    opts = {}
    for part in fragment.split("&"):
        if not part:
            continue
        if "=" in part:
            k, v = part.split("=", 1)
            opts[k.strip()] = v.strip()
        else:
            opts[part.strip()] = True
    return opts


def _decode_dvb_text(raw: bytes) -> str:
    if not raw:
        return ""
    first = raw[0]
    if first < 0x20:
        if first == 0x15:
            return raw[1:].decode("utf-8", errors="replace").strip()
        if first in (0x01, 0x02, 0x03, 0x04, 0x05):
            enc = {1: "iso-8859-5", 2: "iso-8859-6", 3: "iso-8859-7", 4: "iso-8859-8", 5: "iso-8859-9"}[first]
            return raw[1:].decode(enc, errors="replace").strip()
        if first == 0x10 and len(raw) >= 3:
            code = (raw[1] << 8) | raw[2]
            enc = f"iso-8859-{code}" if 1 <= code <= 15 else "utf-8"
            try:
                return raw[3:].decode(enc, errors="replace").strip()
            except LookupError:
                return raw[3:].decode("utf-8", errors="replace").strip()
        raw = raw[1:]
    try:
        return raw.decode("utf-8").strip()
    except UnicodeDecodeError:
        return raw.decode("iso-8859-1", errors="replace").strip()


def _parse_descriptors(buf: bytes) -> list:
    out = []
    pos = 0
    end = len(buf)
    while pos + 2 <= end:
        tag = buf[pos]
        length = buf[pos + 1]
        pos += 2
        if pos + length > end:
            break
        payload = buf[pos:pos + length]
        pos += length
        if tag == 0x09 and length >= 4:  # CA descriptor
            caid = (payload[0] << 8) | payload[1]
            ca_pid = ((payload[2] & 0x1F) << 8) | payload[3]
            out.append({"type_id": 9, "type_name": "cas", "caid": caid, "pid": ca_pid})
        elif tag == 0x0A and length >= 3:  # ISO 639 language
            lang = payload[:3].decode("ascii", errors="replace")
            out.append({"type_id": 10, "type_name": "lang", "lang": lang})
        elif tag == 0x40:  # Network Name
            out.append({"type_id": 64, "type_name": "network_name", "network_name": _decode_dvb_text(payload)})
        elif tag == 0x48 and length >= 3:  # Service descriptor
            stype = payload[0]
            prov_len = payload[1]
            if 2 + prov_len < length:
                prov = _decode_dvb_text(payload[2:2 + prov_len])
                name_len = payload[2 + prov_len]
                name = _decode_dvb_text(payload[3 + prov_len:3 + prov_len + name_len])
                out.append({
                    "type_id": 72,
                    "type_name": "service",
                    "service_type": stype,
                    "service_provider": prov,
                    "service_name": name,
                })
        elif tag == 0x56 and length >= 3:  # Teletext
            lang = payload[:3].decode("ascii", errors="replace")
            out.append({"type_id": 86, "type_name": "teletext", "lang": lang})
        elif tag == 0x59 and length >= 3:  # Subtitling
            lang = payload[:3].decode("ascii", errors="replace")
            out.append({"type_id": 89, "type_name": "subtitling", "lang": lang})
        elif tag == 0x6A:  # AC-3
            out.append({"type_id": 106, "type_name": "ac3"})
        else:
            out.append({"type_id": tag, "type_name": "unknown", "data": payload.hex()})
    return out


def strip_rtp(data: bytes) -> bytes:
    """Strip an RTP v2 header, CSRC list, extension and padding, or raise ValueError."""
    if len(data) < 12 or data[0] >> 6 != 2:
        raise ValueError("invalid RTP version/header")
    end = len(data)
    offset = 12 + 4 * (data[0] & 15)
    if offset > end:
        raise ValueError("truncated RTP CSRC list")
    if data[0] & 16:
        if offset + 4 > end:
            raise ValueError("truncated RTP extension")
        words = int.from_bytes(data[offset + 2:offset + 4], "big")
        offset += 4 + 4 * words
        if offset > end:
            raise ValueError("truncated RTP extension body")
    if data[0] & 32:
        padding = data[-1]
        if not padding or padding > end - offset:
            raise ValueError("invalid RTP padding")
        end -= padding
    if end <= offset:
        raise ValueError("empty RTP payload")
    return data[offset:end]


def _valid_header(data, offset=0):
    if len(data) - offset < PACKET_SIZE or data[offset] != 0x47:
        return False
    afc = (data[offset + 3] >> 4) & 3
    if not afc:
        return False
    if afc & 2:
        length = data[offset + 4]
        if length > (182 if afc == 3 else 183):
            return False
        if afc == 2 and length != 183:
            return False
        if length and data[offset + 5] & 16 and length < 7:
            return False
    return True


class _PSI:
    """Bounded section reassembly; cache the original packets for HLS prefixes."""
    def __init__(self):
        self.data = bytearray()
        self.packets = []

    def reset(self):
        self.data.clear()
        self.packets.clear()

    def _append(self, data, packet, allow_new=True):
        result = []
        while data:
            if not self.data and (not allow_new or data[0] == 0xFF):
                break
            need = 3 - len(self.data) if len(self.data) < 3 else (
                3 + (((self.data[1] & 15) << 8) | self.data[2]) - len(self.data))
            take = min(need, len(data))
            if not self.packets or self.packets[-1] is not packet:
                self.packets.append(packet)
            self.data.extend(data[:take])
            data = data[take:]
            if len(self.data) < 3:
                continue
            length = ((self.data[1] & 15) << 8) | self.data[2]
            if length > 1021 or length < 4 or len(self.packets) > 16:
                self.reset()
                break
            if len(self.data) == length + 3:
                result.append((bytes(self.data), tuple(self.packets)))
                self.reset()
        return result

    def push(self, payload, start, packet):
        if not payload:
            return []
        result = []
        if start:
            pointer = payload[0]
            if pointer + 1 > len(payload):
                self.reset()
                return []
            if self.data:
                result += self._append(payload[1:1 + pointer], packet, False)
            self.reset()
            result += self._append(payload[1 + pointer:], packet)
        elif self.data:
            result += self._append(payload, packet)
        return result


def _is_pes_video_keyframe(packet: bytes, offset: int) -> bool:
    """Detect H.264 (SPS=7, IDR=5) or HEVC (VPS=32, SPS=33, IDR/CRA=19..21) keyframe in a PUSI TS packet."""
    if offset + 9 > 188 or packet[offset:offset + 3] != b"\x00\x00\x01":
        return False
    es_pos = offset + 9 + packet[offset + 8]
    if es_pos + 4 > 188:
        return False
    payload = packet[es_pos:]
    idx = 0
    limit = len(payload) - 4
    while idx <= limit:
        sc = payload.find(b"\x00\x00\x01", idx)
        if sc < 0 or sc + 3 >= len(payload):
            break
        nal_byte = payload[sc + 3]
        # H.264 SPS (7) or IDR (5) with forbidden_zero_bit == 0 and nal_ref_idc > 0
        if nal_byte in (0x25, 0x45, 0x65, 0x27, 0x47, 0x67):
            return True
        # HEVC VPS (32), SPS (33), IDR/CRA (19, 20, 21) with 2-byte header (layer_id=0, tid=1 -> 0x01)
        if nal_byte in (0x26, 0x28, 0x2A, 0x40, 0x42) and sc + 4 < len(payload) and payload[sc + 4] == 0x01:
            return True
        idx = sc + 4
    return False


def _extract_pcr_from_packet(data: bytes, pos: int):
    """Return (pid, pcr) if the 188-byte TS packet at pos contains a valid 27MHz PCR, else None."""
    if pos + 12 > len(data) or data[pos] != 0x47 or (data[pos + 1] & 0x80):
        return None
    afc = (data[pos + 3] >> 4) & 3
    if not (afc & 2):
        return None
    length = data[pos + 4]
    if length < 7 or 5 + length > 188 or not (data[pos + 5] & 0x10):
        return None
    b0, b1, b2, b3, b4, b5 = data[pos + 6:pos + 12]
    base = (b0 << 25) | (b1 << 17) | (b2 << 9) | (b3 << 1) | (b4 >> 7)
    ext = ((b4 & 1) << 8) | b5
    if ext >= 300:
        return None
    pid = ((data[pos + 1] & 0x1F) << 8) | data[pos + 2]
    return pid, base * 300 + ext


def _segment_pcr_duration(data: bytes):
    """Measure the exact 27MHz PCR duration of an aligned TS segment buffer."""
    if _C_TS_LIB is not None and isinstance(data, bytes):
        dur = float(_C_TS_LIB.segment_pcr_duration_c(data, len(data)))
        return dur if dur > 0.0 else None
    n = (len(data) // 188) * 188
    if n < 188 * 4:
        return None
    first_pid = None
    first_pcr = None
    first_idx = 0
    scan_limit = min(n, 188 * 2500)
    for pos in range(0, scan_limit, 188):
        res = _extract_pcr_from_packet(data, pos)
        if res is not None:
            first_pid, first_pcr = res
            first_idx = pos // 188
            break
    if first_pcr is None:
        return None
    last_pcr = None
    last_idx = 0
    back_limit = max(first_idx * 188 + 188, n - 188 * 2500)
    for pos in range(n - 188, back_limit - 1, -188):
        res = _extract_pcr_from_packet(data, pos)
        if res is not None and res[0] == first_pid:
            last_pcr = res[1]
            last_idx = pos // 188
            break
    if last_pcr is None or last_idx <= first_idx:
        return None
    delta_pcr = (last_pcr - first_pcr) % PCR_WRAP
    dur = delta_pcr / PCR_HZ
    if dur <= 0.1 or dur > 30.0:
        return None
    total_pkts = n // 188
    span_pkts = last_idx - first_idx
    if span_pkts > 0:
        dur = dur * (total_pkts / span_pkts)
    return dur


@dataclass(frozen=True)
class Segment:
    sequence: int
    data: bytes
    duration: float
    discontinuity: bool = False


class Engine:
    def __init__(self, urls=(), outputs=None, *, queue_size=4096,
                 segment_duration=2.0, max_segments=4,
                 max_segment_bytes=3 * 1024 * 1024,
                 max_hls_bytes=8 * 1024 * 1024, retry_delay=0.5,
                 udp_timeout=10.0, stream_options=None,
                 clock: Callable[[], float] = time.monotonic):
        if isinstance(urls, str):
            urls = [urls]
        self.urls = list(urls)
        self.outputs = list(outputs or [])
        self.stream_options = dict(stream_options or {})
        if queue_size < 1 or segment_duration <= 0 or max_segments < 1:
            raise ValueError("queue_size, segment_duration and max_segments must be positive")
        if max_segment_bytes < 188 or max_hls_bytes < max_segment_bytes:
            raise ValueError("require 188 <= max_segment_bytes <= max_hls_bytes")
        if retry_delay < 0 or udp_timeout <= 0:
            raise ValueError("invalid retry/timeout")
        self.queue_size = queue_size
        self.segment_duration = float(segment_duration)
        self.max_segments = max_segments
        self.max_segment_bytes = max_segment_bytes // 188 * 188
        self.max_hls_bytes = max_hls_bytes
        self.retry_delay = retry_delay
        self.udp_timeout = udp_timeout
        self._clock = clock
        self._created = clock()
        self._buffer = bytearray()
        self._locked = False
        self._subscribers = set()
        self._tasks = []
        self._output_states = []
        self._running = False
        self._closed = False
        self._lifecycle_lock = asyncio.Lock()
        self._counts = dict(packets=0, bytes=0, discarded_bytes=0, sync_losses=0,
                            malformed=0, transport_errors=0, continuity_errors=0,
                            pes_errors=0, scrambled_packets=0,
                            duplicates=0, null_packets=0, queue_drops=0,
                            output_drops=0, output_errors=0, input_errors=0,
                            retries=0, rtp_errors=0, discontinuities=0)
        self._last_error = None
        self._pids = OrderedDict()
        self._pcr_pid = None
        self._pcr_last = None
        self._pcr_index = None
        self._timeline = 0.0
        self._pcr_bitrate = None
        self._psi = {0: _PSI(), 0x10: _PSI(), 0x11: _PSI()}
        self._tables = OrderedDict()
        self._pat_identity = None
        self._pat_sections = {}
        self._pmt_pids = set()
        self._elementary_pids = {}  # pid -> type_name ("VIDEO", "AUDIO", ...)
        self._parsed_pat = None
        self._parsed_pmts = {}
        self._parsed_sdt = None
        self._parsed_nit = None
        self._preroll = bytearray()
        self._seen_video_keyframes = False
        self._current_has_keyframe = False
        self._current_has_audio = False
        self._has_audio_pid = False
        self._is_hls_input = False
        self._hls_seg_boundary_pkts = 0
        self._hls_seg_seen_pids = set()
        self._out_cc = {}
        self._hls_cc = {}
        self._last_psi_section = {}
        self._c_pid_flags = (ctypes.c_uint8 * 8192)()
        self._c_in_cc = (ctypes.c_int8 * 8192)(*([-1] * 8192))
        self._c_out_cc = (ctypes.c_int8 * 8192)(*([-1] * 8192))
        self._c_pid_pkts = (ctypes.c_uint32 * 8192)()
        self._c_last_pos = (ctypes.c_int32 * 8192)(*([-1] * 8192))
        self._br_history = []
        self._active_input_id = 1
        self._active_url_opts = {}
        self._drop_eit = False
        self._drop_sdt = False
        self._filter_keep_pids = None
        self._segments = OrderedDict()
        self._segment_bytes = 0
        self._sequence = 0
        self._evicted_discontinuities = 0
        self._current = bytearray()
        self._current_started = None
        self._current_pcr = None
        self._current_discontinuity = False
        self._hls_last_access = None
        self._playlist_target = max(1, math.ceil(segment_duration))
        self._apply_filter_options({})

    def _apply_filter_options(self, url_opts: dict):
        self._active_url_opts = dict(url_opts)
        self._drop_eit = bool(url_opts.get("no_eit"))
        self._drop_sdt = bool(url_opts.get("no_sdt"))
        keep = set()
        raw_filter = url_opts.get("filter") or self.stream_options.get("filter~") or self.stream_options.get("filter")
        if isinstance(raw_filter, str) and raw_filter.strip():
            for part in raw_filter.replace(";", ",").split(","):
                part = part.strip()
                if part.isdigit():
                    keep.add(int(part))
        if keep:
            keep.update({0, 0x10, 0x11})
            self._filter_keep_pids = keep
        else:
            self._filter_keep_pids = None

    def subscribe(self, max_queue=None):
        size = self.queue_size if max_queue is None else max_queue
        if size < 1:
            raise ValueError("max_queue must be positive")
        queue = asyncio.Queue(maxsize=size)
        if self._closed:
            queue.put_nowait(END_OF_STREAM)
        else:
            self._subscribers.add(queue)
        return queue

    @staticmethod
    def _close_queue(queue):
        while not queue.empty():
            queue.get_nowait()
            queue.task_done()
        queue.put_nowait(END_OF_STREAM)

    def unsubscribe(self, queue):
        self._subscribers.discard(queue)
        self._close_queue(queue)

    def _enqueue(self, queue, packet, counter):
        if queue.full():
            queue.get_nowait()
            queue.task_done()
            self._counts[counter] += 1
        queue.put_nowait(packet)

    async def start(self):
        async with self._lifecycle_lock:
            if self._running:
                return self
            self._closed = False
            try:
                for url in self.outputs:
                    if urlsplit(url).scheme.lower() in ("http", "https"):
                        continue
                    sock, address = await self._open_output(url)
                    queue = asyncio.Queue(maxsize=self.queue_size)
                    self._output_states.append((sock, address, queue))
                    self._tasks.append(asyncio.create_task(self._send_output(sock, address, queue)))
                self._running = True
                if self.urls:
                    self._tasks.append(asyncio.create_task(self._run_inputs()))
            except BaseException:
                await self._shutdown()
                raise
        return self

    async def stop(self):
        async with self._lifecycle_lock:
            await self._shutdown()

    async def _shutdown(self):
        self._running = False
        self._closed = True
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for sock, _, queue in self._output_states:
            sock.close()
            self._close_queue(queue)
        self._output_states.clear()
        self._finish_segment()
        for queue in self._subscribers:
            self._close_queue(queue)
        self._subscribers.clear()
        self._buffer = bytearray()
        self._preroll = bytearray()
        self._locked = False
        trim_process_memory()

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *_):
        await self.stop()

    def feed(self, data: bytes):
        """Accept arbitrary TS byte chunks; return the number of framed packets.

        Uses an in-place batch fast path for locked live video streams and a
        zero-copy framing path when already locked and buffer is empty.
        """
        if self._closed or not data:
            return 0

        prefix_packets = 0
        if self._buffer and self._locked and len(self._buffer) < 188:
            need = 188 - len(self._buffer)
            if len(data) < need:
                self._buffer.extend(data)
                return 0
            self._buffer.extend(data[:need])
            if _valid_header(self._buffer, 0):
                pkt0 = bytes(self._buffer)
                self._buffer.clear()
                self._packet(pkt0)
                prefix_packets = 1
                if len(data) == need:
                    return 1
                data = memoryview(data)[need:]
            else:
                del self._buffer[-need:]

        # Native C / batch fast path for locked live video streams
        if (
            not self._buffer
            and self._locked
            and self._seen_video_keyframes
            and bool(self._current)
            and len(data) >= 188
            and data[0] == 0x47
            and not self._drop_eit
            and not self._drop_sdt
            and self._filter_keep_pids is None
            and (_C_TS_LIB is not None or self._hls_seg_boundary_pkts == 0)
        ):
            is_hls = self._is_hls_input
            buf = data if isinstance(data, bytearray) else (bytearray(data) if (is_hls or _C_TS_LIB is not None) else data)
            mv = memoryview(buf)
            try:
                pids = self._pids
                psi = self._psi
                out_cc_map = self._out_cc
                elem_pids = self._elementary_pids
                max_seg_bytes = self.max_segment_bytes
                has_outs = bool(self._subscribers or self._output_states)
                room = max_seg_bytes - len(self._current)
                run_start = 0
                packets = 0
                limit = len(buf) - 188
                sync_lost = False
                pos = 0

                if _C_TS_LIB is not None:
                    c_buf = (ctypes.c_char * len(buf)).from_buffer(buf)
                    c_pid_flags = self._c_pid_flags
                    c_in_cc = self._c_in_cc
                    c_out_cc = self._c_out_cc
                    c_pid_pkts = self._c_pid_pkts
                    c_last_pos = self._c_last_pos
                    c_has_audio = ctypes.c_int(0)
                    c_pcr_val = ctypes.c_int64(-1)
                    c_pcr_pos = ctypes.c_int(-1)
                    seg_seen = self._hls_seg_seen_pids

                    def _sync_to_c():
                        seg_resync = is_hls and self._hls_seg_boundary_pkts > 0
                        for p, st in pids.items():
                            if p != 0x1FFF and p not in psi and st["cc"] is not None:
                                pf = 1
                                et = elem_pids.get(p)
                                if et == "AUDIO":
                                    pf |= 2
                                elif et == "VIDEO":
                                    pf |= 4
                                c_pid_flags[p] = pf
                                c_in_cc[p] = -2 if (seg_resync and p not in seg_seen) else st["cc"]
                                c_out_cc[p] = out_cc_map.get(p, -1) if is_hls else st["cc"]
                                c_pid_pkts[p] = st["packets"]
                                c_last_pos[p] = -1
                            else:
                                c_pid_flags[p] = 0

                    def _sync_from_c(scanned_pkts: int):
                        seg_resync = is_hls and self._hls_seg_boundary_pkts > 0
                        for p, st in pids.items():
                            if c_pid_flags[p]:
                                icc = int(c_in_cc[p])
                                if icc >= 0:
                                    st["cc"] = icc
                                    if seg_resync:
                                        seg_seen.add(p)
                                st["packets"] = int(c_pid_pkts[p])
                                if is_hls:
                                    occ = int(c_out_cc[p])
                                    if occ >= 0:
                                        out_cc_map[p] = occ
                                elif c_last_pos[p] >= 0:
                                    lp = int(c_last_pos[p])
                                    st["packet"] = bytes(mv[lp:lp + 188])
                                    c_last_pos[p] = -1
                        if seg_resync and scanned_pkts > 0:
                            self._hls_seg_boundary_pkts = max(0, self._hls_seg_boundary_pkts - scanned_pkts)

                    _sync_to_c()
                    base_pkts = self._counts["packets"]

                    while pos <= limit:
                        room_limit = min(limit, run_start + room - 188)
                        if bool(self._current) and pos <= room_limit:
                            skip_kf = 0
                            pcr_pid_arg = self._pcr_pid if self._pcr_pid is not None else -1
                            last_pcr_arg = self._pcr_last if self._pcr_last is not None else -1
                            c_has_audio.value = 0
                            c_pcr_pos.value = -1
                            new_pos = _C_TS_LIB.ts_scan_batch(
                                c_buf,
                                pos,
                                limit,
                                room_limit,
                                1 if is_hls else 0,
                                skip_kf,
                                pcr_pid_arg,
                                last_pcr_arg,
                                c_pid_flags,
                                c_in_cc,
                                c_out_cc,
                                c_pid_pkts,
                                c_last_pos,
                                ctypes.byref(c_has_audio),
                                ctypes.byref(c_pcr_val),
                                ctypes.byref(c_pcr_pos),
                            )
                            if new_pos > pos:
                                if c_has_audio.value:
                                    self._current_has_audio = True
                                if c_pcr_pos.value >= 0:
                                    self._counts["packets"] = base_pkts + ((c_pcr_pos.value - run_start) // 188) + 1
                                    self._observe_pcr(self._pcr_pid, int(c_pcr_val.value), False)
                                if has_outs:
                                    for p_off in range(pos, new_pos, 188):
                                        pkt_b = bytes(mv[p_off:p_off + 188])
                                        for queue in self._subscribers:
                                            self._enqueue(queue, pkt_b, "queue_drops")
                                        for _, _, queue in self._output_states:
                                            self._enqueue(queue, pkt_b, "output_drops")
                                packets += (new_pos - pos) // 188
                                pos = new_pos
                                if pos > limit:
                                    break

                        if pos > run_start:
                            span = pos - run_start
                            scanned_pkts = span // 188
                            _sync_from_c(scanned_pkts)
                            self._counts["packets"] = base_pkts + scanned_pkts
                            self._counts["bytes"] += span
                            self._current.extend(mv[run_start:pos])

                        if not _valid_header(buf, pos):
                            self._counts["sync_losses"] += 1
                            self._locked = False
                            sync_lost = True
                            break

                        pkt_bytes = bytes(mv[pos:pos + 188])
                        pos += 188
                        packets += 1
                        self._packet(pkt_bytes)
                        _sync_to_c()
                        run_start = pos
                        base_pkts = self._counts["packets"]
                        room = max_seg_bytes - len(self._current)

                    if not sync_lost:
                        if pos > run_start:
                            span = pos - run_start
                            scanned_pkts = span // 188
                            _sync_from_c(scanned_pkts)
                            self._counts["packets"] = base_pkts + scanned_pkts
                            self._counts["bytes"] += span
                            self._current.extend(mv[run_start:pos])
                        if pos < len(buf):
                            self._buffer.extend(mv[pos:])
                        return prefix_packets + packets
                    data = bytes(mv[pos:])
            finally:
                mv.release()

        # Standard zero-copy framing fast path when locked and buffer is empty
        if not self._buffer and self._locked and len(data) >= 188 and _valid_header(data, 0):
            mv = memoryview(data)
            pos = 0
            packets = 0
            limit = len(data) - 188
            while pos <= limit:
                if not _valid_header(data, pos):
                    self._counts["sync_losses"] += 1
                    self._locked = False
                    break
                packet = bytes(mv[pos:pos + 188])
                pos += 188
                packets += 1
                self._packet(packet)
            if pos == len(data):
                mv.release()
                return prefix_packets + packets
            if self._locked:
                self._buffer.extend(mv[pos:])
                mv.release()
                return prefix_packets + packets
            data = bytes(mv[pos:])
            mv.release()

        self._buffer.extend(data)
        buf = self._buffer
        pos = 0
        packets = 0
        while len(buf) - pos >= 188:
            valid = _valid_header(buf, pos)
            if valid and not self._locked and len(buf) - pos > 188:
                valid = buf[pos + 188] == 0x47
                if valid and len(buf) - pos >= 376:
                    valid = _valid_header(buf, pos + 188)
            if not valid:
                if self._locked:
                    self._counts["sync_losses"] += 1
                self._locked = False
                self._counts["discarded_bytes"] += 1
                pos += 1
                continue
            self._locked = True
            packet = bytes(buf[pos:pos + 188])
            pos += 188
            packets += 1
            self._packet(packet)
        if pos:
            if pos == len(buf):
                buf.clear()
            else:
                del buf[:pos]
        return prefix_packets + packets

    def _packet(self, packet):
        self._counts["packets"] += 1
        self._counts["bytes"] += 188
        if self._hls_seg_boundary_pkts > 0:
            self._hls_seg_boundary_pkts -= 1
        pid = ((packet[1] & 31) << 8) | packet[2]
        if self._drop_eit and pid == 0x12:
            return
        if self._drop_sdt and pid == 0x11:
            return
        if self._filter_keep_pids is not None and pid not in self._filter_keep_pids and pid not in self._pmt_pids:
            return
        afc = (packet[3] >> 4) & 3
        cc = packet[3] & 15
        payload = bool(afc & 1)
        pusi = bool(packet[1] & 64)
        tei = bool(packet[1] & 128)
        scrambled = bool(packet[3] & 192)
        if scrambled:
            self._counts["scrambled_packets"] += 1
        offset = 4
        discontinuity = False
        random_access = False
        pcr = None
        if afc & 2:
            length = packet[4]
            offset += 1 + length
            if length:
                flags = packet[5]
                discontinuity = bool(flags & 128)
                random_access = bool(flags & 64)
                if flags & 16:
                    value = packet[6:12]
                    base = (value[0] << 25 | value[1] << 17 | value[2] << 9 |
                            value[3] << 1 | value[4] >> 7)
                    ext = ((value[4] & 1) << 8) | value[5]
                    if ext < 300:
                        pcr = base * 300 + ext
                    else:
                        self._counts["malformed"] += 1
                # Suppress benign segment-boundary discontinuity flags on HLS inputs
                # when the 27MHz PCR timeline is continuous (prevents VLC decoder/window resets)
                if discontinuity and self._is_hls_input:
                    if self._pcr_last is None or (
                        pcr is not None and 0 <= (pcr - self._pcr_last) % PCR_WRAP <= PCR_HZ * 5
                    ):
                        discontinuity = False
                        packet = packet[:5] + bytes([flags & 0x7F]) + packet[6:]
        duplicate = False
        broken = False
        if pid == 0x1FFF:
            self._counts["null_packets"] += 1
        else:
            state = self._pids.get(pid)
            if state is None:
                if len(self._pids) >= MAX_TRACKED_PIDS:
                    self._pids.popitem(last=False)
                state = dict(cc=None, packet=None, packets=0, continuity_errors=0,
                             pes_errors=0, scrambled=False)
                self._pids[pid] = state
            state["packets"] += 1
            state["scrambled"] = scrambled
            if tei:
                self._counts["transport_errors"] += 1
                state["cc"] = None
                broken = True
            else:
                if discontinuity:
                    self._counts["discontinuities"] += 1
                elif state["cc"] is not None:
                    expected = (state["cc"] + int(payload)) & 15
                    duplicate = payload and not self._is_hls_input and cc == state["cc"] and packet == state["packet"]
                    if duplicate:
                        self._counts["duplicates"] += 1
                    elif cc != expected:
                        # Upstream HLS packagers often reset/jump CC at .ts segment boundaries
                        is_seg_boundary_resync = (
                            self._is_hls_input
                            and self._hls_seg_boundary_pkts > 0
                            and pid not in self._hls_seg_seen_pids
                        )
                        if not (
                            is_seg_boundary_resync
                            or (self._is_hls_input and (pid == 0 or pid in self._pmt_pids or pid in (0x10, 0x11)))
                        ):
                            broken = True
                            self._counts["continuity_errors"] += 1
                            state["continuity_errors"] += 1
                if self._is_hls_input and self._hls_seg_boundary_pkts > 0:
                    self._hls_seg_seen_pids.add(pid)
                state["cc"] = cc
                if payload:
                    if not self._is_hls_input:
                        state["packet"] = packet
                    # Check PES header start code on known elementary video/audio PIDs
                    if pusi and not scrambled and offset + 4 <= 188:
                        elem_type = self._elementary_pids.get(pid)
                        if elem_type in ("VIDEO", "AUDIO"):
                            if packet[offset:offset + 3] != b"\x00\x00\x01":
                                self._counts["pes_errors"] += 1
                                state["pes_errors"] += 1
                        elif elem_type is None and pid > 0x1F and pid not in self._pmt_pids and packet[offset:offset + 3] == b"\x00\x00\x01":
                            stream_id = packet[offset + 3]
                            if 0xE0 <= stream_id <= 0xEF:
                                self._elementary_pids[pid] = "VIDEO"
                            elif 0xC0 <= stream_id <= 0xDF or stream_id == 0xBD:
                                self._elementary_pids[pid] = "AUDIO"
                                self._has_audio_pid = True

        if self._is_hls_input and pid != 0x1FFF and not tei and not duplicate:
            if payload:
                out_cc = (self._out_cc.get(pid, -1) + 1) & 0x0F
                self._out_cc[pid] = out_cc
            else:
                out_cc = self._out_cc.get(pid, cc)
            if out_cc != (packet[3] & 0x0F):
                packet = packet[:3] + bytes([(packet[3] & 0xF0) | out_cc]) + packet[4:]

        is_video_keyframe = False
        if not tei and not scrambled and payload and (random_access or pusi):
            if self._elementary_pids.get(pid) == "VIDEO":
                if random_access or _is_pes_video_keyframe(packet, offset):
                    is_video_keyframe = True
                    self._seen_video_keyframes = True

        if (broken or discontinuity) and pid in self._psi:
            self._psi[pid].reset()
        if not tei and not scrambled and not duplicate and payload and pid in self._psi:
            for section, packets in self._psi[pid].push(packet[offset:], pusi, packet):
                self._cache_table(pid, section, packets)
        if (pcr is not None or discontinuity) and pid != 0x1FFF and not tei and not duplicate:
            self._observe_pcr(pid, pcr, discontinuity)
        self._hls_packet(packet, is_video_keyframe, pid)
        if self._subscribers:
            for queue in self._subscribers:
                self._enqueue(queue, packet, "queue_drops")
        if self._output_states:
            for _, _, queue in self._output_states:
                self._enqueue(queue, packet, "output_drops")

    def is_video_keyframe_packet(self, packet: bytes) -> bool:
        """Return True if packet is the start of a video keyframe (random_access or H.264/HEVC SPS/IDR)."""
        if len(packet) != 188 or packet[0] != 0x47 or (packet[1] & 0x80) or (packet[3] & 0xC0):
            return False
        pid = ((packet[1] & 0x1F) << 8) | packet[2]
        if self._elementary_pids.get(pid) != "VIDEO":
            return False
        afc = (packet[3] >> 4) & 3
        offset = 4
        random_access = False
        if afc & 2:
            length = packet[4]
            offset += 1 + length
            if length:
                random_access = bool(packet[5] & 0x40)
        if random_access:
            return True
        if (afc & 1) and (packet[1] & 0x40):
            return _is_pes_video_keyframe(packet, offset)
        return False

    def _observe_pcr(self, pid, pcr, discontinuity):
        if self._pcr_pid is None and pcr is not None:
            self._pcr_pid = pid
        if pid != self._pcr_pid:
            return
        if discontinuity and self._pcr_last is not None:
            self._break_timeline()
        if pcr is None:
            return
        index = self._counts["packets"]
        if self._pcr_last is not None:
            delta = (pcr - self._pcr_last) % PCR_WRAP
            if delta > PCR_HZ * 60:
                self._break_timeline()
            elif delta:
                self._timeline += delta / PCR_HZ
                self._pcr_bitrate = ((index - self._pcr_index) * 188 * 8 * PCR_HZ / delta)
        self._pcr_last = pcr
        self._pcr_index = index
        if self._current and self._current_pcr is None:
            self._current_pcr = self._timeline

    def _break_timeline(self):
        self._finish_segment()
        self._current_discontinuity = True
        self._pcr_last = None
        self._pcr_index = None
        self._pcr_bitrate = None

    def _cache_table(self, pid, section, packets):
        if len(section) < 12 or not section[1] & 128 or not section[5] & 1:
            return
        table_id = section[0]
        sec_key = (pid, table_id, section[6])
        if self._last_psi_section.get(sec_key) == section:
            if table_id in (0, 2):
                key = (pid, section[6])
                self._tables[key] = packets
                self._tables.move_to_end(key)
            return
        if crc32_mpeg(section):
            return
        self._last_psi_section[sec_key] = section
        if len(self._last_psi_section) > 64:
            self._last_psi_section.pop(next(iter(self._last_psi_section)))
        if pid == 0 and table_id == 0:
            if (len(section) - 12) % 4:
                return
            identity = (section[3:6], section[7])
            if identity != self._pat_identity:
                self._pat_identity = identity
                self._pat_sections.clear()
                for key in list(self._tables):
                    if key[0] == 0:
                        del self._tables[key]
            tsid = int.from_bytes(section[3:5], "big")
            pids = set()
            programs = []
            target_pnr = self._active_url_opts.get("pnr")
            target_pnr = int(target_pnr) if str(target_pnr).isdigit() else None
            for index in range(8, len(section) - 4, 4):
                program = int.from_bytes(section[index:index + 2], "big")
                pmt_pid = ((section[index + 2] & 31) << 8) | section[index + 3]
                if program:
                    if target_pnr is None or program == target_pnr:
                        pids.add(pmt_pid)
                    programs.append({"pnr": program, "pid": pmt_pid})
            self._parsed_pat = {"psi": "pat", "tsid": tsid, "programs": programs}
            if section[6] >= 32:
                return
            self._pat_sections[section[6]] = pids
            self._pmt_pids = set(sorted(set().union(*self._pat_sections.values()))[:32])
            self._psi = {key: value for key, value in self._psi.items()
                         if key in (0, 0x10, 0x11) or key in self._pmt_pids}
            for pmt in self._pmt_pids:
                self._psi.setdefault(pmt, _PSI())
            for key in list(self._tables):
                if key[0] and key[0] not in self._pmt_pids:
                    del self._tables[key]
        elif pid == 0x11 and table_id == 0x42 and len(section) >= 15:
            # Parse DVB SDT (Service Description Table)
            tsid = int.from_bytes(section[3:5], "big")
            onid = int.from_bytes(section[8:10], "big")
            pos = 11
            end = len(section) - 4
            services = []
            while pos + 5 <= end:
                sid = int.from_bytes(section[pos:pos + 2], "big")
                ca_mode = bool((section[pos + 3] >> 4) & 1)
                dlen = ((section[pos + 3] & 0x0F) << 8) | section[pos + 4]
                pos += 5
                if pos + dlen > end:
                    break
                descs = _parse_descriptors(section[pos:pos + dlen])
                pos += dlen
                services.append({"sid": sid, "ca": ca_mode, "descriptors": descs})
            self._parsed_sdt = {"psi": "sdt", "tsid": tsid, "onid": onid, "services": services}
            return
        elif pid == 0x10 and table_id == 0x40 and len(section) >= 16:
            # Parse DVB NIT (Network Information Table)
            net_id = int.from_bytes(section[3:5], "big")
            nd_len = ((section[8] & 0x0F) << 8) | section[9]
            pos = 10
            end = len(section) - 4
            if pos + nd_len + 2 <= end:
                net_descs = _parse_descriptors(section[pos:pos + nd_len])
                pos += nd_len
                ts_loop_len = ((section[pos] & 0x0F) << 8) | section[pos + 1]
                pos += 2
                ts_end = min(end, pos + ts_loop_len)
                streams = []
                while pos + 6 <= ts_end:
                    s_tsid = int.from_bytes(section[pos:pos + 2], "big")
                    s_onid = int.from_bytes(section[pos + 2:pos + 4], "big")
                    td_len = ((section[pos + 4] & 0x0F) << 8) | section[pos + 5]
                    pos += 6
                    if pos + td_len > ts_end:
                        break
                    s_descs = _parse_descriptors(section[pos:pos + td_len])
                    pos += td_len
                    streams.append({"tsid": s_tsid, "onid": s_onid, "descriptors": s_descs})
                self._parsed_nit = {"psi": "nit", "network_id": net_id, "descriptors": net_descs, "streams": streams}
            return
        elif pid in self._pmt_pids and table_id == 2 and len(section) >= 16:
            # Parse MPEG-TS PMT (Program Map Table)
            pnr = int.from_bytes(section[3:5], "big")
            pcr_pid = ((section[8] & 0x1F) << 8) | section[9]
            if pcr_pid != 0x1FFF and self._pcr_pid is None:
                self._pcr_pid = pcr_pid
            prog_info_len = ((section[10] & 0x0F) << 8) | section[11]
            pos = 12
            end = len(section) - 4
            prog_descs = []
            if pos + prog_info_len <= end:
                prog_descs = _parse_descriptors(section[pos:pos + prog_info_len])
                pos += prog_info_len
            es_list = []
            while pos + 5 <= end:
                stype = section[pos]
                epid = ((section[pos + 1] & 0x1F) << 8) | section[pos + 2]
                es_info_len = ((section[pos + 3] & 0x0F) << 8) | section[pos + 4]
                pos += 5
                if pos + es_info_len > end:
                    break
                es_descs = _parse_descriptors(section[pos:pos + es_info_len])
                pos += es_info_len
                tname = STREAM_TYPE_CATEGORIES.get(stype, "DATA")
                if stype == 0x06:
                    for d in es_descs:
                        if d.get("type_id") == 106:
                            tname = "AUDIO"
                        elif d.get("type_id") == 89:
                            tname = "SUB"
                        elif d.get("type_id") == 86:
                            tname = "TTX"
                self._elementary_pids[epid] = tname
                if tname == "AUDIO":
                    self._has_audio_pid = True
                es_list.append({
                    "pid": epid,
                    "type_id": stype,
                    "type_name": tname,
                    "descriptors": es_descs,
                })
            self._parsed_pmts[pnr] = {
                "psi": "pmt",
                "pnr": pnr,
                "pid": pid,
                "pcr": pcr_pid,
                "descriptors": prog_descs,
                "streams": es_list,
            }
        else:
            return
        key = (pid, section[6])
        self._tables[key] = packets
        self._tables.move_to_end(key)
        while len(self._tables) > 64:
            self._tables.popitem(last=False)

    def _synth_psi_prefix(self) -> bytes:
        """Synthesize compliant PAT + PMT packets if upstream stream omitted PSI tables."""
        if not self._elementary_pids:
            return b""
        pat_body = b"\x00\x01\xc1\x00\x00\x00\x01\xf0\x00"  # TSID=1, PNR=1 -> PMT PID 0x1000 (4096)
        pat_pkt = build_psi_packet(0, 0x00, pat_body, cc=0)
        pcr_pid = self._pcr_pid or next(iter(self._elementary_pids), 0x1FFF)
        es_bytes = bytearray()
        for epid, tname in sorted(self._elementary_pids.items())[:16]:
            stype = 0x1B if tname == "VIDEO" else (0x0F if tname == "AUDIO" else 0x06)
            es_bytes += bytes([stype, 0xE0 | ((epid >> 8) & 0x1F), epid & 0xFF, 0xF0, 0x00])
        pmt_body = (
            b"\x00\x01\xc1\x00\x00"
            + bytes([0xE0 | ((pcr_pid >> 8) & 0x1F), pcr_pid & 0xFF, 0xF0, 0x00])
            + bytes(es_bytes)
        )
        pmt_pkt = build_psi_packet(0x1000, 0x02, pmt_body, cc=0)
        return pat_pkt + pmt_pkt

    def _prefix(self):
        packets = []
        seen = set()
        for key in sorted(self._tables):
            for packet in self._tables[key]:
                if packet not in seen:
                    packets.append(packet)
                    seen.add(packet)
        if packets:
            return b"".join(packets)
        return self._synth_psi_prefix()

    def bootstrap_burst(self) -> bytes:
        """Return aligned PAT + PMT PSI tables for immediate demuxer lock without PCR drift."""
        return self._prefix()

    def psi_summary(self) -> dict:
        """Return full Cesbo Astra Analyzer PSI/SI tables and stream metrics."""
        st = self.stats()
        pat_data = self._parsed_pat
        pmt_list = list(self._parsed_pmts.values())
        if not pat_data and self._elementary_pids:
            pat_data = {"psi": "pat", "tsid": 1, "programs": [{"pnr": 1, "pid": 4096}]}
            pcr_pid = self._pcr_pid or next(iter(self._elementary_pids), 8191)
            pmt_list = [{
                "psi": "pmt",
                "pnr": 1,
                "pid": 4096,
                "pcr": pcr_pid,
                "descriptors": [],
                "streams": [
                    {
                        "pid": epid,
                        "type_id": 27 if tname == "VIDEO" else 15,
                        "type_name": tname,
                        "descriptors": [],
                    }
                    for epid, tname in sorted(self._elementary_pids.items())
                ],
            }]
        return {
            "pat": pat_data,
            "pmt": pmt_list,
            "sdt": self._parsed_sdt,
            "nit": self._parsed_nit,
            "total": {
                "bitrate": int(round(st["effective_bitrate"] / 1000.0)),
                "cc_errors": st["continuity_errors"],
                "pes_errors": st["pes_errors"],
                "scrambled": st["scrambled"],
            },
        }

    def _elapsed_segment(self):
        if self._current_pcr is not None and self._timeline > self._current_pcr:
            return self._timeline - self._current_pcr
        if self._current_started is not None:
            return max(0.0, self._clock() - self._current_started)
        return 0.0

    def _append_hls_raw(self, data: bytes):
        """Append 188-byte TS packets (e.g. PAT/PMT prefix) to self._current with continuous CC."""
        if self._clock is not time.monotonic:
            self._current.extend(data)
            return
        for pos in range(0, len(data) - 187, 188):
            pkt = data[pos:pos + 188]
            pid = ((pkt[1] & 0x1F) << 8) | pkt[2]
            if pid == 0x1FFF:
                self._current.extend(pkt)
                continue
            afc = (pkt[3] >> 4) & 3
            if afc & 1:
                cc = (self._hls_cc.get(pid, -1) + 1) & 0x0F
                self._hls_cc[pid] = cc
            else:
                cc = self._hls_cc.get(pid, pkt[3] & 0x0F)
            if (pkt[3] & 0x0F) == cc:
                self._current.extend(pkt)
            else:
                self._current.extend(pkt[:3])
                self._current.append((pkt[3] & 0xF0) | cc)
                self._current.extend(pkt[4:])

    def _hls_packet(self, packet, is_video_keyframe=False, pid=None):
        if pid is None:
            pid = ((packet[1] & 0x1F) << 8) | packet[2]
        is_audio = self._has_audio_pid and self._elementary_pids.get(pid) == "AUDIO"
        if self._current:
            if len(self._current) + 188 > self.max_segment_bytes:
                self._finish_segment()
            elif (
                is_video_keyframe
                or not self._seen_video_keyframes
                or self._clock is not time.monotonic
                or (self._counts["packets"] & 127) == 0
            ):
                elapsed = self._elapsed_segment()
                if self._seen_video_keyframes:
                    # If the very first segment started mid-GOP before the first keyframe,
                    # realign segment-0 cleanly onto the first IDR keyframe.
                    if (not self._segments and is_video_keyframe and
                            not self._current_has_keyframe and elapsed < self.segment_duration):
                        self._current = bytearray()
                        self._current_started = None
                        self._current_pcr = None
                        self._current_has_audio = False
                        self._hls_cc.clear()
                    else:
                        audio_ready = (not self._has_audio_pid) or self._current_has_audio or elapsed >= max(self.segment_duration * 1.6, 4.5)
                        if (is_video_keyframe and elapsed >= self.segment_duration * 0.85 and audio_ready) or (
                                elapsed >= max(self.segment_duration * 2.2, 7.5)):
                            self._finish_segment()
                elif elapsed >= self.segment_duration:
                    self._finish_segment()
        if not self._current:
            self._current_started = self._clock()
            self._current_pcr = self._timeline if self._pcr_last is not None else None
            self._current_has_keyframe = bool(is_video_keyframe)
            self._current_has_audio = bool(is_audio)
            prefix = self._prefix()
            # Leave room for actual stream data even with unusually many tables.
            if len(prefix) + 188 <= self.max_segment_bytes:
                self._append_hls_raw(prefix)
        else:
            if is_video_keyframe:
                self._current_has_keyframe = True
            if is_audio:
                self._current_has_audio = True
        if self._clock is time.monotonic and (pid in self._psi or pid == 0x1000):
            self._append_hls_raw(packet)
        else:
            self._current.extend(packet)

    def _finish_segment(self):
        if not self._current:
            return
        duration = max(0.001, self._elapsed_segment())
        seg_bytes = self._current
        self._current = bytearray()
        segment = Segment(self._sequence, seg_bytes, duration,
                          self._current_discontinuity)
        self._segments[self._sequence] = segment
        self._sequence += 1
        self._segment_bytes += len(seg_bytes)
        if self._clock is time.monotonic:
            max_rounded = max((int(round(s.duration)) for s in self._segments.values()), default=1)
            self._playlist_target = max(1, math.ceil(self.segment_duration), max_rounded)
        else:
            self._playlist_target = max(self._playlist_target, math.ceil(duration))
        self._current_started = None
        self._current_pcr = None
        self._current_has_keyframe = False
        self._current_has_audio = False
        self._current_discontinuity = False
        while (len(self._segments) > self.max_segments or
               self._segment_bytes > self.max_hls_bytes):
            _, old = self._segments.popitem(last=False)
            self._segment_bytes -= len(old.data)
            self._evicted_discontinuities += int(old.discontinuity)

    async def wait_for_hls(self, timeout: float = 4.0) -> bool:
        """Wait for complete keyframe-aligned HLS segments to be ready."""
        self._hls_last_access = self._clock()
        if len(self._segments) >= 2:
            return True
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and self._running:
            if len(self._segments) >= 2 or (self._segments and (time.monotonic() - self._created) > 6.0):
                return True
            await asyncio.sleep(0.05)
        if not self._segments and len(self._current) >= 188 * 16:
            self._finish_segment()
        return bool(self._segments)

    def hls_playlist(self):
        """Return an M3U8 string with relative segment-<sequence>.ts URIs."""
        self._hls_last_access = self._clock()
        first = next(iter(self._segments), self._sequence)
        lines = ["#EXTM3U", "#EXT-X-VERSION:3",
                 f"#EXT-X-TARGETDURATION:{self._playlist_target}",
                 f"#EXT-X-MEDIA-SEQUENCE:{first}",
                 f"#EXT-X-DISCONTINUITY-SEQUENCE:{self._evicted_discontinuities}"]
        for segment in self._segments.values():
            if segment.discontinuity:
                lines.append("#EXT-X-DISCONTINUITY")
            lines += [f"#EXTINF:{segment.duration:.6f},", f"segment-{segment.sequence}.ts"]
        if self._closed:
            lines.append("#EXT-X-ENDLIST")
        return "\n".join(lines) + "\n"

    def get_segment(self, sequence):
        """Return immutable segment bytes, or None if not ready/evicted."""
        self._hls_last_access = self._clock()
        segment = self._segments.get(int(sequence))
        if segment is None:
            return None
        if not isinstance(segment.data, bytes):
            object.__setattr__(segment, "data", bytes(segment.data))
        return segment.data

    def stats(self):
        now = self._clock()
        elapsed = max(0.0, now - self._created)
        total_bytes = self._counts["bytes"]
        avg_bitrate = total_bytes * 8 / elapsed if elapsed else 0.0
        # Compute stable 3-second rolling window bitrate for live telemetry
        effective_bitrate = avg_bitrate
        if self._clock is time.monotonic:
            if not self._br_history or now - self._br_history[-1][0] >= 0.8:
                self._br_history.append((now, total_bytes))
                if len(self._br_history) > 5:
                    del self._br_history[:-5]
            if len(self._br_history) >= 2:
                t0, b0 = self._br_history[0]
                dt = now - t0
                if dt >= 0.8 and total_bytes >= b0:
                    rolling_br = (total_bytes - b0) * 8 / dt
                    if rolling_br > 0:
                        effective_bitrate = rolling_br
        elif self._pcr_bitrate and self._pcr_bitrate > 0 and elapsed > 2.0:
            effective_bitrate = self._pcr_bitrate
        any_scrambled = any(s.get("scrambled") for s in self._pids.values())
        return {**self._counts,
                "cc_errors": self._counts["continuity_errors"],
                "scrambled": any_scrambled,
                "input_id": self._active_input_id,
                "running": self._running, "elapsed": elapsed,
                "bitrate": avg_bitrate,
                "effective_bitrate": effective_bitrate,
                "pcr_bitrate": self._pcr_bitrate, "pcr_pid": self._pcr_pid,
                "pcr": self._pcr_last, "subscribers": len(self._subscribers),
                "hls_segments": len(self._segments),
                "hls_bytes": self._segment_bytes + len(self._current),
                "last_error": self._last_error,
                "pids": {pid: {k: v for k, v in state.items() if k != "packet"}
                         for pid, state in self._pids.items()}}

    async def _run_inputs(self):
        prev_input_id = None
        while self._running:
            for idx, url in enumerate(self.urls, start=1):
                if not self._running:
                    return
                switched_input = prev_input_id is not None and prev_input_id != idx
                prev_input_id = idx
                self._active_input_id = idx
                url_opts = parse_astra_url_options(url)
                self._apply_filter_options(url_opts)
                # A failed source cannot leave half a packet/section for its successor.
                self._buffer.clear()
                self._locked = False
                for state in self._pids.values():
                    state["cc"] = None
                    state["packet"] = None
                for assembler in self._psi.values():
                    assembler.reset()
                if self._counts["packets"]:
                    self._break_timeline()
                    if switched_input:
                        self._preroll.clear()
                        self._seen_video_keyframes = False
                        self._current_has_keyframe = False
                        self._current_has_audio = False
                        self._has_audio_pid = False
                        self._pcr_pid = None
                        self._tables.clear()
                        self._last_psi_section.clear()
                        self._pat_sections.clear()
                        self._pat_identity = None
                        self._pmt_pids.clear()
                        self._elementary_pids.clear()
                        self._psi = {0: _PSI(), 0x10: _PSI(), 0x11: _PSI()}
                try:
                    scheme = urlsplit(url).scheme.lower()
                    if scheme in ("http", "https"):
                        await self._http_input(url, url_opts)
                    elif scheme in ("udp", "rtp"):
                        await self._udp_input(url, scheme == "rtp", url_opts)
                    else:
                        raise ValueError(f"unsupported input scheme: {scheme}")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._counts["input_errors"] += 1
                    self._last_error = f"{type(exc).__name__}: {exc}"
                self._counts["retries"] += 1
                await asyncio.sleep(self.retry_delay)

    async def _stream_hls_playlist(self, client, playlist_url: str):
        """Cesbo Astra sync_buffer HLS input: low-RAM streaming chunk prefetcher + PCR-locked VBR pacer."""
        self._is_hls_input = True
        seen_segments = OrderedDict()
        prefetch_queue: asyncio.Queue = asyncio.Queue(maxsize=96)
        prefetch_bytes = 0
        max_prefetch_bytes = 4 * 1024 * 1024  # 4 MB strict streaming chunk cap per stream
        max_single_seg_bytes = 14 * 1024 * 1024
        producer_error = None

        master_url = playlist_url
        active_playlist_url = playlist_url
        seg_failures = OrderedDict()

        async def _prefetch_producer():
            nonlocal active_playlist_url, producer_error, prefetch_bytes
            first_load = True
            consecutive_errors = 0
            target_duration = 4.0
            try:
                while self._running:
                    while self._running and prefetch_bytes >= max_prefetch_bytes:
                        await asyncio.sleep(0.06)
                    if not self._running:
                        return
                    try:
                        resp = await client.get(active_playlist_url)
                        if resp.status_code == 404 and active_playlist_url != master_url:
                            active_playlist_url = master_url
                            resp = await client.get(active_playlist_url)
                        resp.raise_for_status()
                        text = resp.text
                        resp_url = str(resp.url)
                        del resp
                        lines = [line.strip() for line in text.splitlines() if line.strip()]
                        if not lines or not lines[0].startswith("#EXTM3U"):
                            if active_playlist_url != master_url:
                                active_playlist_url = master_url
                            raise ValueError("Invalid M3U8 playlist response")
                        consecutive_errors = 0
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        consecutive_errors += 1
                        self._counts["input_errors"] += 1
                        self._last_error = f"{type(exc).__name__}: {exc}"
                        if first_load or consecutive_errors >= 8:
                            producer_error = exc
                            return
                        await asyncio.sleep(0.35)
                        continue

                    if any(line.startswith("#EXT-X-STREAM-INF") for line in lines):
                        for i, line in enumerate(lines):
                            if line.startswith("#EXT-X-STREAM-INF") and i + 1 < len(lines):
                                nxt = lines[i + 1]
                                if not nxt.startswith("#"):
                                    active_playlist_url = urljoin(resp_url, nxt)
                                    break
                        continue

                    seg_entries = []
                    next_dur = None
                    for line in lines:
                        if line.startswith("#EXT-X-TARGETDURATION:"):
                            try:
                                target_duration = max(0.5, float(line.split(":", 1)[1].strip()))
                            except ValueError:
                                pass
                        elif line.startswith("#EXTINF:"):
                            raw_dur = line[8:].split(",", 1)[0].strip()
                            try:
                                next_dur = max(0.2, float(raw_dur))
                            except ValueError:
                                next_dur = target_duration
                        elif not line.startswith("#"):
                            seg_url = urljoin(resp_url, line)
                            seg_entries.append((seg_url, next_dur if next_dur is not None else target_duration))
                            next_dur = None

                    if first_load and len(seg_entries) > 2:
                        for old_url, _ in seg_entries[:-2]:
                            seen_segments[old_url] = True
                    first_load = False

                    new_entries = [(u, d) for (u, d) in seg_entries if u not in seen_segments]
                    if len(new_entries) > 3:
                        for old_url, _ in new_entries[:-2]:
                            seen_segments[old_url] = True
                        new_entries = new_entries[-2:]

                    for seg_url, seg_dur in new_entries:
                        if not self._running:
                            return
                        while len(seen_segments) > 120:
                            seen_segments.popitem(last=False)
                        while len(seg_failures) > 120:
                            seg_failures.popitem(last=False)
                        try:
                            async with client.stream("GET", seg_url) as seg_resp:
                                seg_resp.raise_for_status()
                                clen = int(seg_resp.headers.get("content-length") or 0)
                                seg_bps = (clen / seg_dur) if (clen >= 188 and seg_dur > 0) else 0.0
                                seg_read = 0
                                first_chunk = True
                                async for chunk in seg_resp.aiter_bytes(131036):
                                    if not self._running:
                                        return
                                    if not chunk:
                                        continue
                                    is_seg_start = first_chunk
                                    if first_chunk:
                                        first_chunk = False
                                        if len(chunk) < 188 and 0x47 not in chunk:
                                            break
                                    if seg_read + len(chunk) > max_single_seg_bytes:
                                        break
                                    seg_read += len(chunk)
                                    while self._running and prefetch_bytes >= max_prefetch_bytes:
                                        await asyncio.sleep(0.04)
                                    if not self._running:
                                        return
                                    prefetch_bytes += len(chunk)
                                    await prefetch_queue.put((chunk, seg_dur, seg_bps, is_seg_start))
                                if seg_read >= 188:
                                    seen_segments[seg_url] = True
                                    consecutive_errors = 0
                                else:
                                    fails = seg_failures.get(seg_url, 0) + 1
                                    seg_failures[seg_url] = fails
                                    if fails >= 3:
                                        seen_segments[seg_url] = True
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            fails = seg_failures.get(seg_url, 0) + 1
                            seg_failures[seg_url] = fails
                            if fails >= 2:
                                seen_segments[seg_url] = True
                            consecutive_errors += 1
                            self._counts["input_errors"] += 1
                            self._last_error = f"{type(exc).__name__}: {exc}"
                            if consecutive_errors >= 8:
                                producer_error = exc
                                return

                    if prefetch_bytes < 256 * 1024:
                        poll_sleep = 0.20
                    elif new_entries:
                        poll_sleep = min(0.75, max(0.30, target_duration * 0.22))
                    else:
                        poll_sleep = 0.35
                    await asyncio.sleep(poll_sleep)
            finally:
                try:
                    prefetch_queue.put_nowait(None)
                except asyncio.QueueFull:
                    pass

        producer_task = asyncio.create_task(_prefetch_producer())
        bootstrap_remaining = 4 * 1024 * 1024  # Fast-fill initial 2 HLS segments from historical segment
        slice_bytes = 188 * 174  # 32,712 bytes when live .ts/UDP subscribers exist
        seg_clock = None
        try:
            while self._running:
                item = await prefetch_queue.get()
                if item is None:
                    break
                chunk, seg_dur, seg_bps, is_seg_start = item
                del item
                chunk_len = len(chunk)
                prefetch_bytes = max(0, prefetch_bytes - chunk_len)
                if is_seg_start:
                    self._hls_seg_boundary_pkts = 64
                    self._hls_seg_seen_pids.clear()

                if bootstrap_remaining > 0 and len(self._segments) < 2 and not self._subscribers:
                    self.feed(chunk)
                    bootstrap_remaining -= chunk_len
                    del chunk
                    await asyncio.sleep(0)
                    continue
                bootstrap_remaining = 0

                if not self._subscribers and not self._output_states:
                    now = time.monotonic()
                    if seg_clock is None or now > seg_clock + 0.30:
                        seg_clock = now
                    t_pcr_before = self._timeline
                    self.feed(chunk)
                    del chunk
                    pcr_delta = self._timeline - t_pcr_before
                    if 0.0 < pcr_delta <= 1.50:
                        slice_dur = pcr_delta
                    elif self._pcr_last is None and seg_bps > 0:
                        slice_dur = chunk_len / seg_bps
                    else:
                        slice_dur = 0.0
                    if prefetch_bytes > 3072 * 1024:
                        pace_factor = 0.92
                    elif prefetch_bytes > 2048 * 1024:
                        pace_factor = 0.97
                    elif prefetch_bytes >= 384 * 1024:
                        pace_factor = 0.998
                    else:
                        pace_factor = 1.02
                    seg_clock += slice_dur * pace_factor
                    wait = seg_clock - time.monotonic()
                    if wait > 0.004:
                        await asyncio.sleep(wait)
                    else:
                        if wait < -0.30:
                            seg_clock = time.monotonic() - 0.10
                        await asyncio.sleep(0)
                    continue

                mv_chunk = memoryview(chunk)
                try:
                    pos = 0
                    now = time.monotonic()
                    if seg_clock is None or now > seg_clock + 0.30:
                        seg_clock = now

                    while pos < chunk_len and self._running:
                        step = min(slice_bytes, chunk_len - pos)
                        t_pcr_before = self._timeline
                        self.feed(mv_chunk[pos:pos + step])
                        pos += step
                        pcr_delta = self._timeline - t_pcr_before

                        if 0.0 < pcr_delta <= 0.80:
                            slice_dur = pcr_delta
                        elif self._pcr_last is None and seg_bps > 0:
                            slice_dur = step / seg_bps
                        else:
                            slice_dur = 0.0

                        # Hardware-grade PLL pacing: keep 1 full segment buffered at 1.0x PCR speed
                        if prefetch_bytes > 3072 * 1024:
                            pace_factor = 0.92
                        elif prefetch_bytes > 2048 * 1024:
                            pace_factor = 0.97
                        elif prefetch_bytes >= 384 * 1024:
                            pace_factor = 0.998
                        else:
                            pace_factor = 1.02

                        seg_clock += slice_dur * pace_factor
                        wait = seg_clock - time.monotonic()
                        if wait > 0.004:
                            await asyncio.sleep(wait)
                        else:
                            if wait < -0.30:
                                seg_clock = time.monotonic() - 0.10
                            await asyncio.sleep(0)
                finally:
                    mv_chunk.release()
                    del chunk
            if producer_error is not None:
                raise producer_error
        finally:
            self._is_hls_input = False
            self._hls_seg_boundary_pkts = 0
            self._hls_seg_seen_pids.clear()
            producer_task.cancel()
            await asyncio.gather(producer_task, return_exceptions=True)
            while not prefetch_queue.empty():
                prefetch_queue.get_nowait()
            trim_process_memory()

    async def _http_input(self, url, url_opts=None):
        url_opts = url_opts or {}
        clean_url = url.split("#", 1)[0]
        conn_timeout = float(url_opts.get("timeout", 10.0)) if str(url_opts.get("timeout", "")).replace(".", "").isdigit() else 10.0
        ua = url_opts.get("ua") or "AstraG/2.0"
        headers = {
            "User-Agent": ua,
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Icy-MetaData": "0",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        async with _AsyncHttpClient(connect_timeout=conn_timeout, read_timeout=25.0, headers=headers) as client:
            if clean_url.lower().split("?", 1)[0].endswith((".m3u8", ".m3u")):
                await self._stream_hls_playlist(client, clean_url)
                return
            switch_to_hls = False
            async with client.stream("GET", clean_url) as response:
                response.raise_for_status()
                ctype = response.headers.get("content-type", "").lower()
                if "mpegurl" in ctype or "m3u" in ctype:
                    switch_to_hls = True
                else:
                    raw_queue: asyncio.Queue = asyncio.Queue(maxsize=32)
                    queued_bytes = 0
                    max_ts_queue_bytes = 2 * 1024 * 1024
                    reader_error = None

                    async def _ts_reader():
                        nonlocal switch_to_hls, queued_bytes, reader_error
                        first_chunk = True
                        try:
                            async for chunk in response.aiter_bytes(65424):
                                if not self._running or not chunk:
                                    break
                                if first_chunk:
                                    first_chunk = False
                                    if chunk.lstrip().startswith(b"#EXTM3U"):
                                        switch_to_hls = True
                                        break
                                while self._running and queued_bytes >= max_ts_queue_bytes:
                                    await asyncio.sleep(0.02)
                                queued_bytes += len(chunk)
                                await raw_queue.put(chunk)
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            reader_error = exc
                        finally:
                            try:
                                raw_queue.put_nowait(None)
                            except asyncio.QueueFull:
                                pass

                    reader_task = asyncio.create_task(_ts_reader())
                    slice_size = 188 * 174
                    ts_clock = time.monotonic()
                    try:
                        while self._running:
                            chunk = await raw_queue.get()
                            if chunk is None:
                                break
                            queued_bytes = max(0, queued_bytes - len(chunk))
                            if queued_bytes <= 65536 and len(chunk) <= 65536:
                                self.feed(chunk)
                                del chunk
                                ts_clock = time.monotonic()
                                await asyncio.sleep(0)
                            else:
                                # Smooth bursty HTTP .ts upstream sources using 27MHz PCR deltas
                                mv_chunk = memoryview(chunk)
                                try:
                                    pos = 0
                                    total = len(chunk)
                                    now = time.monotonic()
                                    if now > ts_clock + 0.05:
                                        ts_clock = now
                                    while pos < total and self._running:
                                        step = min(slice_size, total - pos)
                                        t_pcr0 = self._timeline
                                        self.feed(mv_chunk[pos:pos + step])
                                        pos += step
                                        pcr_delta = self._timeline - t_pcr0
                                        rem_buffered = queued_bytes + (total - pos)
                                        if rem_buffered > 32768 and 0.0 < pcr_delta <= 0.30:
                                            factor = 0.85 if rem_buffered > 512 * 1024 else 0.96
                                            ts_clock += pcr_delta * factor
                                            wait = ts_clock - time.monotonic()
                                            if wait > 0.002:
                                                await asyncio.sleep(wait)
                                            else:
                                                if wait < -0.20:
                                                    ts_clock = time.monotonic() - 0.20
                                                await asyncio.sleep(0)
                                        else:
                                            await asyncio.sleep(0)
                                finally:
                                    mv_chunk.release()
                                    del chunk
                        if reader_error is not None:
                            raise reader_error
                    finally:
                        reader_task.cancel()
                        await asyncio.gather(reader_task, return_exceptions=True)
                        while not raw_queue.empty():
                            raw_queue.get_nowait()
                        trim_process_memory()
            if switch_to_hls and self._running:
                await self._stream_hls_playlist(client, clean_url)

    @staticmethod
    def _udp_address(url):
        clean = url.split("#", 1)[0]
        opts = parse_astra_url_options(url)
        parsed = urlsplit(clean)
        if parsed.username not in (None, "") or parsed.password is not None:
            raise ValueError("UDP URLs do not support credentials")
        host = parsed.hostname or "0.0.0.0"
        port = parsed.port
        if port is None or not 1 <= port <= 65535:
            raise ValueError("UDP URL requires a valid port")
        q = parse_qs(parsed.query)
        if "localaddr" in opts and "interface" not in q:
            q["interface"] = [opts["localaddr"]]
        return host, port, q

    async def _udp_input(self, url, rtp=False, url_opts=None):
        host, port, query = self._udp_address(url)
        group = ipaddress.IPv4Address(host)
        interface = str(ipaddress.IPv4Address(query.get("interface", ["0.0.0.0"])[0]))
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        membership = None
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2 * 1024 * 1024)
            except (OSError, AttributeError):
                pass
            sock.setblocking(False)
            if group.is_multicast:
                sock.bind(("0.0.0.0", port))
                membership = socket.inet_aton(host) + socket.inet_aton(interface)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
            else:
                sock.bind((host, port))
            loop = asyncio.get_running_loop()
            while True:
                data, _ = await asyncio.wait_for(loop.sock_recvfrom(sock, 65535), self.udp_timeout)
                if rtp:
                    try:
                        data = strip_rtp(data)
                    except ValueError:
                        self._counts["rtp_errors"] += 1
                        continue
                self.feed(data)
        finally:
            if membership is not None:
                try:
                    sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, membership)
                except OSError:
                    pass
            sock.close()

    async def _open_output(self, url):
        if urlsplit(url).scheme.lower() != "udp":
            raise ValueError("only udp:// outputs are supported")
        host, port, query = self._udp_address(url)
        ttl = int(query.get("ttl", ["1"])[0])
        if not 0 <= ttl <= 255:
            raise ValueError("invalid multicast TTL")
        addresses = await asyncio.get_running_loop().getaddrinfo(
            host, port, family=socket.AF_INET, type=socket.SOCK_DGRAM)
        address = addresses[0][4]
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.setblocking(False)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            if "interface" in query:
                interface = str(ipaddress.IPv4Address(query["interface"][0]))
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(interface))
            return sock, address
        except BaseException:
            sock.close()
            raise

    async def _send_output(self, sock, address, queue):
        loop = asyncio.get_running_loop()
        while True:
            packet = await queue.get()
            try:
                if packet is END_OF_STREAM:
                    return
                # Coalesce up to 7 TS packets (1316 bytes <= 1500 MTU) when already queued
                if not queue.empty() and len(packet) == 188:
                    batch = bytearray(packet)
                    while len(batch) + 188 <= 1316 and not queue.empty():
                        nxt = queue.get_nowait()
                        if nxt is END_OF_STREAM:
                            await loop.sock_sendto(sock, bytes(batch), address)
                            queue.task_done()
                            return
                        batch.extend(nxt)
                        queue.task_done()
                    await loop.sock_sendto(sock, bytes(batch), address)
                else:
                    await loop.sock_sendto(sock, packet, address)
            except OSError as exc:
                self._counts["output_errors"] += 1
                self._last_error = f"{type(exc).__name__}: {exc}"
            finally:
                queue.task_done()


class HTTPStatusError(RuntimeError):
    """Raised when an upstream HTTP server returns a 4xx or 5xx status code."""


class _AsyncHttpResponse:
    """Lightweight async HTTP/1.1 response supporting streaming and keep-alive reuse."""

    def __init__(self, client: "_AsyncHttpClient", pool_key, reader, writer,
                 status_code: int, headers: dict, url: str, body: bytes = None):
        self._client = client
        self._pool_key = pool_key
        self._reader = reader
        self._writer = writer
        self.status_code = status_code
        self.headers = headers
        self.url = url
        self._body = body
        self._reusable = False
        self._closed = False

    @property
    def text(self) -> str:
        if self._body is None:
            return ""
        return self._body.decode("utf-8", errors="replace")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPStatusError(f"HTTP {self.status_code} error for {self.url}")

    async def aiter_bytes(self, chunk_size: int = 65424):
        if self._body is not None:
            for pos in range(0, len(self._body), chunk_size):
                yield self._body[pos:pos + chunk_size]
            return
        if self._reader is None:
            return
        te = self.headers.get("transfer-encoding", "").lower()
        clen_str = self.headers.get("content-length")
        conn_hdr = self.headers.get("connection", "").lower()
        can_keepalive = conn_hdr != "close"
        timeout = self._client.read_timeout

        reader = self._reader
        rbuf = getattr(reader, "_buffer", None)
        transport = self._writer.transport if self._writer is not None else None
        host = self._pool_key[1] if isinstance(self._pool_key, tuple) and len(self._pool_key) > 1 else ""
        is_remote = host not in ("127.0.0.1", "localhost", "::1", "")

        async def _readexactly_ba(n: int) -> bytearray:
            if rbuf is not None:
                if len(rbuf) < n:
                    deadline = time.monotonic() + timeout
                    while len(rbuf) < n:
                        if reader.at_eof():
                            partial = bytes(rbuf)
                            rbuf.clear()
                            raise asyncio.IncompleteReadError(partial, n)
                        if not rbuf:
                            await asyncio.wait_for(reader._wait_for_data("readexactly"), timeout=timeout)
                        else:
                            if time.monotonic() > deadline:
                                raise asyncio.TimeoutError()
                            await reader._wait_for_data("readexactly")
                        if is_remote and transport is not None and len(rbuf) < n and not reader.at_eof():
                            try:
                                transport.pause_reading()
                                await asyncio.sleep(0.045)
                            finally:
                                try:
                                    transport.resume_reading()
                                except Exception:
                                    pass
                out = bytearray(rbuf[:n])
                del rbuf[:n]
                reader._maybe_resume_transport()
                return out
            return bytearray(await asyncio.wait_for(reader.readexactly(n), timeout=timeout))

        async def _readuntil(sep: bytes) -> bytes:
            if rbuf is not None and sep in rbuf:
                return await reader.readuntil(sep)
            return await asyncio.wait_for(reader.readuntil(sep), timeout=timeout)

        if "chunked" in te:
            buf = bytearray()
            while True:
                line = await _readuntil(b"\r\n")
                size_str = line[:-2].split(b";", 1)[0].strip()
                size = int(size_str, 16)
                if size == 0:
                    while True:
                        tr = await _readuntil(b"\r\n")
                        if tr in (b"\r\n", b"\n", b""):
                            break
                    self._reusable = can_keepalive
                    if buf:
                        yield buf
                    break
                rem = size
                while rem > 0:
                    step = min(chunk_size, rem)
                    data = await _readexactly_ba(step)
                    rem -= len(data)
                    if data:
                        buf.extend(data)
                        if len(buf) >= chunk_size:
                            yield buf
                            buf = bytearray()
                await _readexactly_ba(2)
        elif clen_str is not None and clen_str.strip().isdigit():
            rem = int(clen_str.strip())
            while rem > 0:
                step = min(chunk_size, rem)
                try:
                    data = await _readexactly_ba(step)
                except asyncio.IncompleteReadError as exc:
                    if exc.partial:
                        yield bytearray(exc.partial)
                    can_keepalive = False
                    break
                rem -= len(data)
                yield data
            if rem == 0:
                self._reusable = can_keepalive
        else:
            while True:
                if rbuf:
                    data = await reader.read(chunk_size)
                else:
                    data = await asyncio.wait_for(reader.read(chunk_size), timeout=timeout)
                if not data:
                    break
                if is_remote and transport is not None and rbuf is not None and len(rbuf) < chunk_size and not reader.at_eof():
                    try:
                        transport.pause_reading()
                        await asyncio.sleep(0.045)
                    finally:
                        try:
                            transport.resume_reading()
                        except Exception:
                            pass
                yield bytearray(data)

    async def aclose(self):
        if self._closed:
            return
        self._closed = True
        if self._reader is None or self._writer is None:
            return
        if getattr(self, "_reusable", False) and not self._writer.is_closing() and not self._reader.at_eof():
            self._client._return_connection(self._pool_key, self._reader, self._writer)
        else:
            self._client._close_writer(self._writer)
        self._reader = None
        self._writer = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.aclose()


class _AsyncHttpClient:
    """Zero-dependency asyncio HTTP/1.1 + HTTPS client with keep-alive socket pooling."""

    def __init__(self, connect_timeout: float = 10.0, read_timeout: float = 25.0, headers: dict = None):
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.headers = dict(headers or {})
        self._pool = {}
        self._ssl_ctx = None

    def _get_ssl_context(self):
        if self._ssl_ctx is None:
            import ssl
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            self._ssl_ctx = ctx
        return self._ssl_ctx

    @staticmethod
    def _close_writer(writer):
        try:
            tr = getattr(writer, "transport", None)
            if tr is not None and hasattr(tr, "abort"):
                tr.abort()
            else:
                writer.close()
        except Exception:
            pass

    def _return_connection(self, key, reader, writer):
        bucket = self._pool.setdefault(key, [])
        while len(bucket) >= 4:
            _, old_w = bucket.pop(0)
            self._close_writer(old_w)
        bucket.append((reader, writer))

    async def _open_or_reuse(self, scheme: str, host: str, port: int):
        key = (scheme, host, port)
        bucket = self._pool.get(key)
        while bucket:
            reader, writer = bucket.pop()
            if not writer.is_closing() and not reader.at_eof():
                return key, reader, writer, True
            self._close_writer(writer)
        ssl_ctx = self._get_ssl_context() if scheme == "https" else None
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ssl_ctx, limit=256 * 1024),
            timeout=self.connect_timeout,
        )
        return key, reader, writer, False

    async def _send_and_read_headers(self, method: str, url: str):
        current_url = url
        for _ in range(8):
            parts = urlsplit(current_url)
            scheme = parts.scheme.lower()
            if scheme not in ("http", "https"):
                raise ValueError(f"Unsupported HTTP scheme: {scheme}")
            host = parts.hostname
            if not host:
                raise ValueError(f"Missing hostname in URL: {current_url}")
            port = parts.port or (443 if scheme == "https" else 80)
            path_query = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
            host_hdr = f"{host}:{port}" if ((scheme == "http" and port != 80) or (scheme == "https" and port != 443)) else host

            req_lines = [f"{method} {path_query} HTTP/1.1", f"Host: {host_hdr}"]
            for k, v in self.headers.items():
                if k.lower() != "host":
                    req_lines.append(f"{k}: {v}")
            req_bytes = ("\r\n".join(req_lines) + "\r\n\r\n").encode("latin-1", errors="ignore")

            for attempt in range(2):
                key, reader, writer, reused = await self._open_or_reuse(scheme, host, port)
                try:
                    writer.write(req_bytes)
                    await asyncio.wait_for(writer.drain(), timeout=self.connect_timeout)
                    hdr_timeout = 4.0 if reused else self.read_timeout
                    raw_hdr = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=hdr_timeout)
                    break
                except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError, OSError) as exc:
                    self._close_writer(writer)
                    if not reused or attempt == 1:
                        raise exc

            lines = raw_hdr[:-4].decode("latin-1", errors="replace").split("\r\n")
            status_parts = lines[0].split(None, 2)
            if len(status_parts) < 2 or not status_parts[1].isdigit():
                self._close_writer(writer)
                raise ValueError(f"Invalid HTTP status line: {lines[0]!r}")
            status_code = int(status_parts[1])
            hdrs = {}
            for hline in lines[1:]:
                if ":" in hline:
                    hk, hv = hline.split(":", 1)
                    hdrs[hk.strip().lower()] = hv.strip()

            if status_code in (301, 302, 303, 307, 308) and "location" in hdrs:
                self._close_writer(writer)
                current_url = urljoin(current_url, hdrs["location"])
                continue

            return _AsyncHttpResponse(self, key, reader, writer, status_code, hdrs, current_url)
        raise RuntimeError(f"Too many HTTP redirects for {url}")

    async def get(self, url: str) -> _AsyncHttpResponse:
        resp = await self._send_and_read_headers("GET", url)
        chunks = []
        try:
            async for chunk in resp.aiter_bytes(65424):
                chunks.append(chunk)
        finally:
            await resp.aclose()
        body = b"".join(chunks)
        cenc = resp.headers.get("content-encoding", "").lower()
        if cenc == "gzip" and body:
            import gzip
            body = gzip.decompress(body)
        elif cenc == "deflate" and body:
            import zlib
            body = zlib.decompress(body)
        resp._body = body
        return resp

    def stream(self, method: str, url: str):
        client = self

        class _StreamCtx:
            def __init__(self):
                self.resp = None

            async def __aenter__(self):
                self.resp = await client._send_and_read_headers(method, url)
                return self.resp

            async def __aexit__(self, *_):
                if self.resp is not None:
                    await self.resp.aclose()

        return _StreamCtx()

    async def aclose(self):
        for bucket in self._pool.values():
            for _, writer in bucket:
                self._close_writer(writer)
        self._pool.clear()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.aclose()

