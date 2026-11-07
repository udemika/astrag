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
from collections import OrderedDict, deque
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
            libc.mallopt(-3, 1048576)  # M_MMAP_THRESHOLD = 1 MB (recycles up to 512KB TS chunks in userspace without mmap/munmap)
            libc.mallopt(-1, 1048576)  # M_TRIM_THRESHOLD = 1 MB
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
    "/Td6WFoAAATm1rRGAgAhARYAAAB0L+Wj4DPvCJBdAD+RRYRoPYmm2orhgzJO8e3vZxg4kd0U2xRLBEwKo/SLmkP53QH45GUEy0Sb"
    "RamEROCYOWMbw9UjFPrL66gG8nmgkDElB+8WCEkfhomxOuWSgKlMsdHNgAtwH/9l/X0wZoIBzOFvFPw8U6KICIeKwBT1Eklx0aiZ"
    "RSSRo8TiH8d3LdddH2Qb+aqMjiw/X+FTCuxLJAMj8+F3loD5L6L5veMtNstjuxEdVCbL0THpF74ptq41P64Z8IjX6M5NY7yq4U/6"
    "UbX4hw/DGLXuFSdvLeCJ23NU4hBieZ26Qb3jgBosaiJy0nbY7122+Awr+Visig6pIkcEJT41QXdBK9hYIJonE4CiCUzKXyXwjUQi"
    "Nz5K/WqFHuOKhv/iwoejp+lgUTjtHUjibuktk6ecVAbKup7xNeC0hi3JNTeqb60DQF1RzPMh8eSHO6RgsZou2ppalCH9VlG7TUak"
    "kIpXRaY2Dj+uBEKM7RrHnwPsIavUESI1hcmp5a5xSuejSBwlBxfKp16AvR+5K5vSsqRjv5hD/8p49Fvn/jEzF4ulrVS+1bzBUzfe"
    "6lWTmkqUoVQNLUx534y/V1m07o4bGb7zDouxIBkP31n9qDKR/r0mMuN552zm4AzSU/LXFQDyYy0dTi4niffRhHF2Yr/MSZntjXOa"
    "s3b+DBOImWTNsWdPAYo+onAnXjdVGIxLj6WrvVa6UTKxFfo+VEO4r+Qy7gKTM4j1kMi81zJKNiOIkrZWFlatj130ODRUWJOPTw5O"
    "CSgKblpiUyZgiG1CPxpyY5NaZSFfaSjqL3IkUBoOdRg9idJaB1WJsgxsuGeXo9s02REcr/aJq5fpihdxhG8Y2R878MOQH0KhuY+4"
    "DT4pbKkFAMnBECQd4UsWpo4REHKRcU9wYUWuvNdv8UlBMUnOXY0G8Fun1LP0yT7LSr9/loHMGXDCP3zS8DGQtDNF8KyQ0PeJFuK9"
    "vqTh/cw71yrvKUpA5g/gIhqgdUjLDGXYx1xnqWYNOvl9CT7wqocOC6mI6k+YXkGISbjClEtuBP81lOwHICUtO6XVCoxScHAHriaO"
    "ZIx0YjSKYhlOirlnpSjT9D/wz95xKvddaOq37eqVIqj27TNSoXFzQIGzQv89W1URdoPOvBMsd7bTKtlPGxUNK+0abd/Q2fzH5Sdf"
    "iI0/BsEqRm4QK7tga/uiUQuhQer+wHPN/6M1KVL2jiHIvEEDDEY+kMsx1e7ydqL6ppztmeULlRRieq3n7YdY/ZaJNOck86+5AOx6"
    "OC7h/T47eaXiwJ4rtC/YsabCkv+S++v4Ni1YzgKXyPgyGWRxmEU8J/sQj0ZRfwkaVQKkOK33QEcg+5LmhG1I34hP9BzPKRuAtp50"
    "t5TfN4kQjPjEFrS9NEv9L7TKv9Dvw0ceEy4S8G+dBAxsgH8+YJiE/YdT0KQsZkekchXX50zJ6wdjswgGbYtEWnljo/Z+wELyuk2V"
    "SoGyIXQabkoz0xyNBEYyOlCNRSdu1uP/q9Aj6VuhV0zB88ylCyV6s54UchP24HLA7ds+w8qckzOaLL9zDGQM34XsYrTzKvbsMUDV"
    "7aRGdGce/cMKlX4N0JxOaD8mFq2xIxFLLEC76USw69qbmyfzD1uyfAQDsVK03ftR7C58EsEQ/BSWT3N88diBUAVCs6xLDg5H2Bfb"
    "bLXwzHp76Bo4UQZFIytYUJOouPc52+bvQQEQnOThFMAohwf8T5cnLZ5nqX2lX2A9gd7zWtrWqt2DaC00zRKEV+iKqQjmSeQ4/Pzz"
    "RmXUDh1YPQb+0Oih/I3kULic8dftHqN//xPS3AJkskZZhI3OYyFlzZNK21idp41lkmtP+11fAPSG51u7ok14u8l4z2dzcKKmqZCJ"
    "L8NCjMSikjfMraSQBKb/aRPkrBDolbCaY/jg13/tZzHuEmT+WKWN/Lp47U4+2/U4dJaYX/ud1O2NlZIsGDt3zee/aBCH3Aim8oB/"
    "xUFWkUwF5VzRqWnfVbXiXovm1/BBBqnFJ4gax5ANiAhvZ1RoCDOnUoU/QWwaMwimhYxRnDUfaCiJ6F7vuP9a4uEfhpcufCG+qGdM"
    "hTD54TKQw5qQqvyCXtaWQoaKZQA4WHmK8iNNrINYmhqMLb+HhnDl6ktNUfUaH3rt9zDQUr8hu0ezjoqpaLTiif2C93p01zWwkhuL"
    "Jdfb8EFqmAeMvuUEhn8p3b5SKukrwTsMJL6gUccRl8lI/2IlGtMuax14ew4ZEOnvoGUlTZQIig6ssJjlf+ffOm7riGSjCT7JMCZ4"
    "UEobEibEDicU/xlPvX2tNc2yYKtwGPtMcx0hgA+XNlguG4dO/AAOpXK5Lz4h4d0MPn1bZLv3c8RP4T9TUJIPwZw/Pud9oNwUqy3r"
    "TNL8/0ZRHAxrjlzD0A/aHB+kQqOIZVdgvdiyA24rHcMXdI5jsW78iox+KJc7nZ41j7khTMDEGEZcocBMyjSW7YL3Tc8fKe4ixubB"
    "qQ9mtcw+PRFBkWX86lMNDIye+J38o8YRfLgQWFKiQRcdx4vBGAutpSWDS434tEltwsATzK/NtnwL1OyO93hdmuVA8uWrUfzBuu3/"
    "/OKaaoqLlA/Xrb2oIGyqTagM+emF02ABemqSTiFNrAqkrdGoJ26SwLrOyWe58nXErwQN28iPzzfjVK6B+k8OxC2I7h7FUb1UBa2G"
    "jDU+Y6na3AyRz6b2MBVDh8/czRgVxN1Te8GS38KncwQyTpuVkdbrVQpVm9DwFQHxw++M9VglBJCMCvlsdUyxBhuuLJOtqnDatkza"
    "DamuMFYt8ekatDmZRD1EQqaAvRiga/CapULgCHD9rqo2UY5ZQR0OPWnbD+ITye2aa9tn9SspLZtYRnYG7EmcVa6P8ai1BRStFZH3"
    "jH2MYOj3LggMUScSKURfu3sHStMn/bp1C8PYkVaHiPsgCoKtzQk2JgKRVDyUbGMAAIg4Gco1SWZAAAGsEfBnAAB4cPU7scRn+wIA"
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

_LIBCRYPTO = None


def _load_libcrypto():
    global _LIBCRYPTO
    if os.name != "posix":
        return
    for soname in ("libcrypto.so.3", "libcrypto.so.1.1", "libcrypto.so"):
        try:
            lib = ctypes.CDLL(soname)
            lib.EVP_CIPHER_CTX_new.argtypes = []
            lib.EVP_CIPHER_CTX_new.restype = ctypes.c_void_p
            lib.EVP_CIPHER_CTX_free.argtypes = [ctypes.c_void_p]
            lib.EVP_CIPHER_CTX_free.restype = None
            lib.EVP_aes_128_cbc.argtypes = []
            lib.EVP_aes_128_cbc.restype = ctypes.c_void_p
            lib.EVP_DecryptInit_ex.argtypes = [
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.c_char_p,
                ctypes.c_char_p,
            ]
            lib.EVP_DecryptInit_ex.restype = ctypes.c_int
            lib.EVP_DecryptUpdate.argtypes = [
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_int),
                ctypes.c_void_p,
                ctypes.c_int,
            ]
            lib.EVP_DecryptUpdate.restype = ctypes.c_int
            lib.EVP_DecryptFinal_ex.argtypes = [
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_int),
            ]
            lib.EVP_DecryptFinal_ex.restype = ctypes.c_int
            _LIBCRYPTO = lib
            break
        except Exception:
            continue


_load_libcrypto()


class _HlsAes128Decryptor:
    """Zero-dependency OpenSSL EVP_aes_128_cbc streaming segment decryptor with 188-byte TS alignment."""

    __slots__ = ("_ctx", "_buf", "_outl", "_rem")

    def __init__(self, key: bytes, iv: bytes):
        self._ctx = None
        self._buf = None
        self._outl = ctypes.c_int(0)
        self._rem = bytearray()
        if _LIBCRYPTO is not None and len(key) == 16 and len(iv) == 16:
            ctx = _LIBCRYPTO.EVP_CIPHER_CTX_new()
            if ctx:
                if _LIBCRYPTO.EVP_DecryptInit_ex(ctx, _LIBCRYPTO.EVP_aes_128_cbc(), None, key, iv) == 1:
                    self._ctx = ctx
                else:
                    _LIBCRYPTO.EVP_CIPHER_CTX_free(ctx)

    def update(self, data: bytes | bytearray) -> bytearray:
        if not self._ctx or not data:
            return data if isinstance(data, bytearray) else bytearray(data)
        dlen = len(data)
        c_in = (ctypes.c_char * dlen).from_buffer(data) if isinstance(data, bytearray) else data
        need = dlen + 32
        if self._buf is None or len(self._buf) < need:
            self._buf = ctypes.create_string_buffer(need)
        self._outl.value = 0
        if _LIBCRYPTO.EVP_DecryptUpdate(self._ctx, self._buf, ctypes.byref(self._outl), c_in, dlen) == 1:
            n = self._outl.value
            if n > 0:
                mv_b = memoryview(self._buf)
                if self._rem:
                    self._rem.extend(mv_b[:n])
                    mv_b.release()
                    aligned = (len(self._rem) // 188) * 188
                    if aligned == 0:
                        return bytearray()
                    out = self._rem[:aligned]
                    del self._rem[:aligned]
                    return out
                aligned = (n // 188) * 188
                if aligned < n:
                    self._rem.extend(mv_b[aligned:n])
                out = bytearray(mv_b[:aligned]) if aligned > 0 else bytearray()
                mv_b.release()
                return out
            return bytearray()
        return bytearray()

    def finalize(self) -> bytearray:
        if not self._ctx:
            out = bytearray(self._rem)
            self._rem.clear()
            return out
        if self._buf is None or len(self._buf) < 32:
            self._buf = ctypes.create_string_buffer(32)
        self._outl.value = 0
        res = _LIBCRYPTO.EVP_DecryptFinal_ex(self._ctx, self._buf, ctypes.byref(self._outl))
        self.close()
        if res == 1 and self._outl.value > 0:
            self._rem.extend(self._buf.raw[: self._outl.value])
        out = bytearray(self._rem)
        self._rem.clear()
        return out

    def close(self):
        if self._ctx and _LIBCRYPTO is not None:
            _LIBCRYPTO.EVP_CIPHER_CTX_free(self._ctx)
            self._ctx = None


def _parse_hls_key_tag(line: str, base_url: str):
    """Parse #EXT-X-KEY tag into (method, key_url, iv_bytes_or_none)."""
    body = line.split(":", 1)[1].strip() if ":" in line else ""
    if not body or "METHOD=NONE" in body.upper():
        return None
    if "METHOD=AES-128" not in body.upper():
        return None
    uri_val = None
    iv_bytes = None
    pos = body.find('URI="')
    if pos != -1:
        end = body.find('"', pos + 5)
        if end != -1:
            uri_val = body[pos + 5 : end].strip()
    if not uri_val:
        return None
    key_url = urljoin(base_url, uri_val)
    iv_pos = body.upper().find("IV=")
    if iv_pos != -1:
        iv_raw = body[iv_pos + 3 :].split(",", 1)[0].strip().strip('"')
        if iv_raw.lower().startswith("0x"):
            iv_raw = iv_raw[2:]
        try:
            iv_bytes = int(iv_raw, 16).to_bytes(16, "big")
        except Exception:
            iv_bytes = None
    return (key_url, iv_bytes)


def _select_best_hls_variant(lines: list[str], resp_url: str) -> str | None:
    """Select the highest-bandwidth video variant from a master M3U8 playlist."""
    candidates = []
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF") and i + 1 < len(lines):
            nxt = lines[i + 1]
            if nxt.startswith("#"):
                continue
            v_url = urljoin(resp_url, nxt)
            bw = 0
            has_video = True
            upper_line = line.upper()
            for attr in line.split(":", 1)[-1].split(","):
                attr_s = attr.strip()
                if attr_s.upper().startswith("BANDWIDTH="):
                    try:
                        bw = int(attr_s.split("=", 1)[1].strip())
                    except ValueError:
                        pass
            if "AUDIO" in nxt.upper() and "RESOLUTION=" not in upper_line:
                has_video = False
            elif bw > 0 and bw < 120000 and "RESOLUTION=" not in upper_line:
                has_video = False
            elif 'CODECS="' in upper_line and "RESOLUTION=" not in upper_line:
                codecs_pos = upper_line.find('CODECS="')
                codecs_end = upper_line.find('"', codecs_pos + 8)
                if codecs_end != -1:
                    codecs_str = upper_line[codecs_pos + 8 : codecs_end]
                    if "AVC" not in codecs_str and "HVC" not in codecs_str and "HEV" not in codecs_str and "MP4V" not in codecs_str:
                        has_video = False
            candidates.append((1 if has_video else 0, bw, v_url))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return candidates[0][2]


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
        self._sub_event = asyncio.Event()
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
        self._c_has_audio = ctypes.c_int(0)
        self._c_pcr_val = ctypes.c_int64(-1)
        self._c_pcr_pos = ctypes.c_int(-1)
        self._c_synced = False
        self._c_psi_locked = False
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
        self._recent_chunks = deque()
        self._recent_bytes = 0
        self._next_psi_check = 0.0
        self._playlist_target = max(1, math.ceil(segment_duration))
        self._apply_filter_options({})

    def _is_hls_active(self) -> bool:
        if self._clock is not time.monotonic:
            return True
        if self._hls_last_access is None:
            return False
        return (self._clock() - self._hls_last_access) <= 20.0

    def _record_recent(self, chunk_obj: bytes | bytearray, now_ts: float):
        clen = len(chunk_obj)
        if clen < 188:
            return
        self._recent_chunks.append((now_ts, chunk_obj))
        self._recent_bytes += clen
        max_recent = (2048 * 1024) if (self._subscribers or self._output_states) else (1280 * 1024)
        cutoff = now_ts - 4.5
        while len(self._recent_chunks) > 2 and (self._recent_bytes > max_recent or self._recent_chunks[0][0] < cutoff):
            _, old_c = self._recent_chunks.popleft()
            self._recent_bytes = max(0, self._recent_bytes - len(old_c))

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
            self._sub_event.set()
        return queue

    @staticmethod
    def _close_queue(queue):
        while not queue.empty():
            queue.get_nowait()
            queue.task_done()
        queue.put_nowait(END_OF_STREAM)

    def unsubscribe(self, queue):
        self._subscribers.discard(queue)
        if not self._subscribers:
            self._sub_event.clear()
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
        start_off = 0
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
                start_off = need
            else:
                del self._buffer[-need:]

        # Native C / batch fast path for locked live video streams
        hls_active = self._is_hls_active()
        if not hls_active and (self._current or self._segments):
            self._current.clear()
            self._segments.clear()
            self._segment_bytes = 0
            self._current_started = None
            self._current_pcr = None
            self._current_has_keyframe = False
            self._current_has_audio = False
            self._hls_cc.clear()

        if (
            not self._buffer
            and self._locked
            and (self._seen_video_keyframes or self._counts["packets"] >= 64)
            and (not hls_active or bool(self._current))
            and (len(data) - start_off) >= 188
            and data[start_off] == 0x47
            and not self._drop_eit
            and not self._drop_sdt
            and self._filter_keep_pids is None
            and (_C_TS_LIB is not None or self._hls_seg_boundary_pkts == 0)
        ):
            is_hls = self._is_hls_input
            has_outs = bool(self._subscribers or self._output_states)
            is_writable_buf = isinstance(data, bytearray) or (isinstance(data, memoryview) and not data.readonly)
            need_cc_rewrite = is_hls and (is_writable_buf or hls_active or has_outs)
            if isinstance(data, bytearray):
                buf = data if start_off == 0 else memoryview(data)[start_off:]
                start_off = 0
            elif isinstance(data, memoryview) and not data.readonly:
                buf = data if start_off == 0 else data[start_off:]
                start_off = 0
            elif need_cc_rewrite or start_off > 0 or _C_TS_LIB is None or isinstance(data, memoryview):
                buf = bytearray(memoryview(data)[start_off:])
                start_off = 0
            else:
                buf = data
            mv = buf if isinstance(buf, memoryview) else memoryview(buf)
            try:
                pids = self._pids
                psi = self._psi
                out_cc_map = self._out_cc
                elem_pids = self._elementary_pids
                max_seg_bytes = self.max_segment_bytes
                room = (max_seg_bytes - len(self._current)) if hls_active else (len(buf) + 188)
                run_start = start_off
                packets = 0
                limit = len(buf) - 188
                sync_lost = False
                pos = start_off

                if _C_TS_LIB is not None:
                    c_buf = buf if isinstance(buf, bytes) else (ctypes.c_char * len(buf)).from_buffer(buf)
                    c_pid_flags = self._c_pid_flags
                    c_in_cc = self._c_in_cc
                    c_out_cc = self._c_out_cc
                    c_pid_pkts = self._c_pid_pkts
                    c_last_pos = self._c_last_pos
                    c_has_audio = self._c_has_audio
                    c_pcr_val = self._c_pcr_val
                    c_pcr_pos = self._c_pcr_pos
                    seg_seen = self._hls_seg_seen_pids
                    now_ts = self._clock() if self._clock is time.monotonic else 0.0
                    psi_locked = (
                        self._clock is time.monotonic
                        and not hls_active
                        and self._parsed_pat is not None
                        and bool(self._parsed_pmts)
                        and now_ts < self._next_psi_check
                    )

                    def _sync_to_c():
                        seg_resync = is_hls and self._hls_seg_boundary_pkts > 0
                        for p, st in pids.items():
                            if p != 0x1FFF and (psi_locked or p not in psi) and st["cc"] is not None:
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
                        self._c_synced = not seg_resync
                        self._c_psi_locked = psi_locked

                    def _sync_from_c(scanned_pkts: int, copy_last_pkt: bool = False):
                        seg_resync = is_hls and self._hls_seg_boundary_pkts > 0
                        for p, st in pids.items():
                            if c_pid_flags[p]:
                                icc = int(c_in_cc[p])
                                if icc >= 0:
                                    st["cc"] = icc
                                    if seg_resync and scanned_pkts > 0:
                                        seg_seen.add(p)
                                st["packets"] = int(c_pid_pkts[p])
                                if need_cc_rewrite:
                                    occ = int(c_out_cc[p])
                                    if occ >= 0:
                                        out_cc_map[p] = occ
                                elif copy_last_pkt and not is_hls and c_last_pos[p] >= 0:
                                    lp = int(c_last_pos[p])
                                    st["packet"] = bytes(mv[lp:lp + 188])
                                    c_last_pos[p] = -1
                        if seg_resync and scanned_pkts > 0:
                            self._hls_seg_boundary_pkts = max(0, self._hls_seg_boundary_pkts - scanned_pkts)
                            if self._hls_seg_boundary_pkts == 0:
                                self._c_synced = False

                    if not self._c_synced or psi_locked != self._c_psi_locked or (is_hls and self._hls_seg_boundary_pkts > 0):
                        if self._c_synced:
                            _sync_from_c(0, False)
                        _sync_to_c()
                    base_pkts = self._counts["packets"]

                    while pos <= limit:
                        room_limit = min(limit, run_start + room - 188) if hls_active else limit
                        if (not hls_active or bool(self._current)) and pos <= room_limit:
                            elapsed_seg = self._elapsed_segment() if hls_active else 0.0
                            max_seg_dur = (self.segment_duration * 1.65) if self._seen_video_keyframes else self.segment_duration
                            if hls_active and elapsed_seg >= max_seg_dur:
                                if pos > run_start:
                                    span = pos - run_start
                                    scanned_pkts = span // 188
                                    _sync_from_c(scanned_pkts, False)
                                    self._counts["packets"] = base_pkts + scanned_pkts
                                    self._counts["bytes"] += span
                                    self._current.extend(mv[run_start:pos])
                                    run_start = pos
                                    base_pkts = self._counts["packets"]
                                self._finish_segment()
                                room = max_seg_bytes - len(self._current)
                                room_limit = min(limit, run_start + room - 188)
                            if self._clock is not time.monotonic or not self._seen_video_keyframes:
                                skip_kf = 0
                            elif not hls_active:
                                skip_kf = 1
                            elif not self._current_has_keyframe or elapsed_seg >= self.segment_duration * 0.80:
                                skip_kf = 0
                            else:
                                skip_kf = 1
                            pcr_pid_arg = self._pcr_pid if self._pcr_pid is not None else -1
                            last_pcr_arg = self._pcr_last if self._pcr_last is not None else -1
                            c_has_audio.value = 0
                            c_pcr_pos.value = -1
                            new_pos = _C_TS_LIB.ts_scan_batch(
                                c_buf,
                                pos,
                                limit,
                                room_limit,
                                1 if need_cc_rewrite else 0,
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
                                    batch_subs = [q for q in self._subscribers if getattr(q, "_batch_ok", False)]
                                    pkt_subs = [q for q in self._subscribers if not getattr(q, "_batch_ok", False)]
                                    if batch_subs:
                                        for b_off in range(pos, new_pos, 32712):
                                            b_end = min(new_pos, b_off + 32712)
                                            chunk_b = bytes(mv[b_off:b_end])
                                            for queue in batch_subs:
                                                self._enqueue(queue, chunk_b, "queue_drops")
                                    if pkt_subs or self._output_states:
                                        for p_off in range(pos, new_pos, 188):
                                            pkt_b = bytes(mv[p_off:p_off + 188])
                                            for queue in pkt_subs:
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
                            _sync_from_c(scanned_pkts, self._clock is not time.monotonic)
                            self._counts["packets"] = base_pkts + scanned_pkts
                            self._counts["bytes"] += span
                            if hls_active:
                                self._current.extend(mv[run_start:pos])

                        if not _valid_header(buf, pos):
                            self._counts["sync_losses"] += 1
                            self._locked = False
                            self._c_synced = False
                            sync_lost = True
                            break

                        pkt_bytes = bytes(mv[pos:pos + 188])
                        pos += 188
                        packets += 1
                        self._packet(pkt_bytes)
                        if (
                            self._clock is time.monotonic
                            and not hls_active
                            and self._parsed_pat is not None
                            and bool(self._parsed_pmts)
                            and now_ts >= self._next_psi_check
                        ):
                            self._next_psi_check = now_ts + 5.0
                            psi_locked = True
                        _sync_to_c()
                        run_start = pos
                        base_pkts = self._counts["packets"]
                        room = (max_seg_bytes - len(self._current)) if hls_active else (len(buf) + 188)

                    if not sync_lost:
                        if pos > run_start:
                            span = pos - run_start
                            scanned_pkts = span // 188
                            if not self._c_synced or self._clock is not time.monotonic or (is_hls and self._hls_seg_boundary_pkts > 0):
                                _sync_from_c(scanned_pkts, self._clock is not time.monotonic)
                            self._counts["packets"] = base_pkts + scanned_pkts
                            self._counts["bytes"] += span
                            if hls_active:
                                self._current.extend(mv[run_start:pos])
                        if pos > 0 and self._clock is time.monotonic:
                            self._record_recent(buf if (pos == len(buf) and isinstance(buf, (bytes, bytearray))) else bytes(mv[:pos]), now_ts)
                        if pos < len(buf):
                            self._buffer.extend(mv[pos:])
                        return prefix_packets + packets
                    data = bytes(mv[pos:])
                    start_off = 0
            finally:
                mv.release()

        if start_off > 0:
            data = memoryview(data)[start_off:]

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

    def _flush_c_state(self):
        if not self._c_synced:
            return
        is_hls = self._is_hls_input
        c_pid_flags = self._c_pid_flags
        c_in_cc = self._c_in_cc
        c_out_cc = self._c_out_cc
        c_pid_pkts = self._c_pid_pkts
        out_cc_map = self._out_cc
        seg_resync = is_hls and self._hls_seg_boundary_pkts > 0
        seg_seen = self._hls_seg_seen_pids
        for p, st in self._pids.items():
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
        self._c_synced = False

    def _packet(self, packet):
        if self._c_synced:
            self._flush_c_state()
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
                pkt_span = max(1, index - (self._pcr_index or 0))
                inst_br = pkt_span * 188 * 8 * PCR_HZ / delta
                if self._is_hls_input and (delta > PCR_HZ * 2.5 or inst_br < 100_000):
                    # Guard against multi-second PCR jumps across skipped/spliced HLS segments
                    pass
                else:
                    self._timeline += delta / PCR_HZ
                    if self._is_hls_input and self._pcr_bitrate and 100_000 <= self._pcr_bitrate <= 120_000_000:
                        self._pcr_bitrate = 0.80 * self._pcr_bitrate + 0.20 * inst_br
                    else:
                        self._pcr_bitrate = inst_br
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

    def gop_preroll_burst(self) -> bytes:
        """Return keyframe-aligned current GOP (PAT + PMT + IDR keyframe up to live edge) for instant startup."""
        if (
            self._current_has_keyframe
            and 188 <= len(self._current) <= 2 * 1024 * 1024
            and (len(self._current) % 188) == 0
        ):
            return bytes(self._current)
        if not self._recent_chunks:
            return b""
        # Extract keyframe-aligned tail directly from _recent_chunks without activating HLS or clearing buffer
        tail_chunks = []
        tail_bytes = 0
        for _, ch in reversed(self._recent_chunks):
            tail_chunks.append(ch)
            tail_bytes += len(ch)
            if tail_bytes >= 1536 * 1024:
                break
        tail_chunks.reverse()
        raw = b"".join(tail_chunks)
        n_len = (len(raw) // 188) * 188
        if n_len < 188 * 16:
            return b""
        last_kf = -1
        scan_start = max(0, n_len - (1024 * 1024 // 188) * 188)
        min_burst_bytes = 188 * 400  # At least ~75 KB so players/VLC lock instantaneously
        for pos in range(n_len - 188, scan_start - 1, -188):
            if self.is_video_keyframe_packet(raw[pos:pos + 188]):
                last_kf = pos
                if n_len - pos >= min_burst_bytes:
                    break
        if last_kf < 0 or (n_len - last_kf) < min_burst_bytes:
            last_kf = max(0, n_len - 188 * 512)
        prefix = self._prefix()
        return prefix + raw[last_kf:n_len]

    def psi_summary(self) -> dict:
        """Return full Cesbo Astra Analyzer PSI/SI tables and stream metrics."""
        st = self.stats(include_pids=False)
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
        pcr_el = (self._timeline - self._current_pcr) if (self._current_pcr is not None and self._timeline > self._current_pcr) else 0.0
        wall_el = max(0.0, self._clock() - self._current_started) if self._current_started is not None else 0.0
        if self._clock is time.monotonic:
            return max(pcr_el, wall_el)
        return pcr_el if pcr_el > 0.0 else wall_el

    def _append_hls_raw(self, data: bytes):
        """Append 188-byte TS packets (e.g. PAT/PMT prefix) to self._current with continuous CC."""
        if self._clock is not time.monotonic:
            self._current.extend(data)
            return
        is_hls = self._is_hls_input
        for pos in range(0, len(data) - 187, 188):
            pkt = data[pos:pos + 188]
            pid = ((pkt[1] & 0x1F) << 8) | pkt[2]
            if pid == 0x1FFF:
                self._current.extend(pkt)
                continue
            afc = (pkt[3] >> 4) & 3
            if is_hls:
                if afc & 1:
                    cc = (self._out_cc.get(pid, -1) + 1) & 0x0F
                    self._out_cc[pid] = cc
                else:
                    cc = self._out_cc.get(pid, pkt[3] & 0x0F)
            else:
                st = self._pids.get(pid)
                if st is not None and st["cc"] is not None:
                    cc = int(st["cc"]) & 0x0F
                elif afc & 1:
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
        if not self._is_hls_active():
            return
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

    def _bootstrap_hls_from_recent(self):
        """Instantly materialize 2-3 keyframe-aligned HLS segments and live GOP from recent zero-copy chunks."""
        now = self._clock()
        self._hls_last_access = now
        if self._segments or not self._recent_chunks:
            return
        chunks = list(self._recent_chunks)
        self._recent_chunks.clear()
        self._recent_bytes = 0
        raw = bytearray().join(b for _, b in chunks)
        n_len = (len(raw) // 188) * 188
        if n_len < 188 * 32:
            return
        if len(raw) > n_len:
            del raw[n_len:]

        total_span = max(3.5, now - chunks[0][0])
        if self._pcr_bitrate and self._pcr_bitrate > 100000:
            pcr_span = (n_len * 8.0) / self._pcr_bitrate
            if 2.0 <= pcr_span <= 20.0:
                total_span = pcr_span
        bps = max(32768.0, n_len / total_span)

        is_hls = self._is_hls_input
        video_pids = {p for p, t in self._elementary_pids.items() if t == "VIDEO"}
        kf_offsets = []
        if _C_TS_LIB is not None and video_pids:
            c_buf = (ctypes.c_char * n_len).from_buffer(raw)
            tmp_flags = (ctypes.c_uint8 * 8192)()
            tmp_in_cc = (ctypes.c_int8 * 8192)(*([-2] * 8192))
            tmp_out_cc = (ctypes.c_int8 * 8192)(*([-1] * 8192))
            tmp_pkts = (ctypes.c_uint32 * 8192)()
            tmp_last = (ctypes.c_int32 * 8192)(*([-1] * 8192))
            tmp_audio = ctypes.c_int(0)
            tmp_pcr_val = ctypes.c_int64(-1)
            tmp_pcr_pos = ctypes.c_int(-1)
            for p in self._pids:
                if p != 0x1FFF:
                    tmp_flags[p] = 1 | (4 if p in video_pids else 0)
                    if is_hls and p in self._out_cc:
                        tmp_out_cc[p] = self._out_cc[p]
            pos = 0
            limit = n_len - 188
            while pos <= limit:
                new_pos = _C_TS_LIB.ts_scan_batch(
                    c_buf, pos, limit, limit,
                    0,
                    0, -1, -1,
                    tmp_flags, tmp_in_cc, tmp_out_cc, tmp_pkts, tmp_last,
                    ctypes.byref(tmp_audio), ctypes.byref(tmp_pcr_val), ctypes.byref(tmp_pcr_pos),
                )
                if new_pos > limit:
                    break
                pkt = raw[new_pos:new_pos + 188]
                if self.is_video_keyframe_packet(pkt):
                    kf_offsets.append(new_pos)
                pid = ((pkt[1] & 0x1F) << 8) | pkt[2]
                if pid != 0x1FFF:
                    tmp_flags[pid] = 1 | (4 if pid in video_pids else 0)
                    tmp_in_cc[pid] = -2
                    if is_hls:
                        afc = (pkt[3] >> 4) & 3
                        occ = ((int(tmp_out_cc[pid]) + 1) & 0x0F) if (afc & 1) else (int(tmp_out_cc[pid]) if tmp_out_cc[pid] >= 0 else (pkt[3] & 0x0F))
                        tmp_out_cc[pid] = occ
                        raw[new_pos + 3] = (pkt[3] & 0xF0) | occ
                pos = new_pos + 188
            # Do not overwrite self._out_cc from historical _recent_chunks since feed() already rewrote CCs in-place
            pass

        min_seg_bytes = max(188 * 120, int(bps * 1.6))
        seg_starts = []
        for off in kf_offsets:
            if not seg_starts or (off - seg_starts[-1]) >= min_seg_bytes:
                seg_starts.append(off)
        if len(seg_starts) < 3 and n_len >= 188 * 96:
            if len(seg_starts) == 2:
                k0, k1 = seg_starts
                if k0 >= 188 * 32:
                    seg_starts = [0, k0, k1]
                elif (n_len - k1) >= (k1 - k0):
                    mid = k1 + ((((n_len - k1) * 2 // 3) // 188) * 188)
                    seg_starts = [k0, k1, max(k1 + 188, min(n_len - 188 * 16, mid))]
                else:
                    mid = k0 + ((((k1 - k0) // 2) // 188) * 188)
                    seg_starts = [k0, max(k0 + 188, mid), k1]
            elif len(seg_starts) == 1:
                k0 = seg_starts[0]
                if k0 >= int(bps * 1.2) and (n_len - k0) >= int(bps * 1.2):
                    mid_tail = k0 + ((((n_len - k0) * 2 // 3) // 188) * 188)
                    seg_starts = [0, k0, max(k0 + 188, min(n_len - 188 * 16, mid_tail))]
                else:
                    s1 = ((n_len * 2 // 5) // 188) * 188
                    s2 = ((n_len * 4 // 5) // 188) * 188
                    seg_starts = [0, max(188, s1), max(376, s2)]
            else:
                s1 = ((n_len * 2 // 5) // 188) * 188
                s2 = ((n_len * 4 // 5) // 188) * 188
                seg_starts = [0, max(188, s1), max(376, s2)]

        prefix = self._prefix()
        mv_raw = memoryview(raw)
        try:
            for idx in range(len(seg_starts) - 1):
                s_off = seg_starts[idx]
                e_off = seg_starts[idx + 1]
                if e_off <= s_off:
                    continue
                seg_dur = max(1.2, (e_off - s_off) / bps)
                self._current = bytearray()
                if len(prefix) + 188 <= self.max_segment_bytes:
                    self._append_hls_raw(prefix)
                self._current.extend(mv_raw[s_off:e_off])
                segment = Segment(self._sequence, bytes(self._current), seg_dur, False)
                self._segments[self._sequence] = segment
                self._sequence += 1
                self._segment_bytes += len(segment.data)
            last_off = seg_starts[-1]
            cur_dur = max(1.0, min(self.segment_duration * 0.80, (n_len - last_off) / bps))
            self._current = bytearray()
            if len(prefix) + 188 <= self.max_segment_bytes:
                self._append_hls_raw(prefix)
            self._current.extend(mv_raw[last_off:n_len])
            self._current_started = now - cur_dur
            self._current_pcr = (self._timeline - cur_dur) if self._pcr_last is not None else None
            self._current_has_keyframe = True
            self._current_has_audio = True
            max_rounded = max((int(round(s.duration)) for s in self._segments.values()), default=1)
            self._playlist_target = max(1, math.ceil(self.segment_duration), max_rounded)
        finally:
            mv_raw.release()

    async def wait_for_hls(self, timeout: float = 4.0) -> bool:
        """Wait for complete keyframe-aligned HLS segments to be ready."""
        self._hls_last_access = self._clock()
        if len(self._segments) < 2 and self._recent_chunks:
            self._bootstrap_hls_from_recent()
        if len(self._segments) >= 2:
            return True
        t_start = time.monotonic()
        deadline = t_start + timeout
        while time.monotonic() < deadline and self._running:
            if len(self._segments) < 2 and self._recent_chunks:
                self._bootstrap_hls_from_recent()
            if len(self._segments) >= 2:
                return True
            await asyncio.sleep(0.04)
        if not self._segments and len(self._current) >= 188 * 16:
            self._finish_segment()
        return bool(self._segments)

    def hls_playlist(self):
        """Return an M3U8 string with relative segment-<sequence>.ts URIs."""
        self._hls_last_access = self._clock()
        if not self._segments and self._recent_chunks:
            self._bootstrap_hls_from_recent()
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

    def stats(self, include_pids: bool = True):
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
        out = {**self._counts,
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
               "last_error": self._last_error}
        if include_pids:
            if self._c_synced:
                for p, st in self._pids.items():
                    if self._c_pid_flags[p]:
                        icc = int(self._c_in_cc[p])
                        if icc >= 0:
                            st["cc"] = icc
                        st["packets"] = int(self._c_pid_pkts[p])
            out["pids"] = {pid: {k: v for k, v in state.items() if k != "packet"}
                           for pid, state in self._pids.items()}
        return out

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
                self._c_synced = False
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

    async def _stream_hls_playlist(self, client, playlist_url: str, master_url: str | None = None):
        """Cesbo Astra sync_buffer HLS input: low-RAM streaming chunk prefetcher + PCR-locked VBR pacer."""
        self._is_hls_input = True
        seen_segments = OrderedDict()
        prefetch_queue: asyncio.Queue = asyncio.Queue(maxsize=96)
        prefetch_bytes = 0
        max_prefetch_bytes = 3584 * 1024
        idle_prefetch_bytes = 2048 * 1024
        max_single_seg_bytes = 64 * 1024 * 1024  # Support up to 64 MB 4K UHD 12s segments without truncation
        space_event = asyncio.Event()
        space_event.set()
        producer_error = None

        master_url = master_url or playlist_url
        active_playlist_url = playlist_url
        seg_failures = OrderedDict()
        key_cache = OrderedDict()
        last_completed_seq = -1

        def _seg_dedup_key(s_url: str) -> str:
            if "?" not in s_url:
                return s_url
            base_p, qs = s_url.split("?", 1)
            if base_p.lower().endswith(".ts"):
                return base_p
            kept = [
                kv for kv in qs.split("&")
                if kv.split("=", 1)[0].lower() not in ("token", "md5", "auth", "wmsauthsign", "expires", "vtoken", "signature", "hash", "key")
            ]
            return f"{base_p}?{'&'.join(kept)}" if kept else base_p

        async def _prefetch_producer():
            nonlocal active_playlist_url, producer_error, prefetch_bytes, last_completed_seq
            first_load = True
            consecutive_errors = 0
            target_duration = 4.0
            try:
                while self._running:
                    cur_cap = max_prefetch_bytes if (self._subscribers or self._output_states or self._is_hls_active()) else idle_prefetch_bytes
                    while self._running and prefetch_bytes >= cur_cap:
                        space_event.clear()
                        try:
                            await asyncio.wait_for(space_event.wait(), timeout=0.8)
                        except asyncio.TimeoutError:
                            pass
                        cur_cap = max_prefetch_bytes if (self._subscribers or self._output_states or self._is_hls_active()) else idle_prefetch_bytes
                    if not self._running:
                        return
                    try:
                        resp = await client.get(active_playlist_url)
                        if resp.status_code in (401, 403, 404, 410) and active_playlist_url != master_url:
                            resp = await client.get(master_url)
                            if resp.status_code == 200:
                                active_playlist_url = str(resp.url)
                        resp.raise_for_status()
                        text = resp.text
                        text_len = len(text)
                        resp_url = str(resp.url)
                        del resp
                        if not text or not text.lstrip().startswith("#EXTM3U"):
                            if active_playlist_url != master_url:
                                active_playlist_url = master_url
                            raise ValueError("Invalid M3U8 playlist response")
                        consecutive_errors = 0
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        if active_playlist_url != master_url:
                            active_playlist_url = master_url
                        consecutive_errors += 1
                        self._counts["input_errors"] += 1
                        self._last_error = f"{type(exc).__name__}: {exc}"
                        if first_load or consecutive_errors >= 8:
                            producer_error = exc
                            return
                        await asyncio.sleep(0.25)
                        continue

                    if "#EXT-X-STREAM-INF" in text[:8192]:
                        lines = [line.strip() for line in text.splitlines() if line.strip()]
                        best_url = _select_best_hls_variant(lines, resp_url)
                        if best_url:
                            active_playlist_url = best_url
                            continue

                    # Fast O(1) tail parser for mega DVR playlists (e.g. 9000+ segments / 1.2 MB text)
                    media_seq = 0
                    skipped_segs = 0
                    current_key = None
                    if text_len > 16384:
                        head_chunk = text[:4096]
                        for hline in head_chunk.splitlines():
                            hs = hline.strip()
                            if hs.startswith("#EXT-X-TARGETDURATION:"):
                                try:
                                    target_duration = max(0.5, float(hs.split(":", 1)[1].strip()))
                                except ValueError:
                                    pass
                            elif hs.startswith("#EXT-X-MEDIA-SEQUENCE:"):
                                try:
                                    media_seq = int(hs.split(":", 1)[1].strip())
                                except ValueError:
                                    pass
                            elif hs.startswith("#EXT-X-KEY:"):
                                current_key = _parse_hls_key_tag(hs, resp_url)
                        tail_pos = text.rfind("#EXTINF:", 0, max(0, text_len - 4096))
                        if tail_pos == -1:
                            tail_pos = max(0, text_len - 8192)
                            nl_pos = text.find("\n", tail_pos)
                            if nl_pos != -1:
                                tail_pos = nl_pos + 1
                        skipped_segs = text.count("#EXTINF:", 0, tail_pos)
                        last_key_pos = text.rfind("#EXT-X-KEY:", 0, tail_pos)
                        if last_key_pos != -1:
                            key_end = text.find("\n", last_key_pos)
                            kline = text[last_key_pos : (key_end if key_end != -1 else tail_pos)].strip()
                            current_key = _parse_hls_key_tag(kline, resp_url)
                        lines = [line.strip() for line in text[tail_pos:].splitlines() if line.strip()]
                    else:
                        lines = [line.strip() for line in text.splitlines() if line.strip()]
                    del text

                    seg_entries = []
                    next_dur = None
                    seg_idx = media_seq + skipped_segs
                    for line in lines:
                        if line.startswith("#EXT-X-TARGETDURATION:"):
                            try:
                                target_duration = max(0.5, float(line.split(":", 1)[1].strip()))
                            except ValueError:
                                pass
                        elif line.startswith("#EXT-X-MEDIA-SEQUENCE:") and skipped_segs == 0:
                            try:
                                media_seq = int(line.split(":", 1)[1].strip())
                                seg_idx = media_seq
                            except ValueError:
                                pass
                        elif line.startswith("#EXT-X-KEY:"):
                            current_key = _parse_hls_key_tag(line, resp_url)
                        elif line.startswith("#EXTINF:"):
                            raw_dur = line[8:].split(",", 1)[0].strip()
                            try:
                                next_dur = max(0.2, float(raw_dur))
                            except ValueError:
                                next_dur = target_duration
                        elif not line.startswith("#"):
                            seg_url = urljoin(resp_url, line)
                            seg_entries.append((
                                seg_url,
                                next_dur if next_dur is not None else target_duration,
                                seg_idx,
                                current_key,
                            ))
                            seg_idx += 1
                            next_dur = None

                    if len(seg_entries) > 8:
                        seg_entries = seg_entries[-8:]

                    keep_initial = 2 if target_duration >= 4.0 else 3
                    if first_load and len(seg_entries) > keep_initial:
                        for old_url, _, old_seq, _ in seg_entries[:-keep_initial]:
                            seen_segments[_seg_dedup_key(old_url)] = True
                            if media_seq > 0 and old_seq > last_completed_seq:
                                last_completed_seq = old_seq
                    first_load = False

                    new_entries = [
                        item for item in seg_entries
                        if _seg_dedup_key(item[0]) not in seen_segments
                        and (media_seq == 0 or item[2] > last_completed_seq)
                    ]
                    max_batch = 2 if target_duration >= 5.0 else 3
                    if len(new_entries) > max_batch:
                        for old_url, _, old_seq, _ in new_entries[:-max_batch]:
                            seen_segments[_seg_dedup_key(old_url)] = True
                            if media_seq > 0 and old_seq > last_completed_seq:
                                last_completed_seq = old_seq
                        new_entries = new_entries[-max_batch:]

                    batch_started = time.monotonic()
                    for idx_in_batch, (seg_url, seg_dur, seg_seq, seg_key_info) in enumerate(new_entries):
                        if not self._running:
                            return
                        if idx_in_batch > 0 and (time.monotonic() - batch_started) >= max(3.0, target_duration * 0.85):
                            break
                        dedup_k = _seg_dedup_key(seg_url)
                        while len(seen_segments) > 160:
                            seen_segments.popitem(last=False)
                        while len(seg_failures) > 120:
                            seg_failures.popitem(last=False)
                        decryptor = None
                        try:
                            if seg_key_info is not None:
                                key_url, explicit_iv = seg_key_info
                                key_bytes = key_cache.get(key_url)
                                if key_bytes is None:
                                    kresp = await client.get(key_url)
                                    kresp.raise_for_status()
                                    key_bytes = bytes(kresp._body or b"")
                                    if len(key_bytes) == 16:
                                        key_cache[key_url] = key_bytes
                                        while len(key_cache) > 16:
                                            key_cache.popitem(last=False)
                                if key_bytes and len(key_bytes) == 16:
                                    iv_bytes = explicit_iv if explicit_iv is not None else int(seg_seq).to_bytes(16, "big")
                                    decryptor = _HlsAes128Decryptor(key_bytes, iv_bytes)

                            async with client.stream("GET", seg_url) as seg_resp:
                                if seg_resp.status_code in (404, 410):
                                    seen_segments[dedup_k] = True
                                    if media_seq > 0 and seg_seq > last_completed_seq:
                                        last_completed_seq = seg_seq
                                    if seg_resp.status_code == 410 and active_playlist_url != master_url:
                                        active_playlist_url = master_url
                                    break
                                if seg_resp.status_code in (401, 403) and active_playlist_url != master_url:
                                    active_playlist_url = master_url
                                    break
                                seg_resp.raise_for_status()
                                clen = int(seg_resp.headers.get("content-length") or 0)
                                seg_bps = (clen / seg_dur) if (clen >= 188 and seg_dur > 0) else 0.0
                                seg_read = 0
                                first_chunk = True
                                read_chunk_sz = 131036 if (self._subscribers or self._output_states) else 524144
                                async for raw_chunk in seg_resp.aiter_bytes(read_chunk_sz):
                                    if not self._running:
                                        return
                                    if not raw_chunk:
                                        continue
                                    chunk = decryptor.update(raw_chunk) if decryptor is not None else raw_chunk
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
                                    cur_cap = max_prefetch_bytes if (self._subscribers or self._output_states or self._is_hls_active()) else idle_prefetch_bytes
                                    while self._running and prefetch_bytes >= cur_cap:
                                        space_event.clear()
                                        try:
                                            await asyncio.wait_for(space_event.wait(), timeout=0.8)
                                        except asyncio.TimeoutError:
                                            pass
                                        cur_cap = max_prefetch_bytes if (self._subscribers or self._output_states or self._is_hls_active()) else idle_prefetch_bytes
                                    if not self._running:
                                        return
                                    prefetch_bytes += len(chunk)
                                    await prefetch_queue.put((chunk, seg_dur, seg_bps, is_seg_start))
                                if decryptor is not None:
                                    tail_dec = decryptor.finalize()
                                    if tail_dec and self._running:
                                        seg_read += len(tail_dec)
                                        prefetch_bytes += len(tail_dec)
                                        await prefetch_queue.put((tail_dec, seg_dur, seg_bps, False))
                                if seg_read >= 188:
                                    seen_segments[dedup_k] = True
                                    if media_seq > 0 and seg_seq > last_completed_seq:
                                        last_completed_seq = seg_seq
                                    consecutive_errors = 0
                                else:
                                    fails = seg_failures.get(dedup_k, 0) + 1
                                    seg_failures[dedup_k] = fails
                                    if fails >= 2:
                                        seen_segments[dedup_k] = True
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            fails = seg_failures.get(dedup_k, 0) + 1
                            seg_failures[dedup_k] = fails
                            if fails >= 2:
                                seen_segments[dedup_k] = True
                            consecutive_errors += 1
                            self._counts["input_errors"] += 1
                            self._last_error = f"{type(exc).__name__}: {exc}"
                            if consecutive_errors >= 8:
                                producer_error = exc
                                return
                            break
                        finally:
                            if decryptor is not None:
                                decryptor.close()

                    batch_elapsed = time.monotonic() - batch_started
                    if new_entries:
                        poll_sleep = max(0.20, min(2.2, target_duration * 0.45 - batch_elapsed * 0.5))
                    elif text_len > 65536:
                        poll_sleep = min(1.8, max(0.75, target_duration * 0.25))
                    else:
                        poll_sleep = min(2.2, max(0.40, target_duration * 0.28))
                    await asyncio.sleep(poll_sleep)
            finally:
                try:
                    prefetch_queue.put_nowait(None)
                except asyncio.QueueFull:
                    pass

        producer_task = asyncio.create_task(_prefetch_producer())
        bootstrap_remaining = 768 * 1024  # Fast-fill initial buffer from the 2 prefetched initial segments
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
                if not space_event.is_set():
                    space_event.set()
                if is_seg_start:
                    if self._c_synced:
                        self._flush_c_state()
                    self._hls_seg_boundary_pkts = 64
                    self._hls_seg_seen_pids.clear()

                if bootstrap_remaining > 0 and not self._subscribers:
                    self.feed(chunk)
                    bootstrap_remaining -= chunk_len
                    del chunk
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
                    if seg_bps > 0:
                        est_dur = chunk_len / seg_bps
                        if 0.05 <= pcr_delta <= est_dur * 1.45:
                            slice_dur = pcr_delta
                        else:
                            slice_dur = est_dur
                    elif 0.0 < pcr_delta <= 1.50:
                        slice_dur = pcr_delta
                    elif self._pcr_bitrate and self._pcr_bitrate > 100000:
                        slice_dur = (chunk_len * 8.0) / self._pcr_bitrate
                    else:
                        slice_dur = (chunk_len / (384 * 1024)) * max(0.8, seg_dur / 4.0)
                    if prefetch_bytes > 3072 * 1024:
                        pace_factor = 0.92
                    elif prefetch_bytes > 2048 * 1024:
                        pace_factor = 0.97
                    elif prefetch_bytes >= 384 * 1024:
                        pace_factor = 0.998
                    else:
                        pace_factor = 1.015
                    seg_clock += slice_dur * pace_factor
                    wait = seg_clock - time.monotonic()
                    if wait < -0.30:
                        seg_clock = time.monotonic() - 0.10
                    if wait >= 0.045 and self._running and not self._subscribers and not self._output_states:
                        try:
                            await asyncio.wait_for(self._sub_event.wait(), timeout=wait)
                            seg_clock = time.monotonic()
                        except asyncio.TimeoutError:
                            pass
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

                        if 0.0 < pcr_delta <= 1.20:
                            slice_dur = pcr_delta
                        elif seg_bps > 0:
                            slice_dur = step / seg_bps
                        elif self._pcr_bitrate and self._pcr_bitrate > 100000:
                            slice_dur = (step * 8.0) / self._pcr_bitrate
                        else:
                            slice_dur = (step / (384 * 1024)) * max(0.8, seg_dur / 4.0)

                        # Hardware-grade PLL pacing: keep 1 full segment buffered at 1.0x PCR speed
                        if prefetch_bytes > 3072 * 1024:
                            pace_factor = 0.92
                        elif prefetch_bytes > 2048 * 1024:
                            pace_factor = 0.97
                        elif prefetch_bytes >= 384 * 1024:
                            pace_factor = 0.998
                        else:
                            pace_factor = 1.015

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
            target_hls_url = clean_url
            async with client.stream("GET", clean_url) as response:
                response.raise_for_status()
                resp_url_str = str(response.url)
                ctype = response.headers.get("content-type", "").lower()
                if "mpegurl" in ctype or "m3u" in ctype or resp_url_str.lower().split("?", 1)[0].endswith((".m3u8", ".m3u")):
                    switch_to_hls = True
                    target_hls_url = resp_url_str
                else:
                    first_chunk = True
                    slice_size = 188 * 174
                    ts_clock = time.monotonic()
                    rbuf = getattr(getattr(response, "_reader", None), "_buffer", None)
                    try:
                        ts_read_sz = 131036 if (self._subscribers or self._output_states) else 262072
                        async for chunk in response.aiter_bytes(ts_read_sz):
                            if not self._running or not chunk:
                                break
                            if first_chunk:
                                first_chunk = False
                                if chunk.lstrip().startswith(b"#EXTM3U"):
                                    switch_to_hls = True
                                    target_hls_url = resp_url_str
                                    break
                            rem_sock = len(rbuf) if rbuf is not None else 0
                            if not self._subscribers and not self._output_states:
                                self.feed(chunk)
                                del chunk
                                ts_clock = time.monotonic()
                            elif rem_sock <= 65536 and len(chunk) <= 65536:
                                self.feed(chunk)
                                del chunk
                                ts_clock = time.monotonic()
                            else:
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
                                        rem_buffered = (len(rbuf) if rbuf is not None else 0) + (total - pos)
                                        if rem_buffered > 32768 and 0.0 < pcr_delta <= 0.30:
                                            factor = 0.85 if rem_buffered > 512 * 1024 else 0.96
                                            ts_clock += pcr_delta * factor
                                            wait = ts_clock - time.monotonic()
                                            if wait > 0.004:
                                                await asyncio.sleep(wait)
                                            elif wait < -0.20:
                                                ts_clock = time.monotonic() - 0.20
                                finally:
                                    mv_chunk.release()
                                    del chunk
                    finally:
                        trim_process_memory()
            if switch_to_hls and self._running:
                await self._stream_hls_playlist(client, target_hls_url, master_url=clean_url)

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
        tr = getattr(self._writer, "_transport", None) or getattr(self._writer, "transport", None)
        ssl_proto = getattr(tr, "_ssl_protocol", None) if tr is not None else None
        raw_tr = getattr(ssl_proto, "_transport", None) if ssl_proto is not None else tr
        coalesce_paused = False
        last_pause_ts = 0.0
        is_bounded = clen_str is not None and clen_str.strip().isdigit()
        if is_bounded:
            coalesce_window = 0.165 if chunk_size >= 262072 else 0.035
        else:
            coalesce_window = 0.145 if chunk_size >= 262072 else 0.045
        loop = asyncio.get_running_loop()
        watchdog_deadline = time.monotonic() + timeout
        watchdog_handle = None

        def _check_watchdog():
            nonlocal watchdog_handle
            watchdog_handle = None
            if self._closed or reader is None or reader.at_eof():
                return
            now_m = time.monotonic()
            if now_m >= watchdog_deadline:
                waiter = getattr(reader, "_waiter", None)
                if waiter is not None and not waiter.done():
                    waiter.set_exception(asyncio.TimeoutError())
            else:
                watchdog_handle = loop.call_later(max(1.0, watchdog_deadline - now_m), _check_watchdog)

        if rbuf is not None:
            watchdog_handle = loop.call_later(timeout, _check_watchdog)

        def _pause_raw():
            nonlocal coalesce_paused, last_pause_ts
            if raw_tr is not None and not coalesce_paused and not getattr(raw_tr, "_closing", False):
                if not getattr(raw_tr, "_paused", False):
                    try:
                        raw_tr.pause_reading()
                        coalesce_paused = True
                        last_pause_ts = time.monotonic()
                    except Exception:
                        pass

        def _resume_raw():
            nonlocal coalesce_paused
            if coalesce_paused and raw_tr is not None:
                coalesce_paused = False
                if not getattr(ssl_proto, "_ssl_reading_paused", False):
                    try:
                        raw_tr.resume_reading()
                    except Exception:
                        pass

        async def _wait_coalesced(min_bytes: int, force_more: bool = False):
            nonlocal watchdog_deadline
            watchdog_deadline = time.monotonic() + timeout
            if rbuf is None or reader.at_eof():
                _resume_raw()
                return
            if not force_more and len(rbuf) >= min_bytes:
                return
            prev_len = len(rbuf)
            # If already paused from previous chunk yield, let kernel buffer coalesce for coalesce_window before resuming
            if coalesce_paused and min_bytes > 16384 and not force_more:
                wait_s = coalesce_window - (time.monotonic() - last_pause_ts)
                if wait_s > 0.004:
                    await asyncio.sleep(wait_s)
                _resume_raw()
                await asyncio.sleep(0.002 if ssl_proto is not None else 0)
            else:
                _resume_raw()
            # If socket buffer was empty, wait for first packet of burst, then coalesce remaining TCP window in kernel
            if len(rbuf) == 0 and not reader.at_eof():
                _resume_raw()
                watchdog_deadline = time.monotonic() + timeout
                await reader._wait_for_data("read")
                if len(rbuf) < min_bytes and min_bytes > 16384 and not reader.at_eof() and not force_more and (not rbuf or rbuf[0] == 0x47):
                    _pause_raw()
                    if coalesce_paused:
                        await asyncio.sleep(coalesce_window)
                    _resume_raw()
                    await asyncio.sleep(0.002 if ssl_proto is not None else 0)
            if force_more and len(rbuf) == prev_len and not reader.at_eof():
                _resume_raw()
                watchdog_deadline = time.monotonic() + timeout
                await reader._wait_for_data("read")
            while 0 < len(rbuf) < min(188, min_bytes) and rbuf[0] == 0x47 and not reader.at_eof():
                _resume_raw()
                watchdog_deadline = time.monotonic() + timeout
                await reader._wait_for_data("read")
            watchdog_deadline = time.monotonic() + timeout
            if min_bytes > 16384 and not reader.at_eof() and not force_more and rbuf and rbuf[0] == 0x47:
                _pause_raw()

        try:
            if "chunked" in te and rbuf is not None:
                buf = bytearray()
                rem_chunk = 0
                need_crlf = False
                eof_chunked = False
                while not eof_chunked:
                    pos = 0
                    rlen = len(rbuf)
                    if rlen > 0:
                        mv_r = memoryview(rbuf)
                        try:
                            while pos < rlen:
                                if need_crlf:
                                    if rlen - pos < 2:
                                        break
                                    pos += 2
                                    need_crlf = False
                                    continue
                                if rem_chunk > 0:
                                    take_c = min(rem_chunk, rlen - pos)
                                    buf.extend(mv_r[pos:pos + take_c])
                                    pos += take_c
                                    rem_chunk -= take_c
                                    if rem_chunk == 0:
                                        if rlen - pos >= 2:
                                            pos += 2
                                        else:
                                            need_crlf = True
                                            break
                                    continue
                                nl = rbuf.find(b"\r\n", pos)
                                if nl == -1:
                                    break
                                size_str = bytes(mv_r[pos:nl]).split(b";", 1)[0].strip()
                                pos = nl + 2
                                size = int(size_str, 16)
                                if size == 0:
                                    tr_end = rbuf.find(b"\r\n", pos)
                                    if tr_end != -1:
                                        pos = tr_end + 2
                                    eof_chunked = True
                                    self._reusable = can_keepalive
                                    break
                                avail = rlen - pos
                                if avail >= size + 2:
                                    buf.extend(mv_r[pos:pos + size])
                                    pos += size + 2
                                elif avail >= size:
                                    buf.extend(mv_r[pos:pos + size])
                                    pos += size
                                    need_crlf = True
                                    break
                                else:
                                    if avail > 0:
                                        buf.extend(mv_r[pos:rlen])
                                    rem_chunk = size - avail
                                    pos = rlen
                                    break
                        finally:
                            mv_r.release()
                        if pos > 0:
                            del rbuf[:pos]
                            if not coalesce_paused:
                                reader._maybe_resume_transport()
                    while len(buf) >= chunk_size:
                        take = (len(buf) // 188) * 188 if (len(buf) >= 188 and buf[0] == 0x47) else chunk_size
                        if take <= 0:
                            break
                        out = buf[:take]
                        del buf[:take]
                        yield out
                    if eof_chunked:
                        _resume_raw()
                        if buf:
                            yield buf
                        break
                    if reader.at_eof() and pos == 0:
                        _resume_raw()
                        if buf:
                            yield buf
                        break
                    await _wait_coalesced(32712, force_more=(pos == 0))
            elif is_bounded:
                rem = int(clen_str.strip())
                while rem > 0:
                    if rbuf is not None:
                        want = min(chunk_size, rem)
                        target_b = min(want, 262072 if chunk_size >= 262072 else 65424)
                        if len(rbuf) < target_b and len(rbuf) < rem and not reader.at_eof():
                            await _wait_coalesced(target_b)
                        if not rbuf and reader.at_eof():
                            can_keepalive = False
                            break
                        if not rbuf:
                            continue
                        take = min(len(rbuf), chunk_size, rem)
                        if take >= 188 and take < rem and not reader.at_eof() and rbuf[0] == 0x47:
                            take = (take // 188) * 188
                        out = rbuf[:take]
                        del rbuf[:take]
                        if not coalesce_paused:
                            reader._maybe_resume_transport()
                        rem -= len(out)
                        if rem == 0:
                            _resume_raw()
                        yield out
                    else:
                        step = min(chunk_size, rem)
                        data = await asyncio.wait_for(reader.read(step), timeout=timeout)
                        if not data:
                            can_keepalive = False
                            break
                        rem -= len(data)
                        yield bytearray(data)
                if rem == 0:
                    self._reusable = can_keepalive
            else:
                first_stream_read = True
                saw_m3u = False
                target_min = min(chunk_size, 188 * 174)  # 32,712 bytes (174 TS packets) coalesced read
                while True:
                    if rbuf is not None:
                        if first_stream_read and rbuf and (rbuf.lstrip().startswith(b"#EXTM3U") or b"#EXTINF:" in rbuf[:512]):
                            saw_m3u = True
                        if len(rbuf) < target_min and not reader.at_eof():
                            if saw_m3u and not first_stream_read and len(rbuf) == 0:
                                _resume_raw()
                                try:
                                    await asyncio.wait_for(reader._wait_for_data("read"), timeout=0.05)
                                except asyncio.TimeoutError:
                                    break
                            elif not saw_m3u:
                                await _wait_coalesced(target_min)
                                if first_stream_read and rbuf and (rbuf.lstrip().startswith(b"#EXTM3U") or b"#EXTINF:" in rbuf[:512]):
                                    saw_m3u = True
                                    _resume_raw()
                        first_stream_read = False
                        if not rbuf and reader.at_eof():
                            break
                        if not rbuf:
                            if saw_m3u:
                                break
                            continue
                        take = min(len(rbuf), chunk_size)
                        if take >= 188 and not reader.at_eof() and rbuf[0] == 0x47:
                            take = (take // 188) * 188
                        out = rbuf[:take]
                        del rbuf[:take]
                        if not coalesce_paused:
                            reader._maybe_resume_transport()
                        yield out
                    else:
                        data = await asyncio.wait_for(reader.read(chunk_size), timeout=timeout)
                        if not data:
                            break
                        yield bytearray(data)
        finally:
            if watchdog_handle is not None:
                watchdog_handle.cancel()
            _resume_raw()

    async def aclose(self):
        if self._closed:
            return
        self._closed = True
        if self._reader is None or self._writer is None:
            return
        tr = getattr(self._writer, "_transport", None) or getattr(self._writer, "transport", None)
        ssl_proto = getattr(tr, "_ssl_protocol", None) if tr is not None else None
        raw_tr = getattr(ssl_proto, "_transport", None) if ssl_proto is not None else tr
        if raw_tr is not None and getattr(raw_tr, "_paused", False) and not getattr(ssl_proto, "_ssl_reading_paused", False):
            try:
                raw_tr.resume_reading()
            except Exception:
                pass
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


_SHARED_SSL_CTX = None
_SHARED_IO_BUF = bytearray(524288)
_SHARED_IO_MV = memoryview(_SHARED_IO_BUF)


class _AsyncHttpClient:
    """Zero-dependency asyncio HTTP/1.1 + HTTPS client with keep-alive socket pooling."""

    def __init__(self, connect_timeout: float = 10.0, read_timeout: float = 25.0, headers: dict = None):
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.headers = dict(headers or {})
        self._pool = {}

    @staticmethod
    def _get_ssl_context():
        global _SHARED_SSL_CTX
        if _SHARED_SSL_CTX is None:
            import ssl
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            _SHARED_SSL_CTX = ctx
        return _SHARED_SSL_CTX

    @staticmethod
    def _close_writer(writer):
        try:
            tr = getattr(writer, "_transport", None) or getattr(writer, "transport", None)
            if tr is not None and hasattr(tr, "abort"):
                tr.abort()
            else:
                writer.close()
        except Exception:
            pass

    def _sweep_closed_pool(self):
        for k in list(self._pool.keys()):
            b = self._pool[k]
            alive = []
            for r, w in b:
                if w.is_closing() or r.at_eof():
                    self._close_writer(w)
                else:
                    alive.append((r, w))
            if alive:
                self._pool[k] = alive
            else:
                del self._pool[k]

    def _return_connection(self, key, reader, writer):
        self._sweep_closed_pool()
        bucket = self._pool.setdefault(key, [])
        while len(bucket) >= 2:
            _, old_w = bucket.pop(0)
            self._close_writer(old_w)
        bucket.append((reader, writer))

    async def _open_or_reuse(self, scheme: str, host: str, port: int):
        self._sweep_closed_pool()
        key = (scheme, host, port)
        bucket = self._pool.get(key)
        while bucket:
            reader, writer = bucket.pop()
            if not writer.is_closing() and not reader.at_eof():
                return key, reader, writer, True
            self._close_writer(writer)
        ssl_ctx = self._get_ssl_context() if scheme == "https" else None
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ssl_ctx, limit=1024 * 1024),
            timeout=self.connect_timeout,
        )
        sock = writer.get_extra_info("socket")
        if sock is not None:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
            except OSError:
                pass
        tr = getattr(writer, "_transport", None) or getattr(writer, "transport", None)
        ssl_proto = getattr(tr, "_ssl_protocol", None) if tr is not None else None
        if ssl_proto is not None and hasattr(ssl_proto, "_app_protocol_is_buffer"):
            ssl_proto._app_protocol_get_buffer = lambda sz: _SHARED_IO_MV
            ssl_proto._app_protocol_buffer_updated = lambda nbytes: reader.feed_data(_SHARED_IO_MV[:nbytes])
            ssl_proto._app_protocol_is_buffer = True
        elif tr is not None and hasattr(tr, "_read_ready__get_buffer"):
            proto = getattr(tr, "_protocol", None)
            if proto is not None:
                proto.get_buffer = lambda sz: _SHARED_IO_MV
                proto.buffer_updated = lambda nbytes: reader.feed_data(_SHARED_IO_MV[:nbytes])
                tr._read_ready_cb = tr._read_ready__get_buffer
        return key, reader, writer, False

    async def _send_and_read_headers(self, method: str, url: str, accept_gzip: bool = False):
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
                kl = k.lower()
                if kl == "host":
                    continue
                if kl == "accept-encoding" and accept_gzip:
                    req_lines.append("Accept-Encoding: gzip")
                else:
                    req_lines.append(f"{k}: {v}")
            req_bytes = ("\r\n".join(req_lines) + "\r\n\r\n").encode("latin-1", errors="ignore")

            for attempt in range(2):
                key, reader, writer, reused = await self._open_or_reuse(scheme, host, port)
                try:
                    writer.write(req_bytes)
                    await asyncio.wait_for(writer.drain(), timeout=self.connect_timeout)
                    hdr_timeout = 8.0 if reused else self.read_timeout
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
        resp = await self._send_and_read_headers("GET", url, accept_gzip=True)
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

