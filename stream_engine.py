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
from dataclasses import dataclass
import ipaddress
import math
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


@dataclass(frozen=True)
class Segment:
    sequence: int
    data: bytes
    duration: float
    discontinuity: bool = False


class Engine:
    def __init__(self, urls=(), outputs=None, *, queue_size=2048,
                 segment_duration=2.0, max_segments=5,
                 max_segment_bytes=2 * 1024 * 1024,
                 max_hls_bytes=6 * 1024 * 1024, retry_delay=0.5,
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
        self._active_input_id = 1
        self._active_url_opts = {}
        self._filter_keep_pids = None
        self._segments = OrderedDict()
        self._segment_bytes = 0
        self._sequence = 0
        self._evicted_discontinuities = 0
        self._current = bytearray()
        self._current_started = None
        self._current_pcr = None
        self._current_discontinuity = False
        self._playlist_target = max(1, math.ceil(segment_duration))
        self._apply_filter_options({})

    def _apply_filter_options(self, url_opts: dict):
        self._active_url_opts = dict(url_opts)
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
        self._buffer.clear()
        self._preroll.clear()
        self._locked = False

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *_):
        await self.stop()

    def feed(self, data: bytes):
        """Accept arbitrary TS byte chunks; return the number of framed packets.

        Uses a zero-copy fast path when already locked and buffer is empty.
        """
        if self._closed or not data:
            return 0
        # Fast path: buffer is empty, already locked, and data starts with valid 0x47 packets
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
                return packets
            if self._locked:
                self._buffer.extend(mv[pos:])
                return packets
            data = bytes(mv[pos:])

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
        return packets

    def _packet(self, packet):
        self._counts["packets"] += 1
        self._counts["bytes"] += 188
        pid = ((packet[1] & 31) << 8) | packet[2]
        if self._active_url_opts.get("no_eit") and pid == 0x12:
            return
        if self._active_url_opts.get("no_sdt") and pid == 0x11:
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
                    duplicate = payload and cc == state["cc"] and packet == state["packet"]
                    if duplicate:
                        self._counts["duplicates"] += 1
                    elif cc != expected:
                        broken = True
                        self._counts["continuity_errors"] += 1
                        state["continuity_errors"] += 1
                state["cc"] = cc
                if payload:
                    state["packet"] = packet
                    # Check PES header start code on known elementary video/audio PIDs
                    if pusi and not scrambled and offset + 4 <= 188:
                        if pid in self._elementary_pids and self._elementary_pids[pid] in ("VIDEO", "AUDIO"):
                            if packet[offset:offset + 3] != b"\x00\x00\x01":
                                self._counts["pes_errors"] += 1
                                state["pes_errors"] += 1
                        elif pid > 0x1F and pid not in self._pmt_pids and packet[offset:offset + 3] == b"\x00\x00\x01":
                            stream_id = packet[offset + 3]
                            if 0xE0 <= stream_id <= 0xEF:
                                self._elementary_pids.setdefault(pid, "VIDEO")
                            elif 0xC0 <= stream_id <= 0xDF or stream_id == 0xBD:
                                self._elementary_pids.setdefault(pid, "AUDIO")

        if (broken or discontinuity) and pid in self._psi:
            self._psi[pid].reset()
        if not tei and not scrambled and not duplicate and payload and pid in self._psi:
            for section, packets in self._psi[pid].push(packet[offset:], pusi, packet):
                self._cache_table(pid, section, packets)
        if pid != 0x1FFF and not tei and not duplicate:
            self._observe_pcr(pid, pcr, discontinuity)
            # Update bounded pre-roll buffer for instant VLC .ts startup
            if random_access and self._elementary_pids.get(pid) == "VIDEO":
                self._preroll.clear()
            self._preroll.extend(packet)
            if len(self._preroll) > PREROLL_MAX_BYTES:
                excess = ((len(self._preroll) - PREROLL_MAX_BYTES + 187) // 188) * 188
                del self._preroll[:excess]
        self._hls_packet(packet)
        for queue in self._subscribers:
            self._enqueue(queue, packet, "queue_drops")
        for _, _, queue in self._output_states:
            self._enqueue(queue, packet, "output_drops")

    def _observe_pcr(self, pid, pcr, discontinuity):
        if self._pcr_pid is None and pcr is not None:
            self._pcr_pid = pid
        if pid != self._pcr_pid:
            return
        if discontinuity:
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
        if crc32_mpeg(section):
            return
        table_id = section[0]
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
        """Return PAT + PMT prefix followed by recent aligned TS packets for instant VLC playback."""
        prefix = self._prefix()
        preroll = bytes(self._preroll)
        if not prefix and not preroll:
            return b""
        return prefix + preroll

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
                "bitrate": int(round(st["bitrate"] / 1000.0)),
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

    def _hls_packet(self, packet):
        if self._current and (self._elapsed_segment() >= self.segment_duration or
                              len(self._current) + 188 > self.max_segment_bytes):
            self._finish_segment()
        if not self._current:
            self._current_started = self._clock()
            self._current_pcr = self._timeline if self._pcr_last is not None else None
            prefix = self._prefix()
            # Leave room for actual stream data even with unusually many tables.
            if len(prefix) + 188 <= self.max_segment_bytes:
                self._current.extend(prefix)
        self._current.extend(packet)

    def _finish_segment(self):
        if not self._current:
            return
        duration = max(0.001, self._elapsed_segment())
        segment = Segment(self._sequence, bytes(self._current), duration,
                          self._current_discontinuity)
        self._segments[self._sequence] = segment
        self._sequence += 1
        self._segment_bytes += len(segment.data)
        self._playlist_target = max(self._playlist_target, math.ceil(duration))
        self._current.clear()
        self._current_started = None
        self._current_pcr = None
        self._current_discontinuity = False
        while (len(self._segments) > self.max_segments or
               self._segment_bytes > self.max_hls_bytes):
            _, old = self._segments.popitem(last=False)
            self._segment_bytes -= len(old.data)
            self._evicted_discontinuities += int(old.discontinuity)

    async def wait_for_hls(self, timeout: float = 3.0) -> bool:
        """Wait briefly for the first HLS segment or flush an initial segment if ready."""
        if self._segments:
            return True
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and self._running:
            if self._segments:
                return True
            if len(self._current) >= 188 * 48 and self._elapsed_segment() >= 0.5:
                self._finish_segment()
                return True
            await asyncio.sleep(0.05)
        if not self._segments and len(self._current) >= 188 * 16:
            self._finish_segment()
        return bool(self._segments)

    def hls_playlist(self):
        """Return an M3U8 string with relative segment-<sequence>.ts URIs."""
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
        segment = self._segments.get(int(sequence))
        return None if segment is None else segment.data

    def stats(self):
        elapsed = max(0.0, self._clock() - self._created)
        avg_bitrate = self._counts["bytes"] * 8 / elapsed if elapsed else 0.0
        effective_bitrate = self._pcr_bitrate if (self._pcr_bitrate and self._pcr_bitrate > 0 and elapsed > 2.0) else avg_bitrate
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
                        self._pcr_pid = None
                        self._tables.clear()
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
        """Cesbo Astra sync_buffer HLS input: background segment prefetcher + smooth TS pacer."""
        seen_segments = OrderedDict()
        prefetch_queue: asyncio.Queue = asyncio.Queue(maxsize=4)
        producer_error = None

        async def _prefetch_producer():
            nonlocal playlist_url, producer_error
            first_load = True
            consecutive_errors = 0
            target_duration = 2.0
            try:
                while self._running:
                    try:
                        resp = await client.get(playlist_url)
                        resp.raise_for_status()
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
                        await asyncio.sleep(0.4)
                        continue

                    text = resp.text
                    lines = [line.strip() for line in text.splitlines() if line.strip()]
                    if any(line.startswith("#EXT-X-STREAM-INF") for line in lines):
                        for i, line in enumerate(lines):
                            if line.startswith("#EXT-X-STREAM-INF") and i + 1 < len(lines):
                                nxt = lines[i + 1]
                                if not nxt.startswith("#"):
                                    playlist_url = urljoin(str(resp.url), nxt)
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
                            seg_url = urljoin(str(resp.url), line)
                            seg_entries.append((seg_url, next_dur if next_dur is not None else target_duration))
                            next_dur = None

                    if first_load and len(seg_entries) > 2:
                        # Start 2 segments before live edge so sync_buffer immediately has lookahead
                        for old_url, _ in seg_entries[:-2]:
                            seen_segments[old_url] = True
                    first_load = False

                    new_entries = [(u, d) for (u, d) in seg_entries if u not in seen_segments]
                    for seg_url, seg_dur in new_entries:
                        if not self._running:
                            return
                        seen_segments[seg_url] = True
                        while len(seen_segments) > 150:
                            seen_segments.popitem(last=False)
                        try:
                            seg_resp = await client.get(seg_url)
                            seg_resp.raise_for_status()
                            seg_data = seg_resp.content
                            if seg_data:
                                await prefetch_queue.put((seg_dur, seg_data))
                                consecutive_errors = 0
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            consecutive_errors += 1
                            self._counts["input_errors"] += 1
                            self._last_error = f"{type(exc).__name__}: {exc}"
                            if consecutive_errors >= 8:
                                producer_error = exc
                                return
                    poll_sleep = min(0.8, max(0.25, target_duration * 0.35)) if new_entries else 0.35
                    await asyncio.sleep(poll_sleep)
            finally:
                await prefetch_queue.put(None)

        producer_task = asyncio.create_task(_prefetch_producer())
        first_segment = True
        chunk_bytes = 188 * 32  # 6,016 bytes (32 TS packets per paced slice)
        try:
            while self._running:
                item = await prefetch_queue.get()
                if item is None:
                    break
                seg_dur, seg_data = item
                total_len = len(seg_data)
                if total_len <= chunk_bytes:
                    self.feed(seg_data)
                    await asyncio.sleep(0)
                    first_segment = False
                    continue

                pos = 0
                if first_segment:
                    # Immediate bootstrap burst (~48 KB) so PAT/PMT and initial video lock instantly
                    burst_len = min(total_len, 188 * 256)
                    self.feed(seg_data[:burst_len])
                    pos = burst_len
                    first_segment = False
                    await asyncio.sleep(0)
                    if pos >= total_len:
                        continue

                remaining_bytes = total_len - pos
                num_steps = max(1, (remaining_bytes + chunk_bytes - 1) // chunk_bytes)
                # Adaptive sync_buffer PLL pacing based on lookahead queue depth
                qdepth = prefetch_queue.qsize()
                if qdepth >= 2:
                    pace_factor = 0.76
                elif qdepth == 1:
                    pace_factor = 0.90
                else:
                    pace_factor = 0.98
                effective_dur = seg_dur * (remaining_bytes / total_len) * pace_factor
                step_sleep = min(0.05, max(0.0, effective_dur / num_steps))

                while pos < total_len and self._running:
                    t0 = time.monotonic()
                    end = min(total_len, pos + chunk_bytes)
                    self.feed(seg_data[pos:end])
                    pos = end
                    # Re-check queue depth dynamically: if 2+ segments buffered, speed up slightly
                    if prefetch_queue.qsize() >= 2 and step_sleep > 0.005:
                        target_wait = step_sleep * 0.75
                    else:
                        target_wait = step_sleep
                    elapsed = time.monotonic() - t0
                    wait = target_wait - elapsed
                    if wait > 0.001:
                        await asyncio.sleep(wait)
                    else:
                        await asyncio.sleep(0)
            if producer_error is not None:
                raise producer_error
        finally:
            producer_task.cancel()
            await asyncio.gather(producer_task, return_exceptions=True)

    async def _http_input(self, url, url_opts=None):
        import httpx
        url_opts = url_opts or {}
        clean_url = url.split("#", 1)[0]
        conn_timeout = float(url_opts.get("timeout", 10.0)) if str(url_opts.get("timeout", "")).replace(".", "").isdigit() else 10.0
        ua = url_opts.get("ua") or "AstraG/2.0"
        timeout = httpx.Timeout(connect=conn_timeout, read=30.0, write=10.0, pool=10.0)
        limits = httpx.Limits(max_keepalive_connections=6, max_connections=12)
        headers = {"User-Agent": ua, "Accept": "*/*", "Icy-MetaData": "0", "Connection": "keep-alive"}
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout, limits=limits, headers=headers) as client:
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
                    first_chunk = True
                    async for chunk in response.aiter_bytes(chunk_size=188 * 32):
                        if not self._running:
                            return
                        if first_chunk:
                            first_chunk = False
                            stripped = chunk.lstrip()
                            if stripped.startswith(b"#EXTM3U"):
                                switch_to_hls = True
                                break
                        self.feed(chunk)
                        await asyncio.sleep(0)
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
