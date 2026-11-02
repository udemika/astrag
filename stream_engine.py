"""Independent asyncio MPEG-TS transport engine (no transcoder).

Use ``await Engine(urls, outputs=[]).start()``; ``subscribe()`` returns a bounded
asyncio.Queue containing 188-byte packets and finally END_OF_STREAM (None).
Call unsubscribe(queue) or await stop() to close subscriptions. feed(bytes) is
synchronous and may also be used without start(). All methods belong on one
asyncio event-loop thread. Overflow drops the oldest queued packet.

Inputs: HTTP(S), udp://[address]:port, rtp://[address]:port. IPv4 multicast
supports ?interface=192.0.2.1; outputs support udp://host:port?ttl=1. Multiple
inputs are ordered failover sources, never interleaved. HTTP requires httpx.
RAM HLS is a packet-aligned sliding window, NOT keyframe-aligned or guaranteed
independently decodable. PCR drives durations when available; otherwise arrival
time is an estimate. Only completed segments appear in the playlist.
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
from urllib.parse import parse_qs, urlsplit

PACKET_SIZE = 188
PCR_HZ = 27_000_000
PCR_WRAP = (1 << 33) * 300
END_OF_STREAM = None

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
    def __init__(self, urls=(), outputs=None, *, queue_size=256,
                 segment_duration=2.0, max_segments=6,
                 max_segment_bytes=8 * 1024 * 1024,
                 max_hls_bytes=32 * 1024 * 1024, retry_delay=0.5,
                 udp_timeout=10.0, clock: Callable[[], float] = time.monotonic):
        if isinstance(urls, str):
            urls = [urls]
        self.urls = list(urls)
        self.outputs = list(outputs or [])
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
                            duplicates=0, null_packets=0, queue_drops=0,
                            output_drops=0, output_errors=0, input_errors=0,
                            retries=0, rtp_errors=0, discontinuities=0)
        self._last_error = None
        self._pids = {}
        self._pcr_pid = None
        self._pcr_last = None
        self._pcr_index = None
        self._timeline = 0.0
        self._pcr_bitrate = None
        self._psi = {0: _PSI()}
        self._tables = OrderedDict()
        self._pat_identity = None
        self._pat_sections = {}
        self._pmt_pids = set()
        self._segments = OrderedDict()
        self._segment_bytes = 0
        self._sequence = 0
        self._evicted_discontinuities = 0
        self._current = bytearray()
        self._current_started = None
        self._current_pcr = None
        self._current_discontinuity = False
        self._playlist_target = max(1, math.ceil(segment_duration))

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
        self._locked = False

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *_):
        await self.stop()

    def feed(self, data: bytes):
        """Accept arbitrary TS byte chunks; return the number of framed packets.

        A single plausible packet is accepted immediately; when searching within
        a larger chunk, the next available packet sync is also checked.
        """
        if self._closed:
            return 0
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
            del buf[:pos]
        return packets

    def _packet(self, packet):
        self._counts["packets"] += 1
        self._counts["bytes"] += 188
        pid = ((packet[1] & 31) << 8) | packet[2]
        afc = (packet[3] >> 4) & 3
        cc = packet[3] & 15
        payload = bool(afc & 1)
        tei = bool(packet[1] & 128)
        scrambled = bool(packet[3] & 192)
        offset = 4
        discontinuity = False
        pcr = None
        if afc & 2:
            length = packet[4]
            offset += 1 + length
            if length:
                flags = packet[5]
                discontinuity = bool(flags & 128)
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
            state = self._pids.setdefault(pid, dict(cc=None, packet=None, packets=0,
                                                   continuity_errors=0))
            state["packets"] += 1
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
        if (broken or discontinuity) and pid in self._psi:
            self._psi[pid].reset()
        if not tei and not scrambled and not duplicate and payload and pid in self._psi:
            for section, packets in self._psi[pid].push(packet[offset:], bool(packet[1] & 64), packet):
                self._cache_table(pid, section, packets)
        if pid != 0x1FFF and not tei and not duplicate:
            self._observe_pcr(pid, pcr, discontinuity)
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
        crc = 0xFFFFFFFF
        for value in section:
            crc ^= value << 24
            for _ in range(8):
                crc = ((crc << 1) ^ (0x04C11DB7 if crc & 0x80000000 else 0)) & 0xFFFFFFFF
        if crc:
            return
        if pid == 0 and section[0] == 0:
            if (len(section) - 12) % 4:
                return
            identity = (section[3:6], section[7])
            if identity != self._pat_identity:
                self._pat_identity = identity
                self._pat_sections.clear()
                for key in list(self._tables):
                    if key[0] == 0:
                        del self._tables[key]
            pids = set()
            for index in range(8, len(section) - 4, 4):
                program = int.from_bytes(section[index:index + 2], "big")
                if program:
                    pids.add(((section[index + 2] & 31) << 8) | section[index + 3])
            if section[6] >= 32:
                return
            self._pat_sections[section[6]] = pids
            self._pmt_pids = set(sorted(set().union(*self._pat_sections.values()))[:32])
            self._psi = {key: value for key, value in self._psi.items()
                         if key == 0 or key in self._pmt_pids}
            for pmt in self._pmt_pids:
                self._psi.setdefault(pmt, _PSI())
            for key in list(self._tables):
                if key[0] and key[0] not in self._pmt_pids:
                    del self._tables[key]
        elif pid not in self._pmt_pids or section[0] != 2 or len(section) < 16:
            return
        key = (pid, section[6])
        self._tables[key] = packets
        self._tables.move_to_end(key)
        while len(self._tables) > 64:
            self._tables.popitem(last=False)

    def _prefix(self):
        packets = []
        seen = set()
        for key in sorted(self._tables):
            for packet in self._tables[key]:
                if packet not in seen:
                    packets.append(packet)
                    seen.add(packet)
        return b"".join(packets)

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
        return {**self._counts, "running": self._running, "elapsed": elapsed,
                "bitrate": self._counts["bytes"] * 8 / elapsed if elapsed else 0.0,
                "pcr_bitrate": self._pcr_bitrate, "pcr_pid": self._pcr_pid,
                "pcr": self._pcr_last, "subscribers": len(self._subscribers),
                "hls_segments": len(self._segments),
                "hls_bytes": self._segment_bytes + len(self._current),
                "last_error": self._last_error,
                "pids": {pid: {k: v for k, v in state.items() if k != "packet"}
                         for pid, state in self._pids.items()}}

    async def _run_inputs(self):
        while self._running:
            for url in self.urls:
                if not self._running:
                    return
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
                    self._pcr_pid = None
                    self._tables.clear()
                    self._pat_sections.clear()
                    self._pat_identity = None
                    self._pmt_pids.clear()
                    self._psi = {0: _PSI()}
                try:
                    scheme = urlsplit(url).scheme.lower()
                    if scheme in ("http", "https"):
                        await self._http_input(url)
                    elif scheme in ("udp", "rtp"):
                        await self._udp_input(url, scheme == "rtp")
                    else:
                        raise ValueError(f"unsupported input scheme: {scheme}")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._counts["input_errors"] += 1
                    self._last_error = f"{type(exc).__name__}: {exc}"
                self._counts["retries"] += 1
                await asyncio.sleep(self.retry_delay)

    async def _http_input(self, url):
        import httpx
        from urllib.parse import urljoin
        clean_url = url.split("#", 1)[0]
        timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
            if clean_url.lower().split("?", 1)[0].endswith((".m3u8", ".m3u")):
                seen_segments = OrderedDict()
                playlist_url = clean_url
                while self._running:
                    resp = await client.get(playlist_url)
                    resp.raise_for_status()
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
                    seg_urls = [urljoin(str(resp.url), line) for line in lines if not line.startswith("#")]
                    new_segs = [u for u in seg_urls if u not in seen_segments]
                    for seg_url in new_segs:
                        if not self._running:
                            return
                        seen_segments[seg_url] = True
                        while len(seen_segments) > 200:
                            seen_segments.popitem(last=False)
                        seg_resp = await client.get(seg_url)
                        seg_resp.raise_for_status()
                        self.feed(seg_resp.content)
                    await asyncio.sleep(1.0 if new_segs else 1.5)
                return
            async with client.stream("GET", clean_url) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes():
                    self.feed(chunk)

    @staticmethod
    def _udp_address(url):
        parsed = urlsplit(url)
        if parsed.username not in (None, "") or parsed.password is not None:
            raise ValueError("UDP URLs do not support credentials")
        host = parsed.hostname or "0.0.0.0"
        port = parsed.port
        if port is None or not 1 <= port <= 65535:
            raise ValueError("UDP URL requires a valid port")
        return host, port, parse_qs(parsed.query)

    async def _udp_input(self, url, rtp=False):
        host, port, query = self._udp_address(url)
        group = ipaddress.IPv4Address(host)
        interface = str(ipaddress.IPv4Address(query.get("interface", ["0.0.0.0"])[0]))
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        membership = None
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
                await loop.sock_sendto(sock, packet, address)
            except OSError as exc:
                self._counts["output_errors"] += 1
                self._last_error = f"{type(exc).__name__}: {exc}"
            finally:
                queue.task_done()
