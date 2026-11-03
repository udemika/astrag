import asyncio
import ipaddress
import socket
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from stream_engine import END_OF_STREAM, Engine, PCR_HZ, PCR_WRAP, strip_rtp


def packet(pid=256, cc=0, payload=b"x", *, pcr=None, discontinuity=False,
           adaptation_only=False, start=False, tei=False):
    flags = (128 if discontinuity else 0) | (16 if pcr is not None else 0)
    adaptation = b""
    if flags or adaptation_only:
        body = bytes([flags])
        if pcr is not None:
            base, ext = divmod(pcr, 300)
            body += bytes([(base >> 25) & 255, (base >> 17) & 255,
                           (base >> 9) & 255, (base >> 1) & 255,
                           ((base & 1) << 7) | 0x7E | (ext >> 8), ext & 255])
        length = 183 if adaptation_only else len(body)
        adaptation = bytes([length]) + body + b"\xff" * (length - len(body))
    afc = 2 if adaptation_only else (3 if adaptation else 1)
    header = bytes([0x47, (pid >> 8) | (64 if start else 0) | (128 if tei else 0),
                    pid & 255, (afc << 4) | cc])
    return (header + adaptation + (b"" if adaptation_only else payload)).ljust(188, b"\xff")


def crc32_mpeg(data):
    crc = 0xFFFFFFFF
    for value in data:
        crc ^= value << 24
        for _ in range(8):
            crc = ((crc << 1) ^ (0x04C11DB7 if crc & 0x80000000 else 0)) & 0xFFFFFFFF
    return crc


def section(table, body):
    length = len(body) + 4
    raw = bytes([table, 0xB0 | (length >> 8), length & 255]) + body
    return raw + crc32_mpeg(raw).to_bytes(4, "big")


def pat(pmt_pid=100):
    return section(0, b"\x00\x01\xc1\x00\x00\x00\x01" +
                   bytes([0xE0 | (pmt_pid >> 8), pmt_pid & 255]))


def pmt():
    return section(2, b"\x00\x01\xc1\x00\x00\xe1\x00\xf0\x00" +
                   b"\x1b\xe1\x00\xf0\x00")


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class FramingTests(unittest.TestCase):
    def test_arbitrary_chunks_and_resync(self):
        engine = Engine()
        packets = [packet(cc=i) for i in range(10)]
        raw = b"junk" + b"".join(packets[:5]) + b"noise" + b"".join(packets[5:])
        count = 0
        for index in range(0, len(raw), 31):
            count += engine.feed(raw[index:index + 31])
        self.assertEqual(count, 10)
        self.assertEqual(engine.stats()["discarded_bytes"], 9)
        self.assertEqual(engine.stats()["sync_losses"], 1)
        self.assertEqual(engine.stats()["continuity_errors"], 0)

    def test_false_sync_and_invalid_afc(self):
        engine = Engine()
        bogus = b"\x47\x00\x00\x00" + bytes(184)
        malformed = b"\x47\x00\x00\x30\xff" + bytes(183)
        self.assertEqual(engine.feed(bogus + malformed + packet() + packet(cc=1)), 2)
        self.assertEqual(engine.stats()["discarded_bytes"], 376)
        self.assertLess(len(engine._buffer), 188)

    def test_continuity_duplicate_adaptation_null_discontinuity(self):
        engine = Engine()
        data = [packet(cc=15), packet(cc=15),
                packet(cc=15, adaptation_only=True), packet(cc=0),
                packet(cc=3), packet(cc=9, discontinuity=True), packet(cc=10),
                packet(pid=8191, cc=2), packet(pid=8191, cc=12)]
        engine.feed(b"".join(data))
        stats = engine.stats()
        self.assertEqual(stats["duplicates"], 1)
        self.assertEqual(stats["continuity_errors"], 1)
        self.assertEqual(stats["discontinuities"], 1)
        self.assertEqual(stats["null_packets"], 2)

    def test_same_cc_different_payload_is_not_duplicate(self):
        engine = Engine()
        engine.feed(packet(payload=b"a") + packet(payload=b"b"))
        self.assertEqual(engine.stats()["continuity_errors"], 1)
        self.assertEqual(engine.stats()["duplicates"], 0)

    def test_tei_resets_continuity(self):
        engine = Engine()
        engine.feed(packet(cc=0) + packet(cc=9, tei=True) + packet(cc=4))
        self.assertEqual(engine.stats()["transport_errors"], 1)
        self.assertEqual(engine.stats()["continuity_errors"], 0)

    def test_pcr_extension_wrap_bitrate_and_clock(self):
        clock = Clock()
        engine = Engine(clock=clock)
        engine.feed(packet(pcr=PCR_WRAP - 100))
        clock.now = 1
        engine.feed(packet(cc=1, pcr=200))
        stats = engine.stats()
        self.assertEqual(stats["pcr"], 200)
        self.assertAlmostEqual(stats["pcr_bitrate"], 188 * 8 * PCR_HZ / 300)
        self.assertEqual(stats["bitrate"], 2 * 188 * 8)
        engine.feed(packet(cc=2, pcr=1234567, discontinuity=True))
        self.assertIsNone(engine.stats()["pcr_bitrate"])
        clock.now = 2
        self.assertEqual(engine.stats()["bitrate"], 3 * 188 * 8 / 2)

    def test_pcr_is_base_times_300_plus_extension(self):
        engine = Engine()
        engine.feed(packet(pcr=123456789 * 300 + 299))
        self.assertEqual(engine.stats()["pcr"], 123456789 * 300 + 299)

    def test_rtp_variable_headers_and_padding(self):
        data = packet()
        header = bytes([0xB2, 33]) + bytes(10) + bytes(8)
        extension = b"\xbe\xde\x00\x02" + bytes(8)
        self.assertEqual(strip_rtp(header + extension + data + b"\0\0\0\4"), data)
        for invalid in [b"", bytes(12), bytes([0x82]) + bytes(11),
                        bytes([0x90]) + bytes(11),
                        bytes([0xA0]) + bytes(11) + b"\x00",
                        bytes([0xA0]) + bytes(11) + b"\xff"]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                strip_rtp(invalid)


class HLSTests(unittest.TestCase):
    def test_packet_aligned_sliding_window_and_tables(self):
        clock = Clock()
        engine = Engine(clock=clock, segment_duration=1, max_segments=2,
                        max_segment_bytes=188 * 10, max_hls_bytes=188 * 20)
        pat_packet = packet(pid=0, payload=b"\x00" + pat(), start=True)
        pmt_packet = packet(pid=100, payload=b"\x00" + pmt(), start=True)
        engine.feed(pat_packet + pmt_packet + packet(pcr=0))
        for i in range(1, 5):
            clock.now = i
            engine.feed(packet(cc=i, pcr=i * PCR_HZ))
        self.assertIsNone(engine.get_segment(0))
        self.assertEqual(len(engine._segments), 2)
        for segment in engine._segments.values():
            self.assertEqual(len(segment.data) % 188, 0)
            self.assertTrue(segment.data.startswith(pat_packet + pmt_packet))
            self.assertAlmostEqual(segment.duration, 1)
        playlist = engine.hls_playlist()
        self.assertIn("#EXT-X-MEDIA-SEQUENCE:2", playlist)
        self.assertNotIn("INDEPENDENT-SEGMENTS", playlist)
        self.assertNotIn("ENDLIST", playlist)

    def test_memory_bound_without_clock_progress(self):
        engine = Engine(clock=Clock(), max_segments=3, max_segment_bytes=188 * 3,
                        max_hls_bytes=188 * 5)
        engine.feed(b"".join(packet(cc=i % 16) for i in range(1000)))
        self.assertLessEqual(engine.stats()["hls_bytes"], 188 * 8)
        self.assertLessEqual(len(engine._segments), 3)
        for segment in engine._segments.values():
            self.assertGreater(segment.duration, 0)
            self.assertLessEqual(len(segment.data), 188 * 3)

    def test_split_pat_and_pointer_field(self):
        engine = Engine()
        raw = pat()
        # Use adaptation stuffing rather than payload stuffing inside a section.
        prefix = bytes([0x47, 0x40, 0, 0x30, 177, 0]) + b"\xff" * 176
        first = prefix + b"\x00" + raw[:5]
        self.assertEqual(len(first), 188)
        engine.feed(first + packet(pid=0, cc=1, payload=raw[5:]))
        self.assertIn(100, engine._pmt_pids)
        self.assertEqual(len(engine._tables[(0, 0)]), 2)

    def test_discontinuity_and_pcr_duration_not_wall_time(self):
        clock = Clock()
        engine = Engine(clock=clock, segment_duration=1)
        engine.feed(packet(pcr=0))
        clock.now = 0.1
        engine.feed(packet(cc=1, pcr=PCR_HZ))
        self.assertEqual(engine._segments[0].duration, 1)
        engine.feed(packet(cc=7, pcr=0, discontinuity=True))
        engine.feed(packet(cc=8, pcr=PCR_HZ))
        self.assertIn("#EXT-X-DISCONTINUITY\n", engine.hls_playlist())


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_fanout_overflow_sentinel_and_restart(self):
        engine = Engine(queue_size=2)
        slow = engine.subscribe()
        fast = engine.subscribe(4)
        await engine.start()
        engine.feed(packet() + packet(cc=1) + packet(cc=2))
        self.assertEqual(slow.qsize(), 2)
        self.assertEqual(fast.qsize(), 3)
        self.assertEqual((await slow.get())[3] & 15, 1)
        slow.task_done()
        engine.unsubscribe(slow)
        self.assertIs(await slow.get(), END_OF_STREAM)
        slow.task_done()
        await engine.stop()
        self.assertIs(await fast.get(), END_OF_STREAM)
        fast.task_done()
        await fast.join()
        self.assertEqual(engine.stats()["subscribers"], 0)
        self.assertIn("ENDLIST", engine.hls_playlist())
        self.assertIs(await engine.subscribe().get(), END_OF_STREAM)
        await engine.start()
        fresh = engine.subscribe()
        engine.feed(packet(cc=3))
        self.assertIsNotNone(await fresh.get())
        await engine.stop()
        await engine.stop()

    async def test_udp_loopback_output_and_cleanup(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver.bind(("127.0.0.1", 0))
        receiver.setblocking(False)
        engine = Engine(outputs=[f"udp://127.0.0.1:{receiver.getsockname()[1]}"])
        try:
            await engine.start()
            sock = engine._output_states[0][0]
            engine.feed(packet())
            data, _ = await asyncio.wait_for(
                asyncio.get_running_loop().sock_recvfrom(receiver, 2048), 2)
            self.assertEqual(data, packet())
            await engine.stop()
            self.assertEqual(sock.fileno(), -1)
            self.assertEqual(engine._tasks, [])
        finally:
            await engine.stop()
            receiver.close()

    async def test_udp_rtp_input_and_cancellation(self):
        reserve = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        reserve.bind(("127.0.0.1", 0))
        port = reserve.getsockname()[1]
        reserve.close()
        engine = Engine([f"rtp://127.0.0.1:{port}"], retry_delay=0.01)
        queue = engine.subscribe()
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            await engine.start()
            # Retry sends rather than depending on task scheduling/startup latency.
            for _ in range(100):
                sender.sendto(b"\x80\x21" + bytes(10) + packet(), ("127.0.0.1", port))
                try:
                    received = await asyncio.wait_for(queue.get(), 0.02)
                    break
                except asyncio.TimeoutError:
                    continue
            else:
                self.fail("UDP receiver never became ready")
            self.assertEqual(received, packet())
            await engine.stop()
            rebound = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                rebound.bind(("127.0.0.1", port))
            finally:
                rebound.close()
        finally:
            await engine.stop()
            sender.close()

    async def test_multicast_full_slash_four_and_membership_cleanup(self):
        class FakeSocket:
            def __init__(self):
                self.options = []
                self.closed = False

            def setsockopt(self, *args):
                self.options.append(args)

            def setblocking(self, value):
                pass

            def bind(self, address):
                self.address = address

            def close(self):
                self.closed = True

        loop = asyncio.get_running_loop()
        for host in ("224.0.0.1", "230.1.2.3", "239.255.255.250"):
            fake = FakeSocket()
            engine = Engine()
            async def cancelled(*args):
                raise asyncio.CancelledError()
            socket_proxy = SimpleNamespace(**{name: getattr(socket, name) for name in dir(socket)})
            socket_proxy.socket = lambda *args: fake
            with patch("stream_engine.socket", socket_proxy), \
                    patch.object(loop, "sock_recvfrom", side_effect=cancelled):
                with self.assertRaises(asyncio.CancelledError):
                    await engine._udp_input(f"udp://{host}:12345")
            self.assertTrue(fake.closed)
            self.assertEqual(fake.address, ("0.0.0.0", 12345))
            options = [item[1] for item in fake.options]
            self.assertIn(socket.IP_ADD_MEMBERSHIP, options)
            self.assertIn(socket.IP_DROP_MEMBERSHIP, options)
            self.assertTrue(ipaddress.IPv4Address(host).is_multicast)

    async def test_http_status_retry_and_streaming(self):
        requests = 0
        first_connection_closed = asyncio.Event()
        async def handler(reader, writer):
            nonlocal requests
            try:
                await reader.readuntil(b"\r\n\r\n")
                requests += 1
                if requests == 1:
                    body = b"unavailable"
                    writer.write(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 11\r\n\r\n" + body)
                else:
                    body = packet() + packet(cc=1)
                    writer.write(f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
                first_connection_closed.set()
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        engine = Engine([f"http://127.0.0.1:{port}/stream"], retry_delay=0.01)
        queue = engine.subscribe()
        try:
            await engine.start()
            self.assertEqual(await asyncio.wait_for(queue.get(), 5), packet())
            self.assertGreaterEqual(requests, 2)
            self.assertGreaterEqual(engine.stats()["input_errors"], 1)
            self.assertGreaterEqual(engine.stats()["retries"], 1)
            self.assertIn("503", engine.stats()["last_error"])
        finally:
            await engine.stop()
            server.close()
            await server.wait_closed()

    async def test_start_failure_closes_already_open_outputs(self):
        engine = Engine(outputs=["udp://127.0.0.1:12345", "invalid://host"])
        with self.assertRaises(ValueError):
            await engine.start()
        self.assertEqual(engine._output_states, [])
        self.assertEqual(engine._tasks, [])
        await engine.stop()

    def test_psi_summary_and_bootstrap_burst(self):
        engine = Engine()
        pat_packet = packet(pid=0, payload=b"\x00" + pat(100), start=True)
        pmt_packet = packet(pid=100, payload=b"\x00" + pmt(), start=True)
        pes_video = packet(pid=256, cc=0, payload=b"\x00\x00\x01\xe0\x00\x00\x80\x00\x00", start=True)
        engine.feed(pat_packet + pmt_packet + pes_video)
        burst = engine.bootstrap_burst()
        self.assertTrue(burst.startswith(pat_packet + pmt_packet))
        self.assertEqual(len(burst) % 188, 0)
        summary = engine.psi_summary()
        self.assertEqual(summary["pat"]["tsid"], 1)
        self.assertEqual(summary["pmt"][0]["pnr"], 1)
        self.assertEqual(summary["pmt"][0]["streams"][0]["type_name"], "VIDEO")


if __name__ == "__main__":
    unittest.main()
