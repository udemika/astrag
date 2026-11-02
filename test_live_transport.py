"""Loopback integration checks against an explicitly started local backend."""
import asyncio
import json
import os
import socket
import unittest

import httpx

BASE = os.environ.get('ASTRA_TEST_URL', '')


def packet(cc):
    return bytes((0x47, 0x01, 0x00, 0x10 | (cc & 15))) + bytes((0x55,)) * 184


@unittest.skipUnless(BASE, 'Set ASTRA_TEST_URL to an explicitly started loopback test backend')
class LiveTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_udp_two_http_viewers(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
        sock.close()
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sid = 'loopback-integration'
        async with httpx.AsyncClient(base_url=BASE, timeout=10) as client:
            response = await client.post('/control/', json={
                'cmd': 'set-stream', 'id': sid,
                'stream': {'id': sid, 'name': 'Loopback test', 'enable': True,
                           'input': [f'udp://127.0.0.1:{port}'], 'output': []},
            })
            self.assertEqual(response.status_code, 200, response.text)
            ready = [asyncio.Event(), asyncio.Event()]

            async def viewer(index):
                async with client.stream('GET', f'/play/{sid}') as result:
                    self.assertEqual(result.status_code, 200)
                    ready[index].set()
                    chunks = bytearray()
                    async for chunk in result.aiter_bytes():
                        chunks.extend(chunk)
                        if len(chunks) >= 188 * 7:
                            break
                    return bytes(chunks)

            readers = [asyncio.create_task(viewer(i)) for i in range(2)]
            try:
                await asyncio.wait_for(asyncio.gather(*(event.wait() for event in ready)), 5)
                for i in range(20):
                    sender.sendto(b''.join(packet(j) for j in range(i * 7, i * 7 + 7)), ('127.0.0.1', port))
                    await asyncio.sleep(0.02)
                results = await asyncio.wait_for(asyncio.gather(*readers), 5)
                for data in results:
                    self.assertGreaterEqual(len(data), 188 * 7)
                    self.assertEqual(len(data) % 188, 0)
                    self.assertTrue(all(data[i] == 0x47 for i in range(0, len(data), 188)))
                self.assertEqual(results[0][:188 * 7], results[1][:188 * 7])
            finally:
                for task in readers:
                    task.cancel()
                await asyncio.gather(*readers, return_exceptions=True)
                sender.close()
                await client.post('/control/', json={'cmd': 'set-stream', 'id': sid, 'stream': {'remove': True}})


if __name__ == '__main__':
    unittest.main()
