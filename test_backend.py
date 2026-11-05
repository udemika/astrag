import asyncio
import base64
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
import astra_clone_backend as backend


class FakeHLS:
    def playlist(self):
        return "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:7\n#EXTINF:2,\nsegment_7.ts\n"

    def get_segment(self, name):
        return b"segment" if name == "segment_7.ts" else None


class FakeEngine:
    instances = []

    def __init__(self, config):
        self.config = copy.deepcopy(config)
        self.running = False
        self.queues = set()
        self.hls = FakeHLS()
        self.instances.append(self)

    async def start(self):
        self.running = True

    async def stop(self):
        self.running = False
        for queue in self.queues:
            queue.put_nowait(None)

    def stats(self):
        return {"running": self.running, "bitrate": 188000, "cc_errors": 2}

    def subscribe(self):
        queue = asyncio.Queue()
        self.queues.add(queue)
        return queue

    def unsubscribe(self, queue):
        self.queues.discard(queue)


class BackendTests(unittest.TestCase):
    def test_redact_integer_pid_keys_and_nested_secrets(self):
        payload = {"pids": {256: {"cc": 7, "token": "hidden"}},
                   "password": "hidden", "items": [{17: {"secret": "hidden", "count": 2}}]}
        cleaned = backend.redact(payload)
        self.assertEqual(cleaned, {"pids": {256: {"cc": 7}}, "items": [{17: {"count": 2}}]})
        self.assertEqual(json.loads(json.dumps(cleaned))["pids"]["256"]["cc"], 7)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state_path = Path(self.temp.name) / "state.json"
        FakeEngine.instances = []
        for name, value in (("STATE_FILE", self.state_path), ("state", backend.ServerState()),
                            ("read_options", lambda: {}), ("make_engine", FakeEngine)):
            patcher = patch.object(backend, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = TestClient(backend.app)
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def cmd(self, cmd, **payload):
        return self.client.post("/control/", json={"cmd": cmd, **payload})

    def stream(self, sid="one", **values):
        data = {"name": "Channel", "input": ["udp://239.1.1.1:1234"], **values}
        return self.cmd("set-stream", id=sid, stream=data)

    def enable_auth(self):
        for login, role in (("root", 1), ("watch", 2)):
            self.assertEqual(self.cmd("set-user", id=login,
                                     user={"type": role, "password": "VeryStrongPass123"}).status_code, 200)
        self.assertEqual(self.cmd("set-auth", auth={"enable": True}).status_code, 200)

    def basic(self, login="root", password="VeryStrongPass123"):
        return {"Authorization": "Basic " + base64.b64encode(f"{login}:{password}".encode()).decode()}

    def test_frontend_named_payloads_and_nested_remove(self):
        for name in ("category", "server", "softcam", "cas", "adapter"):
            payload = {name: {"name": "test"}}
            if name in ("softcam", "cas", "adapter"):
                payload["id"] = "item"
            self.assertEqual(self.cmd("set-" + name, **payload).status_code, 200)
            payload["id"] = 0 if name in ("category", "server") else "item"
            payload[name] = {"remove": True}
            self.assertEqual(self.cmd("set-" + name, **payload).status_code, 200)
        self.assertEqual(self.cmd("set-settings", settings={"foo": 1}).status_code, 200)
        config = self.cmd("load").json()
        self.assertEqual(config["settings"], {"foo": 1})
        for key in ("categories", "servers", "softcam", "cas", "dvb_tune"):
            self.assertEqual(config[key], [])

    def test_stream_persistence_toggle_remove_reload(self):
        self.assertEqual(self.stream(input=["udp://239.1.1.1:1234", "http://localhost/live"]).status_code, 200)
        first = FakeEngine.instances[-1]
        self.assertEqual(self.cmd("set-stream-input", id="one").status_code, 200)
        self.assertFalse(first.running)
        self.assertEqual(self.cmd("load").json()["make_stream"][0]["input"][0], "http://localhost/live")
        self.assertEqual(self.cmd("restart").status_code, 200)
        self.assertEqual(sum(e.running for e in FakeEngine.instances), 1)
        self.assertEqual(self.cmd("set-stream", id="one", stream={"remove": True}).status_code, 200)
        self.assertEqual(self.cmd("load").json()["make_stream"], [])
        self.assertEqual(json.loads(self.state_path.read_text())["config"]["make_stream"], [])
        self.assertEqual(self.cmd("restart").status_code, 200)
        self.assertEqual(backend.state.engines, {})

    def test_invalid_requests_do_not_mutate(self):
        before = self.cmd("load").json()
        for payload in ({"input": "udp://host:12"}, {"input": [23]},
                        {"input": ["file:///etc/passwd"]}, {"output": ["rtp://host:12"]},
                        {"input": ["udp://host:0"]}):
            self.assertEqual(self.stream(**payload).status_code, 422)
        self.assertEqual(self.cmd("set-settings", data={}).status_code, 422)
        self.assertEqual(self.client.post("/control/", content="[").status_code, 400)
        self.assertEqual(self.client.post("/control/", json=[]).status_code, 422)
        self.assertEqual(self.cmd("load").json(), before)
        self.assertEqual(self.cmd("scan-init").status_code, 501)
        self.assertEqual(self.cmd("restart-adapter").status_code, 501)

    def test_atomic_failure_rolls_back_runtime_and_disk(self):
        self.assertEqual(self.stream().status_code, 200)
        saved = self.state_path.read_bytes()
        with patch.object(backend.os, "replace", side_effect=OSError("disk failure")):
            self.assertEqual(self.stream(name="changed").status_code, 500)
        self.assertEqual(self.state_path.read_bytes(), saved)
        self.assertEqual(self.cmd("load").json()["make_stream"][0]["name"], "Channel")
        self.assertEqual(sum(e.running for e in FakeEngine.instances), 1)
        self.assertFalse(list(self.state_path.parent.glob(".astra-*.tmp")))

    def test_upload_rebuild_and_invalid_reload(self):
        self.assertEqual(self.stream().status_code, 200)
        old_engine = FakeEngine.instances[-1]
        config = self.cmd("load").json()
        config["make_stream"][0]["id"] = "replacement"
        self.assertEqual(self.cmd("upload", config=config).status_code, 200)
        self.assertFalse(old_engine.running)
        self.assertEqual(set(backend.state.engines), {"replacement"})
        invalid = copy.deepcopy(config)
        invalid["make_stream"][0]["input"] = ["file:///bad"]
        self.state_path.write_text(json.dumps({"config": invalid}), encoding="utf-8")
        self.assertEqual(self.cmd("restart").status_code, 422)
        self.assertEqual(set(backend.state.engines), {"replacement"})
        self.assertEqual(sum(e.running for e in FakeEngine.instances), 1)

    def test_auth_roles_media_and_reset(self):
        self.stream()
        self.assertIn("admin", self.cmd("load").json()["users"])
        self.assertEqual(self.client.get("/playlist.m3u8").status_code, 200)
        self.assertIn("/play/one", self.client.get("/playlist.m3u8").text)
        self.assertEqual(self.client.get("/playlist.xspf").status_code, 200)
        self.enable_auth()
        self.assertEqual(self.cmd("status").status_code, 403)
        self.assertEqual(self.client.post("/control/", json={"cmd": "status"},
                                         headers=self.basic("admin", "admin")).status_code, 200)
        self.assertEqual(self.client.post("/control/", json={"cmd": "status"},
                                         headers=self.basic("admin", "wrong")).status_code, 403)
        self.assertEqual(self.client.get("/play/one.m3u8").status_code, 403)
        self.assertEqual(self.client.get("/play/one/segment/segment_7.ts").status_code, 403)
        self.assertEqual(self.client.get("/play/one.m3u8?token=admin").status_code, 200)
        response = self.client.post("/control/", json={"cmd": "status"}, headers=self.basic("watch"))
        self.assertTrue(response.json()["observer"])
        self.assertEqual(self.client.post("/control/", json={"cmd": "set-settings", "settings": {}},
                                         headers=self.basic("watch")).status_code, 403)
        self.assertEqual(self.client.post("/control/reset-password/", json={"login": "root"},
                                         headers=self.basic()).status_code, 422)
        self.assertEqual(self.client.post("/control/reset-password/",
                                         json={"login": "root", "password": "NewStrongPassword456"},
                                         headers=self.basic()).status_code, 200)
        self.assertEqual(self.client.post("/control/", json={"cmd": "status"}, headers=self.basic()).status_code, 403)
        # Verify Settings -> Users (set-user) login rename and password update
        self.assertEqual(self.client.post("/control/", json={
            "cmd": "set-user", "old_id": "admin", "id": "superadmin",
            "user": {"type": 1, "enable": True, "password": "CustomAdminPass789"},
        }, headers=self.basic("root", "NewStrongPassword456")).status_code, 200)
        self.assertEqual(self.client.post("/control/", json={"cmd": "status"},
                                         headers=self.basic("admin", "admin")).status_code, 403)
        self.assertEqual(self.client.post("/control/", json={"cmd": "status"},
                                         headers=self.basic("superadmin", "CustomAdminPass789")).status_code, 200)

    def test_websocket_top_level_envelope_and_redaction(self):
        with self.client.websocket_connect("/control/event/") as ws:
            self.assertEqual(len(backend.state.websocket_clients), 0)
            ws.send_json({"scope": "auth", "auth": None})
            response = ws.receive_json()
            self.assertEqual(response["scope"], "auth")
            self.assertIn("id", response)
            self.assertNotIn("data", response)
            self.assertEqual(self.stream().status_code, 200)
            event = ws.receive_json()
            self.assertEqual(event["scope"], "set-stream")
            self.assertEqual(event["stream"]["id"], "one")
            self.cmd("set-user", id="alice", user={"password": "NeverBroadcast123", "token": "secret", "type": 1})
            event = ws.receive_json()
            self.assertNotIn("password", event["user"])
            self.assertNotIn("token", event["user"])

    def test_dashboard_telemetry_has_input_node_and_expected_fields(self):
        self.stream()
        with self.client.websocket_connect("/control/event/") as ws:
            ws.send_json({"scope": "auth", "auth": None})
            ws.receive_json()
            event = ws.receive_json()
            self.assertEqual(event["scope"], "stream_event")
            self.assertEqual(event["channel_id"], "one")
            self.assertEqual(event["input_id"], 1)
            self.assertEqual(event["bitrate"], 188)
            self.assertTrue(event["onair"])
            self.assertEqual(event["cc_error"], 2)
            self.assertIn("pes_error", event)
            self.assertNotIn("data", event)

    def test_ws_requires_login_colon_password(self):
        self.enable_auth()
        with self.client.websocket_connect("/control/event/") as ws:
            ws.send_json({"scope": "auth", "auth": "VeryStrongPass123"})
            self.assertFalse(ws.receive_json()["authorized"])
            self.assertEqual(len(backend.state.websocket_clients), 0)
            ws.send_json({"scope": "auth", "auth": "watch:VeryStrongPass123"})
            self.assertTrue(ws.receive_json()["authorized"])

    def test_hls_bare_flags_and_close_session(self):
        self.stream()
        response = self.client.get("/play/one.m3u8?hls")
        self.assertIn("/play/one/segment/segment_7.ts", response.text)
        self.assertNotIn("ENDLIST", response.text)
        self.assertIn("/play/one\n", self.client.get("/play/one.m3u8?ts").text)
        self.assertEqual(self.client.get("/play/one.m3u8?udp").status_code, 501)
        self.assertEqual(self.client.get("/play/one/segment/absent.ts").status_code, 404)
        queue = asyncio.Queue()
        backend.state.sessions[17] = {"queue": queue}
        self.assertEqual(self.cmd("close-session", id=17).status_code, 200)
        self.assertIsNone(queue.get_nowait())
        self.assertEqual(self.cmd("close-session", id="17").status_code, 422)
        self.assertEqual(self.cmd("close-session", id=17).status_code, 404)
        self.assertEqual(self.client.head("/play/one.ts").status_code, 200)
        self.assertEqual(self.client.head("/play/one.m3u8").status_code, 200)
        scan_resp = self.cmd("scan-init", scan="stream://one")
        self.assertEqual(scan_resp.status_code, 200)
        self.assertEqual(scan_resp.json()["scan-init"], "ok")
        scan_id = scan_resp.json()["id"]
        self.assertEqual(self.cmd("scan-kill", id=scan_id).status_code, 200)
        # Verify custom HTTP OUTPUT LIST URL (e.g. http://62.60.250.90:18000/1) routes TS, .ts, .m3u8, /index.m3u8, and segments
        self.assertEqual(self.stream(sid="a001", output=["http://62.60.250.90:18000/1"]).status_code, 200)
        self.assertEqual(self.client.head("/1").status_code, 200)
        self.assertEqual(self.client.head("/1.ts").status_code, 200)
        self.assertEqual(self.client.get("/1.m3u8").status_code, 200)
        self.assertEqual(self.client.get("/1/index.m3u8").status_code, 200)
        self.assertEqual(self.client.get("/1/segment/segment_7.ts").status_code, 200)

    def test_stream_url_query_preserved_for_admin_and_no_duplicates(self):
        full_url = "http://31.184.196.172:620/ntv.php?id=5006d4375028edc979000160#pnr=100"
        with self.client.websocket_connect("/control/event/") as ws:
            ws.send_json({"scope": "auth", "auth": None})
            ws.receive_json()
            self.assertEqual(self.cmd("set-stream", id="-", stream={
                "id": "boets", "name": "Матч Боец", "type": "spts", "enable": True, "input": [full_url], "output": []
            }).status_code, 200)
            ev1 = ws.receive_json()
            self.assertEqual(ev1["scope"], "set-stream")
            self.assertEqual(ev1["stream"]["id"], "boets")
            self.assertEqual(ev1["stream"]["input"][0], full_url)
            # Edit existing stream and verify no duplicates in make_stream and URL query remains intact
            self.assertEqual(self.cmd("set-stream", id="boets", stream={
                "id": "boets", "name": "Матч Боец HD", "type": "spts", "enable": True, "input": [full_url], "output": []
            }).status_code, 200)
            ev2 = ws.receive_json()
            self.assertEqual(ev2["stream"]["input"][0], full_url)
            loaded = self.cmd("load").json()["make_stream"]
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["name"], "Матч Боец HD")
            self.assertEqual(loaded[0]["input"][0], full_url)


if __name__ == "__main__":
    unittest.main()

