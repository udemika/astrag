import base64
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
import astra_clone_backend as backend
from account_security import FailedAuthLimiter, hash_password, verify_password


class AccountTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'state.json'
        config = backend.defaults()
        config['auth']['enable'] = True
        config['users'] = {
            'root': {'role': 'admin', 'password': 'OriginalPassword123'},
            'watch': {'type': 2, 'password': 'OriginalPassword123'},
        }
        self.path.write_text(json.dumps({'config': config}), encoding='utf-8')
        for name, value in [('STATE_FILE', self.path), ('state', backend.ServerState()), ('read_options', lambda: {})]:
            p = patch.object(backend, name, value)
            p.start()
            self.addCleanup(p.stop)
        self.client = TestClient(backend.app)
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def auth(self, login='watch', password='OriginalPassword123'):
        return {'Authorization': 'Basic ' + base64.b64encode(f'{login}:{password}'.encode()).decode()}

    def change(self, login='watch', **payload):
        body = {'current_password': 'OriginalPassword123', 'new_login': 'renamed',
                'new_password': 'ReplacementPassword456', **payload}
        return self.client.post('/control/account/', headers=self.auth(login), json=body)

    def status(self, login, password):
        return self.client.post('/control/', headers=self.auth(login, password), json={'cmd': 'status'})

    def test_self_change_hash_persistence_and_restart(self):
        for login in ('watch', 'root'):
            with self.subTest(login=login):
                new_login = login + '_new'
                engines = backend.state.engines
                with patch.object(backend.state, 'stop_engines', side_effect=AssertionError('engine restart')):
                    self.assertEqual(self.change(login, new_login=new_login).status_code, 200)
                self.assertIs(backend.state.engines, engines)
                saved = json.loads(self.path.read_text())['config']['users']
                self.assertNotIn(login, saved)
                stored = saved[new_login]['password']
                self.assertTrue(stored.startswith('pbkdf2_sha256$'))
                self.assertNotIn('ReplacementPassword456', self.path.read_text())
                self.assertTrue(verify_password('ReplacementPassword456', stored))
                self.assertEqual(self.status(login, 'OriginalPassword123').status_code, 403)
                self.assertEqual(self.status(new_login, 'OriginalPassword123').status_code, 403)
                self.assertEqual(self.status(new_login, 'ReplacementPassword456').status_code, 200)
                self.client.portal.call(backend.reload_config)
                self.assertEqual(self.status(new_login, 'ReplacementPassword456').status_code, 200)

    def test_duplicate_wrong_current_and_missing_auth_do_not_mutate(self):
        before = self.path.read_bytes()
        self.assertEqual(self.change(new_login='root').status_code, 409)
        self.assertEqual(self.change(current_password='wrong').status_code, 403)
        self.assertEqual(self.client.post('/control/account/', json={}).status_code, 403)
        self.assertEqual(before, self.path.read_bytes())

    def test_origin_validation(self):
        payload = {'current_password': 'OriginalPassword123', 'new_login': 'watch',
                   'new_password': 'ReplacementPassword456'}
        before = self.path.read_bytes()
        for origin in ('https://evil.example', 'http://testserver.evil', 'null',
                       'http://testserver:81', 'https://testserver', 'http://testserver/path'):
            self.assertEqual(self.client.post('/control/account/', json=payload,
                             headers={**self.auth(), 'Origin': origin}).status_code, 403)
        self.assertEqual(before, self.path.read_bytes())
        self.assertEqual(self.client.post('/control/account/', json=payload,
                         headers={**self.auth(), 'Origin': 'http://testserver'}).status_code, 200)

    def test_invalid_new_credentials(self):
        for login in ('', 'a:b', 'a/b', 'a b', 'x' * 65, '\u0438\u043c\u044f'):
            self.assertEqual(self.change(new_login=login).status_code, 422)
        for password in ('short123', 'lettersbutnodigits', '123456789012', 'a1' * 129, None):
            self.assertEqual(self.change(new_password=password).status_code, 422)

    def test_atomic_failure_preserves_credentials(self):
        before = self.path.read_bytes()
        with patch.object(backend.os, 'replace', side_effect=OSError('disk failure')):
            self.assertEqual(self.change().status_code, 500)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.status('watch', 'OriginalPassword123').status_code, 200)
        self.assertFalse(list(self.path.parent.glob('.astra-*.tmp')))

    def test_websocket_revoked_without_holding_lock(self):
        with self.client.websocket_connect('/control/event/') as ws:
            ws.send_json({'scope': 'auth', 'auth': 'watch:OriginalPassword123'})
            self.assertTrue(ws.receive_json()['authorized'])
            self.assertEqual(self.change().status_code, 200)
            with self.assertRaises(WebSocketDisconnect):
                ws.receive_json()
            self.assertFalse(backend.state.websocket_clients)
        self.assertFalse(backend.state.lock.locked())

    def test_account_page_no_credential_storage(self):
        response = self.client.get('/account/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertIn('/control/account/', response.text)
        self.assertNotIn('localStorage', response.text)
        self.assertNotIn('document.cookie', response.text)

    def test_legacy_and_no_default_account(self):
        self.assertEqual(self.status('root', 'OriginalPassword123').status_code, 200)
        self.assertEqual(self.status('admin', 'admin').status_code, 403)
        backend.state.config['auth']['enable'] = False
        self.assertEqual(self.change().status_code, 403)

    def test_failed_auth_rate_limit_http_and_ws(self):
        backend.state.auth_limiter = FailedAuthLimiter(host_limit=2)
        self.assertEqual(self.status('watch', 'wrong').status_code, 403)
        self.assertEqual(self.status('watch', 'wrong').status_code, 403)
        self.assertEqual(self.status('watch', 'OriginalPassword123').status_code, 429)
        with self.client.websocket_connect('/control/event/') as ws:
            ws.send_json({'scope': 'auth', 'auth': 'watch:OriginalPassword123'})
            self.assertFalse(ws.receive_json()['authorized'])


class SecurityHelperTests(unittest.TestCase):
    def test_hash_random_salt_and_legacy(self):
        first = hash_password('StrongPassword123')
        self.assertNotEqual(first, hash_password('StrongPassword123'))
        self.assertTrue(verify_password('StrongPassword123', first))
        self.assertFalse(verify_password('wrong', first))
        self.assertTrue(verify_password('legacy', 'legacy'))
        self.assertFalse(verify_password('anything', 'pbkdf2_sha256$broken'))

    def test_limiter_bounded_global_and_expiry(self):
        now = [0]
        limiter = FailedAuthLimiter(host_limit=2, global_limit=4, max_hosts=2, clock=lambda: now[0])
        for host in ('a', 'b', 'c', 'd'):
            limiter.failed(host)
        self.assertLessEqual(len(limiter.hosts), 2)
        self.assertTrue(limiter.blocked('new'))
        now[0] = 61
        self.assertFalse(limiter.blocked('a'))
        self.assertFalse(limiter.hosts)


if __name__ == '__main__':
    unittest.main()
