"""Standard-library credential hashing and bounded authentication throttling."""
import hashlib
import hmac
import re
import secrets
import time
from collections import deque


PREFIX = "pbkdf2_sha256$"
ITERATIONS = 600_000
LOGIN = re.compile(r"[A-Za-z0-9_.@-]{1,64}\Z")


def valid_login(value):
    return isinstance(value, str) and LOGIN.fullmatch(value) is not None


def valid_password(value):
    return (isinstance(value, str) and 12 <= len(value) <= 256
            and any(c.isalpha() for c in value) and any(c.isdigit() for c in value))


def hash_password(password):
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, ITERATIONS)
    return f"{PREFIX}{ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(password, stored):
    if not isinstance(password, str) or not isinstance(stored, str) or not stored or len(password) > 256:
        return False
    if not stored.startswith(PREFIX):
        return hmac.compare_digest(password.encode("utf-8"), stored.encode("utf-8"))
    try:
        _, rounds, salt, digest = stored.split("$")
        rounds = int(rounds)
        salt, digest = bytes.fromhex(salt), bytes.fromhex(digest)
        if not 100_000 <= rounds <= 2_000_000 or len(salt) != 16 or len(digest) != 32:
            return False
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds)
        return hmac.compare_digest(actual, digest)
    except (ValueError, OverflowError):
        return False


class FailedAuthLimiter:
    """One process-wide window plus bounded direct-peer windows (never trust XFF)."""
    def __init__(self, host_limit=20, global_limit=200, window=60, max_hosts=1024, clock=time.monotonic):
        self.host_limit = host_limit
        self.global_limit = global_limit
        self.window = window
        self.max_hosts = max_hosts
        self.clock = clock
        self.total = deque()
        self.hosts = {}

    def _prune(self):
        cutoff = self.clock() - self.window
        while self.total and self.total[0] <= cutoff:
            self.total.popleft()
        for host, entries in list(self.hosts.items()):
            while entries and entries[0] <= cutoff:
                entries.popleft()
            if not entries:
                del self.hosts[host]

    def blocked(self, host):
        self._prune()
        return len(self.total) >= self.global_limit or len(self.hosts.get(host, ())) >= self.host_limit

    def failed(self, host):
        self._prune()
        now = self.clock()
        if len(self.total) < self.global_limit:
            self.total.append(now)
        if host in self.hosts or len(self.hosts) < self.max_hosts:
            entries = self.hosts.setdefault(host, deque())
            if len(entries) < self.host_limit:
                entries.append(now)
