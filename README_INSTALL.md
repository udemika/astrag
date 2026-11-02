# Experimental Astra-compatible server

This project combines the original embedded Astra control-panel HTML with an
independent Python backend. It is **not a complete Astra implementation** and
is not production-ready. UI visibility does not prove a command, media protocol,
statistics field, authentication policy, or streaming feature is implemented.
Unsupported functionality must not be interpreted as successful operation.

## Original UI and extraction

The original executable is read as data; it is never executed by the extractor.
Only extract/distribute proprietary resources when you have the necessary rights.
The HTML retains the original copyright notices.

```bash
python -m pip install 'pyelftools>=0.31,<0.33'
python extract_ui_files.py --binary /path/to/astra --www ./www
python -m unittest test_ui_extraction -v
```

`extract_ui_files.py` uses pyelftools ELF **section file offsets**, searches for
complete NUL-terminated HTML, validates strict UTF-8 and required document tags,
and selects the largest valid document. It does not use hardcoded offsets or
split at CSS/JavaScript comments. `index.html` is written as original bytes,
including its final newline, excluding only the C-string terminator. No synthetic
fallback page is generated when extraction fails.

The supplied executable yielded:

- section `.data`, file offset `0x723740`, exclusive end `0x78ce66`;
- **431,910 bytes**, with inline CSS and one inline JavaScript block;
- SHA-256 `67640126902fbfcbda0a093fb4d53dae8428b9422fff668075d6fe24f1917aa6`.

The extraction manifest defaults to `extraction-manifest.json` beside (not inside)
`www`; this workspace's run used `backups/ui-extraction-manifest.json`. It records
source range, byte count, SHA-256, and dependency references. Existing index files
are copied to uniquely named `backups/extracted-www/` files before replacement.
Other `www` files are never deleted. The pre-change tree was also preserved under
`backups/baseline/www`. Legacy `www/js`, `www/css`, and `_extract_report.json` are
left untouched for traceability; **they are not validated original resources**.
The restored index uses its own inline CSS/JS, not those legacy fragments. Do not
publish legacy files or their old report as evidence of a successful extraction.

### Unresolved external resources

The exact HTML requests **`/mod.js`** and **`/favicon.png`**. A byte search found
`/mod.js` only in this HTML, and `favicon` in the HTML and a server route string.
No PNG signature was found in ELF section bytes. These bounded searches do not
establish whether resources are compressed, generated at runtime, stored outside
the executable, or optional. No original standalone module or favicon has been
recovered, and no invented replacements are supplied. The backend may return
404 for these dependencies; inspect browser errors and do not claim complete
visual/behavioral fidelity. Inline JavaScript passes `node --check`, which checks
syntax only, not browser behavior or backend compatibility.

## Local run (Python 3.10+)

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
export ASTRA_PORT=8000
export ASTRA_STATIC_DIR="$PWD/www"
export ASTRA_STATE_FILE="$PWD/local-state.json"
python astra_clone_backend.py
```

`pyelftools` is needed only for extraction and extraction tests, not server
execution; it is included in `requirements.txt` for convenience. Dependency
ranges are bounded and Python-3.10-compatible but are not a reproducible lockfile.
If omitting pyelftools, install the remaining runtime requirements explicitly.
Do not expose this development server directly to the Internet. Protect the
control API and verify actual authentication/authorization before use with real
streams or sensitive configuration. The default port is 8000; check backend
binding behavior and your firewall before starting.

## Ubuntu / systemd installation

The installer is **intended for Ubuntu 22.04 / 24.04, but installation has not been
tested on Ubuntu** in this Windows workspace. Review it before running as root.
It invokes apt and pip and therefore requires network access to package sources.
Run from the project directory containing both `astra_clone_backend.py` and
`stream_engine.py`, `requirements.txt`, `astra-clone.service`, and `www/index.html`:

```bash
sudo bash install.sh
sudoedit /etc/astra-clone/astra-clone.conf
sudo systemctl enable --now astra-clone.service
sudo systemctl status astra-clone.service
sudo journalctl -u astra-clone.service -f
```

By default the installer **does not enable, start, stop, or restart** the service.
Use `sudo bash install.sh --start` only when enabling and starting/restarting it
is explicitly desired. On reinstall, an already-running process is not stopped;
restart it explicitly to load the updated files. Reinstallation is not atomic.
Configuration and mutable state are preserved. Review changes before restarting.

| Path | Purpose / ownership |
| --- | --- |
| `/opt/astra-clone/` | Root-owned code, `stream_engine.py`, requirements, venv |
| `/opt/astra-clone/www/` | Root-owned static files; service cannot modify them |
| `/opt/astra-clone/backups/` | Root-only snapshots of installed static files before updates |
| `/etc/astra-clone/astra-clone.conf` | Root-owned mode 0640, group `astra-clone`; preserved on reinstall |
| `/var/lib/astra-clone/state.json` | Mutable backend state, dedicated `astra-clone` system user |
| `/run/astra-clone/` | systemd-managed runtime directory |
| systemd journal | stdout/stderr logs (`journalctl -u astra-clone`) |

The config is a systemd `EnvironmentFile` (not arbitrary shell). The installer
creates it only if absent, using these backend settings:

```ini
ASTRA_PORT=8000
ASTRA_STATIC_DIR=/opt/astra-clone/www
ASTRA_STATE_FILE=/var/lib/astra-clone/state.json
ASTRA_CONFIG_FILE=/etc/astra-clone/astra-clone.conf
```

`ASTRA_CONFIG_FILE` identifies configuration; it is **not** the writable state
file. Do not put mutable UI/API state into `/etc`. Restart after editing settings.
The unit uses `Type=simple`, a dedicated unprivileged `astra-clone` account,
`RuntimeDirectory` and `StateDirectory`, and no PID file or unsupported reload
signal. Filesystem hardening permits persistent writes only to state/runtime
paths (plus a private temporary directory). Changing state paths requires updating
the unit's allowed write paths and filesystem ownership too. No broad writable
`/etc`, `/var/log`, or application-code directory is granted to the service.

## Verification boundaries

In this workspace the extractor tests and syntax check of the recovered inline
JavaScript were run. These do not verify systemd installation, Ubuntu package
resolution, browser interactions, every API command, media transport, or feature
parity. Consult backend tests and the reverse-engineering report for narrower
claims; do not assume the original UI makes unimplemented features operational.


## Internet access and account changes

The backend now binds to `127.0.0.1` by default. Do not publish unencrypted Basic authentication on port 8000. Use a TLS reverse proxy; `Caddyfile.example` provides HTTPS/WebSocket forwarding for a real domain pointing to the server. Install/configure Caddy separately, replace the example domain, allow TCP 80/443 and keep 8000 inaccessible externally. No public deployment is performed by these files.

Before exposing the service, stop it and create the first administrator interactively (no default password):

```bash
sudo systemctl stop astra-clone
sudo -u astra-clone /opt/astra-clone/venv/bin/python /opt/astra-clone/create_admin.py --state /var/lib/astra-clone/state.json
sudo systemctl start astra-clone
```

Open `https://YOUR-DOMAIN/` for the original dashboard login. Open `https://YOUR-DOMAIN/account/` to verify credentials or change your own login/password; current password is required. Changed passwords use salted PBKDF2-SHA256 hashes, existing control WebSockets are disconnected, and old credentials cease to work. The account page does not store credentials. Log in to the original dashboard separately; avoid its legacy Remember-me option on shared machines. Password changes do not revoke already running media deliveries.

The account endpoint rejects cross-origin changes and throttles failed authentication. This does not replace HTTPS, firewall policy, or proxy-level request limits. Reverse-proxy clients share the backend peer limit; tune proxy protection before public deployment. Original management commands retain their existing authorization policy.
