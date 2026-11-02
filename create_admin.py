"""Create the first administrator offline without putting passwords in arguments."""
import argparse
import getpass
import json
import os
from pathlib import Path
import tempfile
from account_security import hash_password, valid_login, valid_password


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', type=Path, required=True)
    args = parser.parse_args()
    login = input('Administrator login: ').strip()
    password = getpass.getpass('Password (12+ characters, letters and digits): ')
    if not valid_login(login) or not valid_password(password):
        raise SystemExit('Invalid login or password')
    if password != getpass.getpass('Repeat password: '):
        raise SystemExit('Passwords do not match')
    path = args.state
    data = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    config = data.get('config', data)
    if config.get('users'):
        raise SystemExit('Users already exist; use the authenticated account page instead')
    config['users'] = {login: {'password': hash_password(password), 'role': 'admin'}}
    config['auth'] = dict(config.get('auth', {}), enable=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent, delete=False) as f:
            temporary = f.name
            os.chmod(temporary, 0o600)
            json.dump(data if 'config' in data else config, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)
    print('Administrator saved; authentication enabled. Restart the service.')


if __name__ == '__main__':
    main()
