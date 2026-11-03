"""Build a clean, containing-directory release without original binaries or backups."""
import argparse
import hashlib
import tarfile
from pathlib import Path

FILES = (
    'astra_clone_backend.py', 'astrag_backend.py', 'stream_engine.py', 'extract_ui_files.py',
    'account_security.py', 'create_admin.py', 'test_account_security.py', 'Caddyfile.example',
    'install.sh', 'astra-clone.service', 'astrag.service', 'requirements.txt',
    'README_INSTALL.md', 'REVERSE_ENGINEERING_REPORT.md', 'COMPATIBILITY.md',
    'VERIFICATION.md', 'test_backend.py', 'test_stream_engine.py',
    'test_ui_extraction.py', 'test_live_transport.py', 'build_release.py', 'binary_evidence.json',
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    output = args.output or root.parent / 'astrag_release.tar.gz'
    missing = [name for name in FILES if not (root / name).is_file()]
    if missing:
        raise SystemExit('Missing release files: ' + ', '.join(missing))
    resources = [root / 'www' / 'index.html', root / 'www' / 'account.html', root / 'www' / 'mod.js']
    manifest = root / 'ui-extraction-manifest.json'
    if not manifest.is_file():
        manifest = root / 'backups' / 'ui-extraction-manifest.json'
    if not manifest.is_file():
        raise SystemExit('Missing UI extraction manifest')
    files = [root / name for name in FILES] + resources
    with tarfile.open(output, 'w:gz') as archive:
        for path in files:
            archive.add(path, arcname='AstraG/' + path.relative_to(root).as_posix())
        archive.add(manifest, arcname='AstraG/ui-extraction-manifest.json')
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_name(output.name + '.sha256').write_text(digest + '  ' + output.name + '\n', encoding='ascii')
    print(str(output))
    print('SHA256:', digest)
    print('Files:', len(files), 'Bytes:', output.stat().st_size)


if __name__ == '__main__':
    main()
