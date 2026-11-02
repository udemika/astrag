#!/usr/bin/env python3
"""Extract the embedded Astra HTML without inventing a replacement UI.

The extractor uses ELF section file ranges, not addresses copied from one build.
It writes the complete, validated HTML byte-for-byte and leaves existing static
files alone.  The manifest is deliberately outside ``www`` so it is not served.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from elftools.elf.elffile import ELFFile

DEFAULT_BINARY = Path(r"C:\Users\akzam\Music\PLAYLIST\Rea1\astra")
DEFAULT_WWW = Path(__file__).resolve().parent / "www"

_REF_RE = re.compile(rb"(?:src|href)\s*=\s*[\"']([^\"']+)[\"']", re.I)


def section_bytes(binary: Path) -> Iterable[tuple[str, int, bytes]]:
    with binary.open("rb") as stream:
        elf = ELFFile(stream)
        for section in elf.iter_sections():
            if section["sh_type"] == "SHT_NOBITS" or section["sh_size"] == 0:
                continue
            yield section.name, int(section["sh_offset"]), section.data()


def find_html(binary: Path) -> tuple[str, int, bytes]:
    candidates: list[tuple[str, int, bytes]] = []
    for name, offset, data in section_bytes(binary):
        for match in re.finditer(rb"<!doctype\s+html\s*>", data, re.I):
            pos = match.start()
            end = data.find(b"\x00", pos)
            if end < 0:
                continue
            raw = data[pos:end]
            if not re.search(rb"</html>\s*$", raw, re.I):
                continue
            try:
                text = raw.decode("utf-8", errors="strict")
            except UnicodeDecodeError:
                continue
            if not all(re.search(pattern, text, re.I) for pattern in
                       (r"<html\b", r"<head\b", r"</head>", r"<body\b", r"</body>")):
                continue
            candidates.append((name, offset + pos, raw))
    if not candidates:
        raise ValueError("no complete UTF-8 <!doctype html> resource found in ELF sections")
    # Prefer the largest complete document; small HTML strings can be templates.
    return max(candidates, key=lambda item: len(item[2]))


def inventory(raw: bytes) -> dict[str, object]:
    refs = []
    for match in _REF_RE.finditer(raw):
        value = match.group(1).decode("utf-8")
        refs.append(value)
    unique = list(dict.fromkeys(refs))
    local = [r for r in unique if r.startswith("/") and not r.startswith("//")]
    return {
        "referenced_resources": unique,
        "local_dependencies_not_extracted": local,
        "note": "Only the embedded HTML is extracted. A URL reference is not resource content. No original mod.js or favicon bytes have been established; no replacements are fabricated.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--www", type=Path, default=DEFAULT_WWW)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--backup-dir", type=Path)
    args = parser.parse_args(argv)
    if not args.binary.is_file():
        parser.error(f"binary not found: {args.binary}")

    section, offset, raw = find_html(args.binary)
    args.www.mkdir(parents=True, exist_ok=True)
    index = args.www / "index.html"
    backup_dir = args.backup_dir or args.www.parent / "backups" / "extracted-www"
    if index.exists():
        backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = backup_dir / f"index.html.{stamp}.bak"
        counter = 1
        while backup.exists():
            backup = backup_dir / f"index.html.{stamp}.{counter}.bak"
            counter += 1
        shutil.copy2(index, backup)
        print(f"preserved existing {index} as {backup}")
    index.write_bytes(raw)

    manifest_path = args.manifest or args.www.parent / "extraction-manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "binary": str(args.binary),
        "section": section,
        "offset": offset,
        "offset_hex": hex(offset),
        "size": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "output": str(index),
        "inventory": inventory(raw),
    }
    manifest_path.write_text(json.dumps(record, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"extracted {len(raw)} bytes from {section} at {offset:#x}")
    print(f"sha256 {record['sha256']}")
    print(f"manifest {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
