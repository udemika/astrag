"""Offline extractor tests; no original executable is run."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import extract_ui_files as ui

HTML = b'<!DOCTYPE html>\n<html><head><title>test</title></head><body>caf\xc3\xa9</body></html>\n'


class ExtractionTests(unittest.TestCase):
    def find(self, data):
        with patch.object(ui, "section_bytes", return_value=[(".data", 1234, data)]):
            return ui.find_html(Path("unused"))

    def test_complete_nul_terminated_bytes(self):
        name, offset, raw = self.find(b"prefix\x00" + HTML + b"\x00garbage")
        self.assertEqual((name, offset, raw), (".data", 1241, HTML))

    def test_rejects_truncated(self):
        for data in (HTML[:-10] + b"\x00", HTML, b"<!DOCTYPE html>fake</html>\x00"):
            with self.subTest(data=data), self.assertRaises(ValueError):
                self.find(data)

    def test_strict_utf8(self):
        with self.assertRaises(ValueError):
            self.find(HTML.replace(b"caf\xc3\xa9", b"\xff") + b"\x00")

    def test_largest_document_and_case(self):
        other = HTML.replace(b"DOCTYPE", b"doctype") + b"\x00"
        larger = HTML.replace(b"test", b"larger test")
        self.assertEqual(self.find(other + larger + b"\x00")[2], larger)

    def test_preserves_existing_www_and_hashes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / "binary"
            binary.write_bytes(b"fixture")
            www = root / "www"
            www.mkdir()
            (www / "index.html").write_bytes(b"old ui")
            (www / "untouched.txt").write_bytes(b"keep")
            with patch.object(ui, "find_html", return_value=(".data", 99, HTML)):
                ui.main(["--binary", str(binary), "--www", str(www)])
            self.assertEqual((www / "index.html").read_bytes(), HTML)
            self.assertEqual((www / "untouched.txt").read_bytes(), b"keep")
            self.assertEqual(next((root / "backups/extracted-www").iterdir()).read_bytes(), b"old ui")
            manifest = json.loads((root / "extraction-manifest.json").read_text())
            self.assertEqual(manifest["sha256"], hashlib.sha256(HTML).hexdigest())
            self.assertFalse((www / "extraction-manifest.json").exists())

    def test_inventory_no_invented_resources(self):
        record = ui.inventory(b'<script src="/mod.js"></script><link href="/favicon.png">')
        self.assertEqual(record["local_dependencies_not_extracted"], ["/mod.js", "/favicon.png"])


if __name__ == "__main__":
    unittest.main()
