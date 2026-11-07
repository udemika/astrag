#!/usr/bin/env python3
"""
Извлечение встроенного веб-интерфейса (HTML/JS/CSS/PNG/GZIP)
из секций .rodata, .data.rel.ro и .data бинарника Astra Media Server.
"""

import os
import re
import sys
import json
import zlib
import struct
from pathlib import Path
from elftools.elf.elffile import ELFFile

# Путь к бинарнику по умолчанию (можно переопределить первым аргументом CLI)
DEFAULT_BIN = r"C:\Users\akzam\Music\PLAYLIST\Rea1\astra"
DEFAULT_OUT = Path("./www")

TARGET_SECTIONS = [".rodata", ".data.rel.ro", ".data"]


class ElfMemoryMapper:
    """Маппинг виртуальных адресов ELF (vaddr) в файловые смещения и чтение строк."""

    def __init__(self, elf: ELFFile, raw_bytes: bytes):
        self.raw = raw_bytes
        self.sections = {}
        for sec in elf.iter_sections():
            if sec.name in TARGET_SECTIONS:
                self.sections[sec.name] = {
                    "vaddr": sec["sh_addr"],
                    "offset": sec["sh_offset"],
                    "size": sec["sh_size"],
                }

    def vaddr_to_offset(self, vaddr: int) -> int | None:
        for info in self.sections.values():
            start = info["vaddr"]
            end = start + info["size"]
            if start <= vaddr < end:
                return info["offset"] + (vaddr - start)
        return None

    def read_cstring(self, offset: int, max_len: int = 2_000_000) -> bytes | None:
        if offset < 0 or offset >= len(self.raw):
            return None
        end = self.raw.find(b"\x00", offset, offset + max_len)
        if end == -1:
            return None
        return self.raw[offset:end]


def is_printable_text(data: bytes, min_ratio: float = 0.92) -> bool:
    """Проверяет, является ли буфер валидным UTF-8/ASCII текстом (с учетом \n, \r, \t)."""
    if not data:
        return False
    allowed = sum(1 for b in data if b in (9, 10, 13) or 32 <= b <= 126 or b >= 128)
    return (allowed / len(data)) >= min_ratio


def classify_text_block(text: str, index: int) -> tuple[str, str] | None:
    """
    Определяет тип контента (html, js, css, lua) и имя файла.
    Возвращает (subfolder_or_type, filename) или None, если это не веб-ресурс.
    """
    stripped = text.strip()
    if len(stripped) < 40:
        return None

    # 1. Проверка явного заголовка-комментария в начале файла: /* sessions.js */
    header_match = re.match(r"^/\*\s*([a-zA-Z0-9_.\-/]+\.(?:js|css|html))\s*\*/", stripped)
    if header_match:
        fname = Path(header_match.group(1)).name
        ext = fname.rsplit(".", 1)[-1].lower()
        return ext, fname

    # 2. Проверка HTML-шаблонов и страниц
    if any(tag in stripped for tag in ["<!DOCTYPE html", "<html", "<div class=", "<table", "<form"]):
        if "<html" in stripped.lower() or "<!doctype html" in stripped.lower():
            return "html", f"index_{index:03d}.html"
        return "html", f"template_{index:03d}.html"

    # 3. Проверка JavaScript-модулей Astra (MainModule, SessionsModule, AngularJS, WebSocketService)
    js_markers = [
        "window.", "App.factory(", "App.controller(", "App.directive(",
        "MainModule", "SessionsModule", "SettingsUsersModule", "SettingsServersModule",
        "WebSocketService", "appHost.config", "$.forEach(", "app.on(", "function ("
    ]
    js_score = sum(1 for m in js_markers if m in stripped)
    if js_score >= 2 or (js_score >= 1 and ("\n" in stripped and ";" in stripped)):
        # Попытка вытащить имя модуля из window.<ModuleName> =
        mod_match = re.search(r"window\.([A-Za-z0-9_]+)\s*=", stripped)
        if mod_match:
            mod_name = re.sub(r"(?<!^)(?=[A-Z])", "-", mod_match.group(1)).lower()
            return "js", f"{mod_name}_{index:03d}.js"
        return "js", f"script_{index:03d}.js"

    # 4. Проверка CSS
    if re.search(r"[.#][a-zA-Z0-9_-]+\s*\{[^}]*:[a-zA-Z0-9#%\s-]+;[^}]*\}", stripped):
        if "function" not in stripped and "var " not in stripped:
            return "css", f"style_{index:03d}.css"

    return None


def scan_pointer_tables(mapper: ElfMemoryMapper) -> list[dict]:
    """Ищет пары 64-битных указателей (имя_ресурса -> содержимое) в .data.rel.ro и .data."""
    found = []
    asset_ext = re.compile(r"^/?[a-zA-Z0-9_.\-/]+\.(html|js|css|png|ico|svg|json|woff2?|ttf)$", re.I)

    for sec_name in (".data.rel.ro", ".data"):
        sec = mapper.sections.get(sec_name)
        if not sec:
            continue
        start_off = sec["offset"]
        end_off = start_off + sec["size"]

        # Шаг 8 байт (размер указателя в ELF64 x86_64)
        for off in range(start_off, end_off - 16, 8):
            ptr1, ptr2 = struct.unpack_from("<QQ", mapper.raw, off)
            off1 = mapper.vaddr_to_offset(ptr1)
            off2 = mapper.vaddr_to_offset(ptr2)
            if off1 is None or off2 is None:
                continue

            name_bytes = mapper.read_cstring(off1, max_len=256)
            if not name_bytes or len(name_bytes) < 3:
                continue

            try:
                name_str = name_bytes.decode("ascii")
            except UnicodeDecodeError:
                continue

            if asset_ext.match(name_str):
                content_bytes = mapper.read_cstring(off2, max_len=5_000_000)
                if content_bytes and len(content_bytes) > 16:
                    found.append({
                        "source": f"ptr_table:{sec_name}@0x{off:x}",
                        "name": name_str.lstrip("/"),
                        "offset": f"0x{off2:08x}",
                        "size": len(content_bytes),
                        "data": content_bytes,
                    })
    return found


def scan_null_terminated_blocks(mapper: ElfMemoryMapper) -> list[dict]:
    """Сканирует .rodata, .data.rel.ro и .data на наличие крупных текстовых блоков C-строк."""
    found = []
    idx = 0

    for sec_name in TARGET_SECTIONS:
        sec = mapper.sections.get(sec_name)
        if not sec:
            continue
        start = sec["offset"]
        end = start + sec["size"]
        sec_data = mapper.raw[start:end]

        # Разбиваем секцию по нулевым байтам (C-strings)
        pos = 0
        for chunk in sec_data.split(b"\x00"):
            chunk_len = len(chunk)
            chunk_offset = start + pos
            pos += chunk_len + 1

            if chunk_len < 45 or not is_printable_text(chunk):
                continue

            text = chunk.decode("utf-8", errors="replace")
            classification = classify_text_block(text, idx)
            if not classification:
                continue

            kind, filename = classification
            idx += 1
            found.append({
                "source": f"cstring:{sec_name}@0x{chunk_offset:08x}",
                "type": kind,
                "name": filename,
                "offset": f"0x{chunk_offset:08x}",
                "size": chunk_len,
                "data": text.encode("utf-8"),
            })
    return found


def carve_binary_assets(mapper: ElfMemoryMapper) -> list[dict]:
    """Вырезает PNG-файлы (например, /favicon.png) и проверяет наличие GZIP-архивов."""
    found = []
    png_sig = b"\x89PNG\r\n\x1a\n"
    png_end = b"IEND\xaeB`\x82"
    gzip_sig = b"\x1f\x8b\x08"

    for sec_name in TARGET_SECTIONS:
        sec = mapper.sections.get(sec_name)
        if not sec:
            continue
        start = sec["offset"]
        end = start + sec["size"]
        data = mapper.raw[start:end]

        # 1. Поиск PNG
        p = 0
        png_idx = 0
        while True:
            idx = data.find(png_sig, p)
            if idx == -1:
                break
            end_idx = data.find(png_end, idx)
            if end_idx != -1:
                png_bytes = data[idx : end_idx + len(png_end)]
                fname = "favicon.png" if png_idx == 0 else f"image_{png_idx:02d}.png"
                found.append({
                    "source": f"carve_png:{sec_name}@0x{start + idx:08x}",
                    "type": "png",
                    "name": fname,
                    "offset": f"0x{start + idx:08x}",
                    "size": len(png_bytes),
                    "data": png_bytes,
                })
                png_idx += 1
                p = end_idx + len(png_end)
            else:
                p = idx + 8

        # 2. Поиск сжатых GZIP-ресурсов
        g = 0
        gz_idx = 0
        while True:
            idx = data.find(gzip_sig, g)
            if idx == -1:
                break
            # Пробуем распаковать поток с текущей позиции
            try:
                decomp = zlib.decompressobj(16 + zlib.MAX_WBITS)
                unpacked = decomp.decompress(data[idx : idx + 2_000_000])
                if len(unpacked) > 64 and is_printable_text(unpacked):
                    text = unpacked.decode("utf-8", errors="replace")
                    cls = classify_text_block(text, 900 + gz_idx)
                    ext_name = cls[1] if cls else f"decompressed_{gz_idx:03d}.txt"
                    found.append({
                        "source": f"gzip:{sec_name}@0x{start + idx:08x}",
                        "type": "gzip_decompressed",
                        "name": ext_name,
                        "offset": f"0x{start + idx:08x}",
                        "size": len(unpacked),
                        "data": unpacked,
                    })
                    gz_idx += 1
            except Exception:
                pass
            g = idx + 3

    return found


def build_combined_ui(out_dir: Path, extracted_items: list[dict]):
    """
    Сохраняет все найденные файлы в ./www/ и собирает единый index.html + app_bundle.js,
    если модули были разбиты на отдельные C-строки в секции .data.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "js").mkdir(exist_ok=True)
    (out_dir / "css").mkdir(exist_ok=True)
    (out_dir / "templates").mkdir(exist_ok=True)

    manifest = []
    js_Interactive_parts = []
    css_parts = []
    html_templates = []
    has_root_index = False

    seen_hashes = set()

    for item in extracted_items:
        content_hash = hash(item["data"])
        if content_hash in seen_hashes:
            continue
        seen_hashes.add(content_hash)

        name = item["name"]
        kind = item.get("type", "")

        # Определяем путь сохранения
        if name == "favicon.png" or name == "index.html":
            target_path = out_dir / name
            if name == "index.html":
                has_root_index = True
        elif kind == "js" or name.endswith(".js"):
            target_path = out_dir / "js" / Path(name).name
            js_Interactive_parts.append(( item["offset"], Path(name).name, item["data"].decode("utf-8", errors="replace") ))
        elif kind == "css" or name.endswith(".css"):
            target_path = out_dir / "css" / Path(name).name
            css_parts.append(item["data"].decode("utf-8", errors="replace"))
        elif kind == "html" or name.endswith(".html"):
            # Проверяем, не полноценный ли это index.html
            text_lower = item["data"][:500].decode("utf-8", errors="ignore").lower()
            if ("<!doctype html" in text_lower or "<html" in text_lower) and "%d %s" not in text_lower:
                target_path = out_dir / "index.html"
                has_root_index = True
            else:
                target_path = out_dir / "templates" / Path(name).name
                html_templates.append((Path(name).name, item["data"].decode("utf-8", errors="replace")))
        else:
            target_path = out_dir / Path(name).name

        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(item["data"])

        manifest.append({
            "file": str(target_path.relative_to(out_dir)).replace("\\", "/"),
            "source": item["source"],
            "offset": item["offset"],
            "size": item["size"],
        })

    # Сортируем JS-модули строго по порядку их следования в бинарнике (от 0x0071... до 0x0078...)
    js_Interactive_parts.sort(key=lambda x: int(x[0], 16))

    if js_Interactive_parts:
        bundle_path = out_dir / "js" / "astra_bundle.js"
        with open(bundle_path, "w", encoding="utf-8") as bf:
            for off, fname, code in js_Interactive_parts:
                bf.write(f"\n/* === OFFSET {off} : {fname} === */\n")
                bf.write(code)
                bf.write("\n")

    # Если готового корневого index.html не было (шаблоны собирались динамически), генерируем его
    if not has_root_index and (js_Interactive_parts or html_templates):
        css_links = "\n".join(
            f'  <link rel="stylesheet" href="/{m["file"]}">'
            for m in manifest if m["file"].endswith(".css")
        )
        index_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Astra Control Panel</title>
  <link rel="icon" href="/favicon.png" type="image/png">
{css_links}
</head>
<body>
  <div id="app"></div>
  <script src="/js/astra_bundle.js"></script>
</body>
</html>
"""
        (out_dir / "index.html").write_text(index_html, encoding="utf-8")

    # Сохраняем манифест со всеми смещениями
    manifest_path = out_dir / "extracted_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    return manifest


def main():
    bin_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(DEFAULT_BIN)
    out_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_OUT

    if not bin_path.is_file():
        print(f"[!] Файл бинарника не найден: {bin_path}")
        print(f"Использование: python {sys.argv[0]} <путь_к_бинарнику_astra> [папка_вывода]")
        sys.exit(1)

    raw_bytes = bin_path.read_bytes()
    with open(bin_path, "rb") as f:
        elf = ELFFile(f)
        mapper = ElfMemoryMapper(elf, raw_bytes)

    print(f"[*] Открыт ELF-файл: {bin_path} ({len(raw_bytes):,} байт)")
    for name, info in mapper.sections.items():
        print(f"    Секция {name:12s}: vaddr=0x{info['vaddr']:08x} offset=0x{info['offset']:08x} size={info['size']:,}")

    extracted = []

    print("[1/3] Поиск таблиц указателей в .data.rel.ro и .data...")
    ptr_assets = scan_pointer_tables(mapper)
    print(f"      Найдено по указателям: {len(ptr_assets)}")
    extracted.extend(ptr_assets)

    print("[2/3] Извлечение текстовых C-буферов (HTML/JS/CSS) из .rodata, .data.rel.ro, .data...")
    text_assets = scan_null_terminated_blocks(mapper)
    print(f"      Найдено текстовых модулей: {len(text_assets)}")
    extracted.extend(text_assets)

    print("[3/3] Карвинг бинарных ресурсов (PNG / GZIP)...")
    bin_assets = carve_binary_assets(mapper)
    print(f"      Найдено бинарных/сжатых ресурсов: {len(bin_assets)}")
    extracted.extend(bin_assets)

    manifest = build_combined_ui(out_dir, extracted)
    print(f"\n[+] Выгрузка завершена! Сохранено уникальных файлов: {len(manifest)} в папку: {out_dir.resolve()}")
    print(f"[+] Полный список файлов и смещений: {(out_dir / 'extracted_manifest.json').resolve()}")


if __name__ == "__main__":
    main()