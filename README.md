# AstraG — High-Performance IPTV & MPEG-TS / HLS Broadcast Server (Ubuntu 22.04 / 24.04)

**AstraG** — высокопроизводительный сервер приёма, анализа и вещания потоков **MPEG-TS (`.ts`)** и **HLS (`.m3u8`)** с веб-панелью управления **AstraG Control Panel** (полная совместимость с архитектурой **Cesbo Astra**).

---

## Скриншоты интерфейса AstraG

### 1. Окно входа в панель управления (`admin` / `admin`)
![Окно входа AstraG](docs/screenshots/astrag-login.png)

### 2. Основное окно управления потоками (Dashboard)
![Основное окно AstraG](docs/screenshots/astrag-main.png)

---

## Быстрая установка одной командой (Ubuntu 22.04 / 24.04)

Команда автоматически устанавливает все системные зависимости, клонирует `AstraG` в `/opt/astrag`, настраивает виртуальное окружение Python, создаёт и **сразу запускает** системный сервис `astrag.service` в автозагрузке на порту `8000` (а также открывает порт в `ufw`, если файрвол активен):

```bash
rm -rf /tmp/astrag && git clone https://github.com/onlyoktv/astrag.git /tmp/astrag && sudo bash /tmp/astrag/install.sh --start --port 8000 && rm -rf /tmp/astrag
```

Или напрямую через `curl` (укажите ссылку на ваш репозиторий в `--repo`, если используете форк):

```bash
curl -fsSL https://raw.githubusercontent.com/onlyoktv/astrag/main/install.sh | sudo bash -s -- --start --port 8000
```

> **После выполнения команды панель сразу доступна по адресу:**
> * **Веб-интерфейс:** `http://<IP_ВАШЕГО_СЕРВЕРА>:8000`
> * **Логин по умолчанию:** `admin`
> * **Пароль по умолчанию:** `admin`
> * **Общий плейлист каналов (M3U8):** `http://<IP_ВАШЕГО_СЕРВЕРА>:8000/playlist.m3u8`

---

## Обновление AstraG на сервере Ubuntu

Если проект уже установлен, обновить движок и перезапустить сервис (с полным сохранением всех потоков и пользователей в `/var/lib/astrag/state.json`) можно одной командой:

```bash
rm -rf /tmp/astrag && git clone https://github.com/onlyoktv/astrag.git /tmp/astrag && sudo bash /tmp/astrag/install.sh --update --port 8000 && rm -rf /tmp/astrag
```

---

## Полное удаление AstraG с сервера (Uninstall)

Чтобы **полностью удалить** `AstraG` (включая старые версии `astra-clone`, systemd-сервисы, виртуальное окружение, конфигурации `/etc/astrag` и базу состояния `/var/lib/astrag`), выполните одну команду:

```bash
sudo systemctl stop astrag astra-clone 2>/dev/null || true && sudo systemctl disable astrag astra-clone 2>/dev/null || true && sudo pkill -f "astra_clone_backend.py|astrag_backend.py" 2>/dev/null || true && sudo rm -f /etc/systemd/system/astrag.service /etc/systemd/system/astra-clone.service /usr/bin/astrag /usr/bin/astra-clone && sudo systemctl daemon-reload && sudo rm -rf /opt/astrag /opt/astra-clone /etc/astrag /etc/astra-clone /var/lib/astrag /var/lib/astra-clone && sudo userdel astrag 2>/dev/null || true && sudo userdel astra-clone 2>/dev/null || true && echo "AstraG полностью удалён"
```

Или через встроенный флаг установщика:

```bash
sudo bash install.sh --uninstall
```

---

## Основные возможности движка (`stream_engine.py` + `astrag_backend.py`)

* **Приём потоков (INPUT LIST):**
  * `http://` и `https://` — прямые потоки MPEG-TS и HLS-плейлисты (`.m3u8` / `.m3u`, включая Master/Variant плейлисты) с фоновой предзагрузкой сегментов (`sync_buffer`) и VBR-пейсингом по 27 МГц меткам **PCR**.
  * `udp://` и `rtp://` — Unicast и Multicast (`?interface=IP` / `#localaddr=IP`) с увеличенным буфером сокета `SO_RCVBUF = 2 МБ`.
  * Автоматический **Failover** (резервирование между несколькими входами `#1`, `#2`, ...) и фильтрация по `#pnr=`, `#filter=`, `#no_sdt`, `#no_eit`.
* **Отдача потоков (OUTPUT LIST и встроенные ссылки):**
  * Встроенная отдача **MPEG-TS (`.ts`)**: `/play/<id>.ts`, `/play/<id>`, `/live/<id>.ts` с выравниванием входа по ключевому кадру видео (**Clean Entry Gate: IDR + Audio PUSI**) и пакетированием по `32 КБ`.
  * Встроенная отдача **HLS (`.m3u8`)**: `/play/<id>.m3u8`, `/play/<id>/index.m3u8` с нарезкой сегментов строго по ключевым кадрам **H.264 / HEVC IDR**.
  * Произвольные **HTTP Outputs** в `OUTPUT LIST` (например, `http://0.0.0.0:18000/1` или `http://<IP>:18000/mychannel.ts`) с автоматическим поднятием дополнительных HTTP-портов.
  * Вещание в **UDP Unicast / Multicast** (`udp://239.x.x.x:1234?ttl=32`) с пакетированием по 7 TS-пакетов (`1316 байт <= 1500 MTU`).
* **Анализатор потока и управление пользователями:**
  * Полный разбор таблиц `PAT`, `PMT`, `SDT`, `NIT`, контроль ошибок `CC` и `PES`, детектор скремблирования (`Analyze` в карточке потока).
  * Управление пользователями, ролями и токенами доступа в `Settings -> Users`.

---

## Структура путей на сервере Ubuntu

| Путь | Назначение |
| --- | --- |
| `/opt/astrag/` | Код движка (`astrag_backend.py`, `astra_clone_backend.py`, `stream_engine.py`) и `venv` |
| `/opt/astrag/www/` | Веб-интерфейс (`index.html`, `mod.js`, `account.html`) |
| `/etc/astrag/astrag.conf` | Конфигурация сервиса (`ASTRA_HOST`, `ASTRA_PORT`) |
| `/var/lib/astrag/state.json` | База данных потоков, настроек и пользователей |
| `/usr/bin/astrag` | CLI-скрипт запуска |
| `/etc/systemd/system/astrag.service` | Unit-файл systemd |
