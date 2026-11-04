# AstraG — High-Performance MPEG-TS & HLS Broadcast Server (Cesbo Astra-Compatible)

**AstraG** — высокопроизводительный сервер приема, анализа, резервирования и вещания телевизионных потоков **MPEG-TS (`.ts`)** и **Apple HLS (`.m3u8`)** по протоколам `HTTP / HTTPS / UDP / RTP` с веб-интерфейсом управления в стиле **Cesbo Astra**.

Весь сервер и веб-интерфейс упакованы в **единый автономный исполняемый файл `astrag`**, который устанавливается и управляется точно так же, как оригинальный бинарник **Cesbo Astra** (`astrag init`, `astrag remove`, `systemctl start astrag`).

---

## Скриншоты интерфейса

### Окно входа в веб-панель AstraG
![Вход в AstraG](docs/screenshots/astrag-login.png)

### Основное окно управления потоками (Dashboard)
![Основное окно AstraG](docs/screenshots/astrag-main.png)

---

## Установка на сервер (Ubuntu 22.04 / 24.04 LTS)

### Вариант 1: Установка одним файлом `astrag` (как в оригинальной Cesbo Astra)

Скачайте готовый исполняемый файл `astrag` в `/usr/bin/astrag`, сделайте его исполняемым и зарегистрируйте системный сервис:

```bash
sudo curl -Lo /usr/bin/astrag https://raw.githubusercontent.com/udemika/astrag/main/astrag
sudo chmod +x /usr/bin/astrag
sudo astrag init 8000
sudo systemctl start astrag
sudo systemctl enable astrag
```

#### То же самое одной командой (быстрая установка и автозапуск):
```bash
sudo curl -Lo /usr/bin/astrag https://raw.githubusercontent.com/udemika/astrag/main/astrag && sudo chmod +x /usr/bin/astrag && sudo astrag init 8000 --start
```

> При выполнении `astrag init` автоматически проверяются и доустанавливаются только штатные системные пакеты Ubuntu (`apt` без `pip` и без `venv`), создается конфигурация `/etc/astrag/astrag.conf` и регистрируется служба `systemd`.

---

### Вариант 2: Сборка в нативный ELF-бинарник (опционально)

Если вы хотите скомпилировать `astrag` на сервере в монолитный машинный ELF-бинарник `/opt/astrag/astrag-bin`:

```bash
sudo curl -Lo /usr/bin/astrag https://raw.githubusercontent.com/udemika/astrag/main/astrag
sudo chmod +x /usr/bin/astrag
sudo astrag init 8000 --binary --start
```

---

### Вариант 3: Установка через скрипт `install.sh`

```bash
# Установка на системных APT-пакетах Ubuntu (без pip и без venv):
curl -fsSL https://raw.githubusercontent.com/udemika/astrag/main/install.sh | sudo bash -s -- --apt --start --port 8000

# Или сборка в нативный бинарник:
curl -fsSL https://raw.githubusercontent.com/udemika/astrag/main/install.sh | sudo bash -s -- --binary --start --port 8000
```

---

## Вход в веб-интерфейс

После запуска откройте в браузере:
```text
http://<IP_ВАШЕГО_СЕРВЕРА>:8000/
```

Данные для входа по умолчанию:
- **Логин**: `admin`
- **Пароль**: `admin`

*(Пароль можно сразу изменить в верхнем меню **Settings → Users** или **Account**).*

---

## Управление сервисом (в стиле Cesbo Astra)

Управление службой осуществляется стандартными командами `systemd`:

```bash
# Запуск сервера
sudo systemctl start astrag

# Остановка сервера
sudo systemctl stop astrag

# Перезапуск сервера
sudo systemctl restart astrag

# Включение автозапуска при загрузке ОС
sudo systemctl enable astrag

# Отключение автозапуска
sudo systemctl disable astrag

# Просмотр статуса и логов
sudo systemctl status astrag
sudo journalctl -u astrag -f
```

### Запуск дополнительного экземпляра на другом порту
Как и в оригинальной Cesbo Astra, вы можете зарегистрировать несколько независимых сервисов на разных портах:
```bash
sudo astrag init 8001 astrag-backup
sudo systemctl start astrag-backup
sudo systemctl enable astrag-backup
```

### Сброс пароля администратора через консоль
```bash
sudo astrag reset-password admin НовыйПароль123
sudo systemctl restart astrag
```

---

## Обновление AstraG

Для обновления достаточно скачать свежий файл `/usr/bin/astrag` и перезапустить сервис (все ваши потоки и настройки в `/var/lib/astrag/state.json` сохраняются):

```bash
sudo curl -Lo /usr/bin/astrag https://raw.githubusercontent.com/udemika/astrag/main/astrag
sudo chmod +x /usr/bin/astrag
sudo astrag init 8000
sudo systemctl restart astrag
```

---

## Удаление AstraG

### Стандартное удаление (как в Cesbo Astra)
Остановка сервиса, удаление из автозагрузки и удаление исполняемого файла:

```bash
sudo systemctl stop astrag
sudo systemctl disable astrag
sudo astrag remove
sudo rm -f /usr/bin/astrag
```

### Полное удаление одной командой (включая все настройки и базы `/etc/astrag`, `/var/lib/astrag`, `/opt/astrag`)
```bash
sudo astrag remove --purge
```
*(Либо через внешний скрипт: `curl -fsSL https://raw.githubusercontent.com/udemika/astrag/main/uninstall.sh | sudo bash -s -- --purge`)*

---

## Размещение в отдельном репозитории GitHub

Файл **`astrag`** в корне проекта является **полностью автономным** (содержит внутри себя весь движок `stream_engine.py`, сервер `astra_clone_backend.py`, модуль безопасности и веб-панель `www/`).

Если вы хотите создать отдельный чистый репозиторий только для установки (например, `https://github.com/udemika/astrag-release`), достаточно скопировать в него всего 3 элемента:
1. `astrag` *(готовый автономный установочный и исполняемый файл)*
2. `README.md` *(эта инструкция)*
3. `docs/screenshots/` *(папка со скриншотами `astrag-login.png` и `astrag-main.png`)*
