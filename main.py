import traceback
import logging
import os
import subprocess
import threading
import time
import socket
import sys
from datetime import datetime
from functools import wraps

from flask import Flask, request, jsonify, session, redirect, url_for

from ozon_api_parser import OzonSellerParse
from ozon_interface_parser import InterfaceParser
from data_to_google_sheets import upload_to_google_sheets, write_parser_error_to_sheet

# Импортируем новые модули
from status_manager import (
    STATUS_IDLE, STATUS_RUNNING, STATUS_STOPPED,
    save_status, load_status, get_status_display
)
from config_manager import (
    get_access_key_from_sheets, setup_settings_sheet, get_server_ip
)
from web_interface import get_login_html, get_dashboard_html

# ================= ЗАГРУЗКА КОНФИГУРАЦИИ =================
try:
    import config

    spread_id = getattr(config, 'spread_id', None)
    profile_name = getattr(config, 'profile_name', 'default')
    STORE_NAME = getattr(config, 'STORE_NAME', 'DefaultStore')
    AUTO_INTERVAL = getattr(config, 'AUTO_INTERVAL', 1000)
    HEADLESS_MODE = getattr(config, 'HEADLESS_MODE', True)
    WEB_PORT = getattr(config, 'WEB_PORT', 5000)

    print(f"✅ Конфигурация загружена из config.py")
    print(f"   Магазин: {STORE_NAME}")
    print(f"   Profile: {profile_name}")
    print(f"   Порт: {WEB_PORT}")
    print(f"   Интервал: {AUTO_INTERVAL} сек")
except ImportError as e:
    print(f"❌ Ошибка импорта config.py: {e}")
    sys.exit(1)

# Проверяем наличие обязательных параметров
if not spread_id:
    print("=" * 80)
    print("❌ ОШИБКА: Не указан spread_id в config.py!")
    print("=" * 80)
    sys.exit(1)

app = Flask(__name__)
app.secret_key = os.urandom(24)

# ================= НАСТРОЙКА ЛОГИРОВАНИЯ =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs_{STORE_NAME.replace(" ", "_")}.txt', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================= ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ =================
is_running_flag = False
current_thread = None
stop_requested = False
INTERVAL = AUTO_INTERVAL
headless_mode = HEADLESS_MODE

# ================= ИНИЦИАЛИЗАЦИЯ =================
logger.info("=" * 80)
logger.info(f"🖥 ЗАПУСК ПАРСЕРА ДЛЯ МАГАЗИНА: {STORE_NAME}")
logger.info(f"   Profile: {profile_name}")
logger.info(f"   Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
logger.info("=" * 80)

# Устанавливаем начальный статус
save_status(STATUS_IDLE, "Готов к работе. Ожидание запуска.")

# Запускаем браузер
logger.info("🌐 Запуск браузера...")
interface_parser = InterfaceParser()
browser_started = interface_parser.start_browser(headless=headless_mode)

if browser_started:
    logger.info("✅ Браузер успешно запущен")
else:
    logger.error("❌ Не удалось запустить браузер")


# ================= ФУНКЦИИ ДЛЯ РАБОТЫ С БРАУЗЕРОМ =================
def kill_chrome_processes():
    """Принудительно завершает все процессы chrome.exe"""
    try:
        logger.info("   🔪 Завершение процессов chrome.exe...")
        if os.name == 'nt':
            subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe'], capture_output=True, text=True)
            subprocess.run(['taskkill', '/F', '/IM', 'chromedriver.exe'], capture_output=True, text=True)
        else:
            subprocess.run(['pkill', '-f', 'chrome'], capture_output=True, text=True)
    except Exception as e:
        logger.error(f"   ❌ Ошибка: {e}")


# ================= ОСНОВНОЙ СКРИПТ ПАРСИНГА =================
def my_script():
    global is_running_flag, stop_requested

    start_time = datetime.now()
    save_status(STATUS_RUNNING, "Запуск парсинга...")
    logger.info("=" * 80)
    logger.info(f"🚀 СКРИПТ ЗАПУЩЕН [{start_time.strftime('%Y-%m-%d %H:%M:%S')}]")
    logger.info("=" * 80)

    advert_analytic = {}
    position_analytic = {}
    money_spent_advert_dict = {}
    tech_stats = {}

    try:
        if stop_requested:
            logger.info("⏹ Прерываем выполнение")
            save_status(STATUS_IDLE, "Остановлен пользователем")
            return

        save_status(STATUS_RUNNING, "Инициализация...")
        logger.info("📌 Шаг 1: Инициализация OzonSellerParse...")
        parse = OzonSellerParse()
        logger.info("   ✅ OzonSellerParse инициализирован")

        if stop_requested:
            save_status(STATUS_IDLE, "Остановлен пользователем")
            return

        save_status(STATUS_RUNNING, "Получение данных из API...")
        logger.info("📌 Шаг 2: Получение аналитики из API...")
        all_items_dict = parse.main()
        logger.info(f"   ✅ Получено {len(all_items_dict)} товаров")

        if stop_requested:
            save_status(STATUS_IDLE, "Остановлен пользователем")
            return

        if not browser_started:
            logger.error("   ❌ Браузер не запущен")
            write_parser_error_to_sheet("Критическая ошибка: не удалось запустить браузер")
        else:
            save_status(STATUS_RUNNING, "Получение рекламной аналитики...")
            logger.info("📌 Шаг 3: Получение рекламной аналитики...")
            advert_analytic, tech_stats = interface_parser.get_all_advert_analytic()

            if stop_requested:
                save_status(STATUS_IDLE, "Остановлен пользователем")
                return

            logger.info("📌 Шаг 4: Получение данных о расходах...")
            money_spent_advert_dict = interface_parser.get_analytic_money_spent()
            position_analytic = {}
            time.sleep(2)

        if stop_requested:
            save_status(STATUS_IDLE, "Остановлен пользователем")
            return

        save_status(STATUS_RUNNING, "Сохранение данных...")
        import json

        files_to_save = [
            ('tech_dict.json', tech_stats),
            ('all_items_dict.json', all_items_dict),
            ('advert_analytic.json', advert_analytic),
            ('money_spent_advert_dict.json', money_spent_advert_dict)
        ]

        for filename, data in files_to_save:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
            except:
                pass

        if stop_requested:
            save_status(STATUS_IDLE, "Остановлен пользователем")
            return

        save_status(STATUS_RUNNING, "Загрузка в Google Sheets...")
        logger.info("📌 Шаг 5: Загрузка данных в Google Sheets...")
        time.sleep(5)
        upload_to_google_sheets(all_items_dict, advert_analytic, position_analytic,
                                money_spent_advert_dict, tech_stats)
        logger.info("   ✅ Данные загружены")

        duration = (datetime.now() - start_time).total_seconds()
        logger.info("=" * 80)
        logger.info(f"✅ СКРИПТ ЗАВЕРШЁН за {duration:.2f} сек")
        logger.info("=" * 80)

        save_status(STATUS_IDLE, f"Парсинг завершен. Обработано {len(all_items_dict)} товаров")

    except Exception as e:
        logger.error(f"❌ ОШИБКА: {str(e)}")
        logger.error(traceback.format_exc())
        write_parser_error_to_sheet(f"Ошибка: {str(e)}")
        save_status(STATUS_IDLE, f"Ошибка: {str(e)[:100]}")

    finally:
        is_running_flag = False
        stop_requested = False


def run_script():
    """Запускает скрипт в отдельном потоке"""
    global is_running_flag, current_thread, stop_requested

    with threading.Lock():
        if is_running_flag:
            logger.warning("⛔ Скрипт уже выполняется")
            return False
        is_running_flag = True
        stop_requested = False

    current_thread = threading.Thread(target=my_script, daemon=True)
    current_thread.start()
    return True


def stop_script():
    """Останавливает выполнение скрипта"""
    global stop_requested
    logger.info("⏹ Остановка скрипта...")
    stop_requested = True
    save_status(STATUS_IDLE, "Остановка по запросу")


# ================= ФУНКЦИИ ДЛЯ AUTH =================
def get_server_url():
    """Получает URL сервера"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return f"http://{ip}:{WEB_PORT}"
    except:
        return f"http://localhost:{WEB_PORT}"


def check_auth():
    """Проверяет авторизацию"""
    if session.get('authenticated'):
        return True

    access_key, _ = get_access_key_from_sheets(STORE_NAME)
    auth_key = request.headers.get('X-Access-Key')

    if access_key and auth_key == access_key:
        session['authenticated'] = True
        return True

    return False


def require_auth(f):
    """Декоратор для проверки авторизации"""

    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.path in ['/login', '/health']:
            return f(*args, **kwargs)

        if not check_auth():
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Доступ запрещен', 'status': 'unauthorized'}), 401
            return redirect(url_for('login_page'))

        return f(*args, **kwargs)

    return decorated_function


# ================= ВЕБ-ИНТЕРФЕЙС =================
@app.route('/login', methods=['GET', 'POST'])
def login_page():
    """Страница входа"""
    if request.method == 'POST':
        access_key_input = request.form.get('access_key', '').strip()
        access_key, _ = get_access_key_from_sheets(STORE_NAME)

        if access_key and access_key_input == access_key:
            session['authenticated'] = True
            session.permanent = True
            logger.info(f"✅ Вход с IP: {request.remote_addr}")
            return redirect(url_for('index'))
        else:
            logger.warning(f"❌ Неудачная попытка входа с IP: {request.remote_addr}")
            return get_login_html(error="Неверный ключ доступа", server_url=get_server_url(), store_name=STORE_NAME)

    if session.get('authenticated'):
        return redirect(url_for('index'))

    return get_login_html(server_url=get_server_url(), store_name=STORE_NAME)


@app.route('/logout')
def logout():
    """Выход"""
    session.pop('authenticated', None)
    return redirect(url_for('login_page'))


@app.route('/')
@require_auth
def index():
    """Главная страница"""
    status_data = get_status_display()
    return get_dashboard_html(status_data, INTERVAL, get_server_url(), STORE_NAME)


@app.route('/api/status')
@require_auth
def api_status():
    """API статуса"""
    status_data = get_status_display()
    return jsonify({
        'status': status_data['status'],
        'status_name': status_data['status_name'],
        'status_class': status_data['status_class'],
        'message': status_data['message'],
        'last_run': status_data.get('last_run', ''),
        'interval': INTERVAL
    })


@app.route('/api/control', methods=['POST'])
@require_auth
def api_control():
    """API управления"""
    global is_running_flag

    data = request.json
    command = data.get('command')

    if command == 'start':
        if is_running_flag:
            return jsonify({'success': False, 'message': 'Скрипт уже выполняется'})

        if run_script():
            return jsonify({'success': True, 'message': 'Парсинг запущен'})
        return jsonify({'success': False, 'message': 'Не удалось запустить'})

    elif command == 'stop':
        if not is_running_flag:
            return jsonify({'success': False, 'message': 'Скрипт не выполняется'})

        stop_script()
        return jsonify({'success': True, 'message': 'Остановка...'})

    elif command == 'restart':
        if is_running_flag:
            stop_script()
            time.sleep(2)

        if run_script():
            return jsonify({'success': True, 'message': 'Перезапущен'})
        return jsonify({'success': False, 'message': 'Не удалось перезапустить'})

    elif command == 'enable_auto':
        save_status(STATUS_IDLE, "Автозапуск включен")
        return jsonify({'success': True, 'message': 'Автозапуск включен'})

    elif command == 'disable_auto':
        if is_running_flag:
            stop_script()
            time.sleep(1)
        save_status(STATUS_STOPPED, "Автозапуск выключен")
        return jsonify({'success': True, 'message': 'Автозапуск выключен'})

    return jsonify({'success': False, 'message': 'Неизвестная команда'})


@app.route('/health')
def health():
    """Health check"""
    return jsonify({'status': 'ok', 'store': STORE_NAME, 'time': datetime.now().isoformat()})


# ================= АВТОМАТИЧЕСКИЙ ЦИКЛ =================
def auto_loop():
    """Автоматический цикл"""
    logger.info(f"🔄 Автоцикл запущен, интервал: {INTERVAL} сек")

    while True:
        try:
            status_data = load_status()
            current_status = status_data.get('status', STATUS_IDLE)
            last_run_time = status_data.get('last_run_time', 0)
            now = time.time()

            if (current_status != STATUS_STOPPED and
                    not is_running_flag and
                    (now - last_run_time >= INTERVAL) and
                    current_status != STATUS_RUNNING):
                logger.info("⏰ Автозапуск по расписанию")
                run_script()
                save_status(STATUS_RUNNING, "Автоматический запуск")

        except Exception as e:
            logger.error(f"Ошибка в автоцикле: {e}")

        time.sleep(5)


# ================= ЗАПУСК =================
if __name__ == "__main__":
    server_ip = get_server_ip()

    logger.info("-" * 40)
    logger.info("🌐 ДОСТУП К ВЕБ-ПАНЕЛИ:")
    logger.info(f"   Магазин: {STORE_NAME}")
    logger.info(f"   Локальный доступ: http://localhost:{WEB_PORT}")
    logger.info(f"   Локальный IP: http://{server_ip}:{WEB_PORT}")
    logger.info("-" * 40)

    # Создаем лист настроек в Google Sheets
    try:
        from data_to_google_sheets import get_google_sheets_client
        from config import spread_id

        client = get_google_sheets_client()
        spreadsheet = client.open_by_key(spread_id)
        access_key, web_url = setup_settings_sheet(spreadsheet, STORE_NAME)
        if access_key:
            logger.info(f"🔑 Ключ доступа: {access_key}")
            logger.info(f"🌐 URL веб-панели: {web_url}")
    except Exception as e:
        logger.warning(f"Не удалось создать лист настроек: {e}")

    # Запускаем автоцикл
    auto_thread = threading.Thread(target=auto_loop, daemon=True)
    auto_thread.start()

    # Запускаем Flask
    app.run(host="0.0.0.0", port=WEB_PORT, debug=False, threaded=True)