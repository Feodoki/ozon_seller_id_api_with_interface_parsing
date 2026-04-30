import traceback
import logging
import os
import subprocess
from datetime import datetime
from flask import Flask, request, jsonify
import threading
import time

from ozon_api_parser import OzonSellerParse
from ozon_interface_parser import InterfaceParser
from data_to_google_sheets import upload_to_google_sheets, write_parser_error_to_sheet

app = Flask(__name__)

# ================= НАСТРОЙКА ЛОГИРОВАНИЯ =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs.txt', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================= СОСТОЯНИЕ =================
is_running = False
last_run_time = 0
lock = threading.Lock()

INTERVAL = 1000
headless_mode = True

interface_parser = InterfaceParser()
browser_started = interface_parser.start_browser(headless=headless_mode)


def kill_chrome_processes():
    """Принудительно завершает все процессы chrome.exe"""
    try:
        logger.info("   🔪 Завершение процессов chrome.exe...")

        # Способ 1: через taskkill (Windows)
        if os.name == 'nt':  # Windows
            result = subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe'],
                                    capture_output=True, text=True)
            if result.returncode == 0:
                logger.info(f"   ✅ Процессы chrome.exe завершены (taskkill)")
            else:
                logger.warning(f"   ⚠️ taskkill не нашел процессов chrome.exe")

        # Способ 2: через pkill (Linux/Mac)
        else:
            result = subprocess.run(['pkill', '-f', 'chrome'],
                                    capture_output=True, text=True)
            if result.returncode == 0:
                logger.info(f"   ✅ Процессы chrome завершены (pkill)")
            else:
                logger.warning(f"   ⚠️ pkill не нашел процессов chrome")

        # Дополнительная очистка: убиваем все дочерние процессы chrome
        if os.name == 'nt':
            subprocess.run(['taskkill', '/F', '/IM', 'chromedriver.exe'],
                           capture_output=True, text=True)
            logger.info("   🔪 Завершены процессы chromedriver.exe")

    except Exception as e:
        logger.error(f"   ❌ Ошибка при завершении процессов chrome: {e}")


def kill_chrome_processes_alternative():
    """Альтернативный способ завершения процессов через psutil"""
    try:
        import psutil
        killed_count = 0

        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                    proc.kill()
                    killed_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass

        if killed_count > 0:
            logger.info(f"   ✅ Завершено {killed_count} процессов chrome (psutil)")
        else:
            logger.info(f"   ℹ️ Процессов chrome не найдено")

    except ImportError:
        logger.warning("   ⚠️ psutil не установлен, пропускаем")
    except Exception as e:
        logger.error(f"   ❌ Ошибка при завершении процессов: {e}")


# ================= ТВОЙ СКРИПТ =================
def my_script(api_keys):
    global is_running, last_run_time

    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info(f"🚀 СКРИПТ ЗАПУЩЕН [{start_time.strftime('%Y-%m-%d %H:%M:%S')}]")
    logger.info(f"   API KEYS: {api_keys}")
    logger.info("=" * 80)

    # Инициализируем переменные для данных из интерфейса
    advert_analytic = {}
    position_analytic = {}

    try:
        logger.info("📌 Шаг 1: Инициализация OzonSellerParse...")
        parse = OzonSellerParse()
        logger.info("   ✅ OzonSellerParse инициализирован")

        logger.info("📌 Шаг 2: Инициализация InterfaceParser...")
        if interface_parser:
            logger.info("   ✅InterfaceParser успешно работает")

        logger.info("📌 Шаг 3 (без браузера): Получение аналитики из API...")
        all_items_dict = parse.main()
        logger.info(f"   ✅ Получено {len(all_items_dict)} товаров из API")

        if not browser_started:
            logger.error("   ❌ Не удалось запустить браузер после всех попыток")
            write_parser_error_to_sheet("Критическая ошибка: не удалось запустить браузер")
            # Продолжаем выполнение, но advert_analytic и position_analytic останутся пустыми
        else:
            logger.info("   ✅ InterfaceParser инициализирован")

            logger.info("📌 Шаг 4: Получение рекламной аналитики...")
            advert_analytic = interface_parser.get_all_advert_analytic()
            if advert_analytic:
                logger.info(f"   ✅ Получена рекламная аналитика для {len(advert_analytic)} товаров")
            else:
                logger.warning("   ⚠️ Рекламная аналитика не получена (пустой результат)")

            logger.info("📌 Шаг 5: Получение позиций товаров...")
            #position_analytic = interface_parser.get_position_product_from_sku()
            position_analytic = {} # Нет результат, озон изменил вкладку с этим значением
            if position_analytic:
                logger.info(f"   ✅ Получены позиции для {len(position_analytic)} SKU")
            else:
                logger.warning("   ⚠️ Позиции товаров не получены (пустой результат)")

            time.sleep(2)


        #print(all_items_dict, advert_analytic, position_analytic, sep='\n')
        logger.info("📌 Шаг 7: Загрузка данных в Google Sheets...")
        upload_to_google_sheets(all_items_dict, advert_analytic, position_analytic)
        logger.info("   ✅ Данные загружены в Google Sheets")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 80)
        logger.info(f"✅ СКРИПТ УСПЕШНО ЗАВЕРШЁН")
        logger.info(f"   Время начала:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"   Время окончания: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"   Длительность: {duration:.2f} секунд")
        logger.info(f"   Обработано товаров: {len(all_items_dict)}")
        logger.info(f"   Получено рекламных данных: {'да' if advert_analytic else 'нет'}")
        logger.info(f"   Получены позиции: {'да' if position_analytic else 'нет'}")
        logger.info("=" * 80)

    except Exception as e:
        error_time = datetime.now()
        logger.error("=" * 80)
        logger.error(f"❌ ОШИБКА В СКРИПТЕ [{error_time.strftime('%Y-%m-%d %H:%M:%S')}]")
        logger.error(f"   Тип ошибки: {type(e).__name__}")
        logger.error(f"   Сообщение: {str(e)}")
        logger.error(f"   Трассировка:")
        for line in traceback.format_exc().split('\n'):
            if line.strip():
                logger.error(f"      {line}")
        logger.error("=" * 80)

        # Опционально: записать ошибку в отдельный файл
        with open('error_log.txt', 'a', encoding='utf-8') as f:
            f.write(f"\n{'=' * 80}\n")
            f.write(f"[{error_time.strftime('%Y-%m-%d %H:%M:%S')}] ОШИБКА\n")
            f.write(traceback.format_exc())
            f.write(f"\n{'=' * 80}\n")

        write_parser_error_to_sheet(f"Критическая ошибка в скрипте: {str(e)}")

    finally:
        is_running = False
        last_run_time = time.time()
        logger.info("🏁 Состояние сброшено (is_running = False)")


# ================= ЗАПУСК =================
def try_start(api_keys):
    global is_running

    with lock:
        if is_running:
            logger.warning("⛔ Скрипт уже выполняется, запуск отклонён")
            return False

        is_running = True
        logger.info("✅ Скрипт поставлен в очередь на выполнение")

    threading.Thread(target=my_script, args=(api_keys,), daemon=True).start()
    return True


# ================= API =================
@app.route("/run", methods=["POST"])
def run():
    data = request.json or {}
    api_keys = data.get("api_keys", [])

    logger.info(f"📡 Получен запрос /run с API keys: {api_keys}")

    started = try_start(api_keys)

    return jsonify({
        "status": "started" if started else "already_running"
    })


@app.route("/status", methods=["GET"])
def status():
    """Эндпоинт для проверки статуса"""
    return jsonify({
        "is_running": is_running,
        "last_run_time": datetime.fromtimestamp(last_run_time).strftime(
            '%Y-%m-%d %H:%M:%S') if last_run_time > 0 else None,
        "interval_seconds": INTERVAL
    })


# ================= АВТОЦИКЛ =================
def auto_loop():
    global last_run_time

    logger.info("🔄 Автоцикл запущен")
    logger.info(f"   Интервал: {INTERVAL} секунд ({INTERVAL / 60:.0f} минут)")

    while True:
        now = time.time()

        if not is_running and (now - last_run_time >= INTERVAL):
            logger.info("⏰ Автозапуск по расписанию")

            # можно хранить последние ключи
            api_keys = ["AUTO1", "AUTO2", "AUTO3", "AUTO4"]

            started = try_start(api_keys)
            if started:
                last_run_time = time.time()
                logger.info("   ✅ Автозапуск инициирован")
            else:
                logger.warning("   ⚠️ Автозапуск не удался (скрипт уже выполняется)")

        time.sleep(5)  # проверка каждые 5 секунд


# ================= MAIN =================
if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("🔥 СЕРВЕР ЗАПУЩЕН")
    logger.info(f"   Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"   Интервал автозапуска: {INTERVAL} секунд")
    logger.info("=" * 80)

    # запускаем автоцикл
    threading.Thread(target=auto_loop, daemon=True).start()

    app.run(host="0.0.0.0", port=5000, debug=False)