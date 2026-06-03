import json
import secrets
import logging
import os
import socket
from datetime import datetime
from typing import Tuple, Optional, List

logger = logging.getLogger(__name__)

CONFIG_FILE = 'parser_config.json'


def get_server_ip():
    """Получает внешний IP сервера"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"


def save_config(api_keys: List[str], access_key: str, web_url: str) -> None:
    """Сохраняет конфигурацию в файл"""
    config = {
        'api_keys': api_keys,
        'access_key': access_key,
        'web_url': web_url,
        'updated_at': datetime.now().isoformat()
    }
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        logger.info("Конфигурация сохранена")
    except Exception as e:
        logger.error(f"Ошибка сохранения конфигурации: {e}")


def load_config() -> dict:
    """Загружает конфигурацию из файла"""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка загрузки конфигурации: {e}")
    return {'api_keys': [], 'access_key': None, 'web_url': None}


def get_access_key_from_sheets(store_name: str = None) -> Tuple[Optional[str], Optional[str]]:
    """Получает ключ доступа из Google Sheets (лист Настройки)"""
    try:
        from data_to_google_sheets import get_google_sheets_client, get_or_create_sheet, safe_get_values
        from config import spread_id, STORE_NAME

        store = store_name or STORE_NAME

        client = get_google_sheets_client()
        spreadsheet = client.open_by_key(spread_id)

        # Пытаемся найти лист с именем магазина
        sheet_name = f"Настройки_{store}"

        try:
            settings_sheet = spreadsheet.worksheet(sheet_name)
            values = safe_get_values(settings_sheet)

            access_key = None
            web_url = None

            for row in values:
                if row and len(row) >= 2:
                    if row[0] == "Ключ доступа":
                        access_key = row[1]
                    elif row[0] == "URL веб-панели":
                        web_url = row[1]

            return access_key, web_url
        except:
            logger.warning(f"Лист '{sheet_name}' не найден, создаем...")
            return setup_settings_sheet(spreadsheet, store)

    except Exception as e:
        logger.error(f"Ошибка получения ключа из Sheets: {e}")
        return None, None


def setup_settings_sheet(spreadsheet, store_name: str = None) -> Tuple[Optional[str], Optional[str]]:
    """Создает лист Настройки в Google Sheets"""
    try:
        from data_to_google_sheets import get_or_create_sheet, safe_update_cell
        from config import STORE_NAME, WEB_PORT

        store = store_name or STORE_NAME
        sheet_name = f"Настройки_{store}"

        sheet = get_or_create_sheet(spreadsheet, sheet_name, rows=20, cols=3)

        # Устанавливаем заголовки
        safe_update_cell(sheet, "A1", [["Параметр"]], value_input_option='USER_ENTERED')
        safe_update_cell(sheet, "B1", [["Значение"]], value_input_option='USER_ENTERED')
        safe_update_cell(sheet, "C1", [["Описание"]], value_input_option='USER_ENTERED')

        # Форматируем заголовки
        try:
            from gspread_formatting import CellFormat, TextFormat, format_cell_range
            format_cell_range(
                sheet, "A1:C1",
                CellFormat(textFormat=TextFormat(bold=True, fontSize=11))
            )
        except:
            pass

        access_key = secrets.token_hex(16)
        server_ip = get_server_ip()
        web_url = f"http://{server_ip}:{WEB_PORT}"

        settings_data = [
            ["Магазин", store, "Уникальное имя магазина"],
            ["Ключ доступа", access_key, "Скопируйте этот ключ для доступа к веб-панели"],
            ["URL веб-панели", web_url, "Актуальный адрес веб-интерфейса"],
            ["IP сервера", server_ip, "Текущий IP адрес сервера"],
            ["Порт", str(WEB_PORT), "Порт веб-сервера"],
            ["Интервал (сек)", "1000", "Интервал между автоматическими запусками"]
        ]

        for idx, row in enumerate(settings_data, start=2):
            safe_update_cell(sheet, f"A{idx}", [[row[0]]], value_input_option='USER_ENTERED')
            safe_update_cell(sheet, f"B{idx}", [[row[1]]], value_input_option='USER_ENTERED')
            safe_update_cell(sheet, f"C{idx}", [[row[2]]], value_input_option='USER_ENTERED')

        logger.info(f"✅ Лист '{sheet_name}' создан в Google Sheets")
        logger.info(f"🔑 Ключ доступа: {access_key}")
        logger.info(f"🌐 URL веб-панели: {web_url}")
        return access_key, web_url

    except Exception as e:
        logger.error(f"Ошибка создания листа Настройки: {e}")
        return None, None


def update_web_url_in_sheets(store_name: str = None) -> bool:
    """Обновляет URL веб-панели в Google Sheets"""
    try:
        from data_to_google_sheets import get_google_sheets_client, safe_update_cell
        from config import spread_id, STORE_NAME, WEB_PORT

        store = store_name or STORE_NAME
        sheet_name = f"Настройки_{store}"

        client = get_google_sheets_client()
        spreadsheet = client.open_by_key(spread_id)

        try:
            sheet = spreadsheet.worksheet(sheet_name)
            server_ip = get_server_ip()
            web_url = f"http://{server_ip}:{WEB_PORT}"

            # Обновляем URL веб-панели
            safe_update_cell(sheet, "B3", [[web_url]], value_input_option='USER_ENTERED')

            # Обновляем IP сервера
            safe_update_cell(sheet, "B4", [[server_ip]], value_input_option='USER_ENTERED')

            logger.info(f"✅ URL веб-панели обновлен: {web_url}")
            return True
        except:
            logger.warning(f"Лист '{sheet_name}' не найден")
            return False

    except Exception as e:
        logger.error(f"Ошибка обновления URL: {e}")
        return False