import gspread
from gspread.utils import rowcol_to_a1
from gspread_formatting import (
    CellFormat,
    Color,
    TextFormat,
    format_cell_range,
    set_frozen,
    set_column_width,
    Border,
    Borders
)
from google.oauth2.service_account import Credentials
from config import spread_id
import json
import time
from datetime import datetime
import pytz
from typing import Dict, List, Any, Optional, Tuple

# ================= НОВЫЙ МОДУЛЬ: TECHNICAL SHEET =================

# Конфигурация Технического листа
TECHNICAL_SHEET_CONFIG = {
    "sheet_name": "ТЕХНИЧЕСКИЙ ЛИСТ",
    "settings_headers": [
        {"name": "Параметр", "width": 200},
        {"name": "Значение", "width": 150},
        {"name": "Единицы", "width": 100}
    ],
    # ОБНОВЛЕННЫЕ ЗАГОЛОВКИ - только нужные колонки
    "products_headers": [
        {"name": "Артикул", "width": 150},
        {"name": "SKU", "width": 150},
        {"name": "Цена до скидки (₽)", "width": 130},
        {"name": "Цена для покупателя (₽)", "width": 150},
        {"name": "Эквайринг (₽)", "width": 120},
        {"name": "Остатки (шт)", "width": 100},
        {"name": "Комиссия FBO (%)", "width": 120},
        {"name": "Объем товара (л)", "width": 120},
        {"name": "Стоимость логистики (₽)", "width": 140}
    ]
}

# Конфигурация таблицы логистики
LOGISTICS_TABLE_CONFIG = {
    "headers": [
        {"name": "Объем (л)", "width": 120},
        {"name": "Стоимость логистики (₽)", "width": 180}
    ],
    "default_data": [
        [0.5, 45],
        [1.0, 55],
        [1.5, 65],
        [2.0, 75],
        [3.0, 95],
        [5.0, 125],
        [10.0, 175]
    ]
}

# Глобальная переменная для хранения настроек (можно сохранять в Google Sheets)
technical_settings = {
    'tax_rate': 6.0,  # Ставка УСН+НДС (по умолчанию 6%)
    'acquiring_rate': 1.0,  # Эквайринг в процентах (по умолчанию 1%)
    'logistics_prices': {}  # Стоимость логистики по объему
}

# ================= КОНФИГУРАЦИЯ СТРУКТУРЫ ЛИСТОВ =================

# Конфигурация листа DASHBOARD
DASHBOARD_CONFIG = {
    "headers": [
        {"name": "Артикул товара", "width": 200, "format": {"bold": True}},
        {"name": "Сумма продаж за день на текущий момент", "width": 100, "format": {}},
        {"name": "Количество продаж за день", "width": 100, "format": {}},
        {"name": "ДРР (поиск/поиск и рекомендации) %", "width": 100, "format": {}},
        {"name": "ДРР (оплата за заказ) %", "width": 100, "format": {}},
        {"name": "ДРР (общий) %", "width": 100, "format": {}}
    ],
    "frozen_rows": 1,
    "header_color": Color(0.9, 1, 0.9)
}

# Конфигурация листа аналитики товара
ANALYTICS_CONFIG = {
    "headers": [
        {"name": "Дата", "width": 75},
        {"name": "Заказано на сумму", "width": 75},
        {"name": "Заказано товаров", "width": 75},
        {"name": "Позиция в каталоге и поиске", "width": 75},
        {"name": "Показы всего", "width": 75},
        {"name": "Посещения карточки товара", "width": 75},
        {"name": "Конверсия из поиска и каталога в карточку", "width": 75},
        {"name": "Конверсия из поиска и каталога в корзину", "width": 75},
        {"name": "Конверсия в корзину общая", "width": 75}
    ],
    "start_column": "A",
    "block_title": "АНАЛИТИКА",
    "block_color": Color(0.9, 1, 0.9)
}

# Конфигурация рекламных блоков
CAMPAIGN_CONFIGS = {
    "search": {
        "title": "РЕКЛАМА — ПОИСК",
        "start_column": "K",
        "headers": [
            {"name": "Стратегия", "width": 75},
            {"name": "Конкурентная ставка", "width": 75},
            {"name": "Ваша ставка", "width": 75},
            {"name": "Средняя стоимость клика (₽)", "width": 75},
            {"name": "Заказы", "width": 75},
            {"name": "В корзину (шт)", "width": 75},
            {"name": "ДРР (%)", "width": 75},
            {"name": "CTR (%)", "width": 75},
            {"name": "Показы", "width": 75},
            {"name": "Клики", "width": 75},
            {"name": "Бюджет (₽)", "width": 75},
            {"name": "Цена товара (₽)", "width": 75},
            {"name": "Расходы (₽)", "width": 75}
        ],
        "color": Color(0.85, 0.92, 1)
    },
    "recommendations": {
        "title": "РЕКЛАМА — ПОИСК И РЕКОМЕНДАЦИИ",
        "start_column": "Y",
        "headers": [
            {"name": "Стратегия", "width": 75},
            {"name": "Конкурентная ставка", "width": 75},
            {"name": "Ваша ставка", "width": 75},
            {"name": "Средняя стоимость клика (₽)", "width": 75},
            {"name": "Заказы", "width": 75},
            {"name": "В корзину (шт)", "width": 75},
            {"name": "ДРР (%)", "width": 75},
            {"name": "CTR (%)", "width": 75},
            {"name": "Показы", "width": 75},
            {"name": "Клики", "width": 75},
            {"name": "Бюджет (₽)", "width": 75},
            {"name": "Цена товара (₽)", "width": 75},
            {"name": "Расходы (₽)", "width": 75}
        ],
        "color": Color(0.95, 0.9, 1)
    },
    "cpo": {
        "title": "РЕКЛАМА — ОПЛАТА ЗА ЗАКАЗ",
        "start_column": "AM",
        "headers": [
            {"name": "Ставка (₽) [%]", "width": 75},
            {"name": "Цена товара (₽)", "width": 75},
            {"name": "Индекс видимости", "width": 75},
            {"name": "Заказы (Оплата за заказ)", "width": 75},
            {"name": "Заказы (Комбо-модель)", "width": 75},
            {"name": "ДРР (%)", "width": 75},
            {"name": "Расходы (Оплата за заказ) (₽)", "width": 75},
            {"name": "Расходы (Комбо-модель) (₽)", "width": 75}
        ],
        "color": Color(1, 0.95, 0.8)
    }
}


# ================= ФУНКЦИИ ДЛЯ ЛОГИСТИКИ =================

def load_logistics_prices(tech_sheet) -> Dict:
    """Загружает стоимость логистики из Технического листа"""
    logistics_prices = {}

    try:
        # Ищем таблицу логистики (она начинается с заголовка "📦 ТАБЛИЦА СТОИМОСТИ ЛОГИСТИКИ")
        all_values = execute_with_retry(tech_sheet.get_all_values)

        for row_idx, row in enumerate(all_values):
            if row and len(row) > 0 and "ТАБЛИЦА СТОИМОСТИ ЛОГИСТИКИ" in str(row[0]):
                # Нашли заголовок таблицы, данные начинаются через 2 строки
                data_start_row = row_idx + 3
                for data_row in all_values[data_start_row:]:
                    if len(data_row) >= 2 and data_row[0] and data_row[0].strip():
                        try:
                            volume = float(str(data_row[0]).replace(',', '.'))
                            price = float(str(data_row[1]).replace(',', '.'))
                            logistics_prices[volume] = price
                        except (ValueError, TypeError):
                            continue
                break

        if not logistics_prices:
            for vol, price in LOGISTICS_TABLE_CONFIG["default_data"]:
                logistics_prices[vol] = price

        print(f"  📦 Загружено {len(logistics_prices)} правил стоимости логистики")
        return logistics_prices
    except Exception as e:
        print(f"  ⚠️ Ошибка загрузки стоимости логистики: {e}")
        default_prices = {}
        for vol, price in LOGISTICS_TABLE_CONFIG["default_data"]:
            default_prices[vol] = price
        return default_prices


def get_logistics_price(volume_liters: float, logistics_prices: Dict) -> float:
    """Получает стоимость логистики по объему товара"""
    if not logistics_prices or volume_liters <= 0:
        return 0

    volumes = sorted(logistics_prices.keys())
    for vol in volumes:
        if volume_liters <= vol:
            return logistics_prices[vol]

    return logistics_prices[volumes[-1]] if volumes else 0


def setup_logistics_table(tech_sheet, start_row: int):
    """Настраивает таблицу стоимости логистики в Техническом листе"""
    print("  📦 Настройка таблицы стоимости логистики...")

    # Заголовок таблицы логистики
    execute_with_exponential_backoff(
        tech_sheet.update,
        f"A{start_row}",
        [[f"📦 ТАБЛИЦА СТОИМОСТИ ЛОГИСТИКИ (по литражу)"]]
    )
    execute_with_exponential_backoff(
        format_cell_range, tech_sheet, f"A{start_row}:B{start_row}",
        CellFormat(
            textFormat=TextFormat(bold=True, fontSize=11),
            backgroundColor=Color(0.8, 0.85, 0.95)
        )
    )

    # Заголовки столбцов
    headers = [h['name'] for h in LOGISTICS_TABLE_CONFIG["headers"]]
    execute_with_exponential_backoff(tech_sheet.update, f"A{start_row + 1}", [headers])

    execute_with_exponential_backoff(
        format_cell_range, tech_sheet, f"A{start_row + 1}:B{start_row + 1}",
        CellFormat(
            textFormat=TextFormat(bold=True),
            backgroundColor=Color(0.85, 0.95, 0.85)
        )
    )

    # Данные таблицы логистики
    execute_with_exponential_backoff(
        tech_sheet.update,
        f"A{start_row + 2}",
        LOGISTICS_TABLE_CONFIG["default_data"]
    )

    # Устанавливаем ширину колонок
    for idx, header in enumerate(LOGISTICS_TABLE_CONFIG["headers"], start=1):
        col_letter = get_column_letter(idx)
        execute_with_exponential_backoff(set_column_width, tech_sheet, col_letter, header['width'])

    # Примечание
    note_row = start_row + 2 + len(LOGISTICS_TABLE_CONFIG["default_data"]) + 1
    execute_with_exponential_backoff(
        tech_sheet.update,
        f"A{note_row}",
        [["💡 Примечание: Таблицу можно редактировать вручную. Стоимость логистики подтягивается автоматически."]]
    )

    print("  ✅ Таблица стоимости логистики настроена")


def setup_technical_sheet(spreadsheet):
    """
    Настраивает Технический лист с обновленной структурой
    """
    print("\n📋 НАСТРОЙКА ТЕХНИЧЕСКОГО ЛИСТА")

    # Получаем или создаем лист
    tech_sheet = get_or_create_sheet(spreadsheet, TECHNICAL_SHEET_CONFIG["sheet_name"], rows=5000, cols=50)

    # Очищаем лист
    execute_with_exponential_backoff(tech_sheet.clear)
    time.sleep(1)

    # ===== БЛОК НАСТРОЕК (строки 1-5) =====
    print("  ⚙️ Настройка блока параметров...")

    # Заголовок блока настроек
    execute_with_exponential_backoff(tech_sheet.update, "A1", [["⚙️ НАСТРОЙКИ КАЛЬКУЛЯТОРА"]])
    execute_with_exponential_backoff(
        format_cell_range, tech_sheet, "A1:C1",
        CellFormat(
            textFormat=TextFormat(bold=True, fontSize=12),
            backgroundColor=Color(0.8, 0.9, 1)
        )
    )

    # Заголовки таблицы настроек
    settings_headers = [h['name'] for h in TECHNICAL_SHEET_CONFIG["settings_headers"]]
    execute_with_exponential_backoff(tech_sheet.update, "A2", [settings_headers])

    # Форматирование заголовков настроек
    end_col = get_column_letter(len(settings_headers))
    execute_with_exponential_backoff(
        format_cell_range, tech_sheet, f"A2:{end_col}2",
        CellFormat(
            textFormat=TextFormat(bold=True),
            backgroundColor=Color(0.85, 0.95, 0.85)
        )
    )

    # Данные настроек
    settings_data = [
        ["Ставка УСН + НДС (%)", technical_settings['tax_rate'], "%"],
        ["Эквайринг (%)", technical_settings['acquiring_rate'], "%"],
    ]

    execute_with_exponential_backoff(tech_sheet.update, "A3", settings_data)

    # Установка ширины колонок для настроек
    for idx, header in enumerate(TECHNICAL_SHEET_CONFIG["settings_headers"], start=1):
        col_letter = get_column_letter(idx)
        if 'width' in header:
            execute_with_exponential_backoff(set_column_width, tech_sheet, col_letter, header['width'])

    time.sleep(1)


    # ===== ТАБЛИЦА ЛОГИСТИКИ (строки 12-20) =====
    logistics_start_row = 6
    setup_logistics_table(tech_sheet, logistics_start_row)
    time.sleep(1)

    # ===== БЛОК ТОВАРОВ (с строки 22) =====
    products_start_row = 22

    print("  📦 Настройка блока товаров...")

    execute_with_exponential_backoff(tech_sheet.update, f"A{products_start_row}", [["📊 ТОВАРЫ В ПРОДАЖЕ"]])
    execute_with_exponential_backoff(
        format_cell_range, tech_sheet, f"A{products_start_row}:I{products_start_row}",
        CellFormat(
            textFormat=TextFormat(bold=True, fontSize=12),
            backgroundColor=Color(0.8, 0.9, 1)
        )
    )

    # ОБНОВЛЕННЫЕ ЗАГОЛОВКИ (9 колонок)
    products_headers = [h['name'] for h in TECHNICAL_SHEET_CONFIG["products_headers"]]
    execute_with_exponential_backoff(tech_sheet.update, f"A{products_start_row + 1}", [products_headers])

    end_col = get_column_letter(len(products_headers))
    execute_with_exponential_backoff(
        format_cell_range, tech_sheet, f"A{products_start_row + 1}:{end_col}{products_start_row + 1}",
        CellFormat(
            textFormat=TextFormat(bold=True),
            backgroundColor=Color(0.85, 0.95, 0.85)
        )
    )

    for idx, header in enumerate(TECHNICAL_SHEET_CONFIG["products_headers"], start=1):
        col_letter = get_column_letter(idx)
        if 'width' in header:
            execute_with_exponential_backoff(set_column_width, tech_sheet, col_letter, header['width'])

    # Замораживаем строки
    execute_with_exponential_backoff(set_frozen, tech_sheet, rows=products_start_row + 1)

    print("  ✅ Технический лист настроен")
    return tech_sheet, products_start_row + 2  # Возвращаем лист и строку начала данных


def update_technical_sheet(tech_sheet, all_items_dict: Dict, products_start_row: int):
    """
    Обновляет данные в Техническом листе с ОБНОВЛЕННЫМИ колонками
    """
    print("\n📊 ОБНОВЛЕНИЕ ТЕХНИЧЕСКОГО ЛИСТА")

    # Загружаем стоимость логистики
    logistics_prices = load_logistics_prices(tech_sheet)
    technical_settings['logistics_prices'] = logistics_prices

    # Получаем текущие настройки из листа (если они были изменены)
    try:
        tax_rate_cell = tech_sheet.acell('B3').value
        if tax_rate_cell and str(tax_rate_cell) != '—' and str(tax_rate_cell) != '':
            technical_settings['tax_rate'] = float(tax_rate_cell)

        acquiring_rate_cell = tech_sheet.acell('B4').value
        if acquiring_rate_cell and str(acquiring_rate_cell) != '—' and str(acquiring_rate_cell) != '':
            technical_settings['acquiring_rate'] = float(acquiring_rate_cell)
    except Exception as e:
        print(f"  ⚠️ Ошибка чтения настроек: {e}")

    print(
        f"  ⚙️ Используемые настройки: налог={technical_settings['tax_rate']}%, эквайринг={technical_settings['acquiring_rate']}%")

    # Очищаем старые данные товаров
    try:
        all_values = execute_with_retry(tech_sheet.get_all_values)
        total_rows = len(all_values)

        if total_rows >= products_start_row:
            end_col = get_column_letter(len(TECHNICAL_SHEET_CONFIG["products_headers"]))
            start_row = products_start_row
            end_row = total_rows + 10

            clear_range = f"A{start_row}:{end_col}{end_row}"
            execute_with_retry(tech_sheet.batch_clear, [clear_range])
            print(f"  ✅ Очищена область {clear_range}")

        time.sleep(1)
    except Exception as e:
        print(f"  ⚠️ Ошибка при очистке: {e}")

    # Подготавливаем ОБНОВЛЕННЫЕ данные по товарам (9 колонок)
    products_data = []

    for item in all_items_dict.values():
        offer_id = item.get("offer_id")
        skus_list = item.get("skus", [])
        sku = skus_list[0] if skus_list else "—"

        # Получаем цены
        price_before = clean_numeric_value(item.get("price_before", 0))
        current_price = clean_numeric_value(item.get("price", 0))

        # Остатки товара
        stock_balance = clean_int_value(item.get("stock_balance", 0))

        # Комиссия FBO (если есть в данных, иначе 0)
        commission_fbo = clean_numeric_value(item.get("commission_fbo", 0))

        # Объем товара (пока ставим 1, можно позже добавить в данные)
        product_volume = 1.0

        # Рассчитываем стоимость логистики по объему
        logistics_cost = get_logistics_price(product_volume, logistics_prices)

        # Рассчитываем эквайринг (процент от цены до скидки)
        acquiring_fee = round(price_before * (technical_settings['acquiring_rate'] / 100), 2)

        products_data.append([
            offer_id,  # Артикул
            sku,  # SKU
            price_before,  # Цена до скидки
            current_price,  # Цена для покупателя
            acquiring_fee,  # Эквайринг (рассчитывается)
            stock_balance,  # Остатки
            commission_fbo,  # Комиссия FBO
            product_volume,  # Объем товара (пока 1)
            logistics_cost  # Стоимость логистики
        ])

    # Добавляем новые данные
    if products_data:
        print(f"  📝 Добавление данных для {len(products_data)} товаров...")
        range_to_update = f"A{products_start_row}"
        execute_with_retry(
            tech_sheet.update,
            range_to_update,
            products_data,
            value_input_option='USER_ENTERED'
        )
        print(f"  ✅ Добавлено {len(products_data)} товаров")

        last_row = products_start_row + len(products_data) - 1

        # Форматирование обновленных колонок
        # Колонка C (Цена до скидки) - жирный шрифт, фон фиолетовый
        execute_with_retry(
            format_cell_range, tech_sheet, f"C{products_start_row}:C{last_row}",
            CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.95, 0.9, 1))
        )

        # Колонка D (Цена для покупателя) - жирный шрифт, фон зеленый
        execute_with_retry(
            format_cell_range, tech_sheet, f"D{products_start_row}:D{last_row}",
            CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.9, 1, 0.9))
        )

        # Колонка E (Эквайринг) - фон желтый
        execute_with_retry(
            format_cell_range, tech_sheet, f"E{products_start_row}:E{last_row}",
            CellFormat(backgroundColor=Color(1, 0.95, 0.8))
        )

        # Колонка I (Стоимость логистики) - фон голубой
        execute_with_retry(
            format_cell_range, tech_sheet, f"I{products_start_row}:I{last_row}",
            CellFormat(backgroundColor=Color(0.7, 0.85, 1))
        )

        print("  💡 Примечание: Эквайринг рассчитывается автоматически")
        print("  💡 Таблицу стоимости логистики можно редактировать выше")

    print("  ✅ Технический лист обновлен")


# ================= БАЗОВЫЕ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================

def clean_numeric_value(value: Any) -> float:
    """Очищает числовое значение от форматирования и преобразует в float"""
    if value is None or value == '' or value == '—':
        return 0.0
    if isinstance(value, (int, float)):
        return float(value) if not isinstance(value, bool) else 0.0
    if isinstance(value, str):
        cleaned = value.replace('\u202f', '').replace(' ', '').replace(',', '.').strip()
        cleaned = cleaned.replace('%', '').replace('₽', '')
        if cleaned == '' or cleaned == '—':
            return 0.0
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return 0.0
    return 0.0


def clean_int_value(value: Any) -> int:
    """Преобразует значение в целое число"""
    return int(clean_numeric_value(value))


def get_current_date_moscow() -> str:
    """Возвращает текущую дату в московском часовом поясе"""
    moscow_tz = pytz.timezone('Europe/Moscow')
    now_moscow = datetime.now(moscow_tz)
    return now_moscow.strftime("%d.%m.%Y")


def execute_with_exponential_backoff(func, *args, max_retries=10, **kwargs):
    """Выполняет функцию с экспоненциальной задержкой при ошибках 429"""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if '429' in str(e) or 'Quota exceeded' in str(e):
                wait_time = min(30 * (2 ** attempt), 600)
                print(f"  ⏳ Превышен лимит. Пауза {wait_time} сек... "
                      f"(попытка {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                raise e
    raise Exception(f"Не удалось выполнить операцию после {max_retries} попыток")


def execute_with_retry(func, *args, **kwargs):
    """Выполняет функцию с повторными попытками при ошибках 429"""
    return execute_with_exponential_backoff(func, *args, **kwargs)


def batch_update_with_retry(sheet, range_name, values, batch_size=50):
    """Обновляет данные пакетами для соблюдения лимитов"""
    if not values:
        return

    total_rows = len(values)

    for i in range(0, total_rows, batch_size):
        batch_values = values[i:i + batch_size]
        batch_range = f"{range_name.split(':')[0]}{int(range_name.split(':')[0][1:]) + i}"

        try:
            execute_with_retry(
                sheet.update,
                batch_range,
                batch_values,
                value_input_option='USER_ENTERED'
            )
            print(f"    ✅ Обновлен пакет {i // batch_size + 1}/"
                  f"{(total_rows + batch_size - 1) // batch_size}")
            time.sleep(1)
        except Exception as e:
            print(f"    ❌ Ошибка при обновлении пакета: {e}")
            raise


def get_google_sheets_client():
    """Создает и возвращает авторизованного клиента Google Sheets"""
    creds = Credentials.from_service_account_file(
        "google_sheets.json",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    return gspread.authorize(creds)


def get_or_create_sheet(spreadsheet, title: str, rows=1000, cols=30):
    """Получает существующий лист или создает новый"""
    try:
        return execute_with_exponential_backoff(spreadsheet.worksheet, title)
    except gspread.exceptions.WorksheetNotFound:
        return execute_with_exponential_backoff(
            spreadsheet.add_worksheet, title=title, rows=rows, cols=cols
        )


# ================= ФУНКЦИИ ДЛЯ ФОРМАТИРОВАНИЯ ЛИСТОВ =================

def get_column_letter(col_num: int) -> str:
    """Преобразует номер колонки в букву (1->A, 27->AA)"""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(ord('A') + col_num % 26) + result
        col_num //= 26
    return result


def get_column_index(col_letter: str) -> int:
    """Преобразует букву колонки в номер (A->1, AA->27)"""
    result = 0
    for char in col_letter:
        result = result * 26 + (ord(char.upper()) - ord('A') + 1)
    return result


def setup_sheet_headers(sheet, config: Dict, start_row: int = 1):
    """
    Универсальная функция для настройки заголовков листа
    """
    headers_list = [h['name'] for h in config['headers']]

    # Записываем заголовки одной операцией
    end_col = get_column_letter(len(headers_list))
    range_name = f"A{start_row}:{end_col}{start_row}"
    execute_with_exponential_backoff(sheet.update, range_name, [headers_list])
    time.sleep(0.5)

    # Форматируем заголовки
    header_range = f"A{start_row}:{end_col}{start_row}"
    execute_with_exponential_backoff(
        format_cell_range, sheet, header_range,
        CellFormat(
            textFormat=TextFormat(bold=True, fontSize=11),
            backgroundColor=config.get('header_color', Color(0.9, 0.9, 0.9))
        )
    )

    # Замораживаем строки
    if config.get('frozen_rows'):
        execute_with_exponential_backoff(set_frozen, sheet, rows=config['frozen_rows'])
    time.sleep(0.5)

    # Устанавливаем ширину колонок
    for idx, header in enumerate(config['headers'], start=1):
        col_letter = get_column_letter(idx)
        if 'width' in header:
            execute_with_exponential_backoff(
                set_column_width, sheet, col_letter, header['width']
            )
    time.sleep(0.5)


def clear_old_dashboard_data(dashboard, current_total_rows: int):
    """Очищает старые данные в DASHBOARD (кроме заголовков)"""
    if current_total_rows > 1:
        print("  🗑️ Очищаем старые данные...")
        try:
            execute_with_retry(dashboard.batch_clear, [f"A2:F{current_total_rows}"])
            print(f"  ✅ Очищено содержимое строк 2-{current_total_rows}")
            time.sleep(1)
        except Exception as e:
            print(f"  ⚠️ Ошибка при очистке: {e}")


def format_totals_row(sheet, last_row: int, num_columns: int):
    """Форматирует итоговую строку"""
    end_col = get_column_letter(num_columns)
    execute_with_retry(
        format_cell_range, sheet, f"A{last_row}:{end_col}{last_row}",
        CellFormat(
            textFormat=TextFormat(bold=True),
            backgroundColor=Color(0.95, 0.95, 0.95)
        )
    )


def ensure_sheet_rows(sheet, required_rows: int, buffer_rows: int = 10):
    """Убеждается, что в листе достаточно строк"""
    current_rows = len(execute_with_retry(sheet.get_all_values))

    if current_rows < required_rows + buffer_rows:
        rows_to_add = (required_rows + buffer_rows) - current_rows
        print(f"  ➕ Добавляем {rows_to_add} новых строк...")
        try:
            sheet.add_rows(rows_to_add)
            print(f"  ✅ Добавлено {rows_to_add} строк")
            time.sleep(1)
        except Exception as e:
            print(f"  ⚠️ Ошибка при добавлении строк: {e}")


# ================= ФУНКЦИИ ДЛЯ РАБОТЫ С DASHBOARD =================

def extract_campaign_expenses(offer_campaigns: List) -> Tuple[float, float, float, float]:
    """Извлекает расходы и продажи из кампаний товара"""
    expenses_search = 0.0
    selled_search = 0.0
    expenses_cpo = 0.0
    selled_cpo = 0.0

    for camp in offer_campaigns:
        camping_type = camp.get('camping_type', '')
        selled_clean = clean_numeric_value(camp.get('selled', 0))
        expense_clean = clean_numeric_value(camp.get('expense', 0))

        if camping_type in ['Поиск', 'Поиск и рекомендации']:
            expenses_search += expense_clean
            selled_search += selled_clean
        elif camping_type == 'Оплата за заказ':
            expense_model_clean = clean_numeric_value(camp.get('expense_model', 0))
            expenses_cpo += expense_clean + expense_model_clean
            selled_cpo += selled_clean

    return expenses_search, selled_search, expenses_cpo, selled_cpo


def extract_drr_data(drr_all_dict: Optional[Dict], offer_id: str) -> Tuple[float, float]:
    """Извлекает ДРР и расходы из drr_all_dict"""
    drr_value = 0.0
    money_spent = 0.0

    if drr_all_dict and offer_id in drr_all_dict:
        drr_data = drr_all_dict[offer_id]

        if isinstance(drr_data, dict):
            drr_value = clean_numeric_value(drr_data.get('drr', 0))
            money_spent = clean_numeric_value(drr_data.get('money_spent', 0))
        else:
            drr_value = clean_numeric_value(drr_data)

    return drr_value, money_spent


def calculate_drr(expenses: float, revenue: float) -> float:
    """Рассчитывает ДРР в процентах"""
    if revenue > 0:
        return round((expenses / revenue) * 100, 2)
    return 0.0


def log_dashboard_item(offer_id: str, revenue: float, expenses_search: float,
                       selled_search: float, drr_from_dict: float,
                       money_spent: float, drr_search: float,
                       drr_cpo: float, drr_total: float):
    """Логирует данные по товару для DASHBOARD"""
    print(f"\n  📊 {offer_id}:")
    print(f"     Сумма продаж за день: {revenue:,.2f} руб.")
    print(f"     Продажи по поиску+рекомендациям: {selled_search:,.2f} руб., "
          f"расходы: {expenses_search:,.2f} руб.")
    print(f"     ДРР из словаря: {drr_from_dict}%")
    print(f"     Расходы из словаря: {money_spent:,.2f} руб.")
    print(f"     ДРР поиск: {drr_search}%")
    print(f"     ДРР оплата за заказ: {drr_cpo}%")
    print(f"     ДРР общий: {drr_total}%")


def log_dashboard_totals(totals: Dict, total_drr_search: float, total_drr_total: float):
    """Логирует итоговые данные DASHBOARD"""
    print(f"\n  {'=' * 50}")
    print(f"  📊 ИТОГО по всем товарам:")
    print(f"     Общая выручка: {totals['total_revenue_all']:,.2f} руб.")
    print(f"     Общие расходы из drr_all_dict: "
          f"{totals['total_money_spent_from_dict']:,.2f} руб.")
    print(f"     Общие заказы: {totals['total_orders']}")
    print(f"     Продажи по поиску+рекомендациям: "
          f"{totals['total_selled_search']:,.2f} руб., "
          f"расходы: {totals['total_expenses_search']:,.2f} руб.")
    print(f"     Итоговый ДРР (поиск): {total_drr_search}%")
    print(f"     Итоговый ДРР (общий): {total_drr_total}%")
    print(f"  {'=' * 50}\n")


def prepare_dashboard_data(all_items_dict: Dict, campaigns_data: Dict,
                           drr_all_dict: Dict) -> Tuple[List[List], Dict]:
    """Подготавливает данные для листа DASHBOARD"""
    dashboard_rows = []
    totals = {
        'total_orders': 0,
        'total_expenses_search': 0,
        'total_selled_search': 0,
        'total_revenue_all': 0,
        'total_money_spent_from_dict': 0
    }

    for item in all_items_dict.values():
        offer_id = item.get("offer_id")
        total_revenue_item = clean_numeric_value(item.get("total_revenue", 0))
        total_ordered_units = clean_int_value(item.get("total_ordered_units", 0))

        offer_campaigns = campaigns_data.get(offer_id, []) if campaigns_data else []

        expenses_search, selled_search, _, _ = extract_campaign_expenses(offer_campaigns)
        drr_from_dict, money_spent_from_dict = extract_drr_data(drr_all_dict, offer_id)

        drr_search = calculate_drr(expenses_search, selled_search)
        drr_cpo = drr_from_dict
        drr_total = calculate_drr(money_spent_from_dict, total_revenue_item)

        log_dashboard_item(
            offer_id, total_revenue_item, expenses_search, selled_search,
            drr_from_dict, money_spent_from_dict, drr_search, drr_cpo, drr_total
        )

        dashboard_rows.append([
            offer_id,
            total_revenue_item,
            total_ordered_units,
            drr_search,
            drr_cpo,
            drr_total
        ])

        totals['total_orders'] += total_ordered_units
        totals['total_expenses_search'] += expenses_search
        totals['total_selled_search'] += selled_search
        totals['total_revenue_all'] += total_revenue_item
        totals['total_money_spent_from_dict'] += money_spent_from_dict

    # Сортируем по количеству продаж
    dashboard_rows.sort(key=lambda x: x[2], reverse=True)

    return dashboard_rows, totals


def update_dashboard_sheet(dashboard, dashboard_data: List[List]):
    """Обновляет данные в листе DASHBOARD одной операцией"""
    # Очищаем старые данные
    current_data = execute_with_retry(dashboard.get_all_values)
    current_total_rows = len(current_data)

    if current_total_rows > 1:
        clear_old_dashboard_data(dashboard, current_total_rows)

    # Добавляем итоговые строки
    if dashboard_data:
        # Рассчитываем итоги
        total_revenue = sum(row[1] for row in dashboard_data)
        total_orders = sum(row[2] for row in dashboard_data)

        # Пустая строка-разделитель
        dashboard_data.append([""] * len(DASHBOARD_CONFIG['headers']))

        # Итоговая строка
        dashboard_data.append(["ИТОГО", total_revenue, total_orders, 0, 0, 0])

    # Проверяем и добавляем строки при необходимости
    rows_needed = len(dashboard_data) + 1
    ensure_sheet_rows(dashboard, rows_needed)

    # Обновляем все данные одной операцией
    print(f"  📝 Вставка {len(dashboard_data)} строк данных одной операцией...")
    execute_with_retry(dashboard.update, "A2", dashboard_data)
    print(f"  ✅ Вставлено {len(dashboard_data)} строк данных")
    time.sleep(1)

    # Форматируем итоговую строку
    if dashboard_data:
        last_row = len(dashboard_data) + 1
        format_totals_row(dashboard, last_row, len(DASHBOARD_CONFIG['headers']))

        # Форматируем колонку с артикулами
        execute_with_retry(
            format_cell_range, dashboard, f"A2:A{last_row}",
            CellFormat(textFormat=TextFormat(bold=True))
        )


# ================= ФУНКЦИИ ДЛЯ РАБОТЫ С ЛИСТАМИ ТОВАРОВ =================

def setup_product_sheet_structure(sheet, offer_id: str, skus_list: List[str]):
    """Настраивает структуру листа товара одной операцией"""
    print(f"  🆕 Создание нового листа {offer_id}...")

    # Подготавливаем все данные для обновления
    updates = []

    # Базовая информация
    updates.append(("A1", [["Артикул", offer_id]]))
    updates.append(("A2", [["SKU", ", ".join(skus_list)]]))
    updates.append(("A4", [[""]]))

    # Заголовки блоков и колонок
    # Аналитика
    col_letter = ANALYTICS_CONFIG['start_column']
    updates.append((f"{col_letter}5", [[ANALYTICS_CONFIG['block_title']]]))

    headers_list = [h['name'] for h in ANALYTICS_CONFIG['headers']]
    end_col = get_column_letter(
        get_column_index(ANALYTICS_CONFIG['start_column']) + len(headers_list) - 1
    )
    updates.append((f"{ANALYTICS_CONFIG['start_column']}6:{end_col}6", [headers_list]))

    # Рекламные блоки
    for block_config in CAMPAIGN_CONFIGS.values():
        col_letter = block_config['start_column']
        updates.append((f"{col_letter}5", [[block_config['title']]]))

        headers_list = [h['name'] for h in block_config['headers']]
        end_col = get_column_letter(
            get_column_index(block_config['start_column']) + len(headers_list) - 1
        )
        updates.append((f"{block_config['start_column']}6:{end_col}6", [headers_list]))

    # Выполняем все обновления
    for range_name, values in updates:
        execute_with_exponential_backoff(sheet.update, range_name, values)
        time.sleep(0.3)

    # Применяем форматирование
    # Аналитика
    execute_with_exponential_backoff(
        format_cell_range, sheet, f"{ANALYTICS_CONFIG['start_column']}5",
        CellFormat(
            textFormat=TextFormat(bold=True, fontSize=12),
            backgroundColor=ANALYTICS_CONFIG['block_color']
        )
    )

    headers_range = f"{ANALYTICS_CONFIG['start_column']}6:{end_col}6"
    execute_with_exponential_backoff(
        format_cell_range, sheet, headers_range,
        CellFormat(
            textFormat=TextFormat(bold=True),
            backgroundColor=ANALYTICS_CONFIG['block_color']
        )
    )

    # Рекламные блоки
    for block_config in CAMPAIGN_CONFIGS.values():
        col_letter = block_config['start_column']
        execute_with_exponential_backoff(
            format_cell_range, sheet, f"{col_letter}5",
            CellFormat(
                textFormat=TextFormat(bold=True, fontSize=12),
                backgroundColor=block_config['color']
            )
        )

        headers_list = [h['name'] for h in block_config['headers']]
        end_col = get_column_letter(
            get_column_index(block_config['start_column']) + len(headers_list) - 1
        )
        headers_range = f"{block_config['start_column']}6:{end_col}6"
        execute_with_exponential_backoff(
            format_cell_range, sheet, headers_range,
            CellFormat(
                textFormat=TextFormat(bold=True),
                backgroundColor=block_config['color']
            )
        )

    # Замораживаем строки
    execute_with_exponential_backoff(set_frozen, sheet, rows=6)
    time.sleep(1)


def format_single_search_campaign(campaign: Dict) -> List:
    """Форматирует одну кампанию поиска или рекомендаций"""
    return [
        campaign.get('strategy', ''),
        str(campaign.get('concurent_bet', '')),
        str(campaign.get('my_bet', '')),
        round(clean_numeric_value(campaign.get('sr_click', 0)), 2),
        clean_int_value(campaign.get('orders', campaign.get('offers', 0))),
        clean_int_value(campaign.get('to_cart', 0)),
        round(clean_numeric_value(campaign.get('drr', 0)), 2),
        round(clean_numeric_value(campaign.get('ctp', 0)), 2),
        clean_int_value(campaign.get('views', 0)),
        clean_int_value(campaign.get('clicks', 0)),
        round(clean_numeric_value(campaign.get('camping_budget', 0)), 2),
        round(clean_numeric_value(campaign.get('product_price', 0)), 2),
        round(clean_numeric_value(campaign.get('expense', 0)), 2)
    ]


def format_single_cpo_campaign(campaign: Dict) -> List:
    """Форматирует одну кампанию с оплатой за заказ"""
    bet_amount = clean_numeric_value(campaign.get('bet_amount', 0))
    bet_percent = campaign.get('bet_percent', '')
    bet_display = ""
    if bet_percent and bet_amount:
        bet_display = f"{round(bet_amount, 2)} [{bet_percent}%]"

    drr_value = campaign.get('drr', '—')
    expense_value = campaign.get('expense', '—')
    expense_model_value = campaign.get('expense_model', '—')

    return [
        bet_display,
        round(clean_numeric_value(campaign.get('product_price', 0)), 2),
        str(campaign.get('index_view', '')),
        clean_int_value(campaign.get('product_buy_pay', 0)),
        clean_int_value(campaign.get('product_buy_combo_model', 0)),
        round(clean_numeric_value(drr_value), 2) if drr_value != '—' else '—',
        round(clean_numeric_value(expense_value), 2) if expense_value != '—' else '—',
        round(clean_numeric_value(expense_model_value), 2) if expense_model_value != '—' else '—'
    ]


def format_campaign_data(campaigns: List, campaign_type: str) -> List:
    """Форматирует данные рекламных кампаний"""
    if not campaigns:
        num_fields = len(CAMPAIGN_CONFIGS[campaign_type]['headers'])
        return [""] * num_fields

    if len(campaigns) == 1:
        if campaign_type == 'cpo':
            return format_single_cpo_campaign(campaigns[0])
        else:
            return format_single_search_campaign(campaigns[0])

    # Для нескольких кампаний объединяем данные по полям
    result = []
    num_fields = len(CAMPAIGN_CONFIGS[campaign_type]['headers'])

    for field_idx in range(num_fields):
        values = []
        for camp in campaigns:
            if campaign_type == 'cpo':
                formatted = format_single_cpo_campaign(camp)
            else:
                formatted = format_single_search_campaign(camp)

            val = formatted[field_idx] if field_idx < len(formatted) else ""
            if val is not None and str(val) != "" and str(val) != "0":
                values.append(str(val))

        result.append(", ".join(values) if values else "")

    return result


def update_position_data(item: Dict, positions_data: Optional[Dict]) -> float:
    """Обновляет позицию товара из данных positions_data"""
    skus_list = item.get("skus", [])
    position_value = None

    if positions_data:
        for sku in skus_list:
            sku_str = str(sku)
            raw_position = positions_data.get(sku_str) or positions_data.get(sku)
            if raw_position is not None and str(raw_position) != '-' and str(raw_position) != '':
                try:
                    cleaned = str(raw_position).replace(',', '.').strip()
                    position_value = float(cleaned)
                    print(f"  ✅ Обновлена позиция для {item.get('offer_id')} "
                          f"(SKU {sku}): {position_value}")
                    break
                except (ValueError, TypeError) as e:
                    print(f"  ⚠️ Не удалось преобразовать позицию: '{raw_position}' - {e}")
                    continue

    return position_value if position_value is not None else clean_numeric_value(
        item.get("avg_position_category", 0)
    )


def prepare_product_row(item: Dict, campaigns_data: Dict, current_date_str: str) -> List:
    """Подготавливает строку данных для листа товара"""
    offer_id = item.get("offer_id")

    # Данные аналитики
    analytics_row = [
        current_date_str,
        clean_numeric_value(item.get("total_revenue", 0)),
        clean_int_value(item.get("total_ordered_units", 0)),
        round(clean_numeric_value(item.get("avg_position_category", 0)), 0),
        clean_int_value(item.get("total_hits_view", 0)),
        clean_int_value(item.get("total_hits_view_pdp", 0)),
        clean_numeric_value(item.get("avg_conversion_search_to_pdp", 0)),
        clean_numeric_value(item.get("avg_conv_tocart_search", 0)),
        clean_numeric_value(item.get("avg_conv_tocart", 0))
    ]

    # Данные рекламных кампаний
    offer_campaigns = campaigns_data.get(offer_id, []) if campaigns_data else []

    search_campaigns = [
        camp for camp in offer_campaigns
        if camp.get('camping_type') == 'Поиск'
    ]
    rec_campaigns = [
        camp for camp in offer_campaigns
        if camp.get('camping_type') == 'Поиск и рекомендации'
    ]
    cpo_campaigns = [
        camp for camp in offer_campaigns
        if camp.get('camping_type') == 'Оплата за заказ'
    ]

    search_data = format_campaign_data(search_campaigns, 'search')
    rec_data = format_campaign_data(rec_campaigns, 'recommendations')
    cpo_data = format_campaign_data(cpo_campaigns, 'cpo')

    # Собираем полную строку с разделителями
    full_row = analytics_row + [""] + search_data + [""] + rec_data + [""] + cpo_data

    return full_row


def update_product_sheet_batch(sheet, offer_id: str, full_row: List, current_date_str: str):
    """Обновляет данные в листе товара одной операцией"""
    # Получаем все существующие данные
    all_data = execute_with_retry(sheet.get_all_values)

    # Находим индекс строки с текущей датой
    existing_row_index = None
    for i, row in enumerate(all_data[6:], start=7):
        if len(row) > 0 and row[0] == current_date_str:
            existing_row_index = i
            break

    # Подготавливаем данные для обновления
    if existing_row_index:
        # Обновляем существующую строку
        range_label = f"A{existing_row_index}"
        print(f"  🔄 Обновление строки {existing_row_index}")
        execute_with_retry(
            sheet.update, range_label, [full_row],
            value_input_option='USER_ENTERED'
        )
        print(f"  ✅ Обновлена строка за {current_date_str}")
    else:
        # Вставляем новую строку
        print(f"  📝 Добавление новой строки за {current_date_str}")
        execute_with_exponential_backoff(sheet.insert_row, full_row, index=7)
        print(f"  ✅ Добавлена строка за {current_date_str}")

    time.sleep(1)

    # Контроль размера листа
    enforce_sheet_size_limit(sheet, max_rows=500)


def enforce_sheet_size_limit(sheet, max_rows: int = 500):
    """Ограничивает количество строк в листе"""
    current_rows = len(execute_with_exponential_backoff(sheet.get_all_values))

    if current_rows > max_rows:
        rows_to_delete = current_rows - max_rows
        try:
            execute_with_exponential_backoff(sheet.delete_rows, 7, rows_to_delete)
            print(f"  ✅ Удалены старые строки, удалено {rows_to_delete} строк, "
                  f"осталось {max_rows}")
            time.sleep(1)
        except Exception as e:
            print(f"  ⚠️ Не удалось удалить строки: {e}")
    else:
        print(f"  ℹ️ В листе {current_rows} строк, лимит не превышен")


# ================= ФУНКЦИИ ДЛЯ ОБРАБОТКИ ОШИБОК =================

def write_error_to_sheet(error_message: str, sheet_name: str = "ERROR"):
    """Записывает ошибку в лист"""
    try:
        client = get_google_sheets_client()
        spreadsheet = client.open_by_key(spread_id)

        try:
            sheet = spreadsheet.worksheet(sheet_name)
            sheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=5)

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        full_error = f"[{timestamp}] {error_message}"
        sheet.update("A1", full_error)

        print(f"✅ Ошибка записана в лист {sheet_name}")
    except Exception as e:
        print(f"❌ Не удалось записать ошибку: {e}")


def write_parser_error_to_sheet(error_message: str):
    """Записывает ошибки парсера в лист ERROR_PARS"""
    write_error_to_sheet(error_message, "ERROR_PARS")


# ================= ОСНОВНАЯ ФУНКЦИЯ =================

def upload_to_google_sheets(all_items_dict: Dict, campaigns_data: Optional[Dict] = None,
                            positions_data: Optional[Dict] = None,
                            drr_all_dict: Optional[Dict] = None):
    """
    Основная функция загрузки данных в Google Sheets
    """
    print("\n" + "=" * 60)
    print("🚀 НАЧАЛО ЗАГРУЗКИ ДАННЫХ В GOOGLE SHEETS")
    print("=" * 60)

    try:
        # Подключение к Google Sheets
        print("\n🔌 Подключение к Google Sheets...")
        client = get_google_sheets_client()
        spreadsheet = client.open_by_key(spread_id)
        current_date_str = get_current_date_moscow()
        print(f"📅 Текущая дата: {current_date_str}")

        # ================= ОБРАБОТКА DASHBOARD =================
        print("\n" + "=" * 60)
        print("📊 ОБРАБОТКА ЛИСТА DASHBOARD")
        print("=" * 60)

        dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")

        # Настройка структуры DASHBOARD
        current_data = execute_with_retry(dashboard.get_all_values)
        expected_headers = [h['name'] for h in DASHBOARD_CONFIG['headers']]

        if len(current_data) == 0 or (len(current_data) > 0 and current_data[0] != expected_headers):
            setup_sheet_headers(dashboard, DASHBOARD_CONFIG, start_row=1)
        else:
            clear_old_dashboard_data(dashboard, len(current_data))

        # Подготовка и обновление данных DASHBOARD
        dashboard_data, _ = prepare_dashboard_data(all_items_dict, campaigns_data, drr_all_dict)
        update_dashboard_sheet(dashboard, dashboard_data)
        print("✅ DASHBOARD успешно обновлен")

        # ================= НОВЫЙ БЛОК: ТЕХНИЧЕСКИЙ ЛИСТ (ОБНОВЛЕННЫЙ) =================
        print("\n" + "=" * 60)
        print("🔧 ОБРАБОТКА ТЕХНИЧЕСКОГО ЛИСТА")
        print("=" * 60)

        # Настраиваем Технический лист с обновленной структурой
        tech_sheet, products_start_row = setup_technical_sheet(spreadsheet)
        # Обновляем данные в Техническом листе
        update_technical_sheet(tech_sheet, all_items_dict, products_start_row)

        print("✅ ТЕХНИЧЕСКИЙ ЛИСТ успешно обновлен")

        # ================= ОБРАБОТКА ЛИСТОВ ТОВАРОВ =================
        print("\n" + "=" * 60)
        print("📄 ОБРАБОТКА ЛИСТОВ ТОВАРОВ")
        print("=" * 60)

        for idx, item in enumerate(all_items_dict.values()):
            offer_id = item.get("offer_id")
            skus_list = item.get("skus", [])

            # Обновляем цену до скидки в item, если есть
            if 'price_before' not in item:
                item['price_before'] = 0

            print(f"\n🔄 Обработка товара {idx + 1}/{len(all_items_dict)}: {offer_id}")
            print(f"   SKU: {', '.join(skus_list)}")

            # Обновляем позицию товара
            position_category = update_position_data(item, positions_data)
            item['avg_position_category'] = position_category

            # Получаем или создаем лист товара
            try:
                sheet = execute_with_exponential_backoff(spreadsheet.worksheet, offer_id)
                need_setup = False
            except gspread.exceptions.WorksheetNotFound:
                sheet = execute_with_exponential_backoff(
                    spreadsheet.add_worksheet, title=offer_id, rows=2000, cols=60
                )
                need_setup = True
                time.sleep(2)

            # Настраиваем структуру листа если нужно
            if need_setup:
                setup_product_sheet_structure(sheet, offer_id, skus_list)

            # Подготавливаем и обновляем данные
            full_row = prepare_product_row(item, campaigns_data, current_date_str)
            update_product_sheet_batch(sheet, offer_id, full_row, current_date_str)

            # Пауза для соблюдения лимитов
            if (idx + 1) % 5 == 0:
                print(f"\n⏸️ Обработано {idx + 1} товаров, пауза 5 секунд...")
                time.sleep(5)

        print("\n" + "=" * 60)
        print("✅ ВСЕ ДАННЫЕ УСПЕШНО ЗАГРУЖЕНЫ")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        write_error_to_sheet(str(e))
        raise


# ================= ТЕСТОВАЯ ФУНКЦИЯ =================

def test():
    """Тестовая функция для проверки загрузки"""
    with open('all_items_dict.json', 'r', encoding='utf-8') as f:
        all_dict = json.load(f)
    with open('advert_analytic.json', 'r', encoding='utf-8') as f:
        s_dict = json.load(f)
    with open('position_analytic.json', 'r', encoding='utf-8') as f:
        l_dict = json.load(f)

    # Пример нового формата drr_all_dict
    drr_all_dict = {
        'Наручники_МЕХ': {'drr': 15.5, 'money_spent': 561.49},
        'Наручники_Gold': {'drr': 12.3, 'money_spent': 650.17},
    }

    upload_to_google_sheets(all_dict, s_dict, l_dict, drr_all_dict)


if __name__ == "__main__":
    test()