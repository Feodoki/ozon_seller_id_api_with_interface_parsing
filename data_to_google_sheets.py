# data_to_google_sheets.py - ВЕРСИЯ С СОКРАЩЕННЫМИ КОЛОНКАМИ

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

# ================= КОНФИГУРАЦИЯ ТАБЛИЦЫ ЛОГИСТИКИ =================

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

# ================= КОНФИГУРАЦИЯ ТЕХНИЧЕСКОГО ЛИСТА =================

TECHNICAL_SHEET_CONFIG = {
    "sheet_name": "ТЕХНИЧЕСКИЙ ЛИСТ",
    "settings_headers": [
        {"name": "Параметр", "width": 250},
        {"name": "Значение", "width": 150},
        {"name": "Единицы", "width": 100}
    ],
    # СОКРАЩЕННЫЕ ЗАГОЛОВКИ ТОВАРОВ (только нужные колонки)
    "products_headers": [
        {"name": "Артикул", "width": 180},
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

# Глобальная переменная для хранения настроек
technical_settings = {
    'tax_rate': 6.0,
    'acquiring_rate': 1.0,
    'logistics_prices': {}
}

# Константы расположения блоков (с отступами)
SETTINGS_START_ROW = 1  # Настройки с 1 строки
TOTALS_START_ROW = 6  # Итоговые показатели с 6 строки
LOGISTICS_START_ROW = 12  # Таблица логистики с 12 строки (после итогов + 1 пустая)
PRODUCTS_START_ROW = 22  # Товары с 22 строки (после логистики + 2 пустые)


def get_column_index(col_letter: str) -> int:
    """Преобразует букву колонки в номер (A->1, AA->27)"""
    result = 0
    for char in col_letter:
        result = result * 26 + (ord(char.upper()) - ord('A') + 1)
    return result


def load_logistics_prices(tech_sheet) -> Dict:
    """Загружает стоимость логистики из Технического листа"""
    logistics_prices = {}
    start_row = LOGISTICS_START_ROW + 2

    try:
        all_values = execute_with_retry(tech_sheet.get_all_values)
        for row in all_values[start_row - 1:]:
            if len(row) >= 2 and row[0] and row[0].strip():
                try:
                    volume = float(row[0].replace(',', '.'))
                    price = float(row[1].replace(',', '.'))
                    logistics_prices[volume] = price
                except (ValueError, TypeError):
                    continue

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


def setup_logistics_table(tech_sheet):
    """Настраивает таблицу стоимости логистики в Техническом листе"""
    print("  📦 Настройка таблицы стоимости логистики...")

    start_row = LOGISTICS_START_ROW

    # Заголовок таблицы логистики
    execute_with_exponential_backoff(
        tech_sheet.update,
        f"A{start_row}",
        [["📦 ТАБЛИЦА СТОИМОСТИ ЛОГИСТИКИ (по литражу)"]]
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
    Настраивает Технический лист.
    СТРУКТУРА С ОТСТУПАМИ:
    - Строки 1-5:   ⚙️ НАСТРОЙКИ КАЛЬКУЛЯТОРА
    - Строка 6:     (пустая строка-разделитель)
    - Строки 7-11:  📊 ИТОГОВЫЕ ПОКАЗАТЕЛИ
    - Строка 12:    (пустая строка-разделитель)
    - Строки 13-21: 📦 ТАБЛИЦА СТОИМОСТИ ЛОГИСТИКИ
    - Строка 22:    (пустая строка-разделитель)
    - Строки 23+:   📊 ТОВАРЫ В ПРОДАЖЕ + данные товаров
    """
    print("\n📋 НАСТРОЙКА ТЕХНИЧЕСКОГО ЛИСТА")

    tech_sheet = get_or_create_sheet(spreadsheet, TECHNICAL_SHEET_CONFIG["sheet_name"], rows=5000, cols=50)
    execute_with_exponential_backoff(tech_sheet.clear)
    time.sleep(1)

    # ================= БЛОК НАСТРОЕК (строки 1-5) =================
    print("  ⚙️ Настройка блока параметров...")

    execute_with_exponential_backoff(tech_sheet.update, "A1", [["⚙️ НАСТРОЙКИ КАЛЬКУЛЯТОРА"]])
    execute_with_exponential_backoff(
        format_cell_range, tech_sheet, "A1:C1",
        CellFormat(
            textFormat=TextFormat(bold=True, fontSize=12),
            backgroundColor=Color(0.8, 0.9, 1)
        )
    )

    settings_headers = [h['name'] for h in TECHNICAL_SHEET_CONFIG["settings_headers"]]
    execute_with_exponential_backoff(tech_sheet.update, "A2", [settings_headers])

    end_col = get_column_letter(len(settings_headers))
    execute_with_exponential_backoff(
        format_cell_range, tech_sheet, f"A2:{end_col}2",
        CellFormat(
            textFormat=TextFormat(bold=True),
            backgroundColor=Color(0.85, 0.95, 0.85)
        )
    )

    settings_data = [
        ["Ставка УСН + НДС (%)", technical_settings['tax_rate'], "%"],
        ["Эквайринг (%)", technical_settings['acquiring_rate'], "%"],
    ]

    execute_with_exponential_backoff(tech_sheet.update, "A3", settings_data)

    for idx, header in enumerate(TECHNICAL_SHEET_CONFIG["settings_headers"], start=1):
        col_letter = get_column_letter(idx)
        if 'width' in header:
            execute_with_exponential_backoff(set_column_width, tech_sheet, col_letter, header['width'])

    time.sleep(1)

    # ================= ПУСТАЯ СТРОКА-РАЗДЕЛИТЕЛЬ (строка 6) =================
    # execute_with_exponential_backoff(tech_sheet.update, "A6", [[""]])
    #
    # print("  📊 Настройка блока итоговых показателей...")
    #
    # execute_with_exponential_backoff(tech_sheet.update, f"A{TOTALS_START_ROW + 1}", [["📊 ИТОГОВЫЕ ПОКАЗАТЕЛИ"]])
    # execute_with_exponential_backoff(
    #     format_cell_range, tech_sheet, f"A{TOTALS_START_ROW + 1}:C{TOTALS_START_ROW + 1}",
    #     CellFormat(
    #         textFormat=TextFormat(bold=True, fontSize=11),
    #         backgroundColor=Color(0.9, 0.95, 1)
    #     )
    # )
    #
    # totals_data = [
    #     ["Средняя рентабельность (%)", "=IFERROR(AVERAGE(I24:I1000), 0)", "%"],
    #     ["Общая прибыль (₽)", "=СУММ(H24:H1000)", "₽"],
    #     ["Средняя комиссия FBO (%)", "=IFERROR(AVERAGE(G24:G1000), 0)", "%"],
    #     ["Общие расходы на логистику (₽)", "=IFERROR(SUM(I24:I1000), 0)", "₽"],
    #     ["Средняя себестоимость (₽)", "=IFERROR(AVERAGE(C24:C1000), 0)", "₽"]
    # ]
    #
    # execute_with_exponential_backoff(
    #     tech_sheet.update,
    #     f"A{TOTALS_START_ROW + 2}",
    #     totals_data,
    #     value_input_option='USER_ENTERED'
    # )
    #
    # for i in range(len(totals_data)):
    #     execute_with_exponential_backoff(
    #         format_cell_range, tech_sheet, f"A{TOTALS_START_ROW + 2 + i}:C{TOTALS_START_ROW + 2 + i}",
    #         CellFormat(textFormat=TextFormat(bold=True))
    #     )

    time.sleep(1)

    # ================= ПУСТАЯ СТРОКА-РАЗДЕЛИТЕЛЬ (строка 12) =================
    execute_with_exponential_backoff(tech_sheet.update, "A12", [[""]])

    # ================= ТАБЛИЦА ЛОГИСТИКИ (строки 13-21) =================
    global LOGISTICS_START_ROW
    LOGISTICS_START_ROW = 13

    setup_logistics_table(tech_sheet)
    time.sleep(1)

    # ================= ПУСТАЯ СТРОКА-РАЗДЕЛИТЕЛЬ (строка 22) =================
    execute_with_exponential_backoff(tech_sheet.update, "A22", [[""]])

    # ================= БЛОК ТОВАРОВ (с строки 23) =================
    print("  📦 Настройка блока товаров...")

    execute_with_exponential_backoff(tech_sheet.update, f"A{PRODUCTS_START_ROW + 1}", [["📊 ТОВАРЫ В ПРОДАЖЕ"]])
    execute_with_exponential_backoff(
        format_cell_range, tech_sheet, f"A{PRODUCTS_START_ROW + 1}:I{PRODUCTS_START_ROW + 1}",
        CellFormat(
            textFormat=TextFormat(bold=True, fontSize=12),
            backgroundColor=Color(0.8, 0.9, 1)
        )
    )

    # СОКРАЩЕННЫЕ ЗАГОЛОВКИ (9 колонок вместо 13)
    products_headers = [h['name'] for h in TECHNICAL_SHEET_CONFIG["products_headers"]]
    execute_with_exponential_backoff(tech_sheet.update, f"A{PRODUCTS_START_ROW + 2}", [products_headers])

    end_col = get_column_letter(len(products_headers))
    execute_with_exponential_backoff(
        format_cell_range, tech_sheet, f"A{PRODUCTS_START_ROW + 2}:{end_col}{PRODUCTS_START_ROW + 2}",
        CellFormat(
            textFormat=TextFormat(bold=True),
            backgroundColor=Color(0.85, 0.95, 0.85)
        )
    )

    for idx, header in enumerate(TECHNICAL_SHEET_CONFIG["products_headers"], start=1):
        col_letter = get_column_letter(idx)
        if 'width' in header:
            execute_with_exponential_backoff(set_column_width, tech_sheet, col_letter, header['width'])

    execute_with_exponential_backoff(set_frozen, tech_sheet, rows=PRODUCTS_START_ROW + 2)

    # Добавляем формулу для Эквайринга в первую строку товаров
    first_data_row = PRODUCTS_START_ROW + 3
    formula_acquiring = f"=C{first_data_row} * (B$4/100)"
    execute_with_exponential_backoff(
        tech_sheet.update,
        f"E{first_data_row}",
        [[formula_acquiring]],
        value_input_option='USER_ENTERED'
    )
    print(f"  📝 Пример формулы эквайринга в ячейке E{first_data_row}: {formula_acquiring}")

    print("  ✅ Технический лист настроен")
    return tech_sheet, PRODUCTS_START_ROW + 3


def clear_old_products_data(tech_sheet, products_start_row: int):
    """
    Удаляет ТОЛЬКО старые данные товаров.
    Начинает очистку с products_start_row (строка с данными, а не с заголовками)
    """
    print("  🗑️ Удаление старых данных о товарах...")

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


def update_technical_sheet(tech_sheet, advert_analytic_dict: Dict, products_start_row: int):
    """
    Обновляет данные в Техническом листе из advert_analytic.json
    ТОЛЬКО С НУЖНЫМИ КОЛОНКАМИ
    """
    print("\n📊 ОБНОВЛЕНИЕ ТЕХНИЧЕСКОГО ЛИСТА")

    # Очищаем только старые данные товаров
    clear_old_products_data(tech_sheet, products_start_row)

    # Загружаем стоимость логистики
    logistics_prices = load_logistics_prices(tech_sheet)

    # Читаем настройки из листа
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

    products_data = []

    print(f"  📝 Обработка {len(advert_analytic_dict)} товаров...")

    row_index = products_start_row
    for offer_id, campaigns_list in advert_analytic_dict.items():
        if not campaigns_list or not isinstance(campaigns_list, list):
            continue

        # Берем ПЕРВЫЙ объект из списка кампаний
        first_campaign = campaigns_list[0]

        # Извлекаем данные
        sku = str(first_campaign.get("sku", "—"))
        price_before = clean_numeric_value(first_campaign.get("product_price_before", 0))
        stock_balance = clean_int_value(first_campaign.get("stock_balance", 0))
        commission_fbo_percent = clean_numeric_value(first_campaign.get("commission_fbo", 0))
        current_price = clean_numeric_value(first_campaign.get("product_price", 0))
        product_volume = 1.0  # ПОСТАВЛЯЕМ 1 (можно изменить на другое значение по умолчанию)

        # Получаем стоимость логистики по объему (пока volume = 1)
        logistics_cost = get_logistics_price(product_volume, logistics_prices)

        # Формула для эквайринга: =Цена_до_скидки * (Ставка_эквайринга/100)
        # C{row} - колонка Цена до скидки, B$4 - ставка эквайринга
        acquiring_formula = f"=C{row_index} * (B$4/100)"

        # СТРОКА ТОВАРА С СОКРАЩЕННЫМИ ДАННЫМИ (9 колонок)
        products_data.append([
            offer_id,  # Артикул
            sku,  # SKU
            price_before,  # Цена до скидки (₽)
            current_price,  # Цена для покупателя (₽)
            acquiring_formula,  # Эквайринг (₽) - ФОРМУЛА
            stock_balance,  # Остатки (шт)
            commission_fbo_percent,  # Комиссия FBO (%)
            product_volume,  # Объем товара (л) - пока 1
            logistics_cost  # Стоимость логистики (₽) - из таблицы
        ])

        row_index += 1

        # Отладка для первого товара
        if len(products_data) == 1:
            print(f"  📦 Пример товара: {offer_id}")
            print(f"     - SKU: {sku}")
            print(f"     - Цена до скидки: {price_before}")
            print(f"     - Объем: {product_volume} л")
            print(f"     - Стоимость логистики: {logistics_cost} ₽")
            print(f"     - Формула эквайринга: {acquiring_formula}")

    if products_data:
        print(f"  📝 Добавление данных для {len(products_data)} товаров...")

        range_to_update = f"A{products_start_row}"
        execute_with_retry(
            tech_sheet.update,
            range_to_update,
            products_data,
            value_input_option='USER_ENTERED'
        )
        print(f"  ✅ Добавлено {len(products_data)} товаров с формулами")

        last_row = products_start_row + len(products_data) - 1

        # Применяем форматирование
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

        # Колонка E (Эквайринг) - фон желтый (формула)
        execute_with_retry(
            format_cell_range, tech_sheet, f"E{products_start_row}:E{last_row}",
            CellFormat(backgroundColor=Color(1, 0.95, 0.8))
        )

        # Колонка I (Стоимость логистики) - фон голубой
        execute_with_retry(
            format_cell_range, tech_sheet, f"I{products_start_row}:I{last_row}",
            CellFormat(backgroundColor=Color(0.7, 0.85, 1))
        )

        print("  💡 Примечание: Эквайринг рассчитывается автоматически по формуле")
        print("  💡 Таблицу стоимости логистики можно редактировать выше")

    else:
        print("  ⚠️ Нет данных для загрузки!")

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
    try:
        return int(clean_numeric_value(value))
    except (ValueError, TypeError):
        return 0


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


def get_column_letter(col_num: int) -> str:
    """Преобразует номер колонки в букву (1->A, 27->AA)"""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(ord('A') + col_num % 26) + result
        col_num //= 26
    return result


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


# ================= ОСНОВНАЯ ФУНКЦИЯ =================

def update_technical_sheet_only(advert_analytic_dict: Dict):
    """
    Основная функция для обновления только Технического листа
    """
    print("\n" + "=" * 60)
    print("🚀 ЗАПУСК ОБНОВЛЕНИЯ ТЕХНИЧЕСКОГО ЛИСТА")
    print("=" * 60)

    try:
        print("\n🔌 Подключение к Google Sheets...")
        client = get_google_sheets_client()
        spreadsheet = client.open_by_key(spread_id)

        print("\n🔧 ОБРАБОТКА ТЕХНИЧЕСКОГО ЛИСТА")
        tech_sheet, products_start_row = setup_technical_sheet(spreadsheet)
        update_technical_sheet(tech_sheet, advert_analytic_dict, products_start_row)
        print("✅ ТЕХНИЧЕСКИЙ ЛИСТ успешно обновлен")

        print("\n" + "=" * 60)
        print("✅ ОБНОВЛЕНИЕ ТЕХНИЧЕСКОГО ЛИСТА ЗАВЕРШЕНО")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        write_error_to_sheet(str(e))
        raise


# ================= ТЕСТОВАЯ ФУНКЦИЯ =================

def test():
    """
    Тестовая функция для проверки загрузки данных в Google Sheets
    """
    print("\n" + "=" * 60)
    print("🧪 ЗАПУСК ТЕСТОВОЙ ФУНКЦИИ")
    print("=" * 60)

    try:
        print("\n📂 Загрузка тестовых данных...")

        with open('advert_analytic.json', 'r', encoding='utf-8') as f:
            advert_data = json.load(f)
        print(f"  ✅ Загружен advert_analytic.json: {len(advert_data)} товаров")

        update_technical_sheet_only(advert_data)

        print("\n" + "=" * 60)
        print("✅ ТЕСТОВАЯ ФУНКЦИЯ УСПЕШНО ЗАВЕРШЕНА")
        print("=" * 60)

    except FileNotFoundError as e:
        print(f"\n❌ Ошибка: Не найден файл {e}")
    except Exception as e:
        print(f"\n❌ Ошибка в тестовой функции: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test()