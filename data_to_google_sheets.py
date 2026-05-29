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
import os

# ================= НОВЫЙ МОДУЛЬ: TECHNICAL SHEET =================

# Конфигурация Технического листа
TECHNICAL_SHEET_CONFIG = {
    "sheet_name": "ТЕХНИЧЕСКИЙ ЛИСТ",
    "settings_headers": [
        {"name": "Параметр", "width": 200},
        {"name": "Значение", "width": 150},
        {"name": "Единицы", "width": 100}
    ],
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

# Глобальная переменная для хранения настроек
technical_settings = {
    'tax_rate': 6.0,
    'acquiring_rate': 1.0,
    'logistics_prices': {}
}

# ================= КОНФИГУРАЦИЯ СТРУКТУРЫ ЛИСТОВ =================

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
        {"name": "Конверсия в корзину общая", "width": 75},
        {"name": "Общий ДРР (%)", "width": 75}
    ],
    "start_column": "A",
    "block_title": "АНАЛИТИКА",
    "block_color": Color(0.9, 1, 0.9)
}

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

# ================= НОВАЯ КОНФИГУРАЦИЯ: ЛИСТ СТОИМОСТИ ЛОГИСТИКИ =================

LOGISTICS_PRICE_CONFIG = {
    "sheet_name": "Стоимость логистики",
    "headers": [
        {"name": "Объём товара (л)", "width": 150},
        {"name": "Для товаров до 300 руб.", "width": 200},
        {"name": "Для товаров свыше 300 руб.", "width": 200}
    ],
    "default_data": [
        ["0-0,200 л", 17.28, 56],
        ["0,201-0,4 л", 19.32, 63],
        ["0,401-0,6 л", 21.35, 67],
        ["0,601-0,8 л", 22.37, 67],
        ["0,801-1 л", 23.38, 67],
        ["1,001-1,25 л", 25.42, 71],
        ["1,251-1,5 л", 26.43, 74],
        ["1,501-1,75 л", 27.45, 74],
        ["1,751-2 л", 29.48, 74],
        ["2,001-3 л", 31.52, 74],
        ["3,001-4 л", 35.58, 78],
        ["4,001-5 л", 38.63, 89],
        ["5,001-6 л", 42.70, 89],
        ["6,001-7 л", 57.95, 99],
        ["7,001-8 л", 62.02, 99],
        ["8,001-9 л", 65.07, 100],
        ["9,001-10 л", 69.13, 100],
        ["10,001-11 л", 79.30, 102]
    ],
    "note": "💡 Редактируйте эту таблицу при изменении стоимости логистики Озон. Данные автоматически подтягиваются в Технический лист."
}

# ================= НОВАЯ КОНФИГУРАЦИЯ: ЛИСТ НАЦЕНКИ =================

MARKUP_CONFIG = {
    "sheet_name": "Наценка за нелокальную доставку",
    "headers": [
        {"name": "Кластер доставки", "width": 250},
        {"name": "Наценка за нелокальную продажу от вашей цены товара", "width": 350}
    ],
    "default_data": [
        ["Кластер 1", 0],
        ["Кластер 2", 0],
        ["Кластер 3", 0],
    ],
    "note": "💡 Таблица для будущего использования. Наценка будет добавляться к стоимости логистики."
}

# ================= НОВАЯ КОНФИГУРАЦИЯ: ИСТОРИЯ DASHBOARD =================

HISTORY_DASHBOARD_CONFIG = {
    "sheet_name": "История DASHBOARD",
    "headers": [
        {"name": "Дата", "width": 120},
        {"name": "Артикул товара", "width": 200},
        {"name": "Сумма продаж за день (₽)", "width": 150},
        {"name": "Количество продаж (шт)", "width": 130},
        {"name": "ДРР (поиск/поиск и рекомендации) %", "width": 200},
        {"name": "ДРР (оплата за заказ) %", "width": 180},
        {"name": "ДРР (общий) %", "width": 130}
    ],
    "note": "📊 История ежедневных снимков DASHBOARD. Данные добавляются автоматически каждый день."
}

# ================= НОВАЯ КОНФИГУРАЦИЯ: ДРР ОБЩИЙ =================

DRR_TOTAL_CONFIG = {
    "sheet_name": "История ДРР",
    "headers": [
        {"name": "Артикул", "width": 200}
    ],
    "note": "📊 Сводная таблица общего ДРР по всем товарам по дням. Данные обновляются автоматически."
}

# ================= НОВАЯ КОНФИГУРАЦИЯ: ЧП СТРАНИЦЫ =================

CHP_COMMON_CONFIG = {
    "sheet_name": "ЧП_товары_общая",
    "headers": ["Артикул", "Общая сумма ЧП", "31.04", "30.04", "29.04", "28.04"]
}

CHP_DRR_CONFIG = {
    "sheet_name": "ЧП_товары_ДРР",
    "headers": ["Артикул", "Общая сумма ЧП", "31.04", "30.04", "29.04", "28.04"]
}


def setup_drr_total_sheet(spreadsheet):
    """Настраивает лист ДРР ОБЩИЙ"""
    print("\n📊 НАСТРОЙКА ЛИСТА ДРР ОБЩИЙ")
    drr_sheet = get_or_create_sheet(spreadsheet, DRR_TOTAL_CONFIG["sheet_name"], rows=10000, cols=100)
    all_values = execute_with_retry(drr_sheet.get_all_values)

    # Проверяем, нужно ли настраивать структуру
    if len(all_values) < 2 or (len(all_values) > 0 and all_values[0][0] != "Артикул"):
        print("  🆕 Настройка структуры листа ДРР ОБЩИЙ...")
        execute_with_exponential_backoff(drr_sheet.clear)
        time.sleep(1)

        # Заголовок
        execute_with_exponential_backoff(drr_sheet.update, "A1", [[DRR_TOTAL_CONFIG["headers"][0]["name"]]])
        execute_with_exponential_backoff(
            format_cell_range, drr_sheet, "A1",
            CellFormat(textFormat=TextFormat(bold=True, fontSize=11), backgroundColor=Color(0.85, 0.95, 0.85))
        )

        # Устанавливаем ширину столбца Артикул
        execute_with_exponential_backoff(set_column_width, drr_sheet, "A", DRR_TOTAL_CONFIG["headers"][0]["width"])

        # Добавляем примечание
        execute_with_exponential_backoff(drr_sheet.update, "A2", [[DRR_TOTAL_CONFIG["note"]]])
        execute_with_exponential_backoff(
            format_cell_range, drr_sheet, "A2",
            CellFormat(textFormat=TextFormat(italic=True, fontSize=9), backgroundColor=Color(0.95, 0.95, 0.9))
        )

        execute_with_exponential_backoff(set_frozen, drr_sheet, rows=2)
        print("  ✅ Лист ДРР ОБЩИЙ настроен")
    else:
        print("  📄 Лист ДРР ОБЩИЙ уже существует")

    return drr_sheet


def update_drr_total_sheet_from_dashboard(spreadsheet, dashboard, current_date_str):
    """
    Обновляет лист ДРР ОБЩИЙ на основе ТЕКУЩИХ данных из DASHBOARD
    Данные обновляются при каждом запуске, а не в конце дня
    """
    print("\n📊 ОБНОВЛЕНИЕ ЛИСТА ДРР ОБЩИЙ (на основе текущего DASHBOARD)")

    try:
        # Получаем текущие данные из DASHBOARD
        dashboard_data = execute_with_retry(dashboard.get_all_values)

        if len(dashboard_data) <= 1:
            print("  ⚠️ Нет данных в DASHBOARD")
            return False

        # Собираем данные из DASHBOARD: {артикул: ДРР_ОБЩИЙ}
        drr_data = {}
        all_products = []

        # Пропускаем заголовки (строка 0)
        for row in dashboard_data[1:]:
            if not row or len(row) < 6:
                continue

            # Пропускаем итоговую строку и пустые строки
            if row[0] == "ИТОГО" or row[0] == "":
                continue

            product = row[0].strip()  # Артикул товара
            # ДРР общий - это столбец F (индекс 5) в DASHBOARD
            drr_total = clean_numeric_value(row[5]) if len(row) > 5 else 0.0

            if product and product != "":
                drr_data[product] = drr_total
                all_products.append(product)

        if not drr_data:
            print("  ⚠️ Нет данных для отображения")
            return False

        # Сортируем артикулы
        sorted_products = sorted(all_products)

        # Получаем или создаем лист ДРР ОБЩИЙ
        drr_sheet = setup_drr_total_sheet(spreadsheet)

        # Получаем существующие данные
        existing_data = execute_with_retry(drr_sheet.get_all_values)

        # Определяем, есть ли уже колонка с текущей датой
        date_column_index = None
        date_headers = []

        if len(existing_data) > 2:
            # Строка 3 содержит заголовки дат (начиная с столбца B)
            headers_row = existing_data[2] if len(existing_data) > 2 else []

            for idx, header in enumerate(headers_row):
                if header and header.strip() == current_date_str:
                    date_column_index = idx
                    break

            # Собираем все заголовки дат
            for idx, header in enumerate(headers_row):
                if header and header.strip():
                    date_headers.append((idx, header.strip()))

        # Если колонки с текущей датой нет - добавляем новую
        if date_column_index is None:
            # Находим место для новой колонки (по порядку дат)
            new_col_index = 1  # По умолчанию после столбца A
            current_date_obj = datetime.strptime(current_date_str, "%d.%m.%Y")

            for idx, header in date_headers:
                try:
                    header_date = datetime.strptime(header, "%d.%m.%Y")
                    if header_date < current_date_obj:
                        new_col_index = idx + 1
                    else:
                        break
                except:
                    pass

            # Добавляем новый заголовок даты
            col_letter = get_column_letter(new_col_index + 1)
            execute_with_retry(drr_sheet.update, f"{col_letter}3", [[current_date_str]])
            execute_with_retry(
                format_cell_range, drr_sheet, f"{col_letter}3",
                CellFormat(textFormat=TextFormat(bold=True, fontSize=10),
                           backgroundColor=Color(0.9, 0.95, 0.9),
                           horizontalAlignment='CENTER')
            )
            execute_with_exponential_backoff(set_column_width, drr_sheet, col_letter, 100)
            date_column_index = new_col_index

            print(f"  🆕 Добавлена новая колонка для даты: {current_date_str} (столбец {col_letter})")

            # Сдвигаем существующие данные вправо, если нужно
            if new_col_index < len(date_headers):
                for col in range(len(date_headers), new_col_index, -1):
                    old_col_letter = get_column_letter(col + 1)
                    new_col_letter = get_column_letter(col + 2)
                    # Копируем данные (упрощенно - лучше перестраивать весь лист)
                    pass

        # Обновляем значения ДРР для каждого артикула
        col_letter = get_column_letter(date_column_index + 1)
        current_row = 4

        for product in sorted_products:
            # Находим строку с этим артикулом
            product_row_index = None
            for row_idx in range(4, len(existing_data) + 1):
                if row_idx <= len(existing_data):
                    row_data = existing_data[row_idx - 1] if row_idx - 1 < len(existing_data) else []
                    if row_data and row_data[0] == product:
                        product_row_index = row_idx
                        break

            if product_row_index:
                # Обновляем существующую строку
                execute_with_retry(drr_sheet.update, f"{col_letter}{product_row_index}",
                                   [[round(drr_data[product], 2) if drr_data[product] != 0 else ""]])
            else:
                # Добавляем новую строку
                row_data = [product]
                # Заполняем пустыми значениями до нужной колонки
                while len(row_data) < date_column_index:
                    row_data.append("")
                row_data.append(round(drr_data[product], 2) if drr_data[product] != 0 else "")

                execute_with_retry(drr_sheet.update, f"A{current_row}", [row_data])
                current_row += 1

        # Форматируем числовые значения
        execute_with_retry(
            format_cell_range, drr_sheet, f"{col_letter}4:{col_letter}{current_row}",
            CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'},
                       horizontalAlignment='CENTER')
        )

        print(f"  ✅ Лист ДРР ОБЩИЙ обновлен (дата: {current_date_str})")
        print(f"     - Артикулов: {len(sorted_products)}")

        return True

    except Exception as e:
        print(f"  ❌ Ошибка при обновлении ДРР ОБЩИЙ: {e}")
        import traceback
        traceback.print_exc()
        return False


def setup_markup_sheet(spreadsheet):
    """Настраивает лист Наценка за нелокальную доставку"""
    print("\n💰 НАСТРОЙКА ЛИСТА НАЦЕНКИ ЗА НЕЛОКАЛЬНУЮ ДОСТАВКУ")
    sheet = get_or_create_sheet(spreadsheet, MARKUP_CONFIG["sheet_name"], rows=100, cols=5)
    all_values = execute_with_retry(sheet.get_all_values)

    if len(all_values) < 5 or (len(all_values) > 0 and not any("Кластер доставки" in str(row) for row in all_values)):
        print("  🆕 Настройка структуры листа наценки...")
        execute_with_exponential_backoff(sheet.clear)
        time.sleep(1)

        execute_with_exponential_backoff(sheet.update, "A1", [["💰 НАЦЕНКА ЗА НЕЛОКАЛЬНУЮ ДОСТАВКУ"]])
        execute_with_exponential_backoff(
            format_cell_range, sheet, "A1:B1",
            CellFormat(textFormat=TextFormat(bold=True, fontSize=12), backgroundColor=Color(0.8, 0.9, 1))
        )

        headers = [h['name'] for h in MARKUP_CONFIG["headers"]]
        execute_with_exponential_backoff(sheet.update, "A2", [headers])
        execute_with_exponential_backoff(
            format_cell_range, sheet, "A2:B2",
            CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
        )

        execute_with_exponential_backoff(sheet.update, "A3", MARKUP_CONFIG["default_data"])

        for idx, header in enumerate(MARKUP_CONFIG["headers"], start=1):
            col_letter = get_column_letter(idx)
            if 'width' in header:
                execute_with_exponential_backoff(set_column_width, sheet, col_letter, header['width'])

        note_row = 3 + len(MARKUP_CONFIG["default_data"]) + 1
        execute_with_exponential_backoff(sheet.update, f"A{note_row}", [[MARKUP_CONFIG["note"]]])
        execute_with_exponential_backoff(
            format_cell_range, sheet, f"A{note_row}:B{note_row}",
            CellFormat(textFormat=TextFormat(italic=True, fontSize=10), backgroundColor=Color(0.95, 0.95, 0.9))
        )
        print("  ✅ Лист наценки настроен")
    else:
        print("  📄 Лист наценки уже существует")
    return sheet


def setup_logistics_price_sheet(spreadsheet):
    """Настраивает лист Стоимость логистики с тремя столбцами"""
    print("\n📦 НАСТРОЙКА ЛИСТА СТОИМОСТИ ЛОГИСТИКИ")
    sheet = get_or_create_sheet(spreadsheet, LOGISTICS_PRICE_CONFIG["sheet_name"], rows=100, cols=10)
    all_values = execute_with_retry(sheet.get_all_values)

    if len(all_values) < 5 or (len(all_values) > 0 and not any("Объём товара" in str(row) for row in all_values)):
        print("  🆕 Настройка структуры листа стоимости логистики...")
        execute_with_exponential_backoff(sheet.clear)
        time.sleep(1)

        execute_with_exponential_backoff(sheet.update, "A1", [["📊 ТАБЛИЦА СТОИМОСТИ ЛОГИСТИКИ ОЗОН"]])
        execute_with_exponential_backoff(
            format_cell_range, sheet, "A1:C1",
            CellFormat(textFormat=TextFormat(bold=True, fontSize=12), backgroundColor=Color(0.8, 0.9, 1))
        )

        headers = [h['name'] for h in LOGISTICS_PRICE_CONFIG["headers"]]
        execute_with_exponential_backoff(sheet.update, "A2", [headers])
        execute_with_exponential_backoff(
            format_cell_range, sheet, "A2:C2",
            CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
        )

        execute_with_exponential_backoff(sheet.update, "A3", LOGISTICS_PRICE_CONFIG["default_data"])

        for idx, header in enumerate(LOGISTICS_PRICE_CONFIG["headers"], start=1):
            col_letter = get_column_letter(idx)
            if 'width' in header:
                execute_with_exponential_backoff(set_column_width, sheet, col_letter, header['width'])

        note_row = 3 + len(LOGISTICS_PRICE_CONFIG["default_data"]) + 1
        execute_with_exponential_backoff(sheet.update, f"A{note_row}", [[LOGISTICS_PRICE_CONFIG["note"]]])
        execute_with_exponential_backoff(
            format_cell_range, sheet, f"A{note_row}:C{note_row}",
            CellFormat(textFormat=TextFormat(italic=True, fontSize=10), backgroundColor=Color(0.95, 0.95, 0.9))
        )
        print("  ✅ Лист стоимости логистики настроен")
    else:
        print("  📄 Лист стоимости логистики уже существует")
    return sheet


def load_logistics_prices_from_sheet(sheet) -> Dict:
    """Загружает стоимость логистики из отдельного листа"""
    logistics_prices = {'under_300': [], 'over_300': []}
    try:
        all_values = execute_with_retry(sheet.get_all_values)
        data_start_row = None
        for idx, row in enumerate(all_values):
            if row and len(row) > 0 and "Объём товара" in str(row[0]):
                data_start_row = idx + 3
                break

        if data_start_row:
            for row in all_values[data_start_row:]:
                if len(row) >= 3 and row[0] and row[0].strip() and not str(row[0]).startswith('💡'):
                    try:
                        volume_range = str(row[0]).strip()
                        price_under_300 = float(str(row[1]).replace(',', '.').replace(' ', ''))
                        price_over_300 = float(str(row[2]).replace(',', '.').replace(' ', ''))

                        if '-' in volume_range and 'л' in volume_range:
                            range_part = volume_range.replace('л', '').strip()
                            parts = range_part.split('-')
                            min_vol = float(parts[0].replace(',', '.'))
                            max_vol = float(parts[1].replace(',', '.'))
                            logistics_prices['under_300'].append(
                                {'min': min_vol, 'max': max_vol, 'price': price_under_300})
                            logistics_prices['over_300'].append(
                                {'min': min_vol, 'max': max_vol, 'price': price_over_300})
                    except (ValueError, TypeError):
                        continue

        if not logistics_prices['under_300']:
            for vol_range, price_u, price_o in LOGISTICS_PRICE_CONFIG["default_data"]:
                if '-' in vol_range and 'л' in vol_range:
                    range_part = vol_range.replace('л', '').strip()
                    parts = range_part.split('-')
                    min_vol = float(parts[0].replace(',', '.'))
                    max_vol = float(parts[1].replace(',', '.'))
                    logistics_prices['under_300'].append({'min': min_vol, 'max': max_vol, 'price': price_u})
                    logistics_prices['over_300'].append({'min': min_vol, 'max': max_vol, 'price': price_o})

        print(f"  📦 Загружено {len(logistics_prices['under_300'])} правил логистики")
        return logistics_prices
    except Exception as e:
        print(f"  ⚠️ Ошибка загрузки: {e}")
        return logistics_prices


def get_logistics_price_by_volume(volume_liters: float, product_price: float, logistics_prices: Dict) -> float:
    """Получает стоимость логистики по объему товара и его цене"""
    if not logistics_prices or volume_liters <= 0:
        return 0
    rules = logistics_prices.get('under_300' if product_price <= 300 else 'over_300', [])
    if not rules:
        return 0
    for rule in rules:
        if rule['min'] <= volume_liters <= rule['max']:
            return rule['price']
    if volume_liters < rules[0]['min']:
        return rules[0]['price']
    if volume_liters > rules[-1]['max']:
        return rules[-1]['price']
    return 0


def setup_technical_sheet(spreadsheet):
    """Настраивает Технический лист"""
    print("\n📋 НАСТРОЙКА ТЕХНИЧЕСКОГО ЛИСТА")
    tech_sheet = get_or_create_sheet(spreadsheet, TECHNICAL_SHEET_CONFIG["sheet_name"], rows=5000, cols=50)
    all_values = execute_with_retry(tech_sheet.get_all_values)

    if len(all_values) < 20 or (len(all_values) > 0 and not any("ТОВАРЫ В ПРОДАЖЕ" in str(row) for row in all_values)):
        print("  🆕 Настройка структуры Технического листа...")
        execute_with_exponential_backoff(tech_sheet.clear)
        time.sleep(1)

        # Блок настроек
        execute_with_exponential_backoff(tech_sheet.update, "A1", [["⚙️ НАСТРОЙКИ КАЛЬКУЛЯТОРА"]])
        execute_with_exponential_backoff(
            format_cell_range, tech_sheet, "A1:C1",
            CellFormat(textFormat=TextFormat(bold=True, fontSize=12), backgroundColor=Color(0.8, 0.9, 1))
        )

        settings_headers = [h['name'] for h in TECHNICAL_SHEET_CONFIG["settings_headers"]]
        execute_with_exponential_backoff(tech_sheet.update, "A2", [settings_headers])
        execute_with_exponential_backoff(
            format_cell_range, tech_sheet, "A2:C2",
            CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
        )

        settings_data = [
            ["Ставка УСН + НДС (%)", technical_settings['tax_rate'], "%"],
            ["Эквайринг (%)", technical_settings['acquiring_rate'], "%"],
            ["Локальные продажи (%)", "87", "%"]
        ]
        execute_with_exponential_backoff(tech_sheet.update, "A3", settings_data)

        for idx, header in enumerate(TECHNICAL_SHEET_CONFIG["settings_headers"], start=1):
            col_letter = get_column_letter(idx)
            if 'width' in header:
                execute_with_exponential_backoff(set_column_width, tech_sheet, col_letter, header['width'])
        time.sleep(1)

        # Блок товаров
        products_start_row = 8
        execute_with_exponential_backoff(tech_sheet.update, f"A{products_start_row}", [["📊 ТОВАРЫ В ПРОДАЖЕ"]])
        execute_with_exponential_backoff(
            format_cell_range, tech_sheet, f"A{products_start_row}:I{products_start_row}",
            CellFormat(textFormat=TextFormat(bold=True, fontSize=12), backgroundColor=Color(0.8, 0.9, 1))
        )

        products_headers = [h['name'] for h in TECHNICAL_SHEET_CONFIG["products_headers"]]
        execute_with_exponential_backoff(tech_sheet.update, f"A{products_start_row + 1}", [products_headers])

        end_col = get_column_letter(len(products_headers))
        execute_with_exponential_backoff(
            format_cell_range, tech_sheet, f"A{products_start_row + 1}:{end_col}{products_start_row + 1}",
            CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
        )

        for idx, header in enumerate(TECHNICAL_SHEET_CONFIG["products_headers"], start=1):
            col_letter = get_column_letter(idx)
            if 'width' in header:
                execute_with_exponential_backoff(set_column_width, tech_sheet, col_letter, header['width'])

        execute_with_exponential_backoff(set_frozen, tech_sheet, rows=products_start_row + 1)

        note_row = products_start_row + 2
        execute_with_exponential_backoff(tech_sheet.update, f"A{note_row}",
                                         [["💡 Примечание: Стоимость логистики берется из листа 'Стоимость логистики'. Редактируйте таблицу там."]])
        execute_with_exponential_backoff(
            format_cell_range, tech_sheet, f"A{note_row}:I{note_row}",
            CellFormat(textFormat=TextFormat(italic=True, fontSize=9), backgroundColor=Color(0.95, 0.95, 0.9))
        )

        print("  ✅ Технический лист настроен")
        return tech_sheet, products_start_row + 2
    else:
        print("  📄 Технический лист уже существует, обновляем только данные товаров...")
        products_start_row = 8
        for idx, row in enumerate(all_values):
            if row and len(row) > 0 and "ТОВАРЫ В ПРОДАЖЕ" in str(row[0]):
                products_start_row = idx + 3
                break
        print(f"  📍 Строка начала данных товаров: {products_start_row}")
        return tech_sheet, products_start_row


def update_technical_sheet(tech_sheet, campaigns_data: Dict, products_start_row: int, logistics_price_sheet):
    """Обновляет данные в Техническом листе"""
    print("\n📊 ОБНОВЛЕНИЕ ТЕХНИЧЕСКОГО ЛИСТА")

    logistics_prices = load_logistics_prices_from_sheet(logistics_price_sheet)

    try:
        tax_rate_cell = tech_sheet.acell('B3').value
        if tax_rate_cell and str(tax_rate_cell) not in ['—', '']:
            technical_settings['tax_rate'] = float(tax_rate_cell)
        acquiring_rate_cell = tech_sheet.acell('B4').value
        if acquiring_rate_cell and str(acquiring_rate_cell) not in ['—', '']:
            technical_settings['acquiring_rate'] = float(acquiring_rate_cell)
    except Exception as e:
        print(f"  ⚠️ Ошибка чтения настроек: {e}")

    print(
        f"  ⚙️ Используемые настройки: налог={technical_settings['tax_rate']}%, эквайринг={technical_settings['acquiring_rate']}%")

    # Очищаем старые данные
    try:
        all_values = execute_with_retry(tech_sheet.get_all_values)
        total_rows = len(all_values)
        if total_rows >= products_start_row:
            end_col = get_column_letter(len(TECHNICAL_SHEET_CONFIG["products_headers"]))
            clear_range = f"A{products_start_row}:{end_col}{total_rows + 10}"
            execute_with_retry(tech_sheet.batch_clear, [clear_range])
            print(f"  ✅ Очищена область {clear_range}")
        time.sleep(1)
    except Exception as e:
        print(f"  ⚠️ Ошибка при очистке: {e}")

    # Подготавливаем данные
    products_data = []
    for offer_id, campaigns_list in campaigns_data.items():
        if not campaigns_list:
            continue
        first_campaign = campaigns_list[0]
        product_price_before = clean_numeric_value(first_campaign.get('product_price_before', 0))
        product_price = clean_numeric_value(first_campaign.get('product_price', 0))
        stock_balance = clean_int_value(first_campaign.get('stock_balance', 0))
        commission_fbo = clean_numeric_value(first_campaign.get('commission_fbo', 0))
        sku = first_campaign.get('sku', '—')
        product_volume = 1.0
        logistics_cost = get_logistics_price_by_volume(product_volume, product_price, logistics_prices)
        acquiring_fee = round(product_price_before * (technical_settings['acquiring_rate'] / 100), 2)
        products_data.append(
            [offer_id, sku, product_price_before, product_price, acquiring_fee, stock_balance, commission_fbo,
             product_volume, logistics_cost])
        print(f"  📦 {offer_id}: цена до скидки={product_price_before}, цена для покупателя={product_price}")

    if products_data:
        print(f"  📝 Добавление данных для {len(products_data)} товаров...")
        execute_with_retry(tech_sheet.update, f"A{products_start_row}", products_data,
                           value_input_option='USER_ENTERED')
        print(f"  ✅ Добавлено {len(products_data)} товаров")

        last_row = products_start_row + len(products_data) - 1
        for col, color, bold in [('C', Color(0.95, 0.9, 1), True), ('D', Color(0.9, 1, 0.9), True),
                                 ('E', Color(1, 0.95, 0.8), False), ('I', Color(0.7, 0.85, 1), False)]:
            try:
                fmt = CellFormat(backgroundColor=color)
                if bold:
                    fmt.textFormat = TextFormat(bold=True)
                execute_with_retry(format_cell_range, tech_sheet, f"{col}{products_start_row}:{col}{last_row}", fmt)
            except:
                pass
        print("  💡 Примечание: Эквайринг рассчитывается автоматически")
        print("  💡 Стоимость логистики берется из листа 'Стоимость логистики'")
    print("  ✅ Технический лист обновлен")


# ================= НОВЫЙ ФУНКЦИОНАЛ ПО ТЗ =================

def get_commission_rate(commission_str: str) -> float:
    """
    Парсит комиссию FBO из строки вида "1 588–2 179 43%–59%"
    Возвращает среднюю комиссию в процентах
    """
    if not commission_str or commission_str == '—':
        return 0

    try:
        import re
        percent_match = re.search(r'(\d+)%[–-](\d+)%', commission_str)
        if percent_match:
            min_percent = float(percent_match.group(1))
            max_percent = float(percent_match.group(2))
            return round((min_percent + max_percent) / 2, 2)

        numbers = re.findall(r'\d+', commission_str)
        if numbers and len(numbers) >= 2:
            return round((float(numbers[-2]) + float(numbers[-1])) / 2, 2)

        return 0
    except Exception as e:
        print(f"  ⚠️ Ошибка парсинга комиссии: {commission_str} - {e}")
        return 0


def calculate_logistics_cost(volume_l: float, price_before: float, logistics_prices: Dict,
                             non_local_ratio: float, avg_markup: float = 0.08) -> float:
    """
    Рассчитывает стоимость логистики по формуле:
    L = Ставка_доставки(объем) + 0.08 * non_local_ratio * цена_до_скидки
    """
    if volume_l <= 0:
        return 0

    base_rate = 56
    rules = logistics_prices.get('over_300', [])

    for rule in rules:
        if rule['min'] <= volume_l <= rule['max']:
            base_rate = rule['price']
            break

    if rules and volume_l < rules[0]['min']:
        base_rate = rules[0]['price']

    if rules and volume_l > rules[-1]['max']:
        base_rate = rules[-1]['price']

    markup = avg_markup * non_local_ratio * price_before

    return round(base_rate + markup, 2)


def calculate_spp(price_before: float, price_for_buyer: float) -> float:
    """
    Рассчитывает СПП (скидка постоянного покупателя):
    СПП = (1 - цена_для_покупателя / цена_до_скидки) * 100
    """
    if price_before <= 0:
        return 0
    spp = (1 - (price_for_buyer / price_before)) * 100
    return round(spp, 2)


def calculate_tax(price_for_buyer: float, tax_rate: float) -> float:
    """Рассчитывает налог: цена_для_покупателя * ставка_налога / 100"""
    return round(price_for_buyer * (tax_rate / 100), 2)


def calculate_acquiring(price_before: float, acquiring_rate: float) -> float:
    """Рассчитывает эквайринг: цена_до_скидки * ставка_эквайринга / 100"""
    return round(price_before * (acquiring_rate / 100), 2)


def calculate_chp(price_before: float, commission_percent: float, logistics: float,
                  tax: float, cost_price: float, acquiring: float, drr: float) -> float:
    """
    Рассчитывает чистую прибыль (ЧП):
    ЧП = цена_до_скидки - (цена_до_скидки * комиссия/100) - логистика - налог - себестоимость - эквайринг - (цена_до_скидки * ДРР/100)
    """
    commission_amount = price_before * (commission_percent / 100)
    drr_amount = price_before * (drr / 100)

    chp = price_before - commission_amount - logistics - tax - cost_price - acquiring - drr_amount
    return round(chp, 2)


def setup_chp_common_sheet(spreadsheet):
    """Настраивает страницу ЧП_товары_общая"""
    print("\n💰 НАСТРОЙКА ЛИСТА ЧП_товары_общая")

    try:
        # Проверяем, существует ли лист
        try:
            sheet = spreadsheet.worksheet(CHP_COMMON_CONFIG["sheet_name"])
            print(f"  📄 Лист {CHP_COMMON_CONFIG['sheet_name']} уже существует")

            # Проверяем структуру
            all_values = sheet.get_all_values()
            if len(all_values) > 0 and all_values[0] != CHP_COMMON_CONFIG["headers"]:
                print("  🆕 Обновляем структуру листа...")
                # Очищаем и пересоздаем
                sheet.clear()
                time.sleep(1)
                sheet.update("A1", [CHP_COMMON_CONFIG["headers"]])

                # Форматируем заголовки
                format_cell_range(
                    sheet, "A1:F1",
                    CellFormat(textFormat=TextFormat(bold=True, fontSize=11),
                               backgroundColor=Color(0.85, 0.95, 0.85))
                )

                for idx, header in enumerate(CHP_COMMON_CONFIG["headers"], start=1):
                    col_letter = get_column_letter(idx)
                    set_column_width(sheet, col_letter, 150 if idx == 0 else 120)

                set_frozen(sheet, rows=1)
                print("  ✅ Структура листа обновлена")

            return sheet

        except gspread.exceptions.WorksheetNotFound:
            print(f"  🆕 Создание нового листа {CHP_COMMON_CONFIG['sheet_name']}...")
            # Создаем новый лист
            sheet = spreadsheet.add_worksheet(
                title=CHP_COMMON_CONFIG["sheet_name"],
                rows=10000,
                cols=50
            )
            time.sleep(2)

            # Записываем заголовки
            sheet.update("A1", [CHP_COMMON_CONFIG["headers"]])

            # Форматируем
            format_cell_range(
                sheet, "A1:F1",
                CellFormat(textFormat=TextFormat(bold=True, fontSize=11),
                           backgroundColor=Color(0.85, 0.95, 0.85))
            )

            # Устанавливаем ширину столбцов
            for idx, header in enumerate(CHP_COMMON_CONFIG["headers"], start=1):
                col_letter = get_column_letter(idx)
                set_column_width(sheet, col_letter, 150 if idx == 0 else 120)

            set_frozen(sheet, rows=1)
            print("  ✅ Лист ЧП_товары_общая создан и настроен")

            return sheet

    except Exception as e:
        print(f"  ❌ КОНКРЕТНАЯ ОШИБКА при настройке ЧП_товары_общая: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def setup_chp_drr_sheet(spreadsheet):
    """Настраивает страницу ЧП_товары_ДРР"""
    print("\n💰 НАСТРОЙКА ЛИСТА ЧП_товары_ДРР")

    try:
        # Проверяем, существует ли лист
        try:
            sheet = spreadsheet.worksheet(CHP_DRR_CONFIG["sheet_name"])
            print(f"  📄 Лист {CHP_DRR_CONFIG['sheet_name']} уже существует")

            # Проверяем структуру
            all_values = sheet.get_all_values()
            if len(all_values) > 0 and all_values[0] != CHP_DRR_CONFIG["headers"]:
                print("  🆕 Обновляем структуру листа...")
                # Очищаем и пересоздаем
                sheet.clear()
                time.sleep(1)
                sheet.update("A1", [CHP_DRR_CONFIG["headers"]])

                # Форматируем заголовки
                format_cell_range(
                    sheet, "A1:F1",
                    CellFormat(textFormat=TextFormat(bold=True, fontSize=11),
                               backgroundColor=Color(0.85, 0.95, 0.85))
                )

                for idx, header in enumerate(CHP_DRR_CONFIG["headers"], start=1):
                    col_letter = get_column_letter(idx)
                    set_column_width(sheet, col_letter, 150 if idx == 0 else 120)

                set_frozen(sheet, rows=1)
                print("  ✅ Структура листа обновлена")

            return sheet

        except gspread.exceptions.WorksheetNotFound:
            print(f"  🆕 Создание нового листа {CHP_DRR_CONFIG['sheet_name']}...")
            # Создаем новый лист
            sheet = spreadsheet.add_worksheet(
                title=CHP_DRR_CONFIG["sheet_name"],
                rows=10000,
                cols=50
            )
            time.sleep(2)

            # Записываем заголовки
            sheet.update("A1", [CHP_DRR_CONFIG["headers"]])

            # Форматируем
            format_cell_range(
                sheet, "A1:F1",
                CellFormat(textFormat=TextFormat(bold=True, fontSize=11),
                           backgroundColor=Color(0.85, 0.95, 0.85))
            )

            # Устанавливаем ширину столбцов
            for idx, header in enumerate(CHP_DRR_CONFIG["headers"], start=1):
                col_letter = get_column_letter(idx)
                set_column_width(sheet, col_letter, 150 if idx == 0 else 120)

            set_frozen(sheet, rows=1)
            print("  ✅ Лист ЧП_товары_ДРР создан и настроен")

            return sheet

    except Exception as e:
        print(f"  ❌ КОНКРЕТНАЯ ОШИБКА при настройке ЧП_товары_ДРР: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def debug_sheet_creation(spreadsheet):
    """Отладочная функция для проверки создания листов"""
    print("\n🐛 ОТЛАДКА СОЗДАНИЯ ЛИСТОВ")

    try:
        # Проверяем все существующие листы
        worksheets = spreadsheet.worksheets()
        print(f"  📋 Существующие листы ({len(worksheets)}):")
        for ws in worksheets:
            print(f"     - {ws.title}")

        # Проверяем создание тестового листа
        test_title = "TEST_DELETE_ME"
        try:
            test_sheet = spreadsheet.worksheet(test_title)
            print(f"  ⚠️ Тестовый лист уже существует, удаляем...")
            spreadsheet.del_worksheet(test_sheet)
            time.sleep(1)
        except:
            pass

        print(f"  🆕 Создаем тестовый лист {test_title}...")
        test_sheet = spreadsheet.add_worksheet(title=test_title, rows=100, cols=10)
        test_sheet.update("A1", [["Тест"]])
        print(f"  ✅ Тестовый лист создан")

        time.sleep(1)
        print(f"  🗑️ Удаляем тестовый лист...")
        spreadsheet.del_worksheet(test_sheet)
        print(f"  ✅ Тестовый лист удален")

        print("  ✅ Создание листов работает нормально")

    except Exception as e:
        print(f"  ❌ Ошибка при тестировании: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()

def update_technical_sheet_advanced(tech_sheet, campaigns_data: Dict, products_start_row: int,
                                    logistics_price_sheet, current_date_str: str, tech_dict: Dict = None):
    """
    Расширенное обновление Технического листа с добавлением столбцов:
    - Эквайринг (руб)
    - Стоимость логистики (руб)
    - СПП (%)
    - Налог (руб)
    """
    print("\n📊 РАСШИРЕННОЕ ОБНОВЛЕНИЕ ТЕХНИЧЕСКОГО ЛИСТА")

    # Загружаем таблицу логистики
    logistics_prices = load_logistics_prices_from_sheet(logistics_price_sheet)

    # Читаем настройки из листа и tech_dict
    try:
        # Ставка налога (ячейка B3)
        tax_rate_cell = tech_sheet.acell('B3').value
        if tax_rate_cell and str(tax_rate_cell) not in ['—', '']:
            tax_rate = float(tax_rate_cell)
        else:
            tax_rate = 6.0

        # Эквайринг (ячейка B4)
        acquiring_rate_cell = tech_sheet.acell('B4').value
        if acquiring_rate_cell and str(acquiring_rate_cell) not in ['—', '']:
            acquiring_rate = float(acquiring_rate_cell)
        else:
            acquiring_rate = 1.0

        # Процент локальных продаж - сначала из tech_dict, потом из ячейки B5
        if tech_dict and 'local_sales_percent' in tech_dict:
            local_sales_percent = float(tech_dict['local_sales_percent'])
            print(f"  📍 Процент локальных продаж из tech_dict: {local_sales_percent}%")
            # Обновляем ячейку B5 в листе
            execute_with_retry(tech_sheet.update, "B5", [[local_sales_percent]])
        else:
            local_percent_cell = tech_sheet.acell('B5').value
            if local_percent_cell and str(local_percent_cell) not in ['—', '']:
                local_sales_percent = float(local_percent_cell)
            else:
                local_sales_percent = 87.0
            print(f"  📍 Процент локальных продаж из ячейки B5: {local_sales_percent}%")

        print(
            f"  ⚙️ Настройки: налог={tax_rate}%, эквайринг={acquiring_rate}%, локальные продажи={local_sales_percent}%")
    except Exception as e:
        print(f"  ⚠️ Ошибка чтения настроек: {e}")
        tax_rate = 6.0
        acquiring_rate = 1.0
        local_sales_percent = 87.0

    # Добавляем новые заголовки, если их нет
    existing_headers = execute_with_retry(tech_sheet.get_all_values)
    new_headers = ["Эквайринг (₽)", "Стоимость логистики (₽)", "СПП (%)", "Налог (₽)"]

    if len(existing_headers) > 0 and len(existing_headers[0]) >= 9:
        headers_row = existing_headers[8] if len(existing_headers) > 8 else []
        need_add_headers = not any("Эквайринг" in str(h) for h in headers_row)

        if need_add_headers:
            col_offset = len(TECHNICAL_SHEET_CONFIG["products_headers"]) + 1
            for idx, header in enumerate(new_headers):
                col_letter = get_column_letter(col_offset + idx)
                execute_with_retry(tech_sheet.update, f"{col_letter}9", [[header]])
                execute_with_retry(
                    format_cell_range, tech_sheet, f"{col_letter}9",
                    CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
                )
                execute_with_exponential_backoff(set_column_width, tech_sheet, col_letter, 130)

    # Собираем данные для обновления
    non_local_ratio = (100 - local_sales_percent) / 100

    # Получаем все товары из campaigns_data
    for row_idx, (offer_id, campaigns_list) in enumerate(campaigns_data.items()):
        if not campaigns_list:
            continue

        excel_row = products_start_row + row_idx

        # Берем первый кампании для получения основных данных
        first_campaign = campaigns_list[0]

        price_before = clean_numeric_value(first_campaign.get('product_price_before', 0))
        price_for_buyer = clean_numeric_value(first_campaign.get('product_price', 0))
        cost_price = clean_numeric_value(first_campaign.get('cost_price', 0))
        volume_l = clean_numeric_value(first_campaign.get('item_volume_l', 0))
        commission_str = str(first_campaign.get('commission_fbo', '0'))

        # Рассчитываем значения
        commission_percent = get_commission_rate(commission_str)
        acquiring = calculate_acquiring(price_before, acquiring_rate)
        logistics = calculate_logistics_cost(volume_l, price_before, logistics_prices, non_local_ratio)
        spp = calculate_spp(price_before, price_for_buyer)
        tax = calculate_tax(price_for_buyer, tax_rate)

        # Записываем в соответствующие столбцы
        col_offset = len(TECHNICAL_SHEET_CONFIG["products_headers"]) + 1

        # Эквайринг (столбец J)
        execute_with_retry(tech_sheet.update, f"{get_column_letter(col_offset)}{excel_row}", [[acquiring]])
        # Логистика (столбец K)
        execute_with_retry(tech_sheet.update, f"{get_column_letter(col_offset + 1)}{excel_row}", [[logistics]])
        # СПП (столбец L)
        execute_with_retry(tech_sheet.update, f"{get_column_letter(col_offset + 2)}{excel_row}", [[spp]])
        # Налог (столбец M)
        execute_with_retry(tech_sheet.update, f"{get_column_letter(col_offset + 3)}{excel_row}", [[tax]])

        print(
            f"  📦 {offer_id}: цена={price_before:.2f}, комиссия={commission_percent}%, логистика={logistics:.2f}, СПП={spp:.2f}%, налог={tax:.2f}")

    print("  ✅ Технический лист расширенно обновлен")


def update_chp_sheets(spreadsheet, campaigns_data: Dict, logistics_price_sheet, current_date_str: str,
                      tech_dict: Dict = None):
    """
    Обновляет страницы ЧП_товары_общая и ЧП_товары_ДРР
    """
    print("\n💰 ОБНОВЛЕНИЕ ЛИСТОВ ЧП")

    # Получаем настройки из Технического листа и tech_dict
    try:
        tech_sheet = get_or_create_sheet(spreadsheet, TECHNICAL_SHEET_CONFIG["sheet_name"])

        tax_rate_cell = tech_sheet.acell('B3').value
        tax_rate = float(tax_rate_cell) if tax_rate_cell and str(tax_rate_cell) not in ['—', ''] else 6.0

        acquiring_rate_cell = tech_sheet.acell('B4').value
        acquiring_rate = float(acquiring_rate_cell) if acquiring_rate_cell and str(acquiring_rate_cell) not in ['—',
                                                                                                                ''] else 1.0

        if tech_dict and 'local_sales_percent' in tech_dict:
            local_sales_percent = float(tech_dict['local_sales_percent'])
        else:
            local_percent_cell = tech_sheet.acell('B5').value
            local_sales_percent = float(local_percent_cell) if local_percent_cell and str(local_percent_cell) not in [
                '—', ''] else 87.0
    except Exception as e:
        print(f"  ⚠️ Не удалось прочитать настройки из Тех.листа: {e}")
        tax_rate = 6.0
        acquiring_rate = 1.0
        local_sales_percent = 87.0

    # Загружаем таблицу логистики
    logistics_prices = load_logistics_prices_from_sheet(logistics_price_sheet)

    non_local_ratio = (100 - local_sales_percent) / 100

    # Получаем или создаем листы
    chp_common_sheet = setup_chp_common_sheet(spreadsheet)
    chp_drr_sheet = setup_chp_drr_sheet(spreadsheet)

    # Подготавливаем данные для листов
    common_data = []
    drr_data = []

    for offer_id, campaigns_list in campaigns_data.items():
        if not campaigns_list:
            continue

        # Получаем данные из первого кампании
        first_campaign = campaigns_list[0]

        price_before = clean_numeric_value(first_campaign.get('product_price_before', 0))
        price_for_buyer = clean_numeric_value(first_campaign.get('product_price', 0))
        cost_price = clean_numeric_value(first_campaign.get('cost_price', 0))
        volume_l = clean_numeric_value(first_campaign.get('item_volume_l', 0))
        commission_str = str(first_campaign.get('commission_fbo', '0'))

        # Рассчитываем базовые значения
        commission_percent = get_commission_rate(commission_str)
        acquiring = calculate_acquiring(price_before, acquiring_rate)
        logistics = calculate_logistics_cost(volume_l, price_before, logistics_prices, non_local_ratio)
        tax = calculate_tax(price_for_buyer, tax_rate)

        # Получаем ДРР для товара
        drr_total = 0
        drr_search_total = 0

        for campaign in campaigns_list:
            drr_val = campaign.get('drr', '0%')
            if drr_val and drr_val != '—':
                drr_clean = clean_numeric_value(str(drr_val).replace('%', ''))
                camping_type = campaign.get('camping_type', '')
                if camping_type == 'Оплата за заказ':
                    drr_total = max(drr_total, drr_clean)
                else:
                    drr_search_total = max(drr_search_total, drr_clean)

        if drr_total == 0:
            drr_total = drr_search_total

        # Рассчитываем ЧП для общей страницы (с общим ДРР)
        chp_common = calculate_chp(price_before, commission_percent, logistics, tax, cost_price, acquiring, drr_total)

        # Рассчитываем ЧП для страницы с ДРР рекламным
        chp_drr = calculate_chp(price_before, commission_percent, logistics, tax, cost_price, acquiring,
                                drr_search_total)

        common_data.append([offer_id, chp_common, "", "", "", ""])
        drr_data.append([offer_id, chp_drr, "", "", "", ""])

        print(f"  📦 {offer_id}: ЧП_общая={chp_common:.2f} руб, ЧП_ДРР={chp_drr:.2f} руб")

    # Сортируем по убыванию ЧП
    common_data.sort(key=lambda x: x[1], reverse=True)
    drr_data.sort(key=lambda x: x[1], reverse=True)

    # Очищаем старые данные
    execute_with_retry(chp_common_sheet.batch_clear, ["A2:F10000"])
    execute_with_retry(chp_drr_sheet.batch_clear, ["A2:F10000"])

    # Записываем новые данные
    if common_data:
        execute_with_retry(chp_common_sheet.update, "A2", common_data)
        execute_with_retry(chp_drr_sheet.update, "A2", drr_data)

        for sheet in [chp_common_sheet, chp_drr_sheet]:
            execute_with_retry(
                format_cell_range, sheet, "B:B",
                CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'})
            )

        print(f"  ✅ Записано {len(common_data)} товаров в ЧП_товары_общая")
        print(f"  ✅ Записано {len(drr_data)} товаров в ЧП_товары_ДРР")


def add_chp_per_day_column(spreadsheet, campaigns_data: Dict, current_date_str: str, tech_dict: Dict = None):
    """
    Добавляет на дашборд столбец "ЧП / в день" после ДРР
    """
    print("\n📊 ДОБАВЛЕНИЕ СТОЛБЦА 'ЧП / в день' НА DASHBOARD")

    try:
        dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")

        # Получаем настройки из Технического листа
        tech_sheet = get_or_create_sheet(spreadsheet, TECHNICAL_SHEET_CONFIG["sheet_name"])

        tax_rate_cell = tech_sheet.acell('B3').value
        tax_rate = float(tax_rate_cell) if tax_rate_cell and str(tax_rate_cell) not in ['—', ''] else 6.0

        acquiring_rate_cell = tech_sheet.acell('B4').value
        acquiring_rate = float(acquiring_rate_cell) if acquiring_rate_cell and str(acquiring_rate_cell) not in ['—',
                                                                                                                ''] else 1.0

        if tech_dict and 'local_sales_percent' in tech_dict:
            local_sales_percent = float(tech_dict['local_sales_percent'])
        else:
            local_percent_cell = tech_sheet.acell('B5').value
            local_sales_percent = float(local_percent_cell) if local_percent_cell and str(local_percent_cell) not in [
                '—', ''] else 87.0

        # Загружаем таблицу логистики
        logistics_sheet = get_or_create_sheet(spreadsheet, LOGISTICS_PRICE_CONFIG["sheet_name"])
        logistics_prices = load_logistics_prices_from_sheet(logistics_sheet)

        non_local_ratio = (100 - local_sales_percent) / 100

        # Получаем текущие данные дашборда
        dashboard_data = execute_with_retry(dashboard.get_all_values)

        if len(dashboard_data) < 2:
            print("  ⚠️ Нет данных в дашборде")
            return

        # Проверяем, есть ли уже столбец "ЧП / в день"
        headers = dashboard_data[0]
        chp_col_index = None

        for idx, header in enumerate(headers):
            if header and "ЧП / в день" in str(header):
                chp_col_index = idx
                break

        # Если нет, добавляем заголовок
        if chp_col_index is None:
            chp_col_index = len(headers)
            new_col_letter = get_column_letter(chp_col_index + 1)
            execute_with_retry(dashboard.update, f"{new_col_letter}1", [["ЧП / в день"]])
            execute_with_retry(
                format_cell_range, dashboard, f"{new_col_letter}1",
                CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
            )
            execute_with_exponential_backoff(set_column_width, dashboard, new_col_letter, 120)
            print(f"  🆕 Добавлен столбец 'ЧП / в день' (столбец {new_col_letter})")

        # Рассчитываем ЧП для каждого товара
        for row_idx, row in enumerate(dashboard_data[1:], start=2):
            if not row or len(row) < 1:
                continue

            offer_id = row[0]
            if offer_id == "ИТОГО" or not offer_id:
                continue

            # Находим товар в campaigns_data
            if offer_id in campaigns_data and campaigns_data[offer_id]:
                first_campaign = campaigns_data[offer_id][0]

                price_before = clean_numeric_value(first_campaign.get('product_price_before', 0))
                price_for_buyer = clean_numeric_value(first_campaign.get('product_price', 0))
                cost_price = clean_numeric_value(first_campaign.get('cost_price', 0))
                volume_l = clean_numeric_value(first_campaign.get('item_volume_l', 0))
                commission_str = str(first_campaign.get('commission_fbo', '0'))

                # Получаем ДРР из дашборда (столбец F - ДРР общий)
                drr_total = clean_numeric_value(row[5]) if len(row) > 5 else 0

                # Рассчитываем
                commission_percent = get_commission_rate(commission_str)
                acquiring = calculate_acquiring(price_before, acquiring_rate)
                logistics = calculate_logistics_cost(volume_l, price_before, logistics_prices, non_local_ratio)
                tax = calculate_tax(price_for_buyer, tax_rate)
                chp = calculate_chp(price_before, commission_percent, logistics, tax, cost_price, acquiring, drr_total)

                # Записываем в столбец
                col_letter = get_column_letter(chp_col_index + 1)
                execute_with_retry(dashboard.update, f"{col_letter}{row_idx}", [[chp]])

                if row_idx < 10:
                    print(f"  📊 {offer_id}: ЧП={chp:.2f} руб")

        # Форматируем столбец с ЧП
        col_letter = get_column_letter(chp_col_index + 1)
        execute_with_retry(
            format_cell_range, dashboard, f"{col_letter}2:{col_letter}{len(dashboard_data)}",
            CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'}, horizontalAlignment='RIGHT')
        )

        print("  ✅ Столбец 'ЧП / в день' обновлен")

    except Exception as e:
        print(f"  ❌ Ошибка при добавлении ЧП / в день: {e}")
        import traceback
        traceback.print_exc()


def add_spp_column_to_dashboard(spreadsheet, campaigns_data: Dict):
    """
    Добавляет на дашборд столбец СПП после ДРР
    """
    print("\n📊 ДОБАВЛЕНИЕ СТОЛБЦА 'СПП' НА DASHBOARD")

    try:
        dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")

        # Получаем текущие данные дашборда
        dashboard_data = execute_with_retry(dashboard.get_all_values)

        if len(dashboard_data) < 2:
            print("  ⚠️ Нет данных в дашборде")
            return

        # Проверяем, есть ли уже столбец "СПП"
        headers = dashboard_data[0]
        spp_col_index = None

        for idx, header in enumerate(headers):
            if header and header == "СПП":
                spp_col_index = idx
                break

        # Если нет, добавляем заголовок
        if spp_col_index is None:
            spp_col_index = len(headers)
            new_col_letter = get_column_letter(spp_col_index + 1)
            execute_with_retry(dashboard.update, f"{new_col_letter}1", [["СПП"]])
            execute_with_retry(
                format_cell_range, dashboard, f"{new_col_letter}1",
                CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
            )
            execute_with_exponential_backoff(set_column_width, dashboard, new_col_letter, 100)
            print(f"  🆕 Добавлен столбец 'СПП' (столбец {new_col_letter})")

        # Рассчитываем СПП для каждого товара
        for row_idx, row in enumerate(dashboard_data[1:], start=2):
            if not row or len(row) < 1:
                continue

            offer_id = row[0]
            if offer_id == "ИТОГО" or not offer_id:
                continue

            # Находим товар в campaigns_data
            if offer_id in campaigns_data and campaigns_data[offer_id]:
                first_campaign = campaigns_data[offer_id][0]

                price_before = clean_numeric_value(first_campaign.get('product_price_before', 0))
                price_for_buyer = clean_numeric_value(first_campaign.get('product_price', 0))

                spp = calculate_spp(price_before, price_for_buyer)

                # Записываем в столбец
                col_letter = get_column_letter(spp_col_index + 1)
                execute_with_retry(dashboard.update, f"{col_letter}{row_idx}", [[spp]])

                if row_idx < 10:
                    print(f"  📊 {offer_id}: СПП={spp:.2f}%")

        # Форматируем столбец с СПП
        col_letter = get_column_letter(spp_col_index + 1)
        execute_with_retry(
            format_cell_range, dashboard, f"{col_letter}2:{col_letter}{len(dashboard_data)}",
            CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'}, horizontalAlignment='RIGHT')
        )

        print("  ✅ Столбец 'СПП' обновлен")

    except Exception as e:
        print(f"  ❌ Ошибка при добавлении СПП: {e}")
        import traceback
        traceback.print_exc()


# ================= БАЗОВЫЕ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================

def clean_numeric_value(value: Any) -> float:
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
    return int(clean_numeric_value(value))


def get_current_date_moscow() -> str:
    moscow_tz = pytz.timezone('Europe/Moscow')
    return datetime.now(moscow_tz).strftime("%d.%m.%Y")


def execute_with_exponential_backoff(func, *args, max_retries=10, **kwargs):
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str or 'Read timed out' in error_str or 'timeout' in error_str.lower():
                wait_time = min(30 * (2 ** attempt), 600)
                print(f"  ⏳ Превышен лимит или таймаут. Пауза {wait_time} сек... (попытка {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            elif '403' in error_str or 'PermissionError' in error_str:
                # Ошибка авторизации - не повторяем, сразу выводим
                print(f"  ❌ Ошибка авторизации: {error_str[:200]}")
                raise
            else:
                print(f"  ⚠️ Ошибка: {error_str[:200]}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    print(f"  ⏳ Повтор через {wait_time} сек...")
                    time.sleep(wait_time)
                else:
                    raise
    raise Exception(f"Не удалось выполнить операцию после {max_retries} попыток")


def execute_with_retry(func, *args, **kwargs):
    return execute_with_exponential_backoff(func, *args, **kwargs)


def get_google_sheets_client():
    """Создает клиент Google Sheets (старая рабочая версия)"""
    creds = Credentials.from_service_account_file(
        "google_sheets.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    return client


def test_google_sheets_connection():
    """Проверяет подключение к Google Sheets перед загрузкой"""
    print("\n🔌 ПРОВЕРКА ПОДКЛЮЧЕНИЯ К GOOGLE SHEETS")

    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            client = get_google_sheets_client()
            spreadsheet = client.open_by_key(spread_id)
            title = spreadsheet.title
            print(f"  ✅ Подключено к таблице: {title}")
            return client, spreadsheet
        except Exception as e:
            print(f"  ⚠️ Попытка {attempt + 1}/{max_attempts}: {str(e)[:100]}")
            if attempt < max_attempts - 1:
                wait_time = 5 * (attempt + 1)
                print(f"  ⏳ Повтор через {wait_time} секунд...")
                time.sleep(wait_time)
            else:
                print(f"  ❌ Не удалось подключиться после {max_attempts} попыток")
                raise


def get_or_create_sheet(spreadsheet, title: str, rows=1000, cols=30):
    try:
        return execute_with_exponential_backoff(spreadsheet.worksheet, title)
    except gspread.exceptions.WorksheetNotFound:
        return execute_with_exponential_backoff(spreadsheet.add_worksheet, title=title, rows=rows, cols=cols)


def get_column_letter(col_num: int) -> str:
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(ord('A') + col_num % 26) + result
        col_num //= 26
    return result


def get_column_index(col_letter: str) -> int:
    result = 0
    for char in col_letter:
        result = result * 26 + (ord(char.upper()) - ord('A') + 1)
    return result


def setup_sheet_headers(sheet, config: Dict, start_row: int = 1):
    headers_list = [h['name'] for h in config['headers']]
    end_col = get_column_letter(len(headers_list))
    execute_with_exponential_backoff(sheet.update, f"A{start_row}:{end_col}{start_row}", [headers_list])
    time.sleep(0.5)
    execute_with_exponential_backoff(
        format_cell_range, sheet, f"A{start_row}:{end_col}{start_row}",
        CellFormat(textFormat=TextFormat(bold=True, fontSize=11),
                   backgroundColor=config.get('header_color', Color(0.9, 0.9, 0.9)))
    )
    if config.get('frozen_rows'):
        execute_with_exponential_backoff(set_frozen, sheet, rows=config['frozen_rows'])
    time.sleep(0.5)
    for idx, header in enumerate(config['headers'], start=1):
        col_letter = get_column_letter(idx)
        if 'width' in header:
            execute_with_exponential_backoff(set_column_width, sheet, col_letter, header['width'])
    time.sleep(0.5)


def clear_old_dashboard_data(dashboard, current_total_rows: int):
    if current_total_rows > 1:
        print("  🗑️ Очищаем старые данные...")
        try:
            execute_with_retry(dashboard.batch_clear, [f"A2:F{current_total_rows}"])
            print(f"  ✅ Очищено содержимое строк 2-{current_total_rows}")
            time.sleep(1)
        except Exception as e:
            print(f"  ⚠️ Ошибка при очистке: {e}")


def format_totals_row(sheet, last_row: int, num_columns: int):
    end_col = get_column_letter(num_columns)
    execute_with_retry(
        format_cell_range, sheet, f"A{last_row}:{end_col}{last_row}",
        CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.95, 0.95, 0.95))
    )


def ensure_sheet_rows(sheet, required_rows: int, buffer_rows: int = 10):
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
    return round((expenses / revenue) * 100, 2) if revenue > 0 else 0.0


def log_dashboard_item(offer_id: str, revenue: float, expenses_search: float,
                       selled_search: float, drr_from_dict: float,
                       money_spent: float, drr_search: float,
                       drr_cpo: float, drr_total: float):
    print(f"\n  📊 {offer_id}:")
    print(f"     Сумма продаж за день: {revenue:,.2f} руб.")
    print(f"     Продажи по поиску+рекомендациям: {selled_search:,.2f} руб., расходы: {expenses_search:,.2f} руб.")
    print(f"     ДРР из словаря: {drr_from_dict}%")
    print(f"     Расходы из словаря: {money_spent:,.2f} руб.")
    print(f"     ДРР поиск: {drr_search}%")
    print(f"     ДРР оплата за заказ: {drr_cpo}%")
    print(f"     ДРР общий: {drr_total}%")


def prepare_dashboard_data(all_items_dict: Dict, campaigns_data: Dict,
                           drr_all_dict: Dict) -> Tuple[List[List], Dict, Dict]:
    """
    Подготавливает данные для листа DASHBOARD
    Возвращает: (dashboard_rows, totals, drr_for_products)
    """
    dashboard_rows = []
    drr_for_products = {}
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

        drr_for_products[offer_id] = drr_total

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

    dashboard_rows.sort(key=lambda x: x[2], reverse=True)

    return dashboard_rows, totals, drr_for_products


def update_dashboard_sheet(dashboard, dashboard_data: List[List]):
    current_data = execute_with_retry(dashboard.get_all_values)
    current_total_rows = len(current_data)
    if current_total_rows > 1:
        clear_old_dashboard_data(dashboard, current_total_rows)
    if dashboard_data:
        total_revenue = sum(row[1] for row in dashboard_data)
        total_orders = sum(row[2] for row in dashboard_data)
        dashboard_data.append([""] * len(DASHBOARD_CONFIG['headers']))
        dashboard_data.append(["ИТОГО", total_revenue, total_orders, 0, 0, 0])
    rows_needed = len(dashboard_data) + 1
    ensure_sheet_rows(dashboard, rows_needed)
    print(f"  📝 Вставка {len(dashboard_data)} строк данных одной операцией...")
    execute_with_retry(dashboard.update, "A2", dashboard_data)
    print(f"  ✅ Вставлено {len(dashboard_data)} строк данных")
    time.sleep(1)
    if dashboard_data:
        last_row = len(dashboard_data) + 1
        format_totals_row(dashboard, last_row, len(DASHBOARD_CONFIG['headers']))
        execute_with_retry(format_cell_range, dashboard, f"A2:A{last_row}",
                           CellFormat(textFormat=TextFormat(bold=True)))

    return dashboard_data[:-2] if len(dashboard_data) >= 2 else dashboard_data


# ================= НОВАЯ ФУНКЦИЯ ДЛЯ ИСТОРИИ DASHBOARD =================

def setup_history_dashboard_sheet(spreadsheet):
    """Настраивает лист истории DASHBOARD с правильным форматированием чисел"""
    print("\n📜 НАСТРОЙКА ЛИСТА ИСТОРИИ DASHBOARD")
    history_sheet = get_or_create_sheet(spreadsheet, HISTORY_DASHBOARD_CONFIG["sheet_name"], rows=100000, cols=20)
    all_values = execute_with_retry(history_sheet.get_all_values)

    if len(all_values) < 2 or (len(all_values) > 0 and all_values[0][0] != "Дата"):
        print("  🆕 Настройка структуры листа истории...")
        headers = [h['name'] for h in HISTORY_DASHBOARD_CONFIG["headers"]]
        execute_with_exponential_backoff(history_sheet.update, "A1", [headers])

        end_col = get_column_letter(len(headers))
        execute_with_exponential_backoff(
            format_cell_range, history_sheet, f"A1:{end_col}1",
            CellFormat(textFormat=TextFormat(bold=True, fontSize=11), backgroundColor=Color(0.85, 0.95, 0.85))
        )

        for idx, header in enumerate(HISTORY_DASHBOARD_CONFIG["headers"], start=1):
            col_letter = get_column_letter(idx)
            if 'width' in header:
                execute_with_exponential_backoff(set_column_width, history_sheet, col_letter, header['width'])

        execute_with_exponential_backoff(history_sheet.update, f"A2", [[HISTORY_DASHBOARD_CONFIG["note"]]])
        execute_with_exponential_backoff(
            format_cell_range, history_sheet, f"A2:{end_col}2",
            CellFormat(textFormat=TextFormat(italic=True, fontSize=9), backgroundColor=Color(0.95, 0.95, 0.9))
        )

        execute_with_exponential_backoff(
            format_cell_range, history_sheet, "C:C",
            CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0'})
        )

        execute_with_exponential_backoff(
            format_cell_range, history_sheet, "D:D",
            CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0'})
        )

        for col in ['E', 'F', 'G']:
            execute_with_exponential_backoff(
                format_cell_range, history_sheet, f"{col}:{col}",
                CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'})
            )

        execute_with_exponential_backoff(set_frozen, history_sheet, rows=2)
        print("  ✅ Структура листа истории настроена")
    else:
        print("  📄 Лист истории уже существует")

    return history_sheet


def save_dashboard_to_history(spreadsheet, current_date: str):
    """
    Сохраняет текущие данные из листа DASHBOARD в историю
    НОВЫЕ ДАННЫЕ ДОБАВЛЯЮТСЯ ВВЕРХУ (после заголовков)
    """
    print("\n💾 СОХРАНЕНИЕ DASHBOARD В ИСТОРИЮ")

    try:
        dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")
        dashboard_data = execute_with_retry(dashboard.get_all_values)

        if len(dashboard_data) <= 1:
            print("  ⚠️ Нет данных в DASHBOARD для сохранения")
            return False

        history_sheet = setup_history_dashboard_sheet(spreadsheet)

        existing_data = execute_with_retry(history_sheet.get_all_values)

        date_exists = False
        for row in existing_data[2:]:
            if row and len(row) > 0 and row[0] == current_date:
                date_exists = True
                break

        if date_exists:
            print(f"  ⚠️ Данные за {current_date} уже сохранены в истории")
            return False

        history_rows = []
        total_revenue_for_date = 0
        total_orders_for_date = 0

        for row in dashboard_data[1:]:
            if not row or len(row) < 6:
                continue

            if row[0] == "ИТОГО" or row[0] == "":
                if row[0] == "ИТОГО" and len(row) >= 3:
                    total_revenue_for_date = clean_numeric_value(row[1])
                    total_orders_for_date = clean_int_value(row[2])
                continue

            history_row = [
                current_date,
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5]
            ]
            history_rows.append(history_row)

        if not history_rows:
            print("  ⚠️ Нет данных для сохранения в историю")
            return False

        if len(existing_data) <= 2:
            insert_row_index = 3
        else:
            insert_row_index = 3

        print(f"  📍 Вставка новых данных начиная со строки {insert_row_index}")

        required_rows = len(existing_data) + len(history_rows) + 5
        if required_rows > len(existing_data):
            rows_to_add = required_rows - len(existing_data)
            print(f"  📏 Расширение листа: добавляем {rows_to_add} строк")
            try:
                execute_with_exponential_backoff(history_sheet.add_rows, rows_to_add)
                time.sleep(1)
            except Exception as e:
                print(f"  ⚠️ Не удалось добавить строки: {e}")

        if len(existing_data) > insert_row_index:
            old_data = existing_data[insert_row_index:] if insert_row_index < len(existing_data) else []

            end_col = get_column_letter(len(HISTORY_DASHBOARD_CONFIG["headers"]))
            clear_range = f"A{insert_row_index}:{end_col}{len(existing_data) + len(history_rows) + 10}"
            try:
                execute_with_retry(history_sheet.batch_clear, [clear_range])
            except:
                pass
            time.sleep(1)

            execute_with_retry(history_sheet.update, f"A{insert_row_index}", history_rows,
                               value_input_option='USER_ENTERED')

            if old_data:
                old_start_row = insert_row_index + len(history_rows)
                execute_with_retry(history_sheet.update, f"A{old_start_row}", old_data,
                                   value_input_option='USER_ENTERED')
        else:
            execute_with_retry(history_sheet.update, f"A{insert_row_index}", history_rows,
                               value_input_option='USER_ENTERED')

        totals_row_start = insert_row_index + len(history_rows)
        totals_row = [
            f"ИТОГО за {current_date}",
            "",
            total_revenue_for_date if total_revenue_for_date > 0 else sum(
                clean_numeric_value(row[2]) for row in history_rows),
            total_orders_for_date if total_orders_for_date > 0 else sum(
                clean_int_value(row[3]) for row in history_rows),
            "",
            "",
            ""
        ]

        execute_with_retry(history_sheet.update, f"A{totals_row_start}", [totals_row])

        end_col = get_column_letter(len(HISTORY_DASHBOARD_CONFIG["headers"]))
        execute_with_retry(
            format_cell_range, history_sheet, f"A{totals_row_start}:{end_col}{totals_row_start}",
            CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.95, 0.95, 0.95))
        )

        end_row = totals_row_start - 1
        for col in ['C', 'D', 'E', 'F', 'G']:
            try:
                execute_with_retry(
                    format_cell_range, history_sheet, f"{col}{insert_row_index}:{col}{end_row}",
                    CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'})
                )
            except:
                pass

        print(
            f"  ✅ История DASHBOARD обновлена: добавлено {len(history_rows)} записей за {current_date} (вверху листа)")
        return True

    except Exception as e:
        print(f"  ❌ Ошибка при сохранении истории DASHBOARD: {e}")
        import traceback
        traceback.print_exc()
        return False


# ================= ФУНКЦИИ ДЛЯ РАБОТЫ С ЛИСТАМИ ТОВАРОВ =================

def setup_product_sheet_structure(sheet, offer_id: str, skus_list: List[str]):
    print(f"  🆕 Создание нового листа {offer_id}...")
    updates = []
    updates.append(("A1", [["Артикул", offer_id]]))
    updates.append(("A2", [["SKU", ", ".join(skus_list)]]))
    updates.append(("A4", [[""]]))

    col_letter = ANALYTICS_CONFIG['start_column']
    updates.append((f"{col_letter}5", [[ANALYTICS_CONFIG['block_title']]]))

    headers_list = [h['name'] for h in ANALYTICS_CONFIG['headers']]
    end_col = get_column_letter(get_column_index(ANALYTICS_CONFIG['start_column']) + len(headers_list) - 1)
    updates.append((f"{ANALYTICS_CONFIG['start_column']}6:{end_col}6", [headers_list]))

    drr_col_letter = get_column_letter(get_column_index(ANALYTICS_CONFIG['start_column']) + 9)
    updates.append((f"SET_COLUMN_WIDTH_{drr_col_letter}", None))

    for block_config in CAMPAIGN_CONFIGS.values():
        col_letter = block_config['start_column']
        updates.append((f"{col_letter}5", [[block_config['title']]]))
        headers_list = [h['name'] for h in block_config['headers']]
        end_col = get_column_letter(get_column_index(block_config['start_column']) + len(headers_list) - 1)
        updates.append((f"{block_config['start_column']}6:{end_col}6", [headers_list]))

    for range_name, values in updates:
        if values is not None:
            execute_with_exponential_backoff(sheet.update, range_name, values)
            time.sleep(0.3)

    drr_col_letter = get_column_letter(get_column_index(ANALYTICS_CONFIG['start_column']) + 9)
    execute_with_exponential_backoff(set_column_width, sheet, drr_col_letter, 100)

    execute_with_exponential_backoff(
        format_cell_range, sheet, f"{ANALYTICS_CONFIG['start_column']}5",
        CellFormat(textFormat=TextFormat(bold=True, fontSize=12), backgroundColor=ANALYTICS_CONFIG['block_color'])
    )

    for block_config in CAMPAIGN_CONFIGS.values():
        col_letter = block_config['start_column']
        execute_with_exponential_backoff(
            format_cell_range, sheet, f"{col_letter}5",
            CellFormat(textFormat=TextFormat(bold=True, fontSize=12), backgroundColor=block_config['color'])
        )

    execute_with_exponential_backoff(set_frozen, sheet, rows=6)
    time.sleep(1)


def format_single_search_campaign(campaign: Dict) -> List:
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
    bet_amount = clean_numeric_value(campaign.get('bet_amount', 0))
    bet_percent = campaign.get('bet_percent', '')
    bet_display = f"{round(bet_amount, 2)} [{bet_percent}%]" if bet_percent and bet_amount else ""
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
    if not campaigns:
        return [""] * len(CAMPAIGN_CONFIGS[campaign_type]['headers'])
    if len(campaigns) == 1:
        return format_single_cpo_campaign(campaigns[0]) if campaign_type == 'cpo' else format_single_search_campaign(
            campaigns[0])
    result = []
    num_fields = len(CAMPAIGN_CONFIGS[campaign_type]['headers'])
    for field_idx in range(num_fields):
        values = []
        for camp in campaigns:
            formatted = format_single_cpo_campaign(camp) if campaign_type == 'cpo' else format_single_search_campaign(
                camp)
            val = formatted[field_idx] if field_idx < len(formatted) else ""
            if val is not None and str(val) != "" and str(val) != "0":
                values.append(str(val))
        result.append(", ".join(values) if values else "")
    return result


def update_position_data(item: Dict, positions_data: Optional[Dict]) -> float:
    skus_list = item.get("skus", [])
    if positions_data:
        for sku in skus_list:
            sku_str = str(sku)
            raw_position = positions_data.get(sku_str) or positions_data.get(sku)
            if raw_position is not None and str(raw_position) != '-' and str(raw_position) != '':
                try:
                    return float(str(raw_position).replace(',', '.').strip())
                except:
                    continue
    return clean_numeric_value(item.get("avg_position_category", 0))


def prepare_product_row(item: Dict, campaigns_data: Dict, drr_for_products: Dict, current_date_str: str) -> List:
    offer_id = item.get("offer_id")

    drr_total = drr_for_products.get(offer_id, 0.0)

    analytics_row = [
        current_date_str,
        clean_numeric_value(item.get("total_revenue", 0)),
        clean_int_value(item.get("total_ordered_units", 0)),
        round(clean_numeric_value(item.get("avg_position_category", 0)), 0),
        clean_int_value(item.get("total_hits_view", 0)),
        clean_int_value(item.get("total_hits_view_pdp", 0)),
        clean_numeric_value(item.get("avg_conversion_search_to_pdp", 0)),
        clean_numeric_value(item.get("avg_conv_tocart_search", 0)),
        clean_numeric_value(item.get("avg_conv_tocart", 0)),
        drr_total
    ]

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

    full_row = analytics_row + [""] + search_data + [""] + rec_data + [""] + cpo_data

    return full_row


def update_product_sheet_batch(sheet, offer_id: str, full_row: List, current_date_str: str):
    all_data = execute_with_retry(sheet.get_all_values)
    existing_row_index = None
    for i, row in enumerate(all_data[6:], start=7):
        if len(row) > 0 and row[0] == current_date_str:
            existing_row_index = i
            break
    if existing_row_index:
        print(f"  🔄 Обновление строки {existing_row_index}")
        execute_with_retry(sheet.update, f"A{existing_row_index}", [full_row], value_input_option='USER_ENTERED')
        print(f"  ✅ Обновлена строка за {current_date_str}")
    else:
        print(f"  📝 Добавление новой строки за {current_date_str}")
        execute_with_exponential_backoff(sheet.insert_row, full_row, index=7)
        print(f"  ✅ Добавлена строка за {current_date_str}")
    time.sleep(1)
    enforce_sheet_size_limit(sheet, max_rows=500)


def enforce_sheet_size_limit(sheet, max_rows: int = 500):
    current_rows = len(execute_with_exponential_backoff(sheet.get_all_values))
    if current_rows > max_rows:
        rows_to_delete = current_rows - max_rows
        try:
            execute_with_exponential_backoff(sheet.delete_rows, 7, rows_to_delete)
            print(f"  ✅ Удалены старые строки, удалено {rows_to_delete} строк, осталось {max_rows}")
            time.sleep(1)
        except Exception as e:
            print(f"  ⚠️ Не удалось удалить строки: {e}")
    else:
        print(f"  ℹ️ В листе {current_rows} строк, лимит не превышен")


# ================= ФУНКЦИИ ДЛЯ ОБРАБОТКИ ОШИБОК =================

def write_error_to_sheet(error_message: str, sheet_name: str = "ERROR"):
    try:
        client = get_google_sheets_client()
        client.set_timeout(120)
        spreadsheet = client.open_by_key(spread_id)
        try:
            sheet = spreadsheet.worksheet(sheet_name)
            sheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=5)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        full_error = f"[{timestamp}] {error_message}"
        sheet.update("A1", [[full_error]])
        import traceback
        tb = traceback.format_exc()
        if tb and tb != "NoneType: None\n":
            sheet.update("A2", [[tb]])
        print(f"✅ Ошибка записана в лист {sheet_name}")
    except Exception as e:
        print(f"❌ Не удалось записать ошибку: {e}")


def write_parser_error_to_sheet(error_message: str):
    write_error_to_sheet(error_message, "ERROR_PARS")


# ================= ОСНОВНАЯ ФУНКЦИЯ =================

def upload_to_google_sheets(all_items_dict: Dict, campaigns_data: Optional[Dict] = None,
                            positions_data: Optional[Dict] = None,
                            drr_all_dict: Optional[Dict] = None,
                            tech_dict: Optional[Dict] = None,
                            ):
    """
    Основная функция загрузки данных в Google Sheets
    """
    print("\n" + "=" * 60)
    print("🚀 НАЧАЛО ЗАГРУЗКИ ДАННЫХ В GOOGLE SHEETS")
    print("=" * 60)

    try:
        # Подключение к Google Sheets
        print("\n🔌 Подключение к Google Sheets...")
        client, spreadsheet = test_google_sheets_connection()
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

        # Подготовка данных DASHBOARD - получаем также drr_for_products
        dashboard_data, _, drr_for_products = prepare_dashboard_data(all_items_dict, campaigns_data, drr_all_dict)
        update_dashboard_sheet(dashboard, dashboard_data)
        print("✅ DASHBOARD успешно обновлен")

        # ================= НОВЫЙ ЛИСТ: ДРР ОБЩИЙ (сразу после DASHBOARD) =================
        print("\n" + "=" * 60)
        print("📊 ОБНОВЛЕНИЕ СВОДНОЙ ТАБЛИЦЫ ДРР ОБЩИЙ")
        print("=" * 60)

        # Обновляем сводную таблицу на основе текущего DASHBOARD
        update_drr_total_sheet_from_dashboard(spreadsheet, dashboard, current_date_str)

        # ================= ОБРАБОТКА ЛИСТОВ ТОВАРОВ =================
        print("\n" + "=" * 60)
        print("📄 ОБРАБОТКА ЛИСТОВ ТОВАРОВ")
        print("=" * 60)

        for idx, item in enumerate(all_items_dict.values()):
            offer_id = item.get("offer_id")
            skus_list = item.get("skus", [])

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

            # Подготавливаем и обновляем данные - передаём drr_for_products
            full_row = prepare_product_row(item, campaigns_data, drr_for_products, current_date_str)
            update_product_sheet_batch(sheet, offer_id, full_row, current_date_str)

            # Пауза для соблюдения лимитов
            if (idx + 1) % 5 == 0:
                print(f"\n⏸️ Обработано {idx + 1} товаров, пауза 5 секунд...")
                time.sleep(5)

        # ================= НАСТРОЙКА ТЕХНИЧЕСКОГО ЛИСТА =================
        print("\n" + "=" * 60)
        print("📋 НАСТРОЙКА ТЕХНИЧЕСКОГО ЛИСТА")
        print("=" * 60)

        tech_sheet, products_start_row = setup_technical_sheet(spreadsheet)

        # Настраиваем лист стоимости логистики
        logistics_price_sheet = setup_logistics_price_sheet(spreadsheet)

        # ================= РАСШИРЕННОЕ ОБНОВЛЕНИЕ ТЕХНИЧЕСКОГО ЛИСТА =================
        print("\n" + "=" * 60)
        print("📊 РАСШИРЕННОЕ ОБНОВЛЕНИЕ ТЕХНИЧЕСКОГО ЛИСТА (НОВЫЕ СТОЛБЦЫ)")
        print("=" * 60)

        update_technical_sheet_advanced(tech_sheet, campaigns_data, products_start_row,
                                        logistics_price_sheet, current_date_str, tech_dict)

        # ================= ОБНОВЛЕНИЕ ЛИСТОВ ЧП =================
        print("\n" + "=" * 60)
        print("💰 ОБНОВЛЕНИЕ ЛИСТОВ ЧП")
        print("=" * 60)

        # Отладочная проверка
        debug_sheet_creation(spreadsheet)
        update_chp_sheets(spreadsheet, campaigns_data, logistics_price_sheet, current_date_str, tech_dict)

        # ================= ДОБАВЛЕНИЕ СТОЛБЦОВ НА DASHBOARD =================
        print("\n" + "=" * 60)
        print("📊 ДОБАВЛЕНИЕ НОВЫХ СТОЛБЦОВ НА DASHBOARD")
        print("=" * 60)

        # Добавляем столбец ЧП / в день на дашборд
        add_chp_per_day_column(spreadsheet, campaigns_data, current_date_str, tech_dict)

        # Добавляем столбец СПП на дашборд
        add_spp_column_to_dashboard(spreadsheet, campaigns_data)

        # ================= СОХРАНЕНИЕ ИСТОРИИ DASHBOARD (В КОНЦЕ) =================
        print("\n" + "=" * 60)
        print("📜 СОХРАНЕНИЕ ИСТОРИИ DASHBOARD")
        print("=" * 60)

        # Сохраняем данные из DASHBOARD в историю
        save_dashboard_to_history(spreadsheet, current_date_str)

        print("\n" + "=" * 60)
        print("✅ ВСЕ ДАННЫЕ УСПЕШНО ЗАГРУЖЕНЫ")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        write_error_to_sheet(str(e))
        raise


def test():
    with open('all_items_dict.json', 'r', encoding='utf-8') as f:
        all_dict = json.load(f)
    with open('advert_analytic.json', 'r', encoding='utf-8') as f:
        s_dict = json.load(f)
    with open('position_analytic.json', 'r', encoding='utf-8') as f:
        l_dict = json.load(f)
    with open('money_spent_advert_dict.json', 'r', encoding='utf-8') as f:
        money_spent_dict = json.load(f)
    upload_to_google_sheets(all_dict, s_dict, l_dict, money_spent_dict, {})


def test_with_custom_date(custom_date: str = None):
    """
    Тестовая функция с возможностью указать произвольную дату

    Args:
        custom_date: Дата в формате "DD.MM.YYYY" (например "15.03.2026")
                    Если не указана, используется текущая дата
    """
    # Загружаем тестовые данные
    with open('all_items_dict.json', 'r', encoding='utf-8') as f:
        all_dict = json.load(f)
    with open('advert_analytic.json', 'r', encoding='utf-8') as f:
        s_dict = json.load(f)
    with open('position_analytic.json', 'r', encoding='utf-8') as f:
        l_dict = json.load(f)
    with open('money_spent_advert_dict.json', 'r', encoding='utf-8') as f:
        money_spent_dict = json.load(f)

    # Если указана тестовая дата - используем её
    if custom_date:
        print(f"\n🧪 ТЕСТОВЫЙ РЕЖИМ: используется дата {custom_date}")
        # Временно переопределяем функцию получения даты
        original_get_date = get_current_date_moscow
        globals()['get_current_date_moscow'] = lambda: custom_date

        # Загружаем данные с тестовой датой
        upload_to_google_sheets(all_dict, s_dict, l_dict, money_spent_dict, {})

        # Восстанавливаем оригинальную функцию
        globals()['get_current_date_moscow'] = original_get_date
    else:
        # Обычный режим с текущей датой
        upload_to_google_sheets(all_dict, s_dict, l_dict, money_spent_dict, {})


def test_technical_sheet_with_saved_data(custom_date: str = None):
    """
    Тестовая функция для проверки работы Технического листа и нового функционала
    Загружает данные из сохраненных JSON файлов и запускает загрузку в Google Sheets

    Args:
        custom_date: Дата в формате "DD.MM.YYYY" (например "15.05.2026")
                    Если не указана, используется текущая дата
    """
    print("\n" + "=" * 80)
    print("🧪 ТЕСТОВЫЙ РЕЖИМ: ЗАГРУЗКА ИЗ СОХРАНЕННЫХ ДАННЫХ")
    print("=" * 80)

    # Пути к файлам с данными
    files = {
        'all_items_dict': 'all_items_dict.json',
        'advert_analytic': 'advert_analytic.json',
        'position_analytic': 'position_analytic.json',
        'money_spent_advert_dict': 'money_spent_advert_dict.json',
        'tech_dict': 'tech_dict.json'
    }

    # Проверяем наличие файлов
    missing_files = []
    for name, path in files.items():
        if not os.path.exists(path):
            missing_files.append(f"{name} ({path})")

    if missing_files:
        print("\n❌ ОТСУТСТВУЮТ ФАЙЛЫ ДАННЫХ:")
        for f in missing_files:
            print(f"   - {f}")
        print("\n💡 Сначала запустите main.py в обычном режиме, чтобы сохранить данные")
        print("   Или создайте файлы с тестовыми данными вручную")
        return False

    # Загружаем данные
    print("\n📂 ЗАГРУЗКА ДАННЫХ ИЗ ФАЙЛОВ:")

    all_items_dict = {}
    advert_analytic = {}
    position_analytic = {}
    money_spent_advert_dict = {}
    tech_dict = {}

    try:
        with open(files['all_items_dict'], 'r', encoding='utf-8') as f:
            all_items_dict = json.load(f)
        print(f"   ✅ all_items_dict: {len(all_items_dict)} товаров")
    except Exception as e:
        print(f"   ❌ Ошибка загрузки all_items_dict: {e}")

    try:
        with open(files['advert_analytic'], 'r', encoding='utf-8') as f:
            advert_analytic = json.load(f)
        print(f"   ✅ advert_analytic: {len(advert_analytic)} товаров")
    except Exception as e:
        print(f"   ❌ Ошибка загрузки advert_analytic: {e}")

    try:
        with open(files['position_analytic'], 'r', encoding='utf-8') as f:
            position_analytic = json.load(f)
        print(f"   ✅ position_analytic: {len(position_analytic)} записей")
    except Exception as e:
        print(f"   ⚠️ position_analytic не загружен: {e}")

    try:
        with open(files['money_spent_advert_dict'], 'r', encoding='utf-8') as f:
            money_spent_advert_dict = json.load(f)
        print(f"   ✅ money_spent_advert_dict: {len(money_spent_advert_dict)} записей")
    except Exception as e:
        print(f"   ⚠️ money_spent_advert_dict не загружен: {e}")

    try:
        with open(files['tech_dict'], 'r', encoding='utf-8') as f:
            tech_dict = json.load(f)
        print(f"   ✅ tech_dict: загружен")
        if 'local_sales_percent' in tech_dict:
            print(f"      - local_sales_percent: {tech_dict['local_sales_percent']}%")
    except Exception as e:
        print(f"   ⚠️ tech_dict не загружен: {e}")

    # Проверяем, есть ли данные для теста
    if not all_items_dict or not advert_analytic:
        print("\n❌ НЕТ ДАННЫХ ДЛЯ ТЕСТА")
        print("   Убедитесь, что файлы all_items_dict.json и advert_analytic.json существуют")
        return False

    # Выводим информацию о товарах для отладки
    print("\n📊 ИНФОРМАЦИЯ О ТОВАРАХ ИЗ advert_analytic:")
    for offer_id, campaigns in list(advert_analytic.items())[:5]:  # Показываем первые 5
        if campaigns:
            first = campaigns[0]
            print(f"   📦 {offer_id}:")
            print(f"      - Цена до скидки: {first.get('product_price_before', 'Нет')}")
            print(f"      - Цена для покупателя: {first.get('product_price', 'Нет')}")
            print(f"      - Объем: {first.get('item_volume_l', 'Нет')} л")
            print(f"      - Комиссия FBO: {first.get('commission_fbo', 'Нет')}")
            print(f"      - Себестоимость: {first.get('cost_price', 'Нет')}")
            print(f"      - Остатки: {first.get('stock_balance', 'Нет')}")

    # Запускаем загрузку в Google Sheets
    print("\n🚀 ЗАПУСК ЗАГРУЗКИ В GOOGLE SHEETS")
    print("=" * 80)

    if custom_date:
        print(f"📅 Используется тестовая дата: {custom_date}")
        original_get_date = get_current_date_moscow
        globals()['get_current_date_moscow'] = lambda: custom_date

        try:
            upload_to_google_sheets(
                all_items_dict,
                advert_analytic,
                position_analytic,
                money_spent_advert_dict,
                tech_dict
            )
        finally:
            globals()['get_current_date_moscow'] = original_get_date
    else:
        upload_to_google_sheets(
            all_items_dict,
            advert_analytic,
            position_analytic,
            money_spent_advert_dict,
            tech_dict
        )

    print("\n" + "=" * 80)
    print("✅ ТЕСТОВАЯ ЗАГРУЗКА ЗАВЕРШЕНА")
    print("=" * 80)

    return True


def quick_test():
    """
    Быстрый тест с фиксированной датой для проверки Технического листа
    """
    print("\n🔥 БЫСТРЫЙ ТЕСТ ТЕХНИЧЕСКОГО ЛИСТА")
    print("   Используется дата: 15.05.2026")
    test_technical_sheet_with_saved_data(custom_date="15.05.2026")


def debug_technical_sheet():
    """
    Отладочная функция для проверки расчетов Технического листа
    Показывает все промежуточные расчеты без загрузки в Google Sheets
    """
    print("\n" + "=" * 80)
    print("🐛 ОТЛАДКА ТЕХНИЧЕСКОГО ЛИСТА (БЕЗ ЗАГРУЗКИ)")
    print("=" * 80)

    # Загружаем данные
    try:
        with open('advert_analytic.json', 'r', encoding='utf-8') as f:
            advert_analytic = json.load(f)
        print(f"✅ Загружено {len(advert_analytic)} товаров")
    except Exception as e:
        print(f"❌ Ошибка загрузки: {e}")
        return

    try:
        with open('tech_dict.json', 'r', encoding='utf-8') as f:
            tech_dict = json.load(f)
        print(f"✅ Загружен tech_dict")
        local_sales_percent = float(tech_dict.get('local_sales_percent', 87))
        print(f"   - local_sales_percent: {local_sales_percent}%")
    except Exception as e:
        print(f"⚠️ tech_dict не загружен, используем 87%")
        local_sales_percent = 87

    # Параметры расчета
    tax_rate = 6.0
    acquiring_rate = 1.0
    non_local_ratio = (100 - local_sales_percent) / 100
    avg_markup = 0.08

    # Загружаем таблицу логистики (имитация)
    logistics_prices = {
        'over_300': [
            {'min': 0, 'max': 0.200, 'price': 56},
            {'min': 0.201, 'max': 0.4, 'price': 63},
            {'min': 0.401, 'max': 0.6, 'price': 67},
            {'min': 0.601, 'max': 0.8, 'price': 67},
            {'min': 0.801, 'max': 1.0, 'price': 67},
            {'min': 1.001, 'max': 1.25, 'price': 71},
            {'min': 1.251, 'max': 1.5, 'price': 74},
            {'min': 1.501, 'max': 1.75, 'price': 74},
            {'min': 1.751, 'max': 2.0, 'price': 74},
            {'min': 2.001, 'max': 3.0, 'price': 74},
            {'min': 3.001, 'max': 4.0, 'price': 78},
            {'min': 4.001, 'max': 5.0, 'price': 89},
            {'min': 5.001, 'max': 6.0, 'price': 89},
            {'min': 6.001, 'max': 7.0, 'price': 99},
            {'min': 7.001, 'max': 8.0, 'price': 99},
            {'min': 8.001, 'max': 9.0, 'price': 100},
            {'min': 9.001, 'max': 10.0, 'price': 100},
            {'min': 10.001, 'max': 11.0, 'price': 102}
        ]
    }

    print("\n📊 РАСЧЕТЫ ДЛЯ КАЖДОГО ТОВАРА:")
    print("-" * 80)

    for offer_id, campaigns in advert_analytic.items():
        if not campaigns:
            continue

        first = campaigns[0]

        price_before = clean_numeric_value(first.get('product_price_before', 0))
        price_for_buyer = clean_numeric_value(first.get('product_price', 0))
        cost_price = clean_numeric_value(first.get('cost_price', 0))
        volume_l = clean_numeric_value(first.get('item_volume_l', 0))
        commission_str = str(first.get('commission_fbo', '0'))

        # Расчеты
        commission_percent = get_commission_rate(commission_str)
        acquiring = calculate_acquiring(price_before, acquiring_rate)

        # Логистика
        base_rate = 56
        for rule in logistics_prices['over_300']:
            if rule['min'] <= volume_l <= rule['max']:
                base_rate = rule['price']
                break
        logistics = base_rate + (avg_markup * non_local_ratio * price_before)

        spp = calculate_spp(price_before, price_for_buyer)
        tax = calculate_tax(price_for_buyer, tax_rate)

        print(f"\n📦 {offer_id}:")
        print(f"   Цена до скидки: {price_before:.2f} руб")
        print(f"   Цена для покупателя: {price_for_buyer:.2f} руб")
        print(f"   Себестоимость: {cost_price:.2f} руб")
        print(f"   Объем: {volume_l} л")
        print(f"   Комиссия FBO: {commission_percent}%")
        print(f"   Эквайринг ({acquiring_rate}%): {acquiring:.2f} руб")
        print(f"   Базовая ставка логистики: {base_rate:.2f} руб")
        print(f"   Логистика с наценкой: {logistics:.2f} руб")
        print(f"   СПП: {spp:.2f}%")
        print(f"   Налог ({tax_rate}%): {tax:.2f} руб")

        # Поиск ДРР
        drr_total = 0
        for campaign in campaigns:
            drr_val = campaign.get('drr', '0%')
            if drr_val and drr_val != '—':
                drr_clean = clean_numeric_value(str(drr_val).replace('%', ''))
                if campaign.get('camping_type') == 'Оплата за заказ':
                    drr_total = max(drr_total, drr_clean)
                else:
                    drr_total = max(drr_total, drr_clean)

        if drr_total > 0:
            chp = calculate_chp(price_before, commission_percent, logistics, tax, cost_price, acquiring, drr_total)
            print(f"   ДРР: {drr_total}%")
            print(f"   ЧП: {chp:.2f} руб")

    print("\n" + "-" * 80)
    print("✅ ОТЛАДКА ЗАВЕРШЕНА")


# Добавить в конец файла, чтобы можно было запустить тест:
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == "quick":
            quick_test()
        elif sys.argv[1] == "debug":
            debug_technical_sheet()
        elif sys.argv[1] == "custom" and len(sys.argv) > 2:
            test_technical_sheet_with_saved_data(custom_date=sys.argv[2])
        else:
            test_technical_sheet_with_saved_data()
    else:
        test_technical_sheet_with_saved_data()

# if __name__ == "__main__":
#     test_with_custom_date('31.06.2026')