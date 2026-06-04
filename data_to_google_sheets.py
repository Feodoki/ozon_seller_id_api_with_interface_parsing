import traceback

import gspread
from gspread_formatting import (
    CellFormat,
    Color,
    TextFormat,
    format_cell_range,
    set_frozen,
    set_column_width,
)
from google.oauth2.service_account import Credentials
from config import spread_id
import json
import time
from datetime import datetime
import pytz
from typing import Dict, List, Any, Optional, Tuple
import os
import random

QUOTA_RETRY_DELAY = 30
MAX_QUOTA_RETRIES = 20

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
        {"name": "Стоимость логистики (₽)", "width": 140},
        {"name": "СПП (%)", "width": 100},
        {"name": "Себестоимость (₽)", "width": 130}
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
        {"name": "Наценка за нелокальную продажу от вашей цены товара %", "width": 350}
    ],
    "default_data": [
        ["Москва", 8],
        ["Санкт-Петербург", 8],
        ["Дальний Восток", 8],
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
    "headers": ["Артикул", "Общая сумма ЧП"],
    "formulas": {
        "total_chp": "=SUM(C2:2)"
    }
}

CHP_DRR_CONFIG = {
    "sheet_name": "ЧП_товары_ДРР",
    "headers": ["Артикул", "Общая сумма ЧП"],
    "formulas": {
        "total_chp": "=SUM(C2:2)"
    }
}

SPP_HISTORY_CONFIG = {
    "sheet_name": "История СПП",
    "headers": ["Артикул", "Среднее значение СПП"],
    "note": "📊 История ежедневных значений СПП (скидка постоянного покупателя) по всем товарам. Данные добавляются автоматически каждый день."
}


def setup_spp_history_sheet(spreadsheet):
    """Настраивает лист истории СПП"""
    print("\n📊 НАСТРОЙКА ЛИСТА ИСТОРИИ СПП")

    sheet_title = SPP_HISTORY_CONFIG["sheet_name"]

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            try:
                sheet = spreadsheet.worksheet(sheet_title)
                print(f"  📄 Лист {sheet_title} уже существует")
                return sheet
            except gspread.exceptions.WorksheetNotFound:
                print(f"  🆕 Создание листа {sheet_title}...")

                sheet = safe_api_call(spreadsheet.add_worksheet, title=sheet_title, rows=10000, cols=100)
                time.sleep(2)

                # Формируем заголовки
                headers = SPP_HISTORY_CONFIG["headers"]
                safe_update_cell(sheet, "A1", [headers], value_input_option='USER_ENTERED')

                end_col = get_column_letter(len(headers))
                safe_format_range(
                    sheet, f"A1:{end_col}1",
                    CellFormat(
                        textFormat=TextFormat(bold=True, fontSize=11),
                        backgroundColor=Color(0.85, 0.95, 0.85)
                    )
                )

                # Устанавливаем ширину колонок
                safe_api_call(set_column_width, sheet, "A", 200)
                safe_api_call(set_column_width, sheet, "B", 150)

                # Добавляем примечание
                safe_update_cell(sheet, "A2", [[SPP_HISTORY_CONFIG["note"]]], value_input_option='USER_ENTERED')
                safe_format_range(
                    sheet, f"A2:{end_col}2",
                    CellFormat(
                        textFormat=TextFormat(italic=True, fontSize=9),
                        backgroundColor=Color(0.95, 0.95, 0.9)
                    )
                )

                safe_api_call(set_frozen, sheet, rows=2)
                print("  ✅ Лист истории СПП создан и настроен")
                return sheet

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY * (attempt + 1) + random.uniform(0, 5)
                print(f"  ⏳ Квота API при настройке листа истории СПП. Пауза {wait_time:.1f} сек...")
                time.sleep(wait_time)
                continue
            else:
                print(f"  ❌ Ошибка при настройке листа истории СПП: {e}")
                raise

    raise Exception(f"Не удалось настроить лист истории СПП после {MAX_QUOTA_RETRIES} попыток")


def save_spp_to_history(spreadsheet, campaigns_data: Dict, current_date_str: str):
    """Сохраняет текущие значения СПП в историю"""
    print("\n📊 СОХРАНЕНИЕ СПП В ИСТОРИЮ")

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            # Получаем или создаем лист истории СПП
            spp_history_sheet = setup_spp_history_sheet(spreadsheet)

            # Собираем данные СПП для всех товаров
            spp_data = {}
            for offer_id, campaigns_list in campaigns_data.items():
                if not campaigns_list:
                    continue

                first_campaign = campaigns_list[0]
                price_before = clean_numeric_value(first_campaign.get('product_price_before', 0))
                price_for_buyer = clean_numeric_value(first_campaign.get('product_price', 0))
                spp = calculate_spp(price_before, price_for_buyer)

                if spp > 0:  # Сохраняем только положительные значения
                    spp_data[offer_id] = spp

            if not spp_data:
                print("  ⚠️ Нет данных СПП для сохранения")
                return False

            # Получаем существующие данные
            existing_data = safe_get_values(spp_history_sheet)

            # Проверяем, есть ли уже колонка с этой датой
            date_column_index = None
            if len(existing_data) > 2:
                headers_row = existing_data[2] if len(existing_data) > 2 else []
                for idx, header in enumerate(headers_row):
                    if header and header.strip() == current_date_str:
                        date_column_index = idx
                        break

            # Если колонки нет - добавляем новую
            if date_column_index is None:
                # Находим первую свободную колонку после "Среднее значение СПП"
                insert_col_index = 3  # Колонка C (0-based индекс 2)

                # Проверяем существующие колонки с датами
                if len(existing_data) > 2:
                    for idx, header in enumerate(existing_data[2]):
                        if header and idx >= 2:  # Начинаем с колонки C
                            try:
                                # Пытаемся распарсить как дату
                                datetime.strptime(header, "%d.%m.%Y")
                                insert_col_index = idx + 1
                                break
                            except:
                                pass

                col_letter = get_column_letter(insert_col_index)

                # Вставляем новую колонку
                try:
                    spp_history_sheet.insert_cols(insert_col_index, 1)
                except TypeError:
                    try:
                        spp_history_sheet.insert_cols(insert_col_index)
                    except:
                        body = {
                            "requests": [{
                                "insertRange": {
                                    "range": {
                                        "sheetId": spp_history_sheet.id,
                                        "startColumnIndex": insert_col_index - 1,
                                        "endColumnIndex": insert_col_index,
                                        "startRowIndex": 0,
                                        "endRowIndex": spp_history_sheet.row_count
                                    },
                                    "shiftDimension": "COLUMNS"
                                }
                            }]
                        }
                        safe_api_call(spp_history_sheet.spreadsheet.batch_update, body)

                time.sleep(1)

                # Записываем заголовок даты
                safe_update_cell(spp_history_sheet, f"{col_letter}3", [[current_date_str]],
                                 value_input_option='USER_ENTERED')
                safe_format_range(
                    spp_history_sheet, f"{col_letter}3",
                    CellFormat(
                        textFormat=TextFormat(bold=True, fontSize=10),
                        backgroundColor=Color(0.9, 0.95, 0.9),
                        horizontalAlignment='CENTER'
                    )
                )
                safe_api_call(set_column_width, spp_history_sheet, col_letter, 100)
                date_column_index = insert_col_index - 1

                print(f"  🆕 Добавлена новая колонка для даты: {current_date_str} (столбец {col_letter})")

            col_letter = get_column_letter(date_column_index + 1)

            # Получаем существующие артикулы и их строки
            product_row_map = {}
            for row_idx, row in enumerate(existing_data[3:], start=4):  # Начинаем с 4 строки (после шапки и примечания)
                if row and len(row) > 0 and row[0] and row[0] != 'Артикул':
                    product_row_map[row[0]] = row_idx

            # Подготавливаем обновления
            batch_data = []
            current_row = 4

            for offer_id, spp_value in spp_data.items():
                if offer_id in product_row_map:
                    # Обновляем существующую строку
                    batch_data.append({
                        'range': f"{col_letter}{product_row_map[offer_id]}",
                        'values': [[round(spp_value, 2)]]
                    })
                else:
                    # Добавляем новую строку: артикул + значение СПП
                    row_data = [offer_id, spp_value]
                    safe_update_cell(spp_history_sheet, f"A{current_row}", [row_data],
                                     value_input_option='USER_ENTERED')
                    current_row += 1

            # Обновляем данные через batch_update
            if batch_data:
                for item in batch_data:
                    try:
                        safe_update_cell(spp_history_sheet, item['range'], item['values'],
                                         value_input_option='USER_ENTERED')
                        time.sleep(0.3)
                    except Exception as e:
                        print(f"  ⚠️ Ошибка обновления {item['range']}: {e}")

            # Обновляем среднее значение СПП для каждой строки
            # Получаем актуальные данные после всех обновлений
            updated_data = safe_get_values(spp_history_sheet)

            for row_idx in range(4, len(updated_data) + 1):
                if row_idx <= len(updated_data):
                    row_data = updated_data[row_idx - 1] if row_idx - 1 < len(updated_data) else []
                    if row_data and row_data[0] and row_data[0] != 'Артикул':
                        # Собираем все значения СПП начиная с колонки C (индекс 2)
                        spp_values = []
                        for col_idx in range(2, len(row_data)):
                            val = row_data[col_idx]
                            if val and val != '' and val != '—':
                                try:
                                    spp_values.append(float(val))
                                except:
                                    pass

                        if spp_values:
                            avg_spp = sum(spp_values) / len(spp_values)
                            # Обновляем колонку B (среднее значение)
                            safe_update_cell(spp_history_sheet, f"B{row_idx}", [[round(avg_spp, 2)]],
                                             value_input_option='USER_ENTERED')

            # Форматирование числовых колонок
            last_row = len(updated_data) + 5
            for col in [get_column_letter(date_column_index + 1), 'B']:
                try:
                    safe_format_range(
                        spp_history_sheet, f"{col}4:{col}{last_row}",
                        CellFormat(
                            numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'},
                            horizontalAlignment='CENTER'
                        )
                    )
                except:
                    pass

            print(f"  ✅ История СПП обновлена: {len(spp_data)} товаров за {current_date_str}")
            return True

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY * (attempt + 1)
                print(f"  ⏳ Квота API превышена. Пауза {wait_time} сек... (попытка {attempt + 1}/{MAX_QUOTA_RETRIES})")
                time.sleep(wait_time)
                continue
            else:
                print(f"  ❌ Ошибка при сохранении истории СПП: {e}")
                import traceback
                traceback.print_exc()
                return False

    return False

def safe_update_cell(sheet, range_name, values, value_input_option='USER_ENTERED', max_retries=MAX_QUOTA_RETRIES):
    """
    Безопасное обновление ячейки с обработкой 429
    """
    for attempt in range(max_retries):
        try:
            # Используем правильный порядок аргументов: values, range_name
            return sheet.update(values=values, range_name=range_name, value_input_option=value_input_option)
        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY * (attempt + 1) + random.uniform(0, 5)
                print(f"  ⏳ Квота API при update. Пауза {wait_time:.1f} сек... (попытка {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                raise
    raise Exception(f"Не удалось выполнить update после {max_retries} попыток")


def get_or_create_sheet(spreadsheet, title: str, rows=1000, cols=30):
    """Получает существующий лист или создает новый с обработкой 429"""
    title = title.strip()

    print(f"  🔍 Поиск листа: '{title}'")

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            # Список всех существующих листов
            all_sheets = spreadsheet.worksheets()
            print(f"  📋 Существующие листы: {[s.title for s in all_sheets]}")

            # Ищем точное совпадение
            for sheet in all_sheets:
                if sheet.title == title:
                    print(f"  ✅ Найден лист: '{title}'")
                    return sheet

            # Если не найден - создаем
            print(f"  🆕 Создание нового листа: '{title}'")
            new_sheet = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)

            # Проверяем, что создался именно тот лист
            time.sleep(2)
            created_sheet = spreadsheet.worksheet(title)
            print(f"  ✅ Создан лист: '{created_sheet.title}' (id: {created_sheet.id})")

            return created_sheet

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY * (attempt + 1) + random.uniform(0, 5)
                print(f"  ⏳ Квота API при получении/создании листа. Пауза {wait_time:.1f} сек... (попытка {attempt + 1}/{MAX_QUOTA_RETRIES})")
                time.sleep(wait_time)
                continue
            else:
                raise

    raise Exception(f"Не удалось получить/создать лист {title} после {MAX_QUOTA_RETRIES} попыток")

def format_numeric_columns(dashboard):
    """Форматирует числовые колонки DASHBOARD"""
    try:
        # Колонка B (Сумма продаж) - валюта
        safe_format_range(
            dashboard, "B:B",
            CellFormat(numberFormat={'type': 'CURRENCY', 'pattern': '#,##0.00 ₽'})
        )

        # Колонка C (Количество продаж) - целое число
        safe_format_range(
            dashboard, "C:C",
            CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0'})
        )

        # Колонки D, E, F (ДРР) - проценты
        for col in ['D', 'E', 'F']:
            safe_format_range(
                dashboard, f"{col}:{col}",
                CellFormat(numberFormat={'type': 'PERCENT', 'pattern': '#,##0.00%'})
            )

        # Колонка G (ЧП) - валюта
        safe_format_range(
            dashboard, "G:G",
            CellFormat(numberFormat={'type': 'CURRENCY', 'pattern': '#,##0.00 ₽'})
        )

        # Колонка H (СПП) - проценты
        safe_format_range(
            dashboard, "H:H",
            CellFormat(numberFormat={'type': 'PERCENT', 'pattern': '#,##0.00%'})
        )

        print("  ✅ Числовые колонки отформатированы")
    except Exception as e:
        print(f"  ⚠️ Ошибка форматирования: {e}")

# ================= УНИВЕРСАЛЬНЫЕ ФУНКЦИИ ДЛЯ ОБРАБОТКИ 429 =================

def safe_api_call(func, *args, max_retries=MAX_QUOTA_RETRIES, **kwargs):
    """
    Универсальная обертка для безопасного вызова API с обработкой ошибки 429.
    При ошибке 429 ждет с экспоненциальной задержкой и повторяет запрос.
    """
    last_exception = None

    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_str = str(e)
            last_exception = e

            # Проверяем на ошибку квоты 429
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY * (attempt + 1) + random.uniform(0, 5)
                print(f"  ⏳ Превышена квота API. Пауза {wait_time:.1f} сек... (попытка {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                # Другие ошибки не требуют повторных попыток
                raise

    # Если все попытки исчерпаны
    raise last_exception if last_exception else Exception(f"Не удалось выполнить операцию после {max_retries} попыток")


def safe_batch_update(sheet, batch_data, value_input_option='USER_ENTERED', max_retries=MAX_QUOTA_RETRIES):
    """
    Безопасное выполнение batch_update с обработкой 429
    Исправленная версия - правильный формат для gspread
    """
    # Дополнительная проверка: не пишем ли мы в первый лист?
    spreadsheet = sheet.spreadsheet
    all_sheets = spreadsheet.worksheets()
    if all_sheets and all_sheets[0].id == sheet.id:
        if sheet.title not in ["DASHBOARD", "ТЕХНИЧЕСКИЙ ЛИСТ", "ЧП_товары_общая", "ЧП_товары_ДРР", "История ДРР", "История DASHBOARD"]:
            print(f"  🚫 КРИТИЧЕСКАЯ ЗАЩИТА (batch_update): Попытка записи в первый лист '{sheet.title}'")
            print(f"     Данные не будут записаны. Блокировано {len(batch_data)} операций.")
            return None

    for attempt in range(max_retries):
        try:
            # Правильный формат для gspread batch_update
            body = {
                'valueInputOption': value_input_option,
                'data': batch_data
            }
            return sheet.spreadsheet.values_batch_update(body)
        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY * (attempt + 1) + random.uniform(0, 5)
                print(f"  ⏳ Квота API при batch_update. Пауза {wait_time:.1f} сек... (попытка {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                raise
    raise Exception(f"Не удалось выполнить batch_update после {max_retries} попыток")


def safe_get_values(sheet, range_name=None, max_retries=MAX_QUOTA_RETRIES):
    """
    Безопасное получение значений с обработкой 429
    """
    for attempt in range(max_retries):
        try:
            if range_name:
                return sheet.get(range_name)
            else:
                return sheet.get_all_values()
        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY * (attempt + 1) + random.uniform(0, 5)
                print(
                    f"  ⏳ Квота API при get_values. Пауза {wait_time:.1f} сек... (попытка {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                raise
    raise Exception(f"Не удалось получить значения после {max_retries} попыток")


def safe_format_range(sheet, range_name, cell_format, max_retries=MAX_QUOTA_RETRIES):
    """
    Безопасное форматирование диапазона с обработкой 429
    """
    for attempt in range(max_retries):
        try:
            return format_cell_range(sheet, range_name, cell_format)
        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY * (attempt + 1) + random.uniform(0, 5)
                print(
                    f"  ⏳ Квота API при format_range. Пауза {wait_time:.1f} сек... (попытка {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                raise
    raise Exception(f"Не удалось выполнить форматирование после {max_retries} попыток")


def update_total_chp_formula(sheet, start_date_col: int):
    """Обновляет формулу 'Общая сумма ЧП' для всех строк"""
    try:
        all_data = safe_get_values(sheet)
        if len(all_data) <= 1:
            return

        start_col_letter = get_column_letter(start_date_col)
        end_col_letter = get_column_letter(100)  # До колонки CV (100)

        num_rows = len(all_data)

        formulas = []
        for row_idx in range(2, num_rows + 1):
            formula = f"=СУММ({start_col_letter}{row_idx}:{end_col_letter}{row_idx})"
            formulas.append([formula])

        if formulas:
            range_to_fill = f"B2:B{num_rows}"
            safe_update_cell(sheet, range_to_fill, formulas, value_input_option='USER_ENTERED')
            print(f"  📊 Обновлена формула общей суммы ЧП для {len(formulas)} строк")

    except Exception as e:
        print(f"  ⚠️ Ошибка при обновлении формулы: {e}")


def add_date_column_to_chp_sheet(sheet, date_str: str) -> int:
    """Добавляет колонку с датой в лист ЧП и возвращает индекс колонки"""
    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            headers = sheet.row_values(1)
            date_column_index = None

            # Ищем существующую колонку с этой датой
            for idx, header in enumerate(headers):
                if header == date_str:
                    date_column_index = idx + 1
                    print(
                        f"  📅 Колонка с датой {date_str} существует (столбец {get_column_letter(date_column_index)}), будет обновлена")
                    return date_column_index

            # Если колонки нет - создаем новую
            insert_col_index = 3
            first_date_col = None

            for idx, header in enumerate(headers):
                if idx >= 2:
                    try:
                        datetime.strptime(header, "%d.%m.%Y")
                        first_date_col = idx + 1
                        break
                    except:
                        pass

            if first_date_col:
                insert_col_index = first_date_col

            col_letter = get_column_letter(insert_col_index)

            # Вставляем новую колонку с обработкой 429
            try:
                sheet.insert_cols(insert_col_index, 1)
            except TypeError:
                try:
                    sheet.insert_cols(insert_col_index)
                except:
                    body = {
                        "requests": [{
                            "insertRange": {
                                "range": {
                                    "sheetId": sheet.id,
                                    "startColumnIndex": insert_col_index - 1,
                                    "endColumnIndex": insert_col_index,
                                    "startRowIndex": 0,
                                    "endRowIndex": sheet.row_count
                                },
                                "shiftDimension": "COLUMNS"
                            }
                        }]
                    }
                    safe_api_call(sheet.spreadsheet.batch_update, body)

            time.sleep(2)

            # Записываем заголовок даты
            safe_update_cell(sheet, f"{col_letter}1", [[date_str]], value_input_option='USER_ENTERED')

            # Форматируем заголовок даты
            safe_format_range(
                sheet, f"{col_letter}1",
                CellFormat(
                    textFormat=TextFormat(bold=True, fontSize=10),
                    backgroundColor=Color(0.9, 0.95, 0.9),
                    horizontalAlignment='CENTER'
                )
            )

            safe_api_call(set_column_width, sheet, col_letter, 100)

            print(f"  📅 Добавлена колонка с датой {date_str} (столбец {col_letter})")

            update_total_chp_formula(sheet, insert_col_index)

            return insert_col_index

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY
                print(f"  ⏳ Квота API превышена. Пауза {wait_time} сек... (попытка {attempt + 1}/{MAX_QUOTA_RETRIES})")
                time.sleep(wait_time)
                continue
            else:
                print(f"  ❌ Ошибка: {e}")
                raise

    raise Exception(f"Не удалось добавить колонку с датой {date_str} после {MAX_QUOTA_RETRIES} попыток")


def get_or_create_chp_sheet(spreadsheet, config: Dict) -> gspread.Worksheet:
    """Создает или получает лист ЧП с правильной структурой"""
    sheet_title = config["sheet_name"]

    try:
        sheet = spreadsheet.worksheet(sheet_title)
        print(f"  📄 Лист {sheet_title} уже существует")
        return sheet
    except gspread.exceptions.WorksheetNotFound:
        print(f"  🆕 Создание нового листа {sheet_title}...")
        sheet = safe_api_call(spreadsheet.add_worksheet, title=sheet_title, rows=10000, cols=100)
        time.sleep(2)

        safe_update_cell(sheet, "A1", [["Артикул"]], value_input_option='USER_ENTERED')
        safe_update_cell(sheet, "B1", [["Общая сумма ЧП"]], value_input_option='USER_ENTERED')

        safe_format_range(
            sheet, "A1:B1",
            CellFormat(
                textFormat=TextFormat(bold=True, fontSize=11),
                backgroundColor=Color(0.85, 0.95, 0.85)
            )
        )

        safe_api_call(set_column_width, sheet, "A", 200)
        safe_api_call(set_column_width, sheet, "B", 150)
        safe_api_call(set_frozen, sheet, rows=1)

        print(f"  ✅ Лист {sheet_title} создан")
        return sheet


def load_markup_from_sheet(sheet) -> float:
    """Загружает наценки из листа 'Наценка за нелокальную доставку' и возвращает среднее значение"""
    try:
        all_values = safe_get_values(sheet)

        markups = []
        print("  📊 Загрузка наценок из листа:")

        for row_idx, row in enumerate(all_values):
            if not row or len(row) < 2:
                continue

            first_cell = str(row[0]).strip() if row[0] else ""
            second_cell = str(row[1]).strip().replace(',', '.').replace('%', '') if row[1] else ""

            if not first_cell or first_cell == '':
                continue
            if first_cell.startswith('💰') or first_cell.startswith('💡'):
                continue
            if first_cell == "Область" or first_cell == MARKUP_CONFIG["headers"][0]["name"]:
                continue

            if second_cell and second_cell != '':
                try:
                    markup = float(second_cell)
                    markups.append(markup)
                    print(f"     - {first_cell}: {markup}%")
                except (ValueError, TypeError):
                    print(f"     ⚠️ Не удалось распарсить значение для {first_cell}: {second_cell}")
                    continue

        if markups:
            avg_markup = sum(markups) / len(markups)
            print(f"  📊 Загружено {len(markups)} областей, средняя наценка: {avg_markup:.2f}%")
            return round(avg_markup, 2)

        print("  ⚠️ Данные о наценке не найдены, используется значение по умолчанию: 8%")
        return 8.0

    except Exception as e:
        print(f"  ⚠️ Ошибка при загрузке наценки: {e}, используется значение по умолчанию: 8%")
        return 8.0


def get_markup_percent(spreadsheet) -> float:
    """Получает средний процент наценки из листа "Наценка за нелокальную доставку" """
    try:
        markup_sheet = setup_markup_sheet(spreadsheet)
        return load_markup_from_sheet(markup_sheet)
    except Exception as e:
        print(f"  ⚠️ Ошибка при получении наценки: {e}")
        return 8.0


def execute_with_quota_retry(func, *args, max_retries=15, **kwargs):
    """Выполняет функцию с повторными попытками при превышении квоты (429)"""
    return safe_api_call(func, *args, max_retries=max_retries, **kwargs)


def batch_update_sheet(sheet, data: List[List], start_cell: str = "A1"):
    """Массовое обновление данных в листе одной операцией"""
    if not data:
        return

    try:
        end_row = start_cell[0] + str(int(start_cell[1:]) + len(data) - 1)
        end_col = get_column_letter(len(data[0]))
        range_name = f"{start_cell}:{end_col}{int(start_cell[1:]) + len(data) - 1}"

        safe_update_cell(sheet, range_name, data, value_input_option='USER_ENTERED')
        print(f"  ✅ Записано {len(data)} строк данных в {range_name}")
        time.sleep(1)
    except Exception as e:
        print(f"  ❌ Ошибка при массовой записи: {e}")
        raise


def setup_drr_total_sheet(spreadsheet):
    """Настраивает лист ДРР ОБЩИЙ"""
    print("\n📊 НАСТРОЙКА ЛИСТА ДРР ОБЩИЙ")
    drr_sheet = get_or_create_sheet(spreadsheet, DRR_TOTAL_CONFIG["sheet_name"], rows=10000, cols=100)
    all_values = safe_get_values(drr_sheet)

    if len(all_values) < 2 or (len(all_values) > 0 and all_values[0][0] != "Артикул"):
        print("  🆕 Настройка структуры листа ДРР ОБЩИЙ...")
        safe_api_call(drr_sheet.clear)
        time.sleep(1)

        safe_update_cell(drr_sheet, "A1", [[DRR_TOTAL_CONFIG["headers"][0]["name"]]], value_input_option='USER_ENTERED')
        safe_format_range(
            drr_sheet, "A1",
            CellFormat(textFormat=TextFormat(bold=True, fontSize=11), backgroundColor=Color(0.85, 0.95, 0.85))
        )

        safe_api_call(set_column_width, drr_sheet, "A", DRR_TOTAL_CONFIG["headers"][0]["width"])

        safe_update_cell(drr_sheet, "A2", [[DRR_TOTAL_CONFIG["note"]]], value_input_option='USER_ENTERED')
        safe_format_range(
            drr_sheet, "A2",
            CellFormat(textFormat=TextFormat(italic=True, fontSize=9), backgroundColor=Color(0.95, 0.95, 0.9))
        )

        safe_api_call(set_frozen, drr_sheet, rows=2)
        print("  ✅ Лист ДРР ОБЩИЙ настроен")
    else:
        print("  📄 Лист ДРР ОБЩИЙ уже существует")

    return drr_sheet


def update_drr_total_sheet_from_dashboard(spreadsheet, dashboard, current_date_str):
    """Обновляет лист ДРР ОБЩИЙ на основе ТЕКУЩИХ данных из DASHBOARD"""
    print("\n📊 ОБНОВЛЕНИЕ ЛИСТА ДРР ОБЩИЙ (на основе текущего DASHBOARD)")

    try:
        dashboard_data = safe_get_values(dashboard)

        if len(dashboard_data) <= 1:
            print("  ⚠️ Нет данных в DASHBOARD")
            return False

        drr_data = {}
        all_products = []

        for row in dashboard_data[1:]:
            if not row or len(row) < 6:
                continue

            if row[0] == "ИТОГО" or row[0] == "":
                continue

            product = row[0].strip()
            drr_raw = row[5] if len(row) > 5 else 0

            # Заменяем None, '-', '', '—' на 0
            if drr_raw is None or drr_raw == '-' or drr_raw == '' or drr_raw == '—':
                drr_total = 0.0
            else:
                drr_total = clean_numeric_value(drr_raw)

            if product and product != "":
                drr_data[product] = drr_total
                all_products.append(product)

        if not drr_data:
            print("  ⚠️ Нет данных для отображения")
            return False

        sorted_products = sorted(all_products)
        drr_sheet = setup_drr_total_sheet(spreadsheet)
        existing_data = safe_get_values(drr_sheet)

        # Ищем существующую колонку с этой датой (в строке 3)
        date_column_index = None
        if len(existing_data) > 2:
            headers_row = existing_data[2] if len(existing_data) > 2 else []
            for idx, header in enumerate(headers_row):
                if header and header.strip() == current_date_str:
                    date_column_index = idx
                    break

        # Если колонки нет - вставляем новую колонку на позицию 2 (после Артикула, без пустой колонки)
        if date_column_index is None:
            # Вставляем новую колонку на позицию 2 (сразу после Артикула)
            insert_col_index = 2
            col_letter = get_column_letter(insert_col_index)

            # Вставляем новую колонку на позицию 2
            try:
                drr_sheet.insert_cols(insert_col_index, 1)
            except TypeError:
                try:
                    drr_sheet.insert_cols(insert_col_index)
                except:
                    body = {
                        "requests": [{
                            "insertRange": {
                                "range": {
                                    "sheetId": drr_sheet.id,
                                    "startColumnIndex": insert_col_index - 1,
                                    "endColumnIndex": insert_col_index,
                                    "startRowIndex": 0,
                                    "endRowIndex": drr_sheet.row_count
                                },
                                "shiftDimension": "COLUMNS"
                            }
                        }]
                    }
                    safe_api_call(drr_sheet.spreadsheet.batch_update, body)

            time.sleep(1)

            # Записываем заголовок даты
            safe_update_cell(drr_sheet, f"{col_letter}3", [[current_date_str]], value_input_option='USER_ENTERED')
            safe_format_range(
                drr_sheet, f"{col_letter}3",
                CellFormat(textFormat=TextFormat(bold=True, fontSize=10),
                           backgroundColor=Color(0.9, 0.95, 0.9),
                           horizontalAlignment='CENTER')
            )
            safe_api_call(set_column_width, drr_sheet, col_letter, 100)
            date_column_index = insert_col_index - 1  # Индекс после вставки (0-based)

            print(f"  🆕 Добавлена новая колонка для даты: {current_date_str} (столбец {col_letter})")

        col_letter = get_column_letter(date_column_index + 1)

        # Подготавливаем обновления
        batch_data = []
        current_row = 4

        for product in sorted_products:
            product_row_index = None
            for row_idx in range(4, len(existing_data) + 1):
                if row_idx <= len(existing_data):
                    row_data = existing_data[row_idx - 1] if row_idx - 1 < len(existing_data) else []
                    if row_data and row_data[0] == product:
                        product_row_index = row_idx
                        break

            if product_row_index:
                # Всегда записываем значение (0 если было None)
                value = round(drr_data[product], 2)
                batch_data.append({
                    'range': f"{col_letter}{product_row_index}",
                    'values': [[value]]
                })
            else:
                # Новая строка: только артикул и значение ДРР (без пустой колонки)
                row_data = [product, round(drr_data[product], 2)]
                safe_update_cell(drr_sheet, f"A{current_row}", [row_data], value_input_option='USER_ENTERED')
                current_row += 1

        # Обновляем данные через batch_update
        if batch_data:
            for item in batch_data:
                try:
                    safe_update_cell(drr_sheet, item['range'], item['values'], value_input_option='USER_ENTERED')
                    time.sleep(0.3)
                except Exception as e:
                    print(f"  ⚠️ Ошибка обновления {item['range']}: {e}")

        safe_format_range(
            drr_sheet, f"{col_letter}4:{col_letter}{current_row}",
            CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'},
                       horizontalAlignment='CENTER')
        )

        print(f"  ✅ Лист ДРР ОБЩИЙ обновлен (дата: {current_date_str})")
        print(f"     - Артикулов: {len(sorted_products)}")
        print(f"     - Пустые значения заменены на 0")
        print(f"     - Даты: новые даты добавляются слева")

        return True

    except Exception as e:
        print(f"  ❌ Ошибка при обновлении ДРР ОБЩИЙ: {e}")
        import traceback
        traceback.print_exc()
        return False


def setup_markup_sheet(spreadsheet):
    """Настраивает лист Наценка за нелокальную доставку"""
    print("\n💰 НАСТРОЙКА ЛИСТА НАЦЕНКИ ЗА НЕЛОКАЛЬНУЮ ДОСТАВКУ")

    sheet_title = MARKUP_CONFIG["sheet_name"]

    try:
        sheet = spreadsheet.worksheet(sheet_title)
        print(f"  📄 Лист '{sheet_title}' уже существует")
        return sheet

    except gspread.exceptions.WorksheetNotFound:
        print(f"  🆕 Создание листа '{sheet_title}'...")

        sheet = safe_api_call(spreadsheet.add_worksheet, title=sheet_title, rows=100, cols=5)
        time.sleep(2)

        safe_update_cell(sheet, "A1", [["💰 НАЦЕНКА ЗА НЕЛОКАЛЬНУЮ ДОСТАВКУ"]], value_input_option='USER_ENTERED')
        safe_format_range(
            sheet, "A1:B1",
            CellFormat(textFormat=TextFormat(bold=True, fontSize=12), backgroundColor=Color(0.8, 0.9, 1))
        )

        headers = [MARKUP_CONFIG["headers"][0]["name"], MARKUP_CONFIG["headers"][1]["name"]]
        safe_update_cell(sheet, "A2", [headers], value_input_option='USER_ENTERED')
        safe_format_range(
            sheet, "A2:B2",
            CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
        )

        safe_update_cell(sheet, "A3", MARKUP_CONFIG["default_data"], value_input_option='USER_ENTERED')

        for row_idx in range(3, 3 + len(MARKUP_CONFIG["default_data"])):
            safe_format_range(
                sheet, f"A{row_idx}:B{row_idx}",
                CellFormat(textFormat=TextFormat(bold=False), backgroundColor=Color(0.95, 0.95, 0.9))
            )

        note_row = 3 + len(MARKUP_CONFIG["default_data"]) + 1
        safe_update_cell(sheet, f"A{note_row}", [[MARKUP_CONFIG["note"]]], value_input_option='USER_ENTERED')
        end_col = get_column_letter(len(MARKUP_CONFIG["headers"]))
        safe_format_range(
            sheet, f"A{note_row}:{end_col}{note_row}",
            CellFormat(textFormat=TextFormat(italic=True, fontSize=10), backgroundColor=Color(0.95, 0.95, 0.9))
        )

        safe_api_call(set_column_width, sheet, "A", MARKUP_CONFIG["headers"][0]["width"])
        safe_api_call(set_column_width, sheet, "B", MARKUP_CONFIG["headers"][1]["width"])

        print("  ✅ Лист наценки создан и настроен")
        print(f"     📍 Данные по умолчанию: Москва=8%, Санкт-Петербург=8%, Дальний Восток=8%")

        return sheet


def setup_logistics_price_sheet(spreadsheet):
    """Настраивает лист Стоимость логистики с тремя столбцами"""
    print("\n📦 НАСТРОЙКА ЛИСТА СТОИМОСТИ ЛОГИСТИКИ")

    sheet_title = LOGISTICS_PRICE_CONFIG["sheet_name"]

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            # Пытаемся получить существующий лист
            try:
                sheet = spreadsheet.worksheet(sheet_title)
                print("  📄 Лист стоимости логистики уже существует")
                return sheet
            except gspread.exceptions.WorksheetNotFound:
                print("  🆕 Создание листа стоимости логистики...")

                sheet = safe_api_call(spreadsheet.add_worksheet, title=sheet_title, rows=100, cols=10)
                time.sleep(2)

                safe_api_call(sheet.update, "A1", [["📊 ТАБЛИЦА СТОИМОСТИ ЛОГИСТИКИ ОЗОН"]], value_input_option='USER_ENTERED')
                headers = [h['name'] for h in LOGISTICS_PRICE_CONFIG["headers"]]
                safe_api_call(sheet.update, "A2", [headers], value_input_option='USER_ENTERED')
                safe_api_call(sheet.update, "A3", LOGISTICS_PRICE_CONFIG["default_data"], value_input_option='USER_ENTERED')

                note_row = 3 + len(LOGISTICS_PRICE_CONFIG["default_data"]) + 1
                safe_api_call(sheet.update, f"A{note_row}", [[LOGISTICS_PRICE_CONFIG["note"]]],
                             value_input_option='USER_ENTERED')

                time.sleep(1)

                safe_format_range(
                    sheet, "A1:C1",
                    CellFormat(textFormat=TextFormat(bold=True, fontSize=12), backgroundColor=Color(0.8, 0.9, 1))
                )

                safe_format_range(
                    sheet, "A2:C2",
                    CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
                )

                safe_format_range(
                    sheet, f"A{note_row}:C{note_row}",
                    CellFormat(textFormat=TextFormat(italic=True, fontSize=10), backgroundColor=Color(0.95, 0.95, 0.9))
                )

                for idx, header in enumerate(LOGISTICS_PRICE_CONFIG["headers"], start=1):
                    col_letter = get_column_letter(idx)
                    if 'width' in header:
                        safe_api_call(set_column_width, sheet, col_letter, header['width'])

                print("  ✅ Лист стоимости логистики создан и настроен")
                return sheet

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY * (attempt + 1) + random.uniform(0, 5)
                print(f"  ⏳ Квота API при настройке листа логистики. Пауза {wait_time:.1f} сек... (попытка {attempt + 1}/{MAX_QUOTA_RETRIES})")
                time.sleep(wait_time)
                continue
            else:
                print(f"  ❌ Ошибка при настройке листа логистики: {e}")
                raise

    raise Exception(f"Не удалось настроить лист логистики после {MAX_QUOTA_RETRIES} попыток")


def load_logistics_prices_from_sheet(sheet) -> Dict:
    """Загружает стоимость логистики из отдельного листа"""
    logistics_prices = {'under_300': [], 'over_300': []}
    try:
        all_values = safe_get_values(sheet)
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
    """Настраивает Технический лист (создает если нет) с обработкой 429"""
    print("\n📋 НАСТРОЙКА ТЕХНИЧЕСКОГО ЛИСТА")

    sheet_title = TECHNICAL_SHEET_CONFIG["sheet_name"]

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            try:
                tech_sheet = spreadsheet.worksheet(sheet_title)
                print("  📄 Технический лист уже существует")

                # Проверяем и добавляем недостающие столбцы, если лист существует
                all_values = safe_get_values(tech_sheet)
                if len(all_values) > 0 and len(all_values[0]) < len(TECHNICAL_SHEET_CONFIG["products_headers"]):
                    print("  🔧 Добавление недостающих столбцов...")
                    # Находим блок товаров
                    products_start_row = 8
                    for idx, row in enumerate(all_values):
                        if row and len(row) > 0 and "ТОВАРЫ В ПРОДАЖЕ" in str(row[0]):
                            products_start_row = idx + 3
                            break

                    # Добавляем новые заголовки
                    headers_row = products_start_row + 1
                    current_headers = all_values[headers_row - 1] if len(all_values) >= headers_row else []

                    if len(current_headers) < len(TECHNICAL_SHEET_CONFIG["products_headers"]):
                        # Добавляем недостающие заголовки
                        for i in range(len(current_headers), len(TECHNICAL_SHEET_CONFIG["products_headers"])):
                            col_letter = get_column_letter(i + 1)
                            header_name = TECHNICAL_SHEET_CONFIG["products_headers"][i]["name"]
                            safe_update_cell(tech_sheet, f"{col_letter}{headers_row}", [[header_name]],
                                             value_input_option='USER_ENTERED')

                            # Устанавливаем ширину
                            if 'width' in TECHNICAL_SHEET_CONFIG["products_headers"][i]:
                                safe_api_call(set_column_width, tech_sheet, col_letter,
                                              TECHNICAL_SHEET_CONFIG["products_headers"][i]['width'])

                            time.sleep(0.5)
                        print("  ✅ Недостающие столбцы добавлены")

                all_values = safe_get_values(tech_sheet)
                products_start_row = 8
                for idx, row in enumerate(all_values):
                    if row and len(row) > 0 and "ТОВАРЫ В ПРОДАЖЕ" in str(row[0]):
                        products_start_row = idx + 3
                        break

                print(f"  📍 Строка начала данных товаров: {products_start_row}")
                return tech_sheet, products_start_row + 2

            except gspread.exceptions.WorksheetNotFound:
                print("  🆕 Технический лист не найден, создаем новый...")

                tech_sheet = safe_api_call(spreadsheet.add_worksheet, title=sheet_title, rows=5000, cols=50)
                time.sleep(2)
                safe_api_call(tech_sheet.clear)
                time.sleep(1)

                # Блок настроек
                print("  🔍 Настройка блока настроек...")

                safe_update_cell(tech_sheet, "A1", [["⚙️ НАСТРОЙКИ КАЛЬКУЛЯТОРА"]], value_input_option='USER_ENTERED')
                safe_format_range(
                    tech_sheet, "A1:C1",
                    CellFormat(textFormat=TextFormat(bold=True, fontSize=12), backgroundColor=Color(0.8, 0.9, 1))
                )

                settings_headers = [h['name'] for h in TECHNICAL_SHEET_CONFIG["settings_headers"]]
                safe_update_cell(tech_sheet, "A2", [settings_headers], value_input_option='USER_ENTERED')
                safe_format_range(
                    tech_sheet, "A2:C2",
                    CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
                )

                settings_data = [
                    ["Ставка УСН + НДС (%)", technical_settings['tax_rate'], "%"],
                    ["Эквайринг (%)", technical_settings['acquiring_rate'], "%"],
                    ["Локальные продажи (%)", "87", "%"]
                ]
                safe_update_cell(tech_sheet, "A3", settings_data, value_input_option='USER_ENTERED')

                # Установка ширины колонок настроек
                for idx, header in enumerate(TECHNICAL_SHEET_CONFIG["settings_headers"], start=1):
                    col_letter = get_column_letter(idx)
                    if 'width' in header:
                        safe_api_call(set_column_width, tech_sheet, col_letter, header['width'])

                time.sleep(1)

                # Блок товаров
                products_start_row = 8
                print("  🔍 Настройка блока товаров...")

                safe_update_cell(tech_sheet, f"A{products_start_row}", [["📊 ТОВАРЫ В ПРОДАЖЕ"]],
                                 value_input_option='USER_ENTERED')
                safe_format_range(
                    tech_sheet, f"A{products_start_row}:K{products_start_row}",  # Изменено с I на K
                    CellFormat(textFormat=TextFormat(bold=True, fontSize=12), backgroundColor=Color(0.8, 0.9, 1))
                )

                products_headers = [h['name'] for h in TECHNICAL_SHEET_CONFIG["products_headers"]]
                safe_update_cell(tech_sheet, f"A{products_start_row + 1}", [products_headers],
                                 value_input_option='USER_ENTERED')

                end_col = get_column_letter(len(products_headers))
                safe_format_range(
                    tech_sheet, f"A{products_start_row + 1}:{end_col}{products_start_row + 1}",
                    CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
                )

                # Установка ширины колонок товаров
                for idx, header in enumerate(TECHNICAL_SHEET_CONFIG["products_headers"], start=1):
                    col_letter = get_column_letter(idx)
                    if 'width' in header:
                        safe_api_call(set_column_width, tech_sheet, col_letter, header['width'])

                safe_api_call(set_frozen, tech_sheet, rows=products_start_row + 1)

                # Примечание
                note_row = products_start_row + 2
                safe_update_cell(tech_sheet, f"A{note_row}", [[
                    "💡 Примечание: Стоимость логистики берется из листа 'Стоимость логистики'. "
                    "СПП (скидка постоянного покупателя) и себестоимость добавлены автоматически."
                ]], value_input_option='USER_ENTERED')
                safe_format_range(
                    tech_sheet, f"A{note_row}:{end_col}{note_row}",
                    CellFormat(textFormat=TextFormat(italic=True, fontSize=9), backgroundColor=Color(0.95, 0.95, 0.9))
                )

                print("  ✅ Технический лист создан и настроен")
                return tech_sheet, products_start_row + 2

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY * (attempt + 1) + random.uniform(0, 5)
                print(
                    f"  ⏳ Квота API при настройке технического листа. Пауза {wait_time:.1f} сек... (попытка {attempt + 1}/{MAX_QUOTA_RETRIES})")
                time.sleep(wait_time)
                continue
            else:
                print(f"  ❌ Ошибка при настройке технического листа: {e}")
                raise

    raise Exception(f"Не удалось настроить технический лист после {MAX_QUOTA_RETRIES} попыток")


def update_technical_sheet(tech_sheet, campaigns_data: Dict, products_start_row: int, logistics_price_sheet):
    """Обновляет данные в Техническом листе"""
    print("\n📊 ОБНОВЛЕНИЕ ТЕХНИЧЕСКОГО ЛИСТА")

    logistics_prices = load_logistics_prices_from_sheet(logistics_price_sheet)

    try:
        tax_rate_cell = safe_api_call(tech_sheet.acell, 'B3').value
        if tax_rate_cell and str(tax_rate_cell) not in ['—', '']:
            technical_settings['tax_rate'] = float(tax_rate_cell)
        acquiring_rate_cell = safe_api_call(tech_sheet.acell, 'B4').value
        if acquiring_rate_cell and str(acquiring_rate_cell) not in ['—', '']:
            technical_settings['acquiring_rate'] = float(acquiring_rate_cell)
    except Exception as e:
        print(f"  ⚠️ Ошибка чтения настроек: {e}")

    print(
        f"  ⚙️ Используемые настройки: налог={technical_settings['tax_rate']}%, эквайринг={technical_settings['acquiring_rate']}%")

    try:
        all_values = safe_get_values(tech_sheet)
        total_rows = len(all_values)
        if total_rows >= products_start_row:
            end_col = get_column_letter(len(TECHNICAL_SHEET_CONFIG["products_headers"]))
            clear_range = f"A{products_start_row}:{end_col}{total_rows + 10}"
            safe_api_call(tech_sheet.batch_clear, [clear_range])
            print(f"  ✅ Очищена область {clear_range}")
        time.sleep(1)
    except Exception as e:
        print(f"  ⚠️ Ошибка при очистке: {e}")

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
        product_volume = clean_numeric_value(first_campaign.get('item_volume_l', 0))
        logistics_cost = get_logistics_price_by_volume(product_volume, product_price, logistics_prices)
        acquiring_fee = round(product_price_before * (technical_settings['acquiring_rate'] / 100), 2)
        products_data.append(
            [offer_id, sku, product_price_before, product_price, acquiring_fee, stock_balance, commission_fbo,
             product_volume, logistics_cost])
        print(f"  📦 {offer_id}: цена до скидки={product_price_before}, цена для покупателя={product_price}")

    if products_data:
        print(f"  📝 Добавление данных для {len(products_data)} товаров...")
        safe_update_cell(tech_sheet, f"A{products_start_row}", products_data, value_input_option='USER_ENTERED')
        print(f"  ✅ Добавлено {len(products_data)} товаров")

        last_row = products_start_row + len(products_data) - 1
        for col, color, bold in [('C', Color(0.95, 0.9, 1), True), ('D', Color(0.9, 1, 0.9), True),
                                 ('E', Color(1, 0.95, 0.8), False), ('I', Color(0.7, 0.85, 1), False)]:
            try:
                fmt = CellFormat(backgroundColor=color)
                if bold:
                    fmt.textFormat = TextFormat(bold=True)
                safe_format_range(tech_sheet, f"{col}{products_start_row}:{col}{last_row}", fmt)
            except:
                pass
        print("  💡 Примечание: Эквайринг рассчитывается автоматически")
        print("  💡 Стоимость логистики берется из листа 'Стоимость логистики'")
    print("  ✅ Технический лист обновлен")


# ================= НОВЫЙ ФУНКЦИОНАЛ ПО ТЗ =================

def get_commission_rate(commission_str: str) -> float:
    """
    Парсит комиссию FBO из строки вида:
    - "39" (просто число)
    - "1 588–2 179 43%–59%" (диапазон)
    Возвращает комиссию в процентах
    """
    if not commission_str or commission_str == '—':
        return 0

    try:
        import re

        cleaned = str(commission_str).strip()
        number_cleaned = cleaned.replace(' ', '').replace('%', '')

        if number_cleaned.isdigit():
            return float(number_cleaned)

        percent_match = re.search(r'(\d+)%[–-](\d+)%', cleaned)
        if percent_match:
            min_percent = float(percent_match.group(1))
            max_percent = float(percent_match.group(2))
            return round((min_percent + max_percent) / 2, 2)

        numbers = re.findall(r'\d+', cleaned)
        if numbers and len(numbers) >= 2:
            return round((float(numbers[-2]) + float(numbers[-1])) / 2, 2)

        if numbers:
            return float(numbers[0])

        return 0
    except Exception as e:
        print(f"  ⚠️ Ошибка парсинга комиссии: {commission_str} - {e}")
        return 0


def calculate_logistics_cost(volume_l: float, price_before: float, logistics_prices: Dict,
                             local_sales_percent: float, markup_percent: float) -> float:
    """Рассчитывает стоимость логистики"""
    if volume_l <= 0 or price_before <= 0:
        return 0

    non_local_ratio = (100 - local_sales_percent) / 1000
    avg_markup = markup_percent / 100

    rules = logistics_prices.get('over_300', [])
    base_rate = 56

    if rules:
        for rule in rules:
            if rule['min'] <= volume_l <= rule['max']:
                base_rate = rule['price']
                break

        if volume_l < rules[0]['min']:
            base_rate = rules[0]['price']
        elif volume_l > rules[-1]['max']:
            base_rate = rules[-1]['price']

    markup = avg_markup * non_local_ratio * price_before
    total_logistics = base_rate + markup

    return round(total_logistics, 2)


def calculate_spp(price_before: float, price_for_buyer: float) -> float:
    """Рассчитывает СПП (скидка постоянного покупателя)"""
    if price_before <= 0:
        return 0
    spp = (1 - (price_for_buyer / price_before)) * 100
    return round(spp, 2)


def calculate_tax(price_for_buyer: float, tax_rate: float) -> float:
    """Рассчитывает налог"""
    return round(price_for_buyer * (tax_rate / 100), 2)


def calculate_acquiring(price_before: float, acquiring_rate: float) -> float:
    """Рассчитывает эквайринг"""
    return round(price_before * (acquiring_rate / 100), 2)


def calculate_chp(price_before: float, commission_percent: float, logistics: float,
                  tax: float, cost_price: float, acquiring: float, drr_percent: float,
                  offer_id: str = None, verbose: bool = False) -> float:
    """
    Рассчитывает чистую прибыль (ЧП) с детальным логированием

    Формула:
    ЧП = X – (X * Y/100) – L – N – S – E – (X * ДРР_общ/100)

    где:
    X = price_before (цена до скидки)
    Y = commission_percent (комиссия FBO в процентах)
    L = logistics (стоимость логистики)
    N = tax (налог)
    S = cost_price (себестоимость)
    E = acquiring (эквайринг)
    ДРР_общ = drr_percent (общий ДРР в процентах)
    """
    commission_amount = price_before * (commission_percent / 100)
    drr_amount = price_before * (drr_percent / 100)
    chp = price_before - commission_amount - logistics - tax - cost_price - acquiring - drr_amount

    if verbose or offer_id:
        print(f"\n  🔍 ДЕТАЛЬНЫЙ РАСЧЕТ ЧП для {offer_id if offer_id else 'товара'}:")
        print(f"     ┌─────────────────────────────────────────────────────────────")
        print(f"     │ X (Цена до скидки):                    {price_before:>12.2f} руб")
        print(f"     │")
        print(f"     │ ВЫЧИТАЕМ:")
        print(f"     │   Y (Комиссия FBO) {commission_percent:>5}%:          - {commission_amount:>12.2f} руб")
        print(f"     │   L (Логистика):                         - {logistics:>12.2f} руб")
        print(f"     │   N (Налог):                             - {tax:>12.2f} руб")
        print(f"     │   S (Себестоимость):                     - {cost_price:>12.2f} руб")
        print(f"     │   E (Эквайринг):                         - {acquiring:>12.2f} руб")
        print(f"     │   ДРР_общ ({drr_percent:>5}%):            - {drr_amount:>12.2f} руб")
        print(f"     ├─────────────────────────────────────────────────────────────")
        print(
            f"     │ ИТОГО ВЫЧЕТЫ:                           - {commission_amount + logistics + tax + cost_price + acquiring + drr_amount:>12.2f} руб")
        print(f"     │")
        print(f"     │ ЧП = X - ВЫЧЕТЫ:                        = {chp:>12.2f} руб")
        print(f"     └─────────────────────────────────────────────────────────────")

    return round(chp, 2)


def setup_chp_common_sheet(spreadsheet):
    """Настраивает страницу ЧП_товары_общая"""
    print("\n💰 НАСТРОЙКА ЛИСТА ЧП_товары_общая")

    sheet_title = CHP_COMMON_CONFIG["sheet_name"]

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            # Сначала проверяем, существует ли лист
            try:
                sheet = spreadsheet.worksheet(sheet_title)
                print(f"  📄 Лист {sheet_title} уже существует")
                return sheet
            except gspread.exceptions.WorksheetNotFound:
                print(f"  🆕 Создание нового листа {sheet_title}...")

                # УМЕНЬШАЕМ РАЗМЕР: 5000 строк, 20 колонок = 100,000 ячеек вместо 500,000
                sheet = safe_api_call(spreadsheet.add_worksheet, title=sheet_title, rows=5000, cols=20)
                time.sleep(2)

                # Формируем заголовки
                safe_update_cell(sheet, "A1", [CHP_COMMON_CONFIG["headers"]], value_input_option='USER_ENTERED')

                end_col = get_column_letter(len(CHP_COMMON_CONFIG["headers"]))
                safe_format_range(
                    sheet, f"A1:{end_col}1",
                    CellFormat(textFormat=TextFormat(bold=True, fontSize=11),
                               backgroundColor=Color(0.85, 0.95, 0.85))
                )

                # Устанавливаем ширину колонок
                for idx, header in enumerate(CHP_COMMON_CONFIG["headers"], start=1):
                    col_letter = get_column_letter(idx)
                    try:
                        safe_api_call(set_column_width, sheet, col_letter, 150 if idx == 0 else 120)
                    except:
                        pass

                safe_api_call(set_frozen, sheet, rows=1)
                print("  ✅ Лист ЧП_товары_общая создан и настроен")
                return sheet

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY
                print(f"  ⏳ Квота API превышена. Пауза {wait_time} сек... (попытка {attempt + 1}/{MAX_QUOTA_RETRIES})")
                time.sleep(wait_time)
                continue
            elif '10000000 cells' in error_str:
                # Специальная обработка: пробуем создать лист с МЕНЬШИМ размером
                print(f"  ⚠️ Лимит ячеек превышен для 5000x20, пробуем 2000x15...")
                try:
                    sheet = spreadsheet.add_worksheet(title=sheet_title, rows=2000, cols=15)
                    time.sleep(2)

                    safe_update_cell(sheet, "A1", [CHP_COMMON_CONFIG["headers"]], value_input_option='USER_ENTERED')

                    end_col = get_column_letter(len(CHP_COMMON_CONFIG["headers"]))
                    safe_format_range(
                        sheet, f"A1:{end_col}1",
                        CellFormat(textFormat=TextFormat(bold=True, fontSize=11),
                                   backgroundColor=Color(0.85, 0.95, 0.85))
                    )

                    safe_api_call(set_frozen, sheet, rows=1)
                    print("  ✅ Лист ЧП_товары_общая создан с уменьшенным размером")
                    return sheet
                except Exception as e2:
                    print(f"  ❌ Не удалось создать лист даже с уменьшенным размером: {e2}")
                    raise
            else:
                print(f"  ❌ Ошибка при настройке ЧП_товары_общая: {e}")
                raise

    raise Exception(f"Не удалось создать лист {sheet_title} после {MAX_QUOTA_RETRIES} попыток")


def setup_chp_drr_sheet(spreadsheet):
    """Настраивает страницу ЧП_товары_ДРР"""
    print("\n💰 НАСТРОЙКА ЛИСТА ЧП_товары_ДРР")

    sheet_title = CHP_DRR_CONFIG["sheet_name"]

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            # Сначала проверяем, существует ли лист
            try:
                sheet = spreadsheet.worksheet(sheet_title)
                print(f"  📄 Лист {sheet_title} уже существует")
                return sheet
            except gspread.exceptions.WorksheetNotFound:
                print(f"  🆕 Создание нового листа {sheet_title}...")

                # УМЕНЬШАЕМ РАЗМЕР: 5000 строк, 20 колонок
                sheet = safe_api_call(spreadsheet.add_worksheet, title=sheet_title, rows=5000, cols=20)
                time.sleep(2)

                # Формируем заголовки
                safe_update_cell(sheet, "A1", [CHP_DRR_CONFIG["headers"]], value_input_option='USER_ENTERED')

                end_col = get_column_letter(len(CHP_DRR_CONFIG["headers"]))
                safe_format_range(
                    sheet, f"A1:{end_col}1",
                    CellFormat(textFormat=TextFormat(bold=True, fontSize=11),
                               backgroundColor=Color(0.85, 0.95, 0.85))
                )

                # Устанавливаем ширину колонок
                for idx, header in enumerate(CHP_DRR_CONFIG["headers"], start=1):
                    col_letter = get_column_letter(idx)
                    try:
                        safe_api_call(set_column_width, sheet, col_letter, 150 if idx == 0 else 120)
                    except:
                        pass

                safe_api_call(set_frozen, sheet, rows=1)
                print("  ✅ Лист ЧП_товары_ДРР создан и настроен")
                return sheet

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY
                print(f"  ⏳ Квота API превышена. Пауза {wait_time} сек... (попытка {attempt + 1}/{MAX_QUOTA_RETRIES})")
                time.sleep(wait_time)
                continue
            elif '10000000 cells' in error_str:
                # Специальная обработка: пробуем создать лист с МЕНЬШИМ размером
                print(f"  ⚠️ Лимит ячеек превышен для 5000x20, пробуем 2000x15...")
                try:
                    sheet = spreadsheet.add_worksheet(title=sheet_title, rows=2000, cols=15)
                    time.sleep(2)

                    safe_update_cell(sheet, "A1", [CHP_DRR_CONFIG["headers"]], value_input_option='USER_ENTERED')

                    end_col = get_column_letter(len(CHP_DRR_CONFIG["headers"]))
                    safe_format_range(
                        sheet, f"A1:{end_col}1",
                        CellFormat(textFormat=TextFormat(bold=True, fontSize=11),
                                   backgroundColor=Color(0.85, 0.95, 0.85))
                    )

                    safe_api_call(set_frozen, sheet, rows=1)
                    print("  ✅ Лист ЧП_товары_ДРР создан с уменьшенным размером")
                    return sheet
                except Exception as e2:
                    print(f"  ❌ Не удалось создать лист даже с уменьшенным размером: {e2}")
                    raise
            else:
                print(f"  ❌ Ошибка при настройке ЧП_товары_ДРР: {e}")
                raise

    raise Exception(f"Не удалось создать лист {sheet_title} после {MAX_QUOTA_RETRIES} попыток")


def update_technical_sheet_advanced(tech_sheet, campaigns_data: Dict, products_start_row: int,
                                    logistics_price_sheet, current_date_str: str, tech_dict: Dict = None,
                                    spreadsheet=None):
    """Расширенное обновление Технического листа - ОДНИМ ЗАПРОСОМ"""
    print("\n📊 РАСШИРЕННОЕ ОБНОВЛЕНИЕ ТЕХНИЧЕСКОГО ЛИСТА")

    logistics_prices = load_logistics_prices_from_sheet(logistics_price_sheet)

    if spreadsheet:
        markup_percent = get_markup_percent(spreadsheet)
        print(f"  📊 Средняя наценка из листа: {markup_percent}%")
    else:
        markup_percent = 8.0
        print(f"  ⚠️ Spreadsheet не передан, используем наценку по умолчанию: {markup_percent}%")

    try:
        tax_rate_cell = safe_api_call(tech_sheet.acell, 'B3').value
        if tax_rate_cell and str(tax_rate_cell) not in ['—', '']:
            tax_rate = float(tax_rate_cell)
        else:
            tax_rate = 6.0

        acquiring_rate_cell = safe_api_call(tech_sheet.acell, 'B4').value
        if acquiring_rate_cell and str(acquiring_rate_cell) not in ['—', '']:
            acquiring_rate = float(acquiring_rate_cell)
        else:
            acquiring_rate = 1.0

        if tech_dict and 'local_sales_percent' in tech_dict:
            local_sales_percent = float(tech_dict['local_sales_percent'])
            print(f"  📍 Процент локальных продаж из tech_dict: {local_sales_percent}%")
        else:
            local_percent_cell = safe_api_call(tech_sheet.acell, 'B5').value
            if local_percent_cell and str(local_percent_cell) not in ['—', '']:
                local_sales_percent = float(local_percent_cell)
            else:
                local_sales_percent = 87.0
            print(f"  📍 Процент локальных продаж из ячейки B5: {local_sales_percent}%")

        print(
            f"  ⚙️ Настройки: налог={tax_rate}%, эквайринг={acquiring_rate}%, локальные продажи={local_sales_percent}%, наценка={markup_percent}%")
    except Exception as e:
        print(f"  ⚠️ Ошибка чтения настроек: {e}")
        tax_rate = 6.0
        acquiring_rate = 1.0
        local_sales_percent = 87.0

    all_products_data = []

    for offer_id, campaigns_list in campaigns_data.items():
        if not campaigns_list:
            continue

        first_campaign = campaigns_list[0]

        price_before = clean_numeric_value(first_campaign.get('product_price_before', 0))
        price_for_buyer = clean_numeric_value(first_campaign.get('product_price', 0))
        cost_price = clean_numeric_value(first_campaign.get('cost_price', 0))
        volume_l = clean_numeric_value(first_campaign.get('item_volume_l', 0))
        commission_str = str(first_campaign.get('commission_fbo', '0'))
        sku = first_campaign.get('sku', '—')
        stock_balance = clean_int_value(first_campaign.get('stock_balance', 0))

        commission_percent = get_commission_rate(commission_str)
        acquiring = calculate_acquiring(price_before, acquiring_rate)
        logistics = calculate_logistics_cost(volume_l, price_before, logistics_prices,
                                             local_sales_percent, markup_percent)
        spp = calculate_spp(price_before, price_for_buyer)  # РАСЧЕТ СПП
        tax = calculate_tax(price_for_buyer, tax_rate)

        # Добавляем СПП и себестоимость в данные
        all_products_data.append([
            offer_id, sku, price_before, price_for_buyer, acquiring,
            stock_balance, commission_percent, volume_l, logistics,
            spp,           # НОВОЕ: СПП (%)
            cost_price     # НОВОЕ: Себестоимость (₽)
        ])

        print(f"  📦 {offer_id}: цена={price_before:.2f}, логистика={logistics:.2f}, СПП={spp:.2f}%, себестоимость={cost_price:.2f}")

    if all_products_data:
        print(f"\n  📝 Запись {len(all_products_data)} товаров одним запросом...")

        start_row = products_start_row
        end_col = get_column_letter(len(TECHNICAL_SHEET_CONFIG["products_headers"]))
        range_name = f"A{start_row}:{end_col}{start_row + len(all_products_data) - 1}"

        safe_update_cell(tech_sheet, range_name, all_products_data, value_input_option='USER_ENTERED')

        print(f"  ✅ Добавлено {len(all_products_data)} товаров одним запросом")

        last_row = products_start_row + len(all_products_data) - 1

        # Обновляем форматирование для новых столбцов
        for col, color, bold in [('C', Color(0.95, 0.9, 1), True),
                                 ('D', Color(0.9, 1, 0.9), True),
                                 ('E', Color(1, 0.95, 0.8), False),
                                 ('I', Color(0.7, 0.85, 1), False),
                                 ('J', Color(0.85, 0.9, 1), False),   # НОВОЕ: СПП
                                 ('K', Color(1, 0.85, 0.85), False)]:  # НОВОЕ: Себестоимость
            try:
                fmt = CellFormat(backgroundColor=color)
                if bold:
                    fmt.textFormat = TextFormat(bold=True)
                safe_format_range(tech_sheet, f"{col}{products_start_row}:{col}{last_row}", fmt)
                time.sleep(0.5)
            except:
                pass

        # Форматирование СПП как процент
        try:
            safe_format_range(
                tech_sheet, f"J{products_start_row}:J{last_row}",
                CellFormat(numberFormat={'type': 'PERCENT', 'pattern': '#,##0.00%'})
            )
        except:
            pass

        # Форматирование себестоимости как валюта
        try:
            safe_format_range(
                tech_sheet, f"K{products_start_row}:K{last_row}",
                CellFormat(numberFormat={'type': 'CURRENCY', 'pattern': '#,##0.00 ₽'})
            )
        except:
            pass

        print("  💡 Примечание: Эквайринг рассчитывается автоматически")
        print("  💡 Стоимость логистики берется из листа 'Стоимость логистики'")
        print("  💡 СПП (скидка постоянного покупателя) рассчитана автоматически")
        print("  💡 Себестоимость из данных API")

    print("  ✅ Технический лист расширенно обновлен")


def update_chp_sheets(spreadsheet, campaigns_data: Dict, logistics_price_sheet,
                      current_date_str: str, tech_dict: Dict = None,
                      drr_for_products: Dict = None):
    """Обновляет страницы ЧП_товары_общая и ЧП_товары_ДРР"""
    print("\n💰 ОБНОВЛЕНИЕ ЛИСТОВ ЧП")

    try:
        tech_sheet = get_or_create_sheet(spreadsheet, TECHNICAL_SHEET_CONFIG["sheet_name"])

        tax_rate_cell = safe_api_call(tech_sheet.acell, 'B3').value
        tax_rate = float(tax_rate_cell) if tax_rate_cell and str(tax_rate_cell) not in ['—', ''] else 6.0

        acquiring_rate_cell = safe_api_call(tech_sheet.acell, 'B4').value
        acquiring_rate = float(acquiring_rate_cell) if acquiring_rate_cell and str(acquiring_rate_cell) not in ['—',
                                                                                                                ''] else 1.0

        if tech_dict and 'local_sales_percent' in tech_dict:
            local_sales_percent = float(tech_dict['local_sales_percent'])
        else:
            local_percent_cell = safe_api_call(tech_sheet.acell, 'B5').value
            local_sales_percent = float(local_percent_cell) if local_percent_cell and str(local_percent_cell) not in [
                '—', ''] else 87.0

        markup_percent = get_markup_percent(spreadsheet)

    except Exception as e:
        print(f"  ⚠️ Не удалось прочитать настройки: {e}")
        tax_rate = 6.0
        acquiring_rate = 1.0
        local_sales_percent = 87.0
        markup_percent = 8.0

    logistics_prices = load_logistics_prices_from_sheet(logistics_price_sheet)

    # Получаем листы
    chp_common_sheet = get_or_create_sheet(spreadsheet, "ЧП_товары_общая", rows=5000, cols=100)
    chp_drr_sheet = get_or_create_sheet(spreadsheet, "ЧП_товары_ДРР", rows=5000, cols=100)

    # Убеждаемся что есть заголовки
    headers_common = safe_get_values(chp_common_sheet, "A1:B1")
    if not headers_common or not headers_common[0] or headers_common[0][0] != "Артикул":
        safe_update_cell(chp_common_sheet, "A1", [["Артикул"]], value_input_option='USER_ENTERED')
        safe_update_cell(chp_common_sheet, "B1", [["Общая сумма ЧП"]], value_input_option='USER_ENTERED')
        safe_format_range(chp_common_sheet, "A1:B1",
                          CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85)))
        safe_api_call(set_column_width, chp_common_sheet, "A", 200)
        safe_api_call(set_column_width, chp_common_sheet, "B", 150)

    headers_drr = safe_get_values(chp_drr_sheet, "A1:B1")
    if not headers_drr or not headers_drr[0] or headers_drr[0][0] != "Артикул":
        safe_update_cell(chp_drr_sheet, "A1", [["Артикул"]], value_input_option='USER_ENTERED')
        safe_update_cell(chp_drr_sheet, "B1", [["Общая сумма ЧП"]], value_input_option='USER_ENTERED')
        safe_format_range(chp_drr_sheet, "A1:B1",
                          CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85)))
        safe_api_call(set_column_width, chp_drr_sheet, "A", 200)
        safe_api_call(set_column_width, chp_drr_sheet, "B", 150)

    # Проверяем, есть ли колонка с текущей датой
    existing_headers_common = safe_get_values(chp_common_sheet, "1:1")[0] if safe_get_values(chp_common_sheet,
                                                                                             "1:1") else []
    existing_headers_drr = safe_get_values(chp_drr_sheet, "1:1")[0] if safe_get_values(chp_drr_sheet, "1:1") else []

    # Вставляем колонку с датой если её нет (всегда после B, т.е. колонка C)
    if current_date_str not in existing_headers_common:
        # Правильный способ вставки колонки в gspread
        try:
            chp_common_sheet.add_cols(1)  # Добавляем колонку в конец
            # Перемещаем её на позицию 3
            body = {
                "requests": [{
                    "moveDimension": {
                        "source": {
                            "sheetId": chp_common_sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": chp_common_sheet.col_count - 1,
                            "endIndex": chp_common_sheet.col_count
                        },
                        "destinationIndex": 2
                    }
                }]
            }
            spreadsheet.batch_update(body)
        except:
            # Fallback: просто вставляем через insert_cols с правильным синтаксисом
            chp_common_sheet.insert_cols(3)

        safe_update_cell(chp_common_sheet, "C1", [[current_date_str]], value_input_option='USER_ENTERED')
        safe_format_range(chp_common_sheet, "C1",
                          CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.9, 0.95, 0.9)))
        safe_api_call(set_column_width, chp_common_sheet, "C", 100)
        print(f"  📅 Добавлена колонка C с датой {current_date_str} в ЧП_товары_общая")
        time.sleep(1)

    if current_date_str not in existing_headers_drr:
        try:
            chp_drr_sheet.add_cols(1)
            body = {
                "requests": [{
                    "moveDimension": {
                        "source": {
                            "sheetId": chp_drr_sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": chp_drr_sheet.col_count - 1,
                            "endIndex": chp_drr_sheet.col_count
                        },
                        "destinationIndex": 2
                    }
                }]
            }
            spreadsheet.batch_update(body)
        except:
            chp_drr_sheet.insert_cols(3)

        safe_update_cell(chp_drr_sheet, "C1", [[current_date_str]], value_input_option='USER_ENTERED')
        safe_format_range(chp_drr_sheet, "C1",
                          CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.9, 0.95, 0.9)))
        safe_api_call(set_column_width, chp_drr_sheet, "C", 100)
        print(f"  📅 Добавлена колонка C с датой {current_date_str} в ЧП_товары_ДРР")
        time.sleep(1)

    # Получаем существующие артикулы
    existing_data_common = safe_get_values(chp_common_sheet)
    existing_data_drr = safe_get_values(chp_drr_sheet)

    product_row_map_common = {}
    for row_idx, row in enumerate(existing_data_common[1:], start=2):
        if row and len(row) > 0 and row[0] and row[0] != 'Артикул':
            product_row_map_common[row[0]] = row_idx

    product_row_map_drr = {}
    for row_idx, row in enumerate(existing_data_drr[1:], start=2):
        if row and len(row) > 0 and row[0] and row[0] != 'Артикул':
            product_row_map_drr[row[0]] = row_idx

    # Подготавливаем данные
    print(f"\n  📝 ПОДГОТОВКА ДАННЫХ ДЛЯ {len(campaigns_data)} ТОВАРОВ...")

    common_values = {}
    drr_values = {}

    for offer_id, campaigns_list in campaigns_data.items():
        if not campaigns_list:
            continue

        first_campaign = campaigns_list[0]

        price_before = clean_numeric_value(first_campaign.get('product_price_before', 0))
        price_for_buyer = clean_numeric_value(first_campaign.get('product_price', 0))
        cost_price = clean_numeric_value(first_campaign.get('cost_price', 0))
        volume_l = clean_numeric_value(first_campaign.get('item_volume_l', 0))
        commission_str = str(first_campaign.get('commission_fbo', '0'))

        commission_percent = get_commission_rate(commission_str)
        acquiring = calculate_acquiring(price_before, acquiring_rate)
        logistics = calculate_logistics_cost(volume_l, price_before, logistics_prices, local_sales_percent,
                                             markup_percent)
        tax = calculate_tax(price_for_buyer, tax_rate)

        ad_expenses = 0.0
        drr_total = 0.0
        drr_cpo = 0.0

        if drr_for_products and offer_id in drr_for_products:
            drr_data = drr_for_products[offer_id]
            if isinstance(drr_data, dict):
                drr_total = drr_data.get('drr_total', 0.0)
                drr_cpo = drr_data.get('drr_cpo', 0.0)
                ad_expenses = drr_data.get('money_spent', 0.0)
            else:
                drr_total = drr_data

        if drr_total > 0:
            common_values[offer_id] = calculate_chp(price_before, commission_percent, logistics, tax, cost_price,
                                                    acquiring, drr_total, verbose=False)
        else:
            common_values[offer_id] = -ad_expenses if ad_expenses > 0 else 0

        if drr_cpo > 0:
            drr_values[offer_id] = calculate_chp(price_before, commission_percent, logistics, tax, cost_price,
                                                 acquiring, drr_cpo, verbose=False)
        else:
            drr_values[offer_id] = -ad_expenses if ad_expenses > 0 else 0

    # МАССОВАЯ ЗАПИСЬ через update_cells
    print(f"\n  💾 МАССОВАЯ ЗАПИСЬ ДАННЫХ:")

    # Подготавливаем список ячеек для обновления
    cells_to_update_common = []
    for offer_id, value in common_values.items():
        row_num = product_row_map_common.get(offer_id)
        if row_num:
            cells_to_update_common.append(gspread.Cell(row_num, 3, value))

    cells_to_update_drr = []
    for offer_id, value in drr_values.items():
        row_num = product_row_map_drr.get(offer_id)
        if row_num:
            cells_to_update_drr.append(gspread.Cell(row_num, 3, value))

    # Обновляем через update_cells
    if cells_to_update_common:
        for attempt in range(3):
            try:
                chp_common_sheet.update_cells(cells_to_update_common, value_input_option='USER_ENTERED')
                print(f"     ✅ Обновлено {len(cells_to_update_common)} строк в ЧП_товары_общая")
                break
            except Exception as e:
                if '429' in str(e) and attempt < 2:
                    wait_time = 30 * (attempt + 1)
                    print(f"     ⏳ Квота API, пауза {wait_time} сек...")
                    time.sleep(wait_time)
                    continue
                raise

    if cells_to_update_drr:
        for attempt in range(3):
            try:
                chp_drr_sheet.update_cells(cells_to_update_drr, value_input_option='USER_ENTERED')
                print(f"     ✅ Обновлено {len(cells_to_update_drr)} строк в ЧП_товары_ДРР")
                break
            except Exception as e:
                if '429' in str(e) and attempt < 2:
                    wait_time = 30 * (attempt + 1)
                    print(f"     ⏳ Квота API, пауза {wait_time} сек...")
                    time.sleep(wait_time)
                    continue
                raise

    # Добавляем новые артикулы
    new_rows_common = []
    for offer_id in common_values.keys():
        if offer_id not in product_row_map_common:
            new_rows_common.append([offer_id, "", common_values[offer_id]])

    new_rows_drr = []
    for offer_id in drr_values.keys():
        if offer_id not in product_row_map_drr:
            new_rows_drr.append([offer_id, "", drr_values[offer_id]])

    if new_rows_common:
        start_row = len(existing_data_common) + 1
        range_name = f"A{start_row}:C{start_row + len(new_rows_common) - 1}"
        safe_update_cell(chp_common_sheet, range_name, new_rows_common, value_input_option='USER_ENTERED')
        print(f"     ✅ Добавлено {len(new_rows_common)} новых строк в ЧП_товары_общая")

    if new_rows_drr:
        start_row = len(existing_data_drr) + 1
        range_name = f"A{start_row}:C{start_row + len(new_rows_drr) - 1}"
        safe_update_cell(chp_drr_sheet, range_name, new_rows_drr, value_input_option='USER_ENTERED')
        print(f"     ✅ Добавлено {len(new_rows_drr)} новых строк в ЧП_товары_ДРР")

        # Обновляем формулу суммы в колонке B
        try:
            # Получаем последнюю колонку с данными
            last_col_common = len(existing_headers_common) + 1  # +1 потому что добавили новую колонку
            last_col_letter_common = get_column_letter(last_col_common)

            last_col_drr = len(existing_headers_drr) + 1
            last_col_letter_drr = get_column_letter(last_col_drr)

            # Формула для каждой строки своя
            row_count_common = len(existing_data_common) + len(new_rows_common)
            if row_count_common > 1:
                formulas_common = []
                for row_num in range(2, row_count_common + 1):
                    formula = f"=СУММ(C{row_num}:{last_col_letter_common}{row_num})"
                    formulas_common.append([formula])

                # Записываем формулы одним запросом
                safe_update_cell(chp_common_sheet, f"B2:B{row_count_common}", formulas_common,
                                 value_input_option='USER_ENTERED')
                print(f"  📊 Обновлена формула суммы для {len(formulas_common)} строк в ЧП_товары_общая")

            row_count_drr = len(existing_data_drr) + len(new_rows_drr)
            if row_count_drr > 1:
                formulas_drr = []
                for row_num in range(2, row_count_drr + 1):
                    formula = f"=СУММ(C{row_num}:{last_col_letter_drr}{row_num})"
                    formulas_drr.append([formula])

                safe_update_cell(chp_drr_sheet, f"B2:B{row_count_drr}", formulas_drr, value_input_option='USER_ENTERED')
                print(f"  📊 Обновлена формула суммы для {len(formulas_drr)} строк в ЧП_товары_ДРР")

        except Exception as e:
            print(f"  ⚠️ Ошибка при обновлении формул: {e}")

    print(f"\n  ✅ ЧП_товары_общая: обработано {len(common_values)} товаров")
    print(f"  ✅ ЧП_товары_ДРР: обработано {len(drr_values)} товаров")


def add_chp_per_day_column(spreadsheet, campaigns_data: Dict, current_date_str: str, tech_dict: Dict = None,
                           drr_for_products: Dict = None):
    """Добавляет на дашборд столбец 'ЧП / в день' на позицию G (колонка 7)"""
    print("\n📊 ДОБАВЛЕНИЕ СТОЛБЦА 'ЧП / в день' НА DASHBOARD")

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")
            tech_sheet = get_or_create_sheet(spreadsheet, TECHNICAL_SHEET_CONFIG["sheet_name"])

            # Принудительно устанавливаем ЧП / в день в колонку G (7)
            chp_col_index = 6
            col_letter = get_column_letter(chp_col_index + 1)

            # Получаем текущие заголовки
            headers_row = safe_get_values(dashboard, "A1:Z1")
            if headers_row and len(headers_row) > 0:
                current_headers = headers_row[0]
            else:
                current_headers = []

            # Проверяем и устанавливаем правильный заголовок в колонку G
            if len(current_headers) <= chp_col_index or current_headers[chp_col_index] != "ЧП / в день":
                safe_update_cell(dashboard, f"{col_letter}1", [["ЧП / в день"]], value_input_option='USER_ENTERED')
                safe_format_range(
                    dashboard, f"{col_letter}1",
                    CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
                )
                safe_api_call(set_column_width, dashboard, col_letter, 120)
                print(f"  🆕 Установлен столбец 'ЧП / в день' (столбец {col_letter})")
            else:
                print(f"  📄 Столбец 'ЧП / в день' уже существует (столбец {col_letter})")

            # Читаем настройки из технического листа
            try:
                tax_rate_cell = safe_api_call(tech_sheet.acell, 'B3').value
                tax_rate = float(tax_rate_cell) if tax_rate_cell and str(tax_rate_cell) not in ['—', ''] else 6.0

                acquiring_rate_cell = safe_api_call(tech_sheet.acell, 'B4').value
                acquiring_rate = float(acquiring_rate_cell) if acquiring_rate_cell and str(acquiring_rate_cell) not in [
                    '—', ''] else 1.0

                if tech_dict and 'local_sales_percent' in tech_dict:
                    local_sales_percent = float(tech_dict['local_sales_percent'])
                else:
                    local_percent_cell = safe_api_call(tech_sheet.acell, 'B5').value
                    local_sales_percent = float(local_percent_cell) if local_percent_cell and str(
                        local_percent_cell) not in ['—', ''] else 87.0

                logistics_sheet = get_or_create_sheet(spreadsheet, LOGISTICS_PRICE_CONFIG["sheet_name"])
                logistics_prices = load_logistics_prices_from_sheet(logistics_sheet)
                markup_percent = get_markup_percent(spreadsheet)

            except Exception as e:
                print(f"  ⚠️ Ошибка чтения настроек: {e}")
                tax_rate = 6.0
                acquiring_rate = 1.0
                local_sales_percent = 87.0
                logistics_prices = {}
                markup_percent = 8.0

            # Получаем данные дашборда
            dashboard_data = safe_get_values(dashboard)

            if len(dashboard_data) < 2:
                print("  ⚠️ Нет данных в дашборде")
                return

            start_row = 2
            end_row = len(dashboard_data)

            # Очищаем старые данные в столбце G
            if end_row > 1:
                try:
                    clear_range = f"{col_letter}2:{col_letter}{end_row + 5}"
                    safe_api_call(dashboard.batch_clear, [clear_range])
                    print(f"     🗑️ Очищен столбец {col_letter}")
                    time.sleep(1)
                except Exception as e:
                    print(f"     ⚠️ Ошибка очистки: {e}")

            # Подготавливаем значения ЧП
            chp_values = []
            for row_idx, row in enumerate(dashboard_data[1:], start=2):
                if not row or len(row) < 1:
                    chp_values.append([""])
                    continue

                offer_id = row[0]
                if offer_id == "ИТОГО" or not offer_id:
                    chp_values.append([""])
                    continue

                if offer_id in campaigns_data and campaigns_data[offer_id]:
                    first_campaign = campaigns_data[offer_id][0]

                    price_before = clean_numeric_value(first_campaign.get('product_price_before', 0))
                    price_for_buyer = clean_numeric_value(first_campaign.get('product_price', 0))
                    cost_price = clean_numeric_value(first_campaign.get('cost_price', 0))
                    volume_l = clean_numeric_value(first_campaign.get('item_volume_l', 0))
                    commission_str = str(first_campaign.get('commission_fbo', '0'))

                    # Получаем ДРР из drr_for_products (уже обработанный для ЧП)
                    drr_total_for_chp = 0
                    ad_expenses = 0
                    if drr_for_products and offer_id in drr_for_products:
                        drr_data = drr_for_products[offer_id]
                        if isinstance(drr_data, dict):
                            drr_total_for_chp = drr_data.get('drr_total', 0)  # Это уже 0 если был отрицательный
                            ad_expenses = drr_data.get('total_ad_expenses', 0)

                    # Альтернативно, берем из строки дашборда (колонка F, индекс 5)
                    drr_from_row = clean_numeric_value(row[5]) if len(row) > 5 else 0

                    # Если ДРР из строки отрицательный, используем 0 для ЧП
                    if drr_from_row < 0:
                        drr_total_for_chp = 0
                    elif drr_total_for_chp == 0 and drr_from_row > 0:
                        drr_total_for_chp = drr_from_row

                    commission_percent = get_commission_rate(commission_str)
                    acquiring = calculate_acquiring(price_before, acquiring_rate)
                    logistics = calculate_logistics_cost(volume_l, price_before, logistics_prices,
                                                         local_sales_percent, markup_percent)
                    tax = calculate_tax(price_for_buyer, tax_rate)

                    # Если ДРР = 0, то ЧП считается без вычета ДРР, но расходы уже не вычитаются отдельно
                    # потому что формула ЧП: price_before - комиссия - логистика - налог - себестоимость - эквайринг - (price_before * ДРР/100)
                    # Если ДРР = 0, то слагаемое с ДРР просто отсутствует
                    chp = calculate_chp(price_before, commission_percent, logistics, tax,
                                        cost_price, acquiring, drr_total_for_chp, offer_id, verbose=False)
                    chp_values.append([chp])
                else:
                    chp_values.append([""])

            # Записываем значения
            if chp_values:
                range_name = f"{col_letter}{start_row}:{col_letter}{start_row + len(chp_values) - 1}"
                safe_update_cell(dashboard, range_name, chp_values, value_input_option='USER_ENTERED')

                safe_format_range(
                    dashboard, f"{col_letter}{start_row}:{col_letter}{start_row + len(chp_values) - 1}",
                    CellFormat(
                        numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'},
                        horizontalAlignment='RIGHT'
                    )
                )

                print(f"  ✅ Столбец 'ЧП / в день' обновлен ({len(chp_values)} записей)")

            break

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY
                print(f"  ⏳ Квота API превышена. Пауза {wait_time} сек... (попытка {attempt + 1}/{MAX_QUOTA_RETRIES})")
                time.sleep(wait_time)
                continue
            else:
                print(f"  ❌ Ошибка при добавлении ЧП / в день: {e}")
                import traceback
                traceback.print_exc()
                break


def add_spp_column_to_dashboard(spreadsheet, campaigns_data: Dict):
    """Добавляет на дашборд столбец 'СПП' на позицию H (колонка 8)"""
    print("\n📊 ДОБАВЛЕНИЕ СТОЛБЦА 'СПП' НА DASHBOARD")

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")

            # Принудительно устанавливаем СПП в колонку H (8)
            # 0-based индекс = 7, колонка H
            spp_col_index = 7
            col_letter = get_column_letter(spp_col_index + 1)

            # Получаем текущие заголовки
            headers_row = safe_get_values(dashboard, "A1:Z1")
            if headers_row and len(headers_row) > 0:
                current_headers = headers_row[0]
            else:
                current_headers = []

            # Проверяем и устанавливаем правильный заголовок в колонку H
            if len(current_headers) <= spp_col_index or current_headers[spp_col_index] != "СПП":
                # Устанавливаем заголовок
                safe_update_cell(dashboard, f"{col_letter}1", [["СПП"]], value_input_option='USER_ENTERED')
                safe_format_range(
                    dashboard, f"{col_letter}1",
                    CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
                )
                safe_api_call(set_column_width, dashboard, col_letter, 100)
                print(f"  🆕 Установлен столбец 'СПП' (столбец {col_letter})")
            else:
                print(f"  📄 Столбец 'СПП' уже существует (столбец {col_letter})")

            # Получаем данные дашборда
            dashboard_data = safe_get_values(dashboard)

            if len(dashboard_data) < 2:
                print("  ⚠️ Нет данных в дашборде")
                return

            start_row = 2
            end_row = len(dashboard_data)

            # Очищаем старые данные в столбце H
            if end_row > 1:
                try:
                    clear_range = f"{col_letter}2:{col_letter}{end_row + 5}"
                    safe_api_call(dashboard.batch_clear, [clear_range])
                    print(f"     🗑️ Очищен столбец {col_letter}")
                    time.sleep(1)
                except Exception as e:
                    print(f"     ⚠️ Ошибка очистки: {e}")

            # Подготавливаем значения СПП
            spp_values = []
            for row in dashboard_data[1:]:
                if not row or len(row) < 1:
                    spp_values.append([""])
                    continue

                offer_id = row[0]
                if offer_id == "ИТОГО" or not offer_id:
                    spp_values.append([""])
                    continue

                if offer_id in campaigns_data and campaigns_data[offer_id]:
                    first_campaign = campaigns_data[offer_id][0]
                    price_before = clean_numeric_value(first_campaign.get('product_price_before', 0))
                    price_for_buyer = clean_numeric_value(first_campaign.get('product_price', 0))
                    spp = calculate_spp(price_before, price_for_buyer)
                    spp_values.append([spp])
                else:
                    spp_values.append([""])

            # Записываем значения
            if spp_values:
                range_name = f"{col_letter}{start_row}:{col_letter}{start_row + len(spp_values) - 1}"
                safe_update_cell(dashboard, range_name, spp_values, value_input_option='USER_ENTERED')

                safe_format_range(
                    dashboard, f"{col_letter}{start_row}:{col_letter}{start_row + len(spp_values) - 1}",
                    CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'}, horizontalAlignment='RIGHT')
                )

                print(f"  ✅ Столбец 'СПП' обновлен ({len(spp_values)} записей)")

            break

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY
                print(f"  ⏳ Квота API превышена. Пауза {wait_time} сек... (попытка {attempt + 1}/{MAX_QUOTA_RETRIES})")
                time.sleep(wait_time)
                continue
            else:
                print(f"  ❌ Ошибка при добавлении СПП: {e}")
                import traceback
                traceback.print_exc()
                break


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
    """Выполняет функцию, при ошибке 429 ждет 30 секунд и повторяет"""
    return safe_api_call(func, *args, max_retries=max_retries, **kwargs)


def execute_with_retry(func, *args, **kwargs):
    return execute_with_exponential_backoff(func, *args, **kwargs)


def get_google_sheets_client():
    """Создает клиент Google Sheets"""
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
    safe_update_cell(sheet, f"A{start_row}:{end_col}{start_row}", [headers_list], value_input_option='USER_ENTERED')
    time.sleep(0.5)
    safe_format_range(
        sheet, f"A{start_row}:{end_col}{start_row}",
        CellFormat(textFormat=TextFormat(bold=True, fontSize=11),
                   backgroundColor=config.get('header_color', Color(0.9, 0.9, 0.9)))
    )
    if config.get('frozen_rows'):
        safe_api_call(set_frozen, sheet, rows=config['frozen_rows'])
    time.sleep(0.5)
    for idx, header in enumerate(config['headers'], start=1):
        col_letter = get_column_letter(idx)
        if 'width' in header:
            safe_api_call(set_column_width, sheet, col_letter, header['width'])
    time.sleep(0.5)


def clear_old_dashboard_data(dashboard, current_total_rows: int):
    if current_total_rows > 1:
        print("  🗑️ Очищаем старые данные...")
        try:
            safe_api_call(dashboard.batch_clear, [f"A2:F{current_total_rows}"])
            print(f"  ✅ Очищено содержимое строк 2-{current_total_rows}")
            time.sleep(1)
        except Exception as e:
            print(f"  ⚠️ Ошибка при очистке: {e}")


def format_totals_row(sheet, last_row: int, num_columns: int):
    end_col = get_column_letter(num_columns)
    safe_format_range(
        sheet, f"A{last_row}:{end_col}{last_row}",
        CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.95, 0.95, 0.95))
    )


def ensure_sheet_rows(sheet, required_rows: int, buffer_rows: int = 10):
    current_rows = len(safe_get_values(sheet))
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
    """Подготавливает данные для листа DASHBOARD"""
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
        total_revenue_item = max(0, clean_numeric_value(item.get("total_revenue", 0)))
        total_ordered_units = max(0, clean_int_value(item.get("total_ordered_units", 0)))

        offer_campaigns = campaigns_data.get(offer_id, []) if campaigns_data else []

        expenses_search, selled_search, _, _ = extract_campaign_expenses(offer_campaigns)

        drr_cpo = 0.0
        money_spent = 0.0

        if drr_all_dict and offer_id in drr_all_dict:
            drr_data = drr_all_dict[offer_id]
            if isinstance(drr_data, dict):
                drr_cpo = clean_numeric_value(drr_data.get('drr', 0))
                money_spent = clean_numeric_value(drr_data.get('money_spent', 0))
            else:
                drr_cpo = clean_numeric_value(drr_data)

        drr_search = calculate_drr(expenses_search, max(0, selled_search))

        # Считаем общие расходы по всем рекламным кампаниям
        total_ad_expenses = 0.0
        for campaign in offer_campaigns:
            expense = clean_numeric_value(campaign.get('expense', 0))
            expense_model = clean_numeric_value(campaign.get('expense_model', 0))
            total_ad_expenses += expense + expense_model

        # НОВАЯ ЛОГИКА ДЛЯ drr_total
        if drr_cpo == 0 or money_spent == 0:
            # Если ДРР = 0, то для отображения используем расходы со знаком минус
            drr_total_display = -total_ad_expenses if total_ad_expenses > 0 else 0
            # ДЛЯ РАСЧЕТА ЧП используем 0 (так как расходы уже учтены отдельно?)
            drr_total_for_chp = 0
        else:
            drr_total_display = calculate_drr(money_spent, total_revenue_item)
            drr_total_for_chp = drr_total_display

        drr_for_products[offer_id] = {
            'drr_total': drr_total_for_chp,  # Для ЧП - 0 если отрицательный или 0
            'drr_total_display': drr_total_display,  # Для отображения на дашборде
            'drr_cpo': drr_cpo,
            'drr_search': drr_search,
            'money_spent': money_spent if money_spent > 0 else total_ad_expenses,
            'revenue': total_revenue_item,
            'total_ad_expenses': total_ad_expenses
        }

        log_dashboard_item(
            offer_id, total_revenue_item, expenses_search, selled_search,
            drr_cpo, money_spent, drr_search, drr_cpo, drr_total_display
        )

        dashboard_rows.append([
            offer_id,
            total_revenue_item,
            total_ordered_units,
            drr_search,
            drr_cpo,
            drr_total_display if isinstance(drr_total_display, (int, float)) else 0
        ])

        totals['total_orders'] += total_ordered_units
        totals['total_expenses_search'] += expenses_search
        totals['total_selled_search'] += selled_search
        totals['total_revenue_all'] += total_revenue_item
        totals['total_money_spent_from_dict'] += money_spent

    dashboard_rows.sort(key=lambda x: x[2], reverse=True)

    return dashboard_rows, totals, drr_for_products


def update_dashboard_sheet(dashboard, dashboard_data: List[List]):
    """Обновляет лист DASHBOARD с правильной структурой колонок"""
    current_data = safe_get_values(dashboard)
    current_total_rows = len(current_data)

    # Проверяем и устанавливаем правильные заголовки если нужно
    expected_headers = [h['name'] for h in DASHBOARD_CONFIG['headers']]
    if len(current_data) == 0 or (len(current_data) > 0 and current_data[0] != expected_headers):
        setup_sheet_headers(dashboard, DASHBOARD_CONFIG, start_row=1)

        # После установки заголовков нужно убедиться, что есть колонки G и H
        # Они будут добавлены позже функциями add_chp_per_day_column и add_spp_column_to_dashboard
    else:
        if current_total_rows > 1:
            clear_old_dashboard_data(dashboard, current_total_rows)

    if dashboard_data:
        total_revenue = sum(max(0, row[1]) for row in dashboard_data)  # Исправлено: только положительные
        total_orders = sum(max(0, row[2]) for row in dashboard_data)  # Исправлено: только положительные
        dashboard_data.append([""] * len(DASHBOARD_CONFIG['headers']))
        dashboard_data.append(["ИТОГО", total_revenue, total_orders, 0, 0, 0])

    rows_needed = len(dashboard_data) + 1
    ensure_sheet_rows(dashboard, rows_needed)

    print(f"  📝 Вставка {len(dashboard_data)} строк данных одной операцией...")
    safe_update_cell(dashboard, "A2", dashboard_data, value_input_option='USER_ENTERED')
    print(f"  ✅ Вставлено {len(dashboard_data)} строк данных")
    time.sleep(1)

    if dashboard_data:
        last_row = len(dashboard_data) + 1
        format_totals_row(dashboard, last_row, len(DASHBOARD_CONFIG['headers']))
        safe_format_range(dashboard, f"A2:A{last_row}",
                          CellFormat(textFormat=TextFormat(bold=True)))

    return dashboard_data[:-2] if len(dashboard_data) >= 2 else dashboard_data


# ================= НОВАЯ ФУНКЦИЯ ДЛЯ ИСТОРИИ DASHBOARD =================

def setup_history_dashboard_sheet(spreadsheet):
    """Настраивает лист истории DASHBOARD"""
    print("\n📜 НАСТРОЙКА ЛИСТА ИСТОРИИ DASHBOARD")
    history_sheet = get_or_create_sheet(spreadsheet, HISTORY_DASHBOARD_CONFIG["sheet_name"], rows=100000, cols=20)
    all_values = safe_get_values(history_sheet)

    if len(all_values) < 2 or (len(all_values) > 0 and all_values[0][0] != "Дата"):
        print("  🆕 Настройка структуры листа истории...")
        headers = [h['name'] for h in HISTORY_DASHBOARD_CONFIG["headers"]]
        safe_update_cell(history_sheet, "A1", [headers], value_input_option='USER_ENTERED')

        end_col = get_column_letter(len(headers))
        safe_format_range(
            history_sheet, f"A1:{end_col}1",
            CellFormat(textFormat=TextFormat(bold=True, fontSize=11), backgroundColor=Color(0.85, 0.95, 0.85))
        )

        for idx, header in enumerate(HISTORY_DASHBOARD_CONFIG["headers"], start=1):
            col_letter = get_column_letter(idx)
            if 'width' in header:
                safe_api_call(set_column_width, history_sheet, col_letter, header['width'])

        safe_update_cell(history_sheet, "A2", [[HISTORY_DASHBOARD_CONFIG["note"]]], value_input_option='USER_ENTERED')
        safe_format_range(
            history_sheet, f"A2:{end_col}2",
            CellFormat(textFormat=TextFormat(italic=True, fontSize=9), backgroundColor=Color(0.95, 0.95, 0.9))
        )

        safe_format_range(
            history_sheet, "C:C",
            CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0'})
        )

        safe_format_range(
            history_sheet, "D:D",
            CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0'})
        )

        for col in ['E', 'F', 'G']:
            safe_format_range(
                history_sheet, f"{col}:{col}",
                CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'})
            )

        safe_api_call(set_frozen, history_sheet, rows=2)
        print("  ✅ Структура листа истории настроена")
    else:
        print("  📄 Лист истории уже существует")

    return history_sheet


def save_dashboard_to_history(spreadsheet, current_date: str):
    """Сохраняет текущие данные из листа DASHBOARD в историю - добавляет новые записи в начало"""
    print("\n💾 СОХРАНЕНИЕ DASHBOARD В ИСТОРИЮ")

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")
            dashboard_data = safe_get_values(dashboard)

            if len(dashboard_data) <= 1:
                print("  ⚠️ Нет данных в DASHBOARD для сохранения")
                return False

            history_sheet = setup_history_dashboard_sheet(spreadsheet)

            # Формируем новые строки истории
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

                history_rows.append([
                    current_date, row[0], row[1], row[2], row[3], row[4], row[5]
                ])

            if not history_rows:
                print("  ⚠️ Нет данных для сохранения в историю")
                return False

            # Получаем ВСЕ существующие данные (не удаляем ничего)
            existing_data = safe_get_values(history_sheet)

            # Проверяем, есть ли уже данные за эту дату
            date_exists = False
            for row in existing_data:
                if row and len(row) > 0 and row[0] == current_date:
                    date_exists = True
                    break

            if date_exists:
                # Если данные за эту дату уже есть - не добавляем повторно
                print(f"  ℹ️ Данные за {current_date} уже существуют в истории, пропускаем")
                return True

            # Подготавливаем строку ИТОГО
            totals_row = [[
                f"ИТОГО за {current_date}", "",
                total_revenue_for_date if total_revenue_for_date > 0 else sum(
                    clean_numeric_value(row[2]) for row in history_rows),
                total_orders_for_date if total_orders_for_date > 0 else sum(
                    clean_int_value(row[3]) for row in history_rows),
                "", "", ""
            ]]

            # Объединяем: новые данные + ИТОГО + существующие данные (без шапки и примечания)
            # Шапка - строка 1, примечание - строка 2
            new_history_data = history_rows + totals_row

            # Добавляем существующие данные, которые находятся после строки 2 (примечание)
            if len(existing_data) > 2:
                # Пропускаем строки 0-1 (шапка и примечание)
                for row in existing_data[2:]:
                    # Пропускаем пустые строки
                    if row and any(cell.strip() for cell in row):
                        new_history_data.append(row)

            # Очищаем весь лист (кроме форматирования)
            safe_api_call(history_sheet.clear)
            time.sleep(1)

            # Восстанавливаем шапку и примечание
            headers = [h['name'] for h in HISTORY_DASHBOARD_CONFIG["headers"]]
            safe_update_cell(history_sheet, "A1", [headers], value_input_option='USER_ENTERED')

            end_col = get_column_letter(len(headers))
            safe_format_range(
                history_sheet, f"A1:{end_col}1",
                CellFormat(textFormat=TextFormat(bold=True, fontSize=11), backgroundColor=Color(0.85, 0.95, 0.85))
            )

            safe_update_cell(history_sheet, "A2", [[HISTORY_DASHBOARD_CONFIG["note"]]],
                             value_input_option='USER_ENTERED')
            safe_format_range(
                history_sheet, f"A2:{end_col}2",
                CellFormat(textFormat=TextFormat(italic=True, fontSize=9), backgroundColor=Color(0.95, 0.95, 0.9))
            )

            # Записываем ВСЕ данные истории (новые + старые) одним запросом
            if new_history_data:
                range_name = f"A3:{end_col}{3 + len(new_history_data) - 1}"
                safe_update_cell(history_sheet, range_name, new_history_data, value_input_option='USER_ENTERED')

                # Форматирование числовых колонок
                end_row = 3 + len(new_history_data) - 1
                for col in ['C', 'D', 'E', 'F', 'G']:
                    try:
                        safe_format_range(
                            history_sheet, f"{col}3:{col}{end_row}",
                            CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'})
                        )
                    except:
                        pass

            print(f"  ✅ История DASHBOARD обновлена: добавлено {len(history_rows)} записей за {current_date}")
            print(f"     📌 Всего записей в истории: {len(new_history_data)}")
            return True

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY * (attempt + 1)
                print(f"  ⏳ Квота API превышена. Пауза {wait_time} сек... (попытка {attempt + 1}/{MAX_QUOTA_RETRIES})")
                time.sleep(wait_time)
                continue
            else:
                print(f"  ❌ Ошибка при сохранении истории DASHBOARD: {e}")
                import traceback
                traceback.print_exc()
                return False

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
            safe_update_cell(sheet, range_name, values, value_input_option='USER_ENTERED')
            time.sleep(0.3)

    drr_col_letter = get_column_letter(get_column_index(ANALYTICS_CONFIG['start_column']) + 9)
    safe_api_call(set_column_width, sheet, drr_col_letter, 100)

    safe_format_range(
        sheet, f"{ANALYTICS_CONFIG['start_column']}5",
        CellFormat(textFormat=TextFormat(bold=True, fontSize=12), backgroundColor=ANALYTICS_CONFIG['block_color'])
    )

    for block_config in CAMPAIGN_CONFIGS.values():
        col_letter = block_config['start_column']
        safe_format_range(
            sheet, f"{col_letter}5",
            CellFormat(textFormat=TextFormat(bold=True, fontSize=12), backgroundColor=block_config['color'])
        )

    safe_api_call(set_frozen, sheet, rows=6)
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
    """Подготавливает строку данных для листа товара"""
    offer_id = item.get("offer_id")

    drr_total = 0.0
    if drr_for_products and offer_id in drr_for_products:
        drr_data = drr_for_products[offer_id]
        if isinstance(drr_data, dict):
            drr_total = drr_data.get('drr_total', 0.0)
        else:
            drr_total = drr_data

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

    search_campaigns = [camp for camp in offer_campaigns if camp.get('camping_type') == 'Поиск']
    rec_campaigns = [camp for camp in offer_campaigns if camp.get('camping_type') == 'Поиск и рекомендации']
    cpo_campaigns = [camp for camp in offer_campaigns if camp.get('camping_type') == 'Оплата за заказ']

    search_data = format_campaign_data(search_campaigns, 'search')
    rec_data = format_campaign_data(rec_campaigns, 'recommendations')
    cpo_data = format_campaign_data(cpo_campaigns, 'cpo')

    full_row = analytics_row + [""] + search_data + [""] + rec_data + [""] + cpo_data

    return full_row


def update_product_sheet_batch(sheet, offer_id: str, full_row: List, current_date_str: str):
    all_data = safe_get_values(sheet)
    existing_row_index = None
    for i, row in enumerate(all_data[6:], start=7):
        if len(row) > 0 and row[0] == current_date_str:
            existing_row_index = i
            break
    if existing_row_index:
        print(f"  🔄 Обновление строки {existing_row_index}")
        for attempt in range(3):
            try:
                sheet.update(values=[full_row], range_name=f"A{existing_row_index}", value_input_option='USER_ENTERED')
                print(f"  ✅ Обновлена строка за {current_date_str}")
                break
            except Exception as e:
                if '429' in str(e) and attempt < 2:
                    wait_time = 30 * (attempt + 1)
                    print(f"     ⏳ Квота API, пауза {wait_time} сек...")
                    time.sleep(wait_time)
                    continue
                raise
    else:
        print(f"  📝 Добавление новой строки за {current_date_str}")
        for attempt in range(3):
            try:
                safe_api_call(sheet.insert_row, full_row, index=7)
                print(f"  ✅ Добавлена строка за {current_date_str}")
                break
            except Exception as e:
                if '429' in str(e) and attempt < 2:
                    wait_time = 30 * (attempt + 1)
                    print(f"     ⏳ Квота API, пауза {wait_time} сек...")
                    time.sleep(wait_time)
                    continue
                raise
    time.sleep(1)
    enforce_sheet_size_limit(sheet, max_rows=500)


def enforce_sheet_size_limit(sheet, max_rows: int = 500):
    current_rows = len(safe_get_values(sheet))
    if current_rows > max_rows:
        rows_to_delete = current_rows - max_rows
        try:
            safe_api_call(sheet.delete_rows, 7, rows_to_delete)
            print(f"  ✅ Удалены старые строки, удалено {rows_to_delete} строк, осталось {max_rows}")
            time.sleep(1)
        except Exception as e:
            print(f"  ⚠️ Не удалось удалить строки: {e}")
    else:
        print(f"  ℹ️ В листе {current_rows} строк, лимит не превышен")


# ================= ФУНКЦИИ ДЛЯ ОБРАБОТКИ ОШИБОК =================

def write_error_to_sheet(error_message: str, sheet_name: str = "ERROR"):
    """Записывает ошибку в лист с обработкой 429"""
    for attempt in range(3):
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
            sheet.update(range_name="A1", values=[[full_error]], value_input_option='USER_ENTERED')
            import traceback
            tb = traceback.format_exc()
            if tb and tb != "NoneType: None\n":
                sheet.update(range_name="A2", values=[[tb]], value_input_option='USER_ENTERED')
            print(f"✅ Ошибка записана в лист {sheet_name}")
            return
        except Exception as e:
            if '429' in str(e) and attempt < 2:
                wait_time = 30 * (attempt + 1)
                print(f"  ⏳ Ошибка записи ошибки, пауза {wait_time} сек...")
                time.sleep(wait_time)
                continue
            print(f"❌ Не удалось записать ошибку: {e}")
            return


def write_parser_error_to_sheet(error_message: str):
    write_error_to_sheet(error_message, "ERROR_PARS")


# ================= ОСНОВНАЯ ФУНКЦИЯ =================

def upload_to_google_sheets(all_items_dict: Dict, campaigns_data: Optional[Dict] = None,
                            positions_data: Optional[Dict] = None,
                            drr_all_dict: Optional[Dict] = None,
                            tech_dict: Optional[Dict] = None):
    """Основная функция загрузки данных в Google Sheets"""
    print("\n" + "=" * 60)
    print("🚀 НАЧАЛО ЗАГРУЗКИ ДАННЫХ В GOOGLE SHEETS")
    print("=" * 60)
    time.sleep(5)

    try:
        print("\n🔌 Подключение к Google Sheets...")
        client, spreadsheet = test_google_sheets_connection()
        current_date_str = get_current_date_moscow()
        print(f"📅 Текущая дата: {current_date_str}")

        # ================= ОБРАБОТКА DASHBOARD =================
        print("\n" + "=" * 60)
        print("📊 ОБРАБОТКА ЛИСТА DASHBOARD")
        print("=" * 60)

        dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")

        current_data = safe_get_values(dashboard)
        expected_headers = [h['name'] for h in DASHBOARD_CONFIG['headers']]

        if len(current_data) == 0 or (len(current_data) > 0 and current_data[0] != expected_headers):
            setup_sheet_headers(dashboard, DASHBOARD_CONFIG, start_row=1)
        else:
            clear_old_dashboard_data(dashboard, len(current_data))

        dashboard_data, _, drr_for_products = prepare_dashboard_data(all_items_dict, campaigns_data, drr_all_dict)
        update_dashboard_sheet(dashboard, dashboard_data)
        print("✅ DASHBOARD успешно обновлен")

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

        # ================= НОВЫЙ ЛИСТ: ДРР ОБЩИЙ =================
        print("\n" + "=" * 60)
        print("📊 ОБНОВЛЕНИЕ СВОДНОЙ ТАБЛИЦЫ ДРР ОБЩИЙ")
        print("=" * 60)

        update_drr_total_sheet_from_dashboard(spreadsheet, dashboard, current_date_str)

        # ================= НАСТРОЙКА ТЕХНИЧЕСКОГО ЛИСТА =================
        print("\n" + "=" * 60)
        print("📋 НАСТРОЙКА ТЕХНИЧЕСКОГО ЛИСТА")
        print("=" * 60)

        tech_sheet, products_start_row = setup_technical_sheet(spreadsheet)
        logistics_price_sheet = setup_logistics_price_sheet(spreadsheet)

        # ================= РАСШИРЕННОЕ ОБНОВЛЕНИЕ ТЕХНИЧЕСКОГО ЛИСТА =================
        print("\n" + "=" * 60)
        print("📊 РАСШИРЕННОЕ ОБНОВЛЕНИЕ ТЕХНИЧЕСКОГО ЛИСТА")
        print("=" * 60)

        update_technical_sheet_advanced(tech_sheet, campaigns_data, products_start_row,
                                        logistics_price_sheet, current_date_str, tech_dict, spreadsheet)

        # ================= ОБНОВЛЕНИЕ ЛИСТОВ ЧП =================
        print("\n" + "=" * 60)
        print("💰 ОБНОВЛЕНИЕ ЛИСТОВ ЧП")
        print("=" * 60)

        update_chp_sheets(spreadsheet, campaigns_data, logistics_price_sheet,
                          current_date_str, tech_dict, drr_for_products)

        # ================= ДОБАВЛЕНИЕ СТОЛБЦОВ НА DASHBOARD =================
        print("\n" + "=" * 60)
        print("📊 ДОБАВЛЕНИЕ НОВЫХ СТОЛБЦОВ НА DASHBOARD")
        print("=" * 60)

        add_chp_per_day_column(spreadsheet, campaigns_data, current_date_str, tech_dict, drr_for_products)
        add_spp_column_to_dashboard(spreadsheet, campaigns_data)

        # ================= СОХРАНЕНИЕ ИСТОРИИ DASHBOARD =================
        print("\n" + "=" * 60)
        print("📜 СОХРАНЕНИЕ ИСТОРИИ DASHBOARD")
        print("=" * 60)

        save_dashboard_to_history(spreadsheet, current_date_str)

        try:
            # ================= СОХРАНЕНИЕ ИСТОРИИ СПП =================
            print("\n" + "=" * 60)
            print("📊 СОХРАНЕНИЕ ИСТОРИИ СПП")
            print("=" * 60)

            save_spp_to_history(spreadsheet, campaigns_data, current_date_str)
        except:
            print(f"Ошибка сохранения SPP {str(traceback.format_exc())}")

        print("\n" + "=" * 60)
        print("✅ ВСЕ ДАННЫЕ УСПЕШНО ЗАГРУЖЕНЫ")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        write_error_to_sheet(str(e))


def test():
    with open('logs/all_items_dict.json', 'r', encoding='utf-8') as f:
        all_dict = json.load(f)
    with open('logs/advert_analytic.json', 'r', encoding='utf-8') as f:
        s_dict = json.load(f)
    with open('logs/position_analytic.json', 'r', encoding='utf-8') as f:
        l_dict = json.load(f)
    with open('logs/money_spent_advert_dict.json', 'r', encoding='utf-8') as f:
        money_spent_dict = json.load(f)
    upload_to_google_sheets(all_dict, s_dict, l_dict, money_spent_dict, {})


def test_with_custom_date(custom_date: str = None):
    """Тестовая функция с возможностью указать произвольную дату"""
    with open('logs/all_items_dict.json', 'r', encoding='utf-8') as f:
        all_dict = json.load(f)
    with open('logs/advert_analytic.json', 'r', encoding='utf-8') as f:
        s_dict = json.load(f)
    with open('logs/position_analytic.json', 'r', encoding='utf-8') as f:
        l_dict = json.load(f)
    with open('logs/money_spent_advert_dict.json', 'r', encoding='utf-8') as f:
        money_spent_dict = json.load(f)

    if custom_date:
        print(f"\n🧪 ТЕСТОВЫЙ РЕЖИМ: используется дата {custom_date}")
        original_get_date = get_current_date_moscow
        globals()['get_current_date_moscow'] = lambda: custom_date

        upload_to_google_sheets(all_dict, s_dict, l_dict, money_spent_dict, {})

        globals()['get_current_date_moscow'] = original_get_date
    else:
        upload_to_google_sheets(all_dict, s_dict, l_dict, money_spent_dict, {})


def test_technical_sheet_with_saved_data(custom_date: str = None):
    """Тестовая функция для проверки работы Технического листа"""
    print("\n" + "=" * 80)
    print("🧪 ТЕСТОВЫЙ РЕЖИМ: ЗАГРУЗКА ИЗ СОХРАНЕННЫХ ДАННЫХ")
    print("=" * 80)

    files = {
        'all_items_dict': 'logs/all_items_dict.json',
        'advert_analytic': 'logs/advert_analytic.json',
        'position_analytic': 'logs/position_analytic.json',
        'money_spent_advert_dict': 'logs/money_spent_advert_dict.json',
        'tech_dict': 'logs/tech_dict.json'
    }

    missing_files = []
    for name, path in files.items():
        if not os.path.exists(path):
            missing_files.append(f"{name} ({path})")

    if missing_files:
        print("\n❌ ОТСУТСТВУЮТ ФАЙЛЫ ДАННЫХ:")
        for f in missing_files:
            print(f"   - {f}")
        print("\n💡 Сначала запустите main.py в обычном режиме, чтобы сохранить данные")
        return False

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

    if not all_items_dict or not advert_analytic:
        print("\n❌ НЕТ ДАННЫХ ДЛЯ ТЕСТА")
        return False

    print("\n📊 ИНФОРМАЦИЯ О ТОВАРАХ ИЗ advert_analytic:")
    for offer_id, campaigns in list(advert_analytic.items())[:5]:
        if campaigns:
            first = campaigns[0]
            print(f"   📦 {offer_id}:")
            print(f"      - Цена до скидки: {first.get('product_price_before', 'Нет')}")
            print(f"      - Цена для покупателя: {first.get('product_price', 'Нет')}")
            print(f"      - Объем: {first.get('item_volume_l', 'Нет')} л")
            print(f"      - Комиссия FBO: {first.get('commission_fbo', 'Нет')}")
            print(f"      - Себестоимость: {first.get('cost_price', 'Нет')}")
            print(f"      - Остатки: {first.get('stock_balance', 'Нет')}")

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
    """Быстрый тест с фиксированной датой"""
    print("\n🔥 БЫСТРЫЙ ТЕСТ ТЕХНИЧЕСКОГО ЛИСТА")
    print("   Используется дата: 15.05.2026")
    test_technical_sheet_with_saved_data(custom_date="15.05.2026")


def debug_technical_sheet():
    """Отладочная функция для проверки расчетов Технического листа"""
    print("\n" + "=" * 80)
    print("🐛 ОТЛАДКА ТЕХНИЧЕСКОГО ЛИСТА (БЕЗ ЗАГРУЗКИ)")
    print("=" * 80)

    try:
        with open('logs/advert_analytic.json', 'r', encoding='utf-8') as f:
            advert_analytic = json.load(f)
        print(f"✅ Загружено {len(advert_analytic)} товаров")
    except Exception as e:
        print(f"❌ Ошибка загрузки: {e}")
        return

    try:
        with open('logs/tech_dict.json', 'r', encoding='utf-8') as f:
            tech_dict = json.load(f)
        print(f"✅ Загружен tech_dict")
        local_sales_percent = float(tech_dict.get('local_sales_percent', 87))
        print(f"   - local_sales_percent: {local_sales_percent}%")
    except Exception as e:
        print(f"⚠️ tech_dict не загружен, используем 87%")
        local_sales_percent = 87

    tax_rate = 6.0
    acquiring_rate = 1.0
    non_local_ratio = (100 - local_sales_percent) / 100
    avg_markup = 0.08

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

        commission_percent = get_commission_rate(commission_str)
        acquiring = calculate_acquiring(price_before, acquiring_rate)

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