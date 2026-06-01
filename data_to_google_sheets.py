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


def safe_update_cell(sheet, range_name, values, value_input_option='USER_ENTERED', max_retries=MAX_QUOTA_RETRIES):
    """
    Безопасное обновление ячейки с обработкой 429
    """
    for attempt in range(max_retries):
        try:
            return sheet.update(range_name=range_name, values=values, value_input_option=value_input_option)
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
            drr_total = clean_numeric_value(row[5]) if len(row) > 5 else 0.0

            if product and product != "":
                drr_data[product] = drr_total
                all_products.append(product)

        if not drr_data:
            print("  ⚠️ Нет данных для отображения")
            return False

        sorted_products = sorted(all_products)
        drr_sheet = setup_drr_total_sheet(spreadsheet)
        existing_data = safe_get_values(drr_sheet)

        # Ищем существующую колонку с этой датой
        date_column_index = None
        if len(existing_data) > 2:
            headers_row = existing_data[2] if len(existing_data) > 2 else []
            for idx, header in enumerate(headers_row):
                if header and header.strip() == current_date_str:
                    date_column_index = idx
                    break

        # Если колонки нет - ВСТАВЛЯЕМ НОВУЮ КОЛОНКУ СПРАВА ОТ ПОСЛЕДНЕЙ ДАТЫ
        # НО так, чтобы новые даты были СЛЕВА (вставляем после колонки "Артикул")
        if date_column_index is None:
            # Новая колонка всегда вставляется после колонки "Артикул" (колонка B)
            # Это обеспечит порядок: Артикул, самая новая дата, предыдущие даты...
            insert_col_index = 2  # Вставляем после колонки A (Артикул)

            col_letter = get_column_letter(insert_col_index + 1)  # +1 потому что вставляем новую

            # Вставляем новую колонку на позицию 2 (после Артикула)
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
            date_column_index = insert_col_index - 1  # Индекс после вставки

            print(f"  🆕 Добавлена новая колонка для даты: {current_date_str} (столбец {col_letter}) - в начало")

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
                batch_data.append({
                    'range': f"{col_letter}{product_row_index}",
                    'values': [[round(drr_data[product], 2) if drr_data[product] != 0 else ""]]
                })
            else:
                row_data = [product]
                # Заполняем пустыми значениями до новой колонки
                # После вставки новой колонки в начало, старые данные сдвинулись
                while len(row_data) < date_column_index:
                    row_data.append("")
                row_data.append(round(drr_data[product], 2) if drr_data[product] != 0 else "")
                safe_update_cell(drr_sheet, f"A{current_row}", [row_data], value_input_option='USER_ENTERED')
                current_row += 1

        # Обновляем данные
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
        print(f"     - Даты отсортированы: новые слева")

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

    try:
        try:
            sheet = spreadsheet.worksheet(sheet_title)
            print("  📄 Лист стоимости логистики уже существует")
            return sheet
        except gspread.exceptions.WorksheetNotFound:
            print("  🆕 Создание листа стоимости логистики...")

            sheet = safe_api_call(spreadsheet.add_worksheet, title=sheet_title, rows=100, cols=10)
            time.sleep(2)

            safe_update_cell(sheet, "A1", [["📊 ТАБЛИЦА СТОИМОСТИ ЛОГИСТИКИ ОЗОН"]], value_input_option='USER_ENTERED')
            headers = [h['name'] for h in LOGISTICS_PRICE_CONFIG["headers"]]
            safe_update_cell(sheet, "A2", [headers], value_input_option='USER_ENTERED')
            safe_update_cell(sheet, "A3", LOGISTICS_PRICE_CONFIG["default_data"], value_input_option='USER_ENTERED')

            note_row = 3 + len(LOGISTICS_PRICE_CONFIG["default_data"]) + 1
            safe_update_cell(sheet, f"A{note_row}", [[LOGISTICS_PRICE_CONFIG["note"]]],
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
        print(f"  ❌ Ошибка при настройке листа логистики: {e}")
        raise


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
    """Настраивает Технический лист (создает если нет)"""
    print("\n📋 НАСТРОЙКА ТЕХНИЧЕСКОГО ЛИСТА")

    sheet_title = TECHNICAL_SHEET_CONFIG["sheet_name"]

    try:
        tech_sheet = spreadsheet.worksheet(sheet_title)
        print("  📄 Технический лист уже существует")

        all_values = safe_get_values(tech_sheet)
        products_start_row = 8
        for idx, row in enumerate(all_values):
            if row and len(row) > 0 and "ТОВАРЫ В ПРОДАЖЕ" in str(row[0]):
                products_start_row = idx + 3
                break

        print(f"  📍 Строка начала данных товаров: {products_start_row}")
        return tech_sheet, products_start_row

    except gspread.exceptions.WorksheetNotFound:
        print("  🆕 Технический лист не найден, создаем новый...")

        tech_sheet = None
        for attempt in range(MAX_QUOTA_RETRIES):
            try:
                tech_sheet = spreadsheet.add_worksheet(title=sheet_title, rows=5000, cols=50)
                break
            except Exception as e:
                error_str = str(e)
                if '429' in error_str or 'Quota exceeded' in error_str:
                    wait_time = QUOTA_RETRY_DELAY
                    print(
                        f"  ⏳ Квота API при создании листа. Пауза {wait_time} сек... (попытка {attempt + 1}/{MAX_QUOTA_RETRIES})")
                    time.sleep(wait_time)
                    if attempt == MAX_QUOTA_RETRIES - 1:
                        raise
                else:
                    raise

        time.sleep(2)

        for attempt in range(MAX_QUOTA_RETRIES):
            try:
                tech_sheet.clear()
                break
            except Exception as e:
                error_str = str(e)
                if '429' in error_str or 'Quota exceeded' in error_str:
                    wait_time = QUOTA_RETRY_DELAY
                    print(
                        f"  ⏳ Квота API при очистке. Пауза {wait_time} сек... (попытка {attempt + 1}/{MAX_QUOTA_RETRIES})")
                    time.sleep(wait_time)
                else:
                    raise
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
            tech_sheet, f"A{products_start_row}:I{products_start_row}",
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
        safe_update_cell(tech_sheet, f"A{note_row}", [
            ["💡 Примечание: Стоимость логистики берется из листа 'Стоимость логистики'. Редактируйте таблицу там."]
        ], value_input_option='USER_ENTERED')
        safe_format_range(
            tech_sheet, f"A{note_row}:I{note_row}",
            CellFormat(textFormat=TextFormat(italic=True, fontSize=9), backgroundColor=Color(0.95, 0.95, 0.9))
        )

        print("  ✅ Технический лист создан и настроен")
        return tech_sheet, products_start_row + 2


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
            try:
                sheet = spreadsheet.worksheet(sheet_title)
                print(f"  📄 Лист {sheet_title} уже существует")
                return sheet
            except gspread.exceptions.WorksheetNotFound:
                print(f"  🆕 Создание нового листа {sheet_title}...")
                sheet = safe_api_call(spreadsheet.add_worksheet, title=sheet_title, rows=10000, cols=50)
                time.sleep(2)

                safe_update_cell(sheet, "A1", [CHP_COMMON_CONFIG["headers"]], value_input_option='USER_ENTERED')
                safe_format_range(
                    sheet, "A1:F1",
                    CellFormat(textFormat=TextFormat(bold=True, fontSize=11),
                               backgroundColor=Color(0.85, 0.95, 0.85))
                )

                for idx, header in enumerate(CHP_COMMON_CONFIG["headers"], start=1):
                    col_letter = get_column_letter(idx)
                    safe_api_call(set_column_width, sheet, col_letter, 150 if idx == 0 else 120)

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
            try:
                sheet = spreadsheet.worksheet(sheet_title)
                print(f"  📄 Лист {sheet_title} уже существует")
                return sheet
            except gspread.exceptions.WorksheetNotFound:
                print(f"  🆕 Создание нового листа {sheet_title}...")
                sheet = safe_api_call(spreadsheet.add_worksheet, title=sheet_title, rows=10000, cols=50)
                time.sleep(2)

                safe_update_cell(sheet, "A1", [CHP_DRR_CONFIG["headers"]], value_input_option='USER_ENTERED')
                safe_format_range(
                    sheet, "A1:F1",
                    CellFormat(textFormat=TextFormat(bold=True, fontSize=11),
                               backgroundColor=Color(0.85, 0.95, 0.85))
                )

                for idx, header in enumerate(CHP_DRR_CONFIG["headers"], start=1):
                    col_letter = get_column_letter(idx)
                    safe_api_call(set_column_width, sheet, col_letter, 150 if idx == 0 else 120)

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
        spp = calculate_spp(price_before, price_for_buyer)
        tax = calculate_tax(price_for_buyer, tax_rate)

        all_products_data.append([
            offer_id, sku, price_before, price_for_buyer, acquiring,
            stock_balance, commission_percent, volume_l, logistics
        ])

        print(f"  📦 {offer_id}: цена={price_before:.2f}, логистика={logistics:.2f}, СПП={spp:.2f}%, налог={tax:.2f}")

    if all_products_data:
        print(f"\n  📝 Запись {len(all_products_data)} товаров одним запросом...")

        start_row = products_start_row
        end_col = get_column_letter(len(TECHNICAL_SHEET_CONFIG["products_headers"]))
        range_name = f"A{start_row}:{end_col}{start_row + len(all_products_data) - 1}"

        safe_update_cell(tech_sheet, range_name, all_products_data, value_input_option='USER_ENTERED')

        print(f"  ✅ Добавлено {len(all_products_data)} товаров одним запросом")

        last_row = products_start_row + len(all_products_data) - 1

        for col, color, bold in [('C', Color(0.95, 0.9, 1), True),
                                 ('D', Color(0.9, 1, 0.9), True),
                                 ('E', Color(1, 0.95, 0.8), False),
                                 ('I', Color(0.7, 0.85, 1), False)]:
            try:
                fmt = CellFormat(backgroundColor=color)
                if bold:
                    fmt.textFormat = TextFormat(bold=True)
                safe_format_range(tech_sheet, f"{col}{products_start_row}:{col}{last_row}", fmt)
                time.sleep(0.5)
            except:
                pass

        print("  💡 Примечание: Эквайринг рассчитывается автоматически")
        print("  💡 Стоимость логистики берется из листа 'Стоимость логистики'")

    print("  ✅ Технический лист расширенно обновлен")


def update_chp_sheets(spreadsheet, campaigns_data: Dict, logistics_price_sheet,
                      current_date_str: str, tech_dict: Dict = None,
                      drr_for_products: Dict = None):
    """Обновляет страницы ЧП_товары_общая и ЧП_товары_ДРР - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
    print("\n💰 ОБНОВЛЕНИЕ ЛИСТОВ ЧП (ОПТИМИЗИРОВАННО)")

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

    chp_common_sheet = setup_chp_common_sheet(spreadsheet)
    chp_drr_sheet = setup_chp_drr_sheet(spreadsheet)

    date_col_common = add_date_column_to_chp_sheet(chp_common_sheet, current_date_str)
    date_col_drr = add_date_column_to_chp_sheet(chp_drr_sheet, current_date_str)

    col_letter_common = get_column_letter(date_col_common)
    col_letter_drr = get_column_letter(date_col_drr)

    # Получаем все данные один раз
    all_rows_common = safe_get_values(chp_common_sheet)
    all_rows_drr = safe_get_values(chp_drr_sheet)

    product_row_map_common = {}
    product_row_map_drr = {}

    for row_idx, row in enumerate(all_rows_common[1:], start=2):
        if row and len(row) > 0 and row[0] and row[0] != 'Артикул':
            product_row_map_common[row[0]] = row_idx

    for row_idx, row in enumerate(all_rows_drr[1:], start=2):
        if row and len(row) > 0 and row[0] and row[0] != 'Артикул':
            product_row_map_drr[row[0]] = row_idx

    common_updates = []
    drr_updates = []
    new_rows_common = []
    new_rows_drr = []

    print(f"\n  📝 ПОДГОТОВКА ДАННЫХ ДЛЯ {len(campaigns_data)} ТОВАРОВ:")

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
        logistics = calculate_logistics_cost(volume_l, price_before, logistics_prices,
                                             local_sales_percent, markup_percent)
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

        # Расчет для ЧП_общая
        if drr_total > 0:
            chp_common = calculate_chp(price_before, commission_percent, logistics, tax,
                                       cost_price, acquiring, drr_total, offer_id, verbose=False)
            common_value = chp_common
        else:
            common_value = -ad_expenses if ad_expenses > 0 else 0

        # Расчет для ЧП_ДРР
        if drr_cpo > 0:
            chp_drr = calculate_chp(price_before, commission_percent, logistics, tax,
                                    cost_price, acquiring, drr_cpo, offer_id, verbose=False)
            drr_value = chp_drr
        else:
            drr_value = -ad_expenses if ad_expenses > 0 else 0

        if offer_id in product_row_map_common:
            common_updates.append((product_row_map_common[offer_id], common_value))
        else:
            new_rows_common.append([offer_id, "", common_value])

        if offer_id in product_row_map_drr:
            drr_updates.append((product_row_map_drr[offer_id], drr_value))
        else:
            new_rows_drr.append([offer_id, "", drr_value])

    # ПАКЕТНОЕ ОБНОВЛЕНИЕ
    print(f"\n  💾 ПАКЕТНАЯ ЗАПИСЬ ДАННЫХ:")

    # Обновляем существующие строки в ЧП_товары_общая
    if common_updates:
        print(f"     Обновление {len(common_updates)} строк в ЧП_товары_общая...")
        batch_data = []
        for row_num, value in common_updates:
            batch_data.append({
                'range': f"{col_letter_common}{row_num}",
                'values': [[value]]
            })

        try:
            safe_batch_update(chp_common_sheet, batch_data, value_input_option='USER_ENTERED')
            print(f"     ✅ Обновлено {len(common_updates)} строк")
        except Exception as e:
            print(f"     ⚠️ Ошибка пакетного обновления: {e}")
            for row_num, value in common_updates:
                try:
                    safe_update_cell(chp_common_sheet, f"{col_letter_common}{row_num}", [[value]],
                                     value_input_option='USER_ENTERED')
                    time.sleep(0.5)
                except Exception as e2:
                    print(f"     ⚠️ Ошибка обновления строки {row_num}: {e2}")

    # Добавляем новые строки в ЧП_товары_общая
    if new_rows_common:
        print(f"     Добавление {len(new_rows_common)} новых строк в ЧП_товары_общая...")
        try:
            start_row = len(all_rows_common) + 1
            range_name = f"A{start_row}:C{start_row + len(new_rows_common) - 1}"
            safe_update_cell(chp_common_sheet, range_name, new_rows_common, value_input_option='USER_ENTERED')
            print(f"     ✅ Добавлено {len(new_rows_common)} строк")
        except Exception as e:
            print(f"     ⚠️ Ошибка добавления строк: {e}")

    # Обновляем существующие строки в ЧП_товары_ДРР
    if drr_updates:
        print(f"     Обновление {len(drr_updates)} строк в ЧП_товары_ДРР...")
        batch_data = []
        for row_num, value in drr_updates:
            batch_data.append({
                'range': f"{col_letter_drr}{row_num}",
                'values': [[value]]
            })

        try:
            safe_batch_update(chp_drr_sheet, batch_data, value_input_option='USER_ENTERED')
            print(f"     ✅ Обновлено {len(drr_updates)} строк")
        except Exception as e:
            print(f"     ⚠️ Ошибка пакетного обновления: {e}")
            for row_num, value in drr_updates:
                try:
                    safe_update_cell(chp_drr_sheet, f"{col_letter_drr}{row_num}", [[value]],
                                     value_input_option='USER_ENTERED')
                    time.sleep(0.5)
                except Exception as e2:
                    print(f"     ⚠️ Ошибка обновления строки {row_num}: {e2}")

    # Добавляем новые строки в ЧП_товары_ДРР
    if new_rows_drr:
        print(f"     Добавление {len(new_rows_drr)} новых строк в ЧП_товары_ДРР...")
        try:
            all_rows_drr_current = safe_get_values(chp_drr_sheet)
            start_row = len(all_rows_drr_current) + 1
            range_name = f"A{start_row}:C{start_row + len(new_rows_drr) - 1}"
            safe_update_cell(chp_drr_sheet, range_name, new_rows_drr, value_input_option='USER_ENTERED')
            print(f"     ✅ Добавлено {len(new_rows_drr)} строк")
        except Exception as e:
            print(f"     ⚠️ Ошибка добавления строк: {e}")

    # Обновляем формулы сумм
    try:
        update_total_chp_formula(chp_common_sheet, date_col_common)
        update_total_chp_formula(chp_drr_sheet, date_col_drr)
    except Exception as e:
        print(f"  ⚠️ Ошибка при обновлении формул: {e}")

    print(f"\n  ✅ ЧП_товары_общая: обработано {len(common_updates) + len(new_rows_common)} товаров")
    print(f"  ✅ ЧП_товары_ДРР: обработано {len(drr_updates) + len(new_rows_drr)} товаров")


def add_chp_per_day_column(spreadsheet, campaigns_data: Dict, current_date_str: str, tech_dict: Dict = None,
                           drr_for_products: Dict = None):
    """Добавляет на дашборд столбец 'ЧП / в день' - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
    print("\n📊 ДОБАВЛЕНИЕ СТОЛБЦА 'ЧП / в день' НА DASHBOARD")

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")
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
                local_sales_percent = float(local_percent_cell) if local_percent_cell and str(
                    local_percent_cell) not in ['—', ''] else 87.0

            logistics_sheet = get_or_create_sheet(spreadsheet, LOGISTICS_PRICE_CONFIG["sheet_name"])
            logistics_prices = load_logistics_prices_from_sheet(logistics_sheet)
            markup_percent = get_markup_percent(spreadsheet)

            dashboard_data = safe_get_values(dashboard)

            if len(dashboard_data) < 2:
                print("  ⚠️ Нет данных в дашборде")
                return

            headers = dashboard_data[0]
            chp_col_index = None

            for idx, header in enumerate(headers):
                if header and "ЧП / в день" in str(header):
                    chp_col_index = idx
                    break

            if chp_col_index is None:
                chp_col_index = len(headers)
                col_letter = get_column_letter(chp_col_index + 1)
                safe_update_cell(dashboard, f"{col_letter}1", [["ЧП / в день"]], value_input_option='USER_ENTERED')
                safe_format_range(
                    dashboard, f"{col_letter}1",
                    CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
                )
                safe_api_call(set_column_width, dashboard, col_letter, 120)
                print(f"  🆕 Добавлен столбец 'ЧП / в день' (столбец {col_letter})")
            else:
                col_letter = get_column_letter(chp_col_index + 1)

            start_row = 2
            end_row = len(dashboard_data)

            # Очищаем старые данные
            if end_row > 1:
                try:
                    clear_range = f"{col_letter}2:{col_letter}{end_row + 5}"
                    safe_api_call(dashboard.batch_clear, [clear_range])
                    print(f"     🗑️ Очищен столбец {col_letter}")
                    time.sleep(1)
                except Exception as e:
                    print(f"     ⚠️ Ошибка очистки: {e}")

            chp_values = []

            for row in dashboard_data[1:]:
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
                    drr_total = clean_numeric_value(row[5]) if len(row) > 5 else 0

                    ad_expenses = 0.0
                    if drr_for_products and offer_id in drr_for_products:
                        drr_data = drr_for_products[offer_id]
                        if isinstance(drr_data, dict):
                            ad_expenses = drr_data.get('money_spent', 0.0)

                    commission_percent = get_commission_rate(commission_str)
                    acquiring = calculate_acquiring(price_before, acquiring_rate)
                    logistics = calculate_logistics_cost(volume_l, price_before, logistics_prices,
                                                         local_sales_percent, markup_percent)
                    tax = calculate_tax(price_for_buyer, tax_rate)

                    if drr_total > 0:
                        chp = calculate_chp(price_before, commission_percent, logistics, tax,
                                            cost_price, acquiring, drr_total, offer_id, verbose=False)
                        chp_value = chp
                    else:
                        chp_value = -ad_expenses if ad_expenses > 0 else 0

                    chp_values.append([chp_value])
                else:
                    chp_values.append([""])

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

            break  # Успешно завершили, выходим из цикла

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
    """Добавляет на дашборд столбец СПП - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
    print("\n📊 ДОБАВЛЕНИЕ СТОЛБЦА 'СПП' НА DASHBOARD")

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")
            dashboard_data = safe_get_values(dashboard)

            if len(dashboard_data) < 2:
                print("  ⚠️ Нет данных в дашборде")
                return

            headers = dashboard_data[0]
            spp_col_index = None

            for idx, header in enumerate(headers):
                if header and header == "СПП":
                    spp_col_index = idx
                    break

            if spp_col_index is None:
                spp_col_index = len(headers)
                col_letter = get_column_letter(spp_col_index + 1)
                safe_update_cell(dashboard, f"{col_letter}1", [["СПП"]], value_input_option='USER_ENTERED')
                safe_format_range(
                    dashboard, f"{col_letter}1",
                    CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.85, 0.95, 0.85))
                )
                safe_api_call(set_column_width, dashboard, col_letter, 100)
                print(f"  🆕 Добавлен столбец 'СПП' (столбец {col_letter})")
            else:
                col_letter = get_column_letter(spp_col_index + 1)

            start_row = 2
            end_row = len(dashboard_data)

            # Очищаем старые данные в столбце
            if end_row > 1:
                try:
                    clear_range = f"{col_letter}2:{col_letter}{end_row + 5}"
                    safe_api_call(dashboard.batch_clear, [clear_range])
                    print(f"     🗑️ Очищен столбец {col_letter}")
                    time.sleep(1)
                except Exception as e:
                    print(f"     ⚠️ Ошибка очистки: {e}")

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

            if spp_values:
                range_name = f"{col_letter}{start_row}:{col_letter}{start_row + len(spp_values) - 1}"
                safe_update_cell(dashboard, range_name, spp_values, value_input_option='USER_ENTERED')

                safe_format_range(
                    dashboard, f"{col_letter}{start_row}:{col_letter}{start_row + len(spp_values) - 1}",
                    CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'}, horizontalAlignment='RIGHT')
                )

                print(f"  ✅ Столбец 'СПП' обновлен ({len(spp_values)} записей)")

            break  # Успешно завершили

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


def get_or_create_sheet(spreadsheet, title: str, rows=1000, cols=30):
    """Получает существующий лист или создает новый"""
    try:
        return spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        if title == "История DASHBOARD":
            rows = 100000
            print(f"  🆕 Создание листа {title} с {rows} строками")
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


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
        total_revenue_item = clean_numeric_value(item.get("total_revenue", 0))
        total_ordered_units = clean_int_value(item.get("total_ordered_units", 0))

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

        drr_search = calculate_drr(expenses_search, selled_search)
        drr_total = calculate_drr(money_spent, total_revenue_item)

        drr_for_products[offer_id] = {
            'drr_total': drr_total,
            'drr_cpo': drr_cpo,
            'drr_search': drr_search,
            'money_spent': money_spent,
            'revenue': total_revenue_item
        }

        log_dashboard_item(
            offer_id, total_revenue_item, expenses_search, selled_search,
            drr_cpo, money_spent, drr_search, drr_cpo, drr_total
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
        totals['total_money_spent_from_dict'] += money_spent

    dashboard_rows.sort(key=lambda x: x[2], reverse=True)

    return dashboard_rows, totals, drr_for_products


def update_dashboard_sheet(dashboard, dashboard_data: List[List]):
    current_data = safe_get_values(dashboard)
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
    """Сохраняет текущие данные из листа DASHBOARD в историю - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
    print("\n💾 СОХРАНЕНИЕ DASHBOARD В ИСТОРИЮ")

    for attempt in range(MAX_QUOTA_RETRIES):
        try:
            dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")
            dashboard_data = safe_get_values(dashboard)

            if len(dashboard_data) <= 1:
                print("  ⚠️ Нет данных в DASHBOARD для сохранения")
                return False

            history_sheet = setup_history_dashboard_sheet(spreadsheet)
            existing_data = safe_get_values(history_sheet)

            # Находим строки для удаления
            rows_to_delete = []
            for row_idx, row in enumerate(existing_data):
                if row and len(row) > 0 and row[0] == current_date:
                    rows_to_delete.append(row_idx + 1)

            # Удаляем строки
            deleted_count = 0
            for row_idx in sorted(rows_to_delete, reverse=True):
                try:
                    if deleted_count > 0 and deleted_count % 10 == 0:
                        time.sleep(2)
                    history_sheet.delete_rows(row_idx)
                    deleted_count += 1
                except Exception as e:
                    if '429' in str(e):
                        time.sleep(QUOTA_RETRY_DELAY)
                        history_sheet.delete_rows(row_idx)
                        deleted_count += 1
                    else:
                        print(f"     ⚠️ Ошибка при удалении строки {row_idx}: {e}")

            if deleted_count > 0:
                print(f"     🗑️ Удалено {deleted_count} старых записей за {current_date}")

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

            # Получаем актуальные данные после удаления
            existing_data = safe_get_values(history_sheet)

            # Находим место для вставки
            insert_row_index = 3
            if len(existing_data) > 1 and existing_data[1] and len(existing_data[1]) > 0:
                first_cell = str(existing_data[1][0]) if existing_data[1][0] else ""
                if first_cell.startswith('💡') or first_cell.startswith('📊'):
                    insert_row_index = 3
                else:
                    insert_row_index = 2
            else:
                insert_row_index = 2

            # ОДНИМ запросом вставляем все новые данные
            range_name = f"A{insert_row_index}:G{insert_row_index + len(history_rows) - 1}"
            safe_update_cell(history_sheet, range_name, history_rows, value_input_option='USER_ENTERED')

            # Добавляем строку ИТОГО
            totals_row_start = insert_row_index + len(history_rows)
            totals_row = [[
                f"ИТОГО за {current_date}", "",
                total_revenue_for_date if total_revenue_for_date > 0 else sum(
                    clean_numeric_value(row[2]) for row in history_rows),
                total_orders_for_date if total_orders_for_date > 0 else sum(
                    clean_int_value(row[3]) for row in history_rows),
                "", "", ""
            ]]

            safe_update_cell(history_sheet, f"A{totals_row_start}", totals_row, value_input_option='USER_ENTERED')

            # Форматирование
            end_row = totals_row_start - 1
            for col in ['C', 'D', 'E', 'F', 'G']:
                try:
                    safe_format_range(
                        history_sheet, f"{col}{insert_row_index}:{col}{end_row}",
                        CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'})
                    )
                except:
                    pass

            print(f"  ✅ История DASHBOARD обновлена: {len(history_rows)} записей за {current_date}")
            return True

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'Quota exceeded' in error_str:
                wait_time = QUOTA_RETRY_DELAY
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
        safe_update_cell(sheet, f"A{existing_row_index}", [full_row], value_input_option='USER_ENTERED')
        print(f"  ✅ Обновлена строка за {current_date_str}")
    else:
        print(f"  📝 Добавление новой строки за {current_date_str}")
        safe_api_call(sheet.insert_row, full_row, index=7)
        print(f"  ✅ Добавлена строка за {current_date_str}")
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
    """Тестовая функция с возможностью указать произвольную дату"""
    with open('all_items_dict.json', 'r', encoding='utf-8') as f:
        all_dict = json.load(f)
    with open('advert_analytic.json', 'r', encoding='utf-8') as f:
        s_dict = json.load(f)
    with open('position_analytic.json', 'r', encoding='utf-8') as f:
        l_dict = json.load(f)
    with open('money_spent_advert_dict.json', 'r', encoding='utf-8') as f:
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
        'all_items_dict': 'all_items_dict.json',
        'advert_analytic': 'advert_analytic.json',
        'position_analytic': 'position_analytic.json',
        'money_spent_advert_dict': 'money_spent_advert_dict.json',
        'tech_dict': 'tech_dict.json'
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