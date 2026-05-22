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


def migrate_existing_dashboard_to_history(spreadsheet):
    """
    Функция для однократного переноса существующих данных DASHBOARD в историю
    Запустите вручную при необходимости
    """
    print("\n🔄 МИГРАЦИЯ СУЩЕСТВУЮЩИХ ДАННЫХ DASHBOARD В ИСТОРИЮ")

    try:
        dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")
        dashboard_data = execute_with_retry(dashboard.get_all_values)

        if len(dashboard_data) <= 1:
            print("  ⚠️ Нет данных в DASHBOARD для миграции")
            return False

        # Парсим дату из данных? (в DASHBOARD нет даты)
        # Поэтому лучше использовать текущую дату или запросить у пользователя
        current_date = get_current_date_moscow()
        print(f"  📅 Используется дата: {current_date}")

        # Подготавливаем данные (пропускаем заголовки)
        data_rows = []
        for row in dashboard_data[1:]:  # Пропускаем заголовки
            if row and len(row) >= 6 and row[0] not in ["", "ИТОГО"]:
                data_rows.append(row)

        if data_rows:
            result = save_dashboard_history(spreadsheet, data_rows, current_date)
            if result:
                print("  ✅ Миграция данных завершена успешно")
                return True

        return False

    except Exception as e:
        print(f"  ❌ Ошибка при миграции: {e}")
        return False


def save_dashboard_history(spreadsheet, dashboard_data: List[List], current_date: str):
    """
    Сохраняет текущие данные DASHBOARD в историю
    Добавляет новую запись только если дата изменилась
    Автоматически расширяет лист при необходимости
    """
    print("\n📜 СОХРАНЕНИЕ ИСТОРИИ DASHBOARD")

    try:
        # Получаем или создаем лист истории с большим запасом строк
        # Используем 50 000 строк для истории (на 3-5 лет ежедневных записей)
        history_sheet = get_or_create_sheet(spreadsheet, HISTORY_DASHBOARD_CONFIG["sheet_name"], rows=50000, cols=20)

        # Проверяем, нужно ли настраивать заголовки
        all_values = execute_with_retry(history_sheet.get_all_values)

        if len(all_values) < 2 or (len(all_values) > 0 and all_values[0][0] != "Дата"):
            print("  🆕 Настройка структуры листа истории...")
            # Настраиваем заголовки
            headers = [h['name'] for h in HISTORY_DASHBOARD_CONFIG["headers"]]
            execute_with_exponential_backoff(history_sheet.update, "A1", [headers])

            # Форматируем заголовки
            end_col = get_column_letter(len(headers))
            execute_with_exponential_backoff(
                format_cell_range, history_sheet, f"A1:{end_col}1",
                CellFormat(textFormat=TextFormat(bold=True, fontSize=11), backgroundColor=Color(0.85, 0.95, 0.85))
            )

            # Устанавливаем ширину столбцов
            for idx, header in enumerate(HISTORY_DASHBOARD_CONFIG["headers"], start=1):
                col_letter = get_column_letter(idx)
                if 'width' in header:
                    execute_with_exponential_backoff(set_column_width, history_sheet, col_letter, header['width'])

            # Добавляем примечание
            execute_with_exponential_backoff(history_sheet.update, f"A2", [[HISTORY_DASHBOARD_CONFIG["note"]]])
            execute_with_exponential_backoff(
                format_cell_range, history_sheet, f"A2:{end_col}2",
                CellFormat(textFormat=TextFormat(italic=True, fontSize=9), backgroundColor=Color(0.95, 0.95, 0.9))
            )

            execute_with_exponential_backoff(set_frozen, history_sheet, rows=2)
            print("  ✅ Структура листа истории настроена")
            next_row = 3  # Следующая строка для данных (после заголовков и примечания)
        else:
            # Определяем следующую свободную строку
            next_row = len(all_values) + 1
            # Пропускаем строку с примечанием, если она есть
            if len(all_values) > 1 and all_values[1] and "📊" in str(all_values[1][0]):
                next_row = max(next_row, 3)
            print(f"  📄 Лист истории существует, следующая строка: {next_row}")

        # Проверяем, нужно ли сохранять данные сегодня
        # Получаем последнюю дату в истории
        last_date_in_history = None
        if len(all_values) > 2:
            # Ищем последнюю непустую дату
            for row in reversed(all_values[2:]):  # Пропускаем заголовки и примечание
                if row and len(row) > 0 and row[0] and row[0] != current_date:
                    last_date_in_history = row[0]
                    break
                elif row and len(row) > 0 and row[0] == current_date:
                    print(f"  ⚠️ Данные за {current_date} уже сохранены в истории")
                    return False

        # Проверяем, изменилась ли дата с последней записи
        if last_date_in_history == current_date:
            print(f"  ⚠️ Данные за {current_date} уже сохранены, пропускаем")
            return False

        # Подготавливаем данные для сохранения
        history_rows = []
        for row in dashboard_data:
            if not row or len(row) < 6:
                continue
            # Формируем строку: [Дата, Артикул, Сумма продаж, Кол-во продаж, ДРР поиск, ДРР CPO, ДРР общий]
            history_row = [
                current_date,
                row[0],  # Артикул
                row[1],  # Сумма продаж
                row[2],  # Количество продаж
                row[3],  # ДРР поиск
                row[4],  # ДРР оплата за заказ
                row[5]  # ДРР общий
            ]
            history_rows.append(history_row)

        # Добавляем итоговую строку, если она есть (последняя строка в dashboard_data обычно с "ИТОГО")
        if dashboard_data and len(dashboard_data) > 0:
            last_row = dashboard_data[-1]
            if last_row and len(last_row) > 0 and last_row[0] == "ИТОГО":
                total_row = [
                    current_date,
                    "ИТОГО",
                    last_row[1],  # Общая сумма продаж
                    last_row[2],  # Общее количество продаж
                    last_row[3],  # Средний ДРР поиск (можно оставить как есть)
                    last_row[4],  # Средний ДРР CPO
                    last_row[5]  # Средний ДРР общий
                ]
                history_rows.append(total_row)

        # Сохраняем данные
        if history_rows:
            # Рассчитываем необходимый размер листа
            required_rows = next_row + len(history_rows) + 10  # +10 для запаса
            current_rows = len(all_values)

            # Проверяем и расширяем лист если нужно
            if required_rows > current_rows:
                rows_to_add = required_rows - current_rows
                print(f"  📏 Расширение листа: текущий размер {current_rows} строк, нужно {required_rows}")
                print(f"  ➕ Добавление {rows_to_add} строк...")

                try:
                    # Пробуем добавить строки
                    execute_with_exponential_backoff(history_sheet.add_rows, rows_to_add)
                    print(f"  ✅ Добавлено {rows_to_add} строк. Новый размер: {current_rows + rows_to_add} строк")
                    time.sleep(1)
                except Exception as e:
                    print(f"  ⚠️ Не удалось добавить строки через add_rows: {e}")
                    # Альтернативный метод: пытаемся обновить ячейку в нужной строке
                    try:
                        last_cell = f"G{required_rows}"
                        execute_with_exponential_backoff(history_sheet.update, last_cell, [[""]])
                        print(f"  ✅ Лист автоматически расширен записью в {last_cell}")
                        time.sleep(1)
                    except Exception as e2:
                        print(f"  ❌ Ошибка при расширении листа: {e2}")

            print(f"  💾 Сохранение {len(history_rows)} строк в историю (дата: {current_date})")
            print(f"  📍 Диапазон данных: A{next_row}:G{next_row + len(history_rows) - 1}")

            # Обновляем данные одной операцией
            range_start = f"A{next_row}"

            # Дополнительная проверка: убеждаемся что лист достаточно большой
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    execute_with_retry(history_sheet.update, range_start, history_rows,
                                       value_input_option='USER_ENTERED')
                    break
                except Exception as e:
                    if "exceeds grid limits" in str(e).lower() or "grid" in str(e).lower():
                        print(f"  ⚠️ Попытка {attempt + 1}: Превышен размер сетки, расширяем лист...")
                        rows_needed = next_row + len(history_rows) + 50
                        try:
                            history_sheet.add_rows(rows_needed)
                            print(f"  ✅ Добавлено {rows_needed} строк")
                            time.sleep(2)
                        except:
                            # Если add_rows не работает, пробуем прямой update в последнюю ячейку
                            try:
                                last_cell = f"G{rows_needed}"
                                history_sheet.update(last_cell, [[""]])
                                print(f"  ✅ Лист расширен через запись в {last_cell}")
                                time.sleep(2)
                            except:
                                pass
                    else:
                        raise e

            # Форматируем числа
            for col in ['C', 'D', 'E', 'F', 'G']:  # Столбцы с числами
                try:
                    execute_with_retry(
                        format_cell_range, history_sheet, f"{col}{next_row}:{col}{next_row + len(history_rows) - 1}",
                        CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'})
                    )
                except Exception as e:
                    print(f"  ⚠️ Не удалось отформатировать столбец {col}: {e}")

            # Фиксируем заголовки (замораживаем первые 2 строки)
            try:
                execute_with_exponential_backoff(set_frozen, history_sheet, rows=2)
            except:
                pass

            print(f"  ✅ История DASHBOARD обновлена: добавлено {len(history_rows)} записей за {current_date}")
            print(f"  📊 Текущий размер листа: ~{next_row + len(history_rows) - 1} строк")
            return True
        else:
            print("  ⚠️ Нет данных для сохранения в историю")
            return False

    except Exception as e:
        print(f"  ❌ Ошибка при сохранении истории DASHBOARD: {e}")
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

        settings_data = [["Ставка УСН + НДС (%)", technical_settings['tax_rate'], "%"],
                         ["Эквайринг (%)", technical_settings['acquiring_rate'], "%"]]
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
            if '429' in str(e) or 'Quota exceeded' in str(e):
                wait_time = min(30 * (2 ** attempt), 600)
                print(f"  ⏳ Превышен лимит. Пауза {wait_time} сек... (попытка {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                raise e
    raise Exception(f"Не удалось выполнить операцию после {max_retries} попыток")


def execute_with_retry(func, *args, **kwargs):
    return execute_with_exponential_backoff(func, *args, **kwargs)


def get_google_sheets_client():
    creds = Credentials.from_service_account_file(
        "google_sheets.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)


def get_or_create_sheet(spreadsheet, title: str, rows=1000, cols=30):
    try:
        return execute_with_exponential_backoff(spreadsheet.worksheet, title)
    except gspread.exceptions.WorksheetNotFound:
        # Для истории DASHBOARD создаем с максимальным запасом строк
        if title == "История DASHBOARD":
            rows = 100000  # 100 тысяч строк - достаточно на много лет
            print(f"  🆕 Создание листа {title} с {rows} строками (максимальный запас)")
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
                           drr_all_dict: Dict) -> Tuple[List[List], Dict]:
    dashboard_rows = []
    totals = {
        'total_orders': 0, 'total_expenses_search': 0, 'total_selled_search': 0,
        'total_revenue_all': 0, 'total_money_spent_from_dict': 0
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
        log_dashboard_item(offer_id, total_revenue_item, expenses_search, selled_search,
                           drr_from_dict, money_spent_from_dict, drr_search, drr_cpo, drr_total)
        dashboard_rows.append([offer_id, total_revenue_item, total_ordered_units, drr_search, drr_cpo, drr_total])
        totals['total_orders'] += total_ordered_units
        totals['total_expenses_search'] += expenses_search
        totals['total_selled_search'] += selled_search
        totals['total_revenue_all'] += total_revenue_item
        totals['total_money_spent_from_dict'] += money_spent_from_dict
    dashboard_rows.sort(key=lambda x: x[2], reverse=True)
    return dashboard_rows, totals


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

    # Возвращаем данные для истории (без итоговой строки, которую добавили выше)
    # Убираем последние две строки (пустая и ИТОГО)
    return dashboard_data[:-2] if len(dashboard_data) >= 2 else dashboard_data


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

    # Настройка ширины для нового столбца (столбец J, если аналитика начинается с A)
    # Вычисляем номер столбца для ДРР (9-й столбец в списке headers, начиная с 0 = столбец J)
    drr_col_letter = get_column_letter(get_column_index(ANALYTICS_CONFIG['start_column']) + 9)
    updates.append((f"SET_COLUMN_WIDTH_{drr_col_letter}", None))  # Маркер для установки ширины

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

    # Устанавливаем ширину для столбца ДРР
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


def prepare_product_row(item: Dict, campaigns_data: Dict, drr_all_dict: Dict, current_date_str: str) -> List:
    offer_id = item.get("offer_id")

    # Получаем общий ДРР из словаря (как в DASHBOARD)
    drr_total = 0.0
    if drr_all_dict and offer_id in drr_all_dict:
        drr_data = drr_all_dict[offer_id]
        if isinstance(drr_data, dict):
            drr_total = clean_numeric_value(drr_data.get('drr', 0))
        else:
            drr_total = clean_numeric_value(drr_data)

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
        drr_total  # Общий ДРР
    ]

    offer_campaigns = campaigns_data.get(offer_id, []) if campaigns_data else []
    search_campaigns = [c for c in offer_campaigns if c.get('camping_type') == 'Поиск']
    rec_campaigns = [c for c in offer_campaigns if c.get('camping_type') == 'Поиск и рекомендации']
    cpo_campaigns = [c for c in offer_campaigns if c.get('camping_type') == 'Оплата за заказ']

    search_data = format_campaign_data(search_campaigns, 'search')
    rec_data = format_campaign_data(rec_campaigns, 'recommendations')
    cpo_data = format_campaign_data(cpo_campaigns, 'cpo')

    return analytics_row + [""] + search_data + [""] + rec_data + [""] + cpo_data


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
                            drr_all_dict: Optional[Dict] = None):
    print("\n" + "=" * 60)
    print("🚀 НАЧАЛО ЗАГРУЗКИ ДАННЫХ В GOOGLE SHEETS")
    print("=" * 60)

    try:
        print("\n🔌 Подключение к Google Sheets...")
        client = get_google_sheets_client()
        spreadsheet = client.open_by_key(spread_id)
        current_date_str = get_current_date_moscow()
        print(f"📅 Текущая дата: {current_date_str}")

        # Новые листы
        print("\n" + "=" * 60)
        print("📦 НАСТРОЙКА НОВЫХ ЛИСТОВ")
        print("=" * 60)
        logistics_price_sheet = setup_logistics_price_sheet(spreadsheet)
        markup_sheet = setup_markup_sheet(spreadsheet)

        # DASHBOARD
        print("\n" + "=" * 60)
        print("📊 ОБРАБОТКА ЛИСТА DASHBOARD")
        print("=" * 60)
        dashboard = get_or_create_sheet(spreadsheet, "DASHBOARD")
        current_data = execute_with_retry(dashboard.get_all_values)
        expected_headers = [h['name'] for h in DASHBOARD_CONFIG['headers']]
        if len(current_data) == 0 or (len(current_data) > 0 and current_data[0] != expected_headers):
            setup_sheet_headers(dashboard, DASHBOARD_CONFIG, start_row=1)
        else:
            clear_old_dashboard_data(dashboard, len(current_data))
        dashboard_data, _ = prepare_dashboard_data(all_items_dict, campaigns_data, drr_all_dict)

        # Сохраняем данные в историю ПЕРЕД обновлением текущего DASHBOARD
        # Используем текущую дату из current_date_str
        save_dashboard_history(spreadsheet, dashboard_data, current_date_str)

        # Обновляем текущий DASHBOARD
        update_dashboard_sheet(dashboard, dashboard_data)
        print("✅ DASHBOARD успешно обновлен")

        # ТЕХНИЧЕСКИЙ ЛИСТ
        print("\n" + "=" * 60)
        print("🔧 ОБРАБОТКА ТЕХНИЧЕСКОГО ЛИСТА")
        print("=" * 60)
        tech_sheet, products_start_row = setup_technical_sheet(spreadsheet)
        update_technical_sheet(tech_sheet, campaigns_data, products_start_row, logistics_price_sheet)
        print("✅ ТЕХНИЧЕСКИЙ ЛИСТ успешно обновлен")

        # ЛИСТЫ ТОВАРОВ
        print("\n" + "=" * 60)
        print("📄 ОБРАБОТКА ЛИСТОВ ТОВАРОВ")
        print("=" * 60)
        for idx, item in enumerate(all_items_dict.values()):
            offer_id = item.get("offer_id")
            skus_list = item.get("skus", [])
            if 'price_before' not in item:
                item['price_before'] = 0
            print(f"\n🔄 Обработка товара {idx + 1}/{len(all_items_dict)}: {offer_id}")
            print(f"   SKU: {', '.join(skus_list)}")
            position_category = update_position_data(item, positions_data)
            item['avg_position_category'] = position_category
            try:
                sheet = execute_with_exponential_backoff(spreadsheet.worksheet, offer_id)
                need_setup = False
            except gspread.exceptions.WorksheetNotFound:
                sheet = execute_with_exponential_backoff(spreadsheet.add_worksheet, title=offer_id, rows=2000, cols=60)
                need_setup = True
                time.sleep(2)
            if need_setup:
                setup_product_sheet_structure(sheet, offer_id, skus_list)
            full_row = prepare_product_row(item, campaigns_data, drr_all_dict, current_date_str)
            update_product_sheet_batch(sheet, offer_id, full_row, current_date_str)
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


def test():
    with open('all_items_dict.json', 'r', encoding='utf-8') as f:
        all_dict = json.load(f)
    with open('advert_analytic.json', 'r', encoding='utf-8') as f:
        s_dict = json.load(f)
    with open('position_analytic.json', 'r', encoding='utf-8') as f:
        l_dict = json.load(f)
    with open('money_spent_advert_dict.json', 'r', encoding='utf-8') as f:
        money_spent_dict = json.load(f)
    upload_to_google_sheets(all_dict, s_dict, l_dict, money_spent_dict)


if __name__ == "__main__":
    test()