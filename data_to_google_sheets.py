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


# ================= БАЗОВЫЕ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================

def clean_numeric_value(value: Any) -> float:
    """Очищает числовое значение от форматирования и преобразует в float"""
    if value is None or value == '' or value == '—':
        return 0.0
    if isinstance(value, (int, float)):
        return float(value) if not isinstance(value, bool) else 0.0
    if isinstance(value, str):
        # Удаляем пробелы, заменяем запятую на точку, убираем символы
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
                print(
                    f"  ⏳ Превышен лимит. Пауза {wait_time} сек... "
                    f"(попытка {attempt + 1}/{max_retries})"
                )
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
    config должен содержать:
        - headers: список словарей с ключами 'name' и опционально 'width'
        - header_color: Color объект для фона
        - frozen_rows: количество замороженных строк
    """
    headers_list = [h['name'] for h in config['headers']]

    # Записываем заголовки
    end_col = get_column_letter(len(headers_list))
    range_name = f"A{start_row}:{end_col}{start_row}"
    execute_with_exponential_backoff(sheet.update, range_name, [headers_list])
    time.sleep(1)

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
    time.sleep(1)

    # Устанавливаем ширину колонок
    for idx, header in enumerate(config['headers'], start=1):
        col_letter = get_column_letter(idx)
        if 'width' in header:
            execute_with_exponential_backoff(
                set_column_width, sheet, col_letter, header['width']
            )
    time.sleep(1)


def clear_old_dashboard_data(dashboard, current_total_rows: int):
    """Очищает старые данные в DASHBOARD (кроме заголовков)"""
    if current_total_rows > 1:
        print("  🗑️ Очищаем старые данные...")
        try:
            execute_with_retry(dashboard.batch_clear, [f"A2:F{current_total_rows}"])
            print(f"  ✅ Очищено содержимое строк 2-{current_total_rows}")
            time.sleep(2)
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
            time.sleep(2)
        except Exception as e:
            print(f"  ⚠️ Ошибка при добавлении строк: {e}")


# ================= ФУНКЦИИ ДЛЯ РАБОТЫ С DASHBOARD =================

def extract_campaign_expenses(offer_campaigns: List) -> Tuple[float, float, float, float]:
    """
    Извлекает расходы и продажи из кампаний товара
    Возвращает: (expenses_search, selled_search, expenses_cpo, selled_cpo)
    """
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
    """
    Извлекает ДРР и расходы из drr_all_dict
    Возвращает: (drr_value, money_spent)
    """
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
    """
    Подготавливает данные для листа DASHBOARD
    Возвращает: (dashboard_data, totals_dict)
    """
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

        # Извлекаем расходы по кампаниям
        expenses_search, selled_search, _, _ = extract_campaign_expenses(offer_campaigns)

        # Получаем данные из drr_all_dict
        drr_from_dict, money_spent_from_dict = extract_drr_data(drr_all_dict, offer_id)

        # Рассчитываем метрики
        drr_search = calculate_drr(expenses_search, selled_search)
        drr_cpo = drr_from_dict
        drr_total = calculate_drr(money_spent_from_dict, total_revenue_item)

        # Логирование
        log_dashboard_item(
            offer_id, total_revenue_item, expenses_search, selled_search,
            drr_from_dict, money_spent_from_dict, drr_search, drr_cpo, drr_total
        )

        dashboard_rows.append({
            'offer_id': offer_id,
            'revenue': total_revenue_item,
            'orders': total_ordered_units,
            'drr_search': drr_search,
            'drr_cpo': drr_cpo,
            'drr_total': drr_total,
        })

        # Обновляем итоги
        totals['total_orders'] += total_ordered_units
        totals['total_expenses_search'] += expenses_search
        totals['total_selled_search'] += selled_search
        totals['total_revenue_all'] += total_revenue_item
        totals['total_money_spent_from_dict'] += money_spent_from_dict

    # Сортируем по количеству продаж
    dashboard_rows.sort(key=lambda x: x['orders'], reverse=True)

    # Формируем данные для таблицы
    dashboard_data = []
    for row in dashboard_rows:
        dashboard_data.append([
            row['offer_id'],
            row['revenue'],
            row['orders'],
            row['drr_search'],
            row['drr_cpo'],
            row['drr_total']
        ])

    return dashboard_data, totals


def calculate_total_drr(dashboard_data: List[List], totals: Dict) -> List:
    """Добавляет итоговые строки в данные DASHBOARD"""
    # Пустая строка-разделитель
    dashboard_data.append([""] * len(DASHBOARD_CONFIG['headers']))

    # Рассчитываем итоговые метрики
    total_drr_search = calculate_drr(
        totals['total_expenses_search'],
        totals['total_selled_search']
    )
    total_drr_total = calculate_drr(
        totals['total_money_spent_from_dict'],
        totals['total_revenue_all']
    )

    # Логирование итогов
    log_dashboard_totals(totals, total_drr_search, total_drr_total)

    # Добавляем итоговую строку
    dashboard_data.append([
        "ИТОГО",
        totals['total_revenue_all'],
        totals['total_orders'],
        total_drr_search,
        0,  # total_drr_cpo (не вычисляем)
        total_drr_total
    ])

    return dashboard_data


def update_dashboard_sheet(dashboard, dashboard_data: List[List]):
    """Обновляет данные в листе DASHBOARD"""
    # Очищаем старые данные
    current_data = execute_with_retry(dashboard.get_all_values)
    current_total_rows = len(current_data)

    if current_total_rows > 1:
        clear_old_dashboard_data(dashboard, current_total_rows)

    # Проверяем и добавляем строки при необходимости
    rows_needed = len(dashboard_data) + 1  # +1 для заголовков
    ensure_sheet_rows(dashboard, rows_needed)

    # Обновляем данные
    print(f"  📝 Вставка {len(dashboard_data)} строк данных...")
    execute_with_retry(dashboard.update, "A2", dashboard_data)
    print(f"  ✅ Вставлено {len(dashboard_data)} строк данных")
    time.sleep(2)

    # Форматируем итоговую строку
    last_row = len(dashboard_data) + 1
    format_totals_row(dashboard, last_row, len(DASHBOARD_CONFIG['headers']))
    time.sleep(2)

    # Форматируем колонку с артикулами
    if len(dashboard_data) > 0:
        execute_with_retry(
            format_cell_range, dashboard, f"A2:A{last_row}",
            CellFormat(textFormat=TextFormat(bold=True))
        )
        time.sleep(2)


# ================= ФУНКЦИИ ДЛЯ РАБОТЫ С ЛИСТАМИ ТОВАРОВ =================

def setup_product_sheet_structure(sheet, offer_id: str, skus_list: List[str]):
    """
    Настраивает структуру листа товара: заголовки блоков и колонок
    """
    print(f"  🆕 Создание нового листа {offer_id}...")

    # Базовая информация о товаре
    execute_with_exponential_backoff(sheet.update, "A1", [["Артикул", offer_id]])
    time.sleep(1)
    execute_with_exponential_backoff(sheet.update, "A2", [["SKU", ", ".join(skus_list)]])
    time.sleep(1)
    execute_with_exponential_backoff(sheet.update, "A4", [[""]])
    time.sleep(1)

    # Настраиваем блок аналитики
    col_letter = ANALYTICS_CONFIG['start_column']
    execute_with_exponential_backoff(
        sheet.update, f"{col_letter}5", [[ANALYTICS_CONFIG['block_title']]]
    )
    time.sleep(1)

    # Форматируем заголовок блока аналитики
    execute_with_exponential_backoff(
        format_cell_range, sheet, f"{col_letter}5",
        CellFormat(
            textFormat=TextFormat(bold=True, fontSize=12),
            backgroundColor=ANALYTICS_CONFIG['block_color']
        )
    )
    time.sleep(1)

    # Заголовки колонок аналитики
    headers_list = [h['name'] for h in ANALYTICS_CONFIG['headers']]
    end_col = get_column_letter(
        get_column_index(ANALYTICS_CONFIG['start_column']) + len(headers_list) - 1
    )
    headers_range = f"{ANALYTICS_CONFIG['start_column']}6:{end_col}6"

    execute_with_exponential_backoff(sheet.update, headers_range, [headers_list])
    time.sleep(1)

    # Форматирование заголовков аналитики
    execute_with_exponential_backoff(
        format_cell_range, sheet, headers_range,
        CellFormat(
            textFormat=TextFormat(bold=True),
            backgroundColor=ANALYTICS_CONFIG['block_color']
        )
    )
    time.sleep(1)

    # Настраиваем рекламные блоки
    for block_key, block_config in CAMPAIGN_CONFIGS.items():
        col_letter = block_config['start_column']

        # Заголовок блока
        execute_with_exponential_backoff(
            sheet.update, f"{col_letter}5", [[block_config['title']]]
        )
        time.sleep(1)

        # Форматирование заголовка блока
        execute_with_exponential_backoff(
            format_cell_range, sheet, f"{col_letter}5",
            CellFormat(
                textFormat=TextFormat(bold=True, fontSize=12),
                backgroundColor=block_config['color']
            )
        )
        time.sleep(1)

        # Заголовки колонок
        headers_list = [h['name'] for h in block_config['headers']]
        end_col = get_column_letter(
            get_column_index(block_config['start_column']) + len(headers_list) - 1
        )
        headers_range = f"{block_config['start_column']}6:{end_col}6"

        execute_with_exponential_backoff(sheet.update, headers_range, [headers_list])
        time.sleep(1)

        # Форматирование заголовков колонок
        execute_with_exponential_backoff(
            format_cell_range, sheet, headers_range,
            CellFormat(
                textFormat=TextFormat(bold=True),
                backgroundColor=block_config['color']
            )
        )
        time.sleep(1)

    # Замораживаем строки
    execute_with_exponential_backoff(set_frozen, sheet, rows=6)
    time.sleep(1)

    # Устанавливаем ширину колонок для всех блоков
    # Аналитика
    for idx, header in enumerate(ANALYTICS_CONFIG['headers']):
        col = get_column_letter(get_column_index(ANALYTICS_CONFIG['start_column']) + idx)
        if 'width' in header:
            execute_with_exponential_backoff(set_column_width, sheet, col, header['width'])
    time.sleep(0.5)

    # Рекламные блоки
    for block_config in CAMPAIGN_CONFIGS.values():
        for idx, header in enumerate(block_config['headers']):
            col = get_column_letter(get_column_index(block_config['start_column']) + idx)
            if 'width' in header:
                execute_with_exponential_backoff(set_column_width, sheet, col, header['width'])
        time.sleep(0.5)


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


def update_product_sheet(sheet, offer_id: str, full_row: List, current_date_str: str):
    """Обновляет данные в листе товара (добавляет или обновляет строку)"""
    # Ищем существующую строку с текущей датой
    all_data = execute_with_retry(sheet.get_all_values)
    existing_row_index = None

    for i, row in enumerate(all_data[6:], start=7):
        if len(row) > 0 and row[0] == current_date_str:
            existing_row_index = i
            break

    if existing_row_index:
        range_label = f"A{existing_row_index}"
        print(f"  🔄 Обновление строки {existing_row_index}")
        execute_with_retry(
            sheet.update, range_label, [full_row],
            value_input_option='USER_ENTERED'
        )
        print(f"  ✅ Обновлена строка за {current_date_str}")
    else:
        print(f"  📝 Добавление новой строки за {current_date_str}")
        execute_with_exponential_backoff(sheet.insert_row, full_row, index=7)
        print(f"  ✅ Добавлена строка за {current_date_str}")

    time.sleep(2)

    # Контроль размера листа (оставляем только последние 500 строк)
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
            time.sleep(2)
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

    Args:
        all_items_dict: Словарь с данными аналитики по товарам
        campaigns_data: Словарь с данными рекламных кампаний
        positions_data: Словарь с данными позиций товаров
        drr_all_dict: Словарь с ДРР и расходами по товарам
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

        # Подготовка данных DASHBOARD
        dashboard_data, totals = prepare_dashboard_data(
            all_items_dict, campaigns_data, drr_all_dict
        )
        dashboard_data_with_totals = calculate_total_drr(dashboard_data, totals)

        # Обновление DASHBOARD
        update_dashboard_sheet(dashboard, dashboard_data_with_totals)
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
                time.sleep(3)

            # Настраиваем структуру листа если нужно
            if need_setup:
                setup_product_sheet_structure(sheet, offer_id, skus_list)

            # Подготавливаем и обновляем данные
            full_row = prepare_product_row(item, campaigns_data, current_date_str)
            update_product_sheet(sheet, offer_id, full_row, current_date_str)

            # Пауза для соблюдения лимитов
            if (idx + 1) % 3 == 0:
                print(f"\n⏸️ Обработано {idx + 1} товаров, пауза 10 секунд...")
                time.sleep(10)

        print("\n" + "=" * 60)
        print("✅ ВСЕ ДАННЫЕ УСПЕШНО ЗАГРУЖЕНЫ")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
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