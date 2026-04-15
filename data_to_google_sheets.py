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
import random
from datetime import datetime


def write_parser_error_to_sheet(error_message):
    """Записывает ошибки парсера в лист ERROR_PARS"""
    try:
        creds = Credentials.from_service_account_file(
            "google_sheets.json",
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
        )

        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(spread_id)

        # Получаем или создаем лист ERROR_PARS
        try:
            sheet = spreadsheet.worksheet("ERROR_PARS")
            # Очищаем лист
            sheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title="ERROR_PARS", rows=100, cols=5)

        # Записываем ошибку в ячейку A1
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        full_error = f"[{timestamp}] {error_message}"
        sheet.update("A1", full_error)

        print(f"✅ Ошибка записана в лист ERROR_PARS: {error_message[:100]}...")

    except Exception as e:
        print(f"❌ Не удалось записать ошибку в ERROR_PARS: {e}")

def execute_with_exponential_backoff(func, *args, max_retries=10, **kwargs):
    """
    Выполняет функцию с экспоненциальной задержкой при ошибках 429
    """
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if '429' in str(e) or 'Quota exceeded' in str(e):
                # Экспоненциальная задержка: 30, 60, 120, 240 секунд и т.д.
                wait_time = min(30 * (2 ** attempt), 600)  # максимум 10 минут
                print(
                    f"  ⏳ Превышен лимит Google Sheets. Пауза {wait_time} секунд... (попытка {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                raise e

    raise Exception(f"Не удалось выполнить операцию после {max_retries} попыток")


def execute_with_retry(func, *args, **kwargs):
    """Выполняет функцию с повторными попытками при ошибках 429"""
    max_retries = 10
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


def batch_insert_with_retry(sheet, rows, start_row=2, batch_size=5):
    """
    Вставляет строки пакетами с обработкой лимитов
    """
    total_rows = len(rows)
    print(f"  📝 Вставка {total_rows} строк пакетами по {batch_size}...")

    for i in range(0, total_rows, batch_size):
        batch = rows[i:i + batch_size]
        for row_idx, row in enumerate(batch):
            try:
                execute_with_exponential_backoff(sheet.insert_row, row, index=start_row + i + row_idx)
                time.sleep(1)  # небольшая пауза между строками
            except Exception as e:
                print(f"  ❌ Ошибка при вставке строки {i + row_idx + 1}: {e}")
                raise

        print(f"  ✅ Вставлено {min(i + batch_size, total_rows)}/{total_rows} строк")
        if i + batch_size < total_rows:
            time.sleep(3)  # пауза между пакетами


def upload_to_google_sheets(all_items_dict, campaigns_data=None, positions_data=None):
    from google.oauth2.service_account import Credentials
    import gspread
    from datetime import datetime
    import time
    import pytz

    # =========================
    # 🔌 CONNECT
    # =========================
    creds = Credentials.from_service_account_file(
        "google_sheets.json",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(
        spread_id
    )

    # Устанавливаем часовой пояс Москвы и формат даты с временем
    moscow_tz = pytz.timezone('Europe/Moscow')
    now_moscow = datetime.now(moscow_tz)
    current_datetime_str = now_moscow.strftime("%H:%M %d.%m.%Y")

    # =========================================================
    # 📊 DASHBOARD - ПОЛНАЯ ОЧИСТКА И ОБНОВЛЕНИЕ
    # =========================================================
    def get_or_create_sheet(spreadsheet, title, rows=1000, cols=30):
        try:
            return execute_with_exponential_backoff(spreadsheet.worksheet, title)
        except gspread.exceptions.WorksheetNotFound:
            return execute_with_exponential_backoff(spreadsheet.add_worksheet, title=title, rows=rows, cols=cols)

    print("📊 Работа с листом DASHBOARD...")
    dashboard = execute_with_exponential_backoff(get_or_create_sheet, spreadsheet, "DASHBOARD")

    print("🗑️ Очистка листа DASHBOARD...")

    # Полностью очищаем лист: удаляем ВСЕ строки
    try:
        # Получаем количество строк с данными
        all_vals = execute_with_retry(dashboard.get_all_values)
        total_rows = len(all_vals)

        # Удаляем все строки, если они есть
        if total_rows > 0:
            execute_with_retry(dashboard.delete_rows, 1, total_rows)
            print(f"  ✅ Удалено {total_rows} строк")
            time.sleep(2)

        # Создаем чистый лист с 1 пустой строкой
        execute_with_retry(dashboard.add_rows, 1)
        time.sleep(1)

    except Exception as e:
        print(f"  ⚠️ Ошибка при очистке: {e}")
        # Альтернативный способ: очищаем ячейки
        try:
            execute_with_retry(dashboard.batch_clear, ["A1:Z1000"])
            time.sleep(2)
        except:
            pass

    # Заголовки
    dashboard_headers = [
        "Артикул товара",
        "Сумма продаж за день на текущий момент",
        "Количество продаж за день",
        "ДРР на текущий момент (%)"
    ]

    # Добавляем заголовки
    execute_with_exponential_backoff(dashboard.append_row, dashboard_headers)
    time.sleep(1)

    # Форматирование заголовков
    execute_with_exponential_backoff(format_cell_range, dashboard, "A1:D1",
                                     CellFormat(
                                         textFormat=TextFormat(bold=True, fontSize=11),
                                         backgroundColor=Color(0.9, 1, 0.9)
                                     ))
    execute_with_exponential_backoff(set_frozen, dashboard, rows=1)
    time.sleep(1)

    # Устанавливаем ширину колонок
    for col, width in [('A', 250), ('B', 250), ('C', 220), ('D', 220)]:
        execute_with_exponential_backoff(set_column_width, dashboard, col, width)
    time.sleep(1)

    # Собираем данные для DASHBOARD
    dashboard_rows = []
    total_revenue = 0
    total_orders = 0

    for item in all_items_dict.values():
        offer_id = item.get("offer_id")
        total_revenue_item = item.get("total_revenue", 0)
        total_ordered_units = item.get("total_ordered_units", 0)

        # Получаем данные кампаний для расчета ДРР
        offer_campaigns = campaigns_data.get(offer_id, []) if campaigns_data else []
        drr_values = []

        for camp in offer_campaigns:
            drr = camp.get('drr', '0')
            if drr and drr != '—' and drr != '':
                try:
                    if isinstance(drr, str):
                        drr_clean = float(drr.replace(',', '.').replace('%', '').strip())
                    else:
                        drr_clean = float(drr)
                    if drr_clean > 0:
                        drr_values.append(drr_clean)
                except (ValueError, TypeError):
                    pass

        avg_drr = sum(drr_values) / len(drr_values) if drr_values else 0

        dashboard_rows.append([
            offer_id,
            total_revenue_item,
            total_ordered_units,
            round(avg_drr, 2)
        ])

        total_revenue += total_revenue_item
        total_orders += total_ordered_units

    # Добавляем пустую строку и ИТОГО
    dashboard_rows.append(["", "", "", ""])

    # Собираем общий ДРР
    all_drr_values = []
    for item in all_items_dict.values():
        offer_id = item.get("offer_id")
        offer_campaigns = campaigns_data.get(offer_id, []) if campaigns_data else []
        for camp in offer_campaigns:
            drr = camp.get('drr', '0')
            if drr and drr != '—' and drr != '':
                try:
                    if isinstance(drr, str):
                        drr_clean = float(drr.replace(',', '.').replace('%', '').strip())
                    else:
                        drr_clean = float(drr)
                    if drr_clean > 0:
                        all_drr_values.append(drr_clean)
                except (ValueError, TypeError):
                    pass

    total_avg_drr = sum(all_drr_values) / len(all_drr_values) if all_drr_values else 0

    dashboard_rows.append([
        "ИТОГО",
        total_revenue,
        total_orders,
        round(total_avg_drr, 2)
    ])

    # Вставляем данные пакетами с обработкой лимитов
    if dashboard_rows:
        print(f"  📝 Вставка {len(dashboard_rows)} строк...")

        # Сначала вставляем заголовки
        execute_with_retry(dashboard.update, "A1", [dashboard_headers])
        time.sleep(2)

        # Затем вставляем данные построчно с паузами
        for i, row in enumerate(dashboard_rows):
            row_num = i + 2  # начиная со 2 строки
            range_label = f"A{row_num}:D{row_num}"
            execute_with_retry(dashboard.update, range_label, [row])
            time.sleep(1.5)
            if (i + 1) % 10 == 0:
                print(f"    Вставлено {i + 1}/{len(dashboard_rows)} строк")

        last_row_with_data = len(dashboard_rows) + 1

        # Форматирование строки ИТОГО
        execute_with_retry(format_cell_range, dashboard, f"A{last_row_with_data}:D{last_row_with_data}",
                           CellFormat(
                               textFormat=TextFormat(bold=True),
                               backgroundColor=Color(0.95, 0.95, 0.95)
                           ))
        time.sleep(2)

        # Форматирование колонки с артикулами
        if len(dashboard_rows) > 0:
            execute_with_retry(format_cell_range, dashboard, f"A2:A{last_row_with_data}",
                               CellFormat(textFormat=TextFormat(bold=True)))
            time.sleep(2)

        # Добавляем границы
        border_range = f"A1:D{last_row_with_data}"
        borders = Borders(
            top=Border('SOLID', Color(0, 0, 0)),
            bottom=Border('SOLID', Color(0, 0, 0)),
            left=Border('SOLID', Color(0, 0, 0)),
            right=Border('SOLID', Color(0, 0, 0))
        )
        execute_with_retry(format_cell_range, dashboard, border_range,
                           CellFormat(borders=borders))
        time.sleep(2)

        # Ограничиваем количество строк до 500
        current_total_rows = len(execute_with_retry(dashboard.get_all_values))
        if current_total_rows > 500:
            try:
                execute_with_retry(dashboard.delete_rows, 501, current_total_rows)
                print(f"  ✅ Удалены старые строки, осталось 500")
            except Exception as e:
                print(f"  ⚠️ Не удалось удалить строки: {e}")


    print("  ✅ DASHBOARD обновлен")
    time.sleep(5)

    # =========================================================
    # 📄 PRODUCT SHEETS
    # =========================================================
    for idx, item in enumerate(all_items_dict.values()):

        offer_id = item.get("offer_id")

        # Получаем суммарные метрики из новой структуры
        total_metrics = {
            'revenue': item.get("total_revenue", 0),
            'ordered_units': item.get("total_ordered_units", 0),
            'position_category': item.get("avg_position_category", 0),
            'hits_view': item.get("total_hits_view", 0),
            'hits_view_pdp': item.get("total_hits_view_pdp", 0),
            'session_view_pdp': item.get("total_session_view_pdp", 0),
            'session_view_search': item.get("total_session_view_search", 0),
            'conv_tocart_search': item.get("avg_conv_tocart_search", 0),
            'conv_tocart': item.get("avg_conv_tocart", 0),
            'conversion_search_to_pdp': item.get("avg_conversion_search_to_pdp", 0)
        }

        # Получаем список SKU для информации
        skus_list = item.get("skus", [])
        skus_metrics = item.get("skus_metrics", {})

        print(f"Обработка товара {idx + 1}/{len(all_items_dict.values())}: {offer_id} (SKU: {', '.join(skus_list)})")

        # Получаем данные кампаний из campaigns_data
        offer_campaigns = campaigns_data.get(offer_id, []) if campaigns_data else []

        # Получаем актуальную позицию из третьего JSON (если есть)
        position_value = None
        if positions_data:
            for sku in skus_list:
                sku_str = str(sku)
                raw_position = positions_data.get(sku_str) or positions_data.get(sku)
                if raw_position is not None and str(raw_position) != '-' and str(raw_position) != '':
                    try:
                        cleaned = str(raw_position).replace(',', '.').strip()
                        position_value = float(cleaned)
                        print(f"  ✅ Обновлена позиция для {offer_id} (SKU {sku}): {position_value}")
                        break
                    except (ValueError, TypeError) as e:
                        print(f"  ⚠️ Не удалось преобразовать позицию: '{raw_position}' - {e}")
                        continue

        if position_value is not None:
            position_category = position_value
        else:
            position_category = total_metrics.get("position_category", 0)

        # Группируем кампании по типу
        search_campaigns = [camp for camp in offer_campaigns if camp.get('camping_type') == 'Поиск']
        rec_campaigns = [camp for camp in offer_campaigns if camp.get('camping_type') == 'Поиск и рекомендации']
        cpo_campaigns = [camp for camp in offer_campaigns if camp.get('camping_type') == 'Оплата за заказ']

        # Получаем или создаем лист
        try:
            sheet = execute_with_exponential_backoff(spreadsheet.worksheet, offer_id)
            need_setup = False
        except:
            sheet = execute_with_exponential_backoff(spreadsheet.add_worksheet, title=offer_id, rows=2000, cols=60)
            need_setup = True
            time.sleep(3)

        # Только если лист новый - настраиваем заголовки и форматирование
        if need_setup:
            print(f"  🆕 Создание нового листа {offer_id}...")
            # Информация о товаре
            execute_with_exponential_backoff(sheet.update, range_name="A1", values=[["Артикул", offer_id]])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="A2", values=[["SKU", ", ".join(skus_list)]])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="A4", values=[[""]])
            time.sleep(1)

            # Заголовки блоков
            execute_with_exponential_backoff(sheet.update, range_name="A5",
                                             values=[["АНАЛИТИКА (СУММАРНО ПО ВСЕМ SKU)"]])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="K5", values=[["РЕКЛАМА — ПОИСК"]])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="X5", values=[["РЕКЛАМА — ПОИСК И РЕКОМЕНДАЦИИ"]])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="AK5", values=[["РЕКЛАМА — ОПЛАТА ЗА ЗАКАЗ"]])
            time.sleep(1)

            # Заголовки колонок
            analytics_headers = [
                "Дата",
                "Заказано на сумму",
                "Заказано товаров",
                "Позиция в каталоге и поиске",
                "Показы всего",
                "Посещения карточки товара",
                "Конверсия из поиска и каталога в карточку",
                "Конверсия из поиска и каталога в корзину",
                "Конверсия в корзину общая"
            ]

            campaign_headers = [
                "Стратегия", "Конкурентная ставка", "Ваша ставка", "Средняя стоимость клика (₽)",
                "Заказы", "В корзину (шт)", "ДРР (%)", "CTR (%)", "Показы", "Клики",
                "Бюджет (₽)", "Цена товара (₽)"
            ]

            cpo_headers = [
                "Ставка (₽) [%]", "Цена товара (₽)", "Индекс видимости",
                "Заказы (Оплата за заказ)", "Заказы (Комбо-модель)", "ДРР (%)"
            ]

            execute_with_exponential_backoff(sheet.update, range_name="A6", values=[analytics_headers])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="K6", values=[campaign_headers])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="X6", values=[campaign_headers])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="AK6", values=[cpo_headers])
            time.sleep(2)

            # Форматирование
            execute_with_exponential_backoff(format_cell_range, sheet, "A5",
                                             CellFormat(textFormat=TextFormat(bold=True, fontSize=12),
                                                        backgroundColor=Color(0.9, 1, 0.9)))
            time.sleep(1)
            execute_with_exponential_backoff(format_cell_range, sheet, "K5",
                                             CellFormat(textFormat=TextFormat(bold=True, fontSize=12),
                                                        backgroundColor=Color(0.85, 0.92, 1)))
            time.sleep(1)
            execute_with_exponential_backoff(format_cell_range, sheet, "X5",
                                             CellFormat(textFormat=TextFormat(bold=True, fontSize=12),
                                                        backgroundColor=Color(0.95, 0.9, 1)))
            time.sleep(1)
            execute_with_exponential_backoff(format_cell_range, sheet, "AK5",
                                             CellFormat(textFormat=TextFormat(bold=True, fontSize=12),
                                                        backgroundColor=Color(1, 0.95, 0.8)))
            time.sleep(1)

            execute_with_exponential_backoff(format_cell_range, sheet, "A6:I6",
                                             CellFormat(textFormat=TextFormat(bold=True),
                                                        backgroundColor=Color(0.9, 1, 0.9)))
            time.sleep(1)
            execute_with_exponential_backoff(format_cell_range, sheet, "K6:V6",
                                             CellFormat(textFormat=TextFormat(bold=True),
                                                        backgroundColor=Color(0.85, 0.92, 1)))
            time.sleep(1)
            execute_with_exponential_backoff(format_cell_range, sheet, "X6:AI6",
                                             CellFormat(textFormat=TextFormat(bold=True),
                                                        backgroundColor=Color(0.95, 0.9, 1)))
            time.sleep(1)
            execute_with_exponential_backoff(format_cell_range, sheet, "AK6:AT6",
                                             CellFormat(textFormat=TextFormat(bold=True),
                                                        backgroundColor=Color(1, 0.95, 0.8)))
            time.sleep(1)

            execute_with_exponential_backoff(set_frozen, sheet, rows=6)
            time.sleep(1)

            for col in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']:
                execute_with_exponential_backoff(set_column_width, sheet, col, 200)
                time.sleep(0.5)
            for col in ['K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V']:
                execute_with_exponential_backoff(set_column_width, sheet, col, 200)
                time.sleep(0.5)
            for col in ['X', 'Y', 'Z', 'AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI']:
                execute_with_exponential_backoff(set_column_width, sheet, col, 200)
                time.sleep(0.5)
            for col in ['AK', 'AL', 'AM', 'AN', 'AO', 'AP', 'AQ', 'AR', 'AS', 'AT']:
                execute_with_exponential_backoff(set_column_width, sheet, col, 200)
                time.sleep(0.5)

            time.sleep(3)

        # Формируем строку данных аналитики
        analytics_row = [
            current_datetime_str,
            total_metrics.get("revenue", 0),
            total_metrics.get("ordered_units", 0),
            round(position_category, 0),
            total_metrics.get("hits_view", 0),
            total_metrics.get("hits_view_pdp", 0),
            total_metrics.get("conversion_search_to_pdp", 0),
            total_metrics.get("conv_tocart_search", 0),
            total_metrics.get("conv_tocart", 0)
        ]

        # Форматируем данные кампаний
        search_data = format_search_campaigns(search_campaigns)
        rec_data = format_search_campaigns(rec_campaigns)
        cpo_data = format_cpo_campaigns(cpo_campaigns)

        # Объединяем строку с отступами
        full_row = analytics_row + [""] + search_data + [""] + rec_data + [""] + cpo_data

        if len(full_row) != 42:
            print(f"Предупреждение: длина строки {len(full_row)}, ожидается 42")

        # Вставляем новую строку (всегда на позицию 7)
        execute_with_exponential_backoff(sheet.insert_row, full_row, index=7)
        time.sleep(3)  # пауза после вставки

        # Ограничиваем количество строк в листе (максимум 500)
        current_rows = len(execute_with_exponential_backoff(sheet.get_all_values))
        if current_rows > 500:
            try:
                execute_with_exponential_backoff(sheet.delete_rows, 501, current_rows)
                print(f"  ✅ Удалены старые строки в листе {offer_id}, осталось 500")
                time.sleep(2)
            except Exception as e:
                print(f"  ⚠️ Не удалось удалить строки в листе {offer_id}: {e}")

        # Пауза между товарами для соблюдения лимитов
        time.sleep(5)

        if (idx + 1) % 3 == 0:
            print(f"Обработано {idx + 1} товаров, пауза 10 секунд для соблюдения лимитов...")
            time.sleep(10)


def format_search_campaigns(campaigns):
    """Форматирует данные для поиска и поиска+рекомендаций"""
    if not campaigns:
        return [""] * 12

    if len(campaigns) == 1:
        return format_single_search_campaign(campaigns[0])

    # Если кампаний несколько - объединяем через запятую
    result = []
    for field_idx in range(12):
        values = []
        for camp in campaigns:
            formatted = format_single_search_campaign(camp)
            val = formatted[field_idx]
            if val is not None and str(val) != "" and str(val) != "0":
                values.append(str(val))
        result.append(", ".join(values) if values else "")

    return result


def format_single_search_campaign(campaign):
    """Форматирует одну кампанию поиска или рекомендаций"""

    def clean_number(value):
        if value is None or value == '' or value == '—':
            return 0
        if isinstance(value, (int, float)):
            return float(value) if not isinstance(value, bool) else 0
        if isinstance(value, str):
            value = value.replace('\u202f', '').replace(' ', '').replace(',', '.').strip()
            value = value.replace('%', '')
            value = value.replace('₽', '')
            if value == '' or value == '—':
                return 0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0

    def clean_int(value):
        return int(clean_number(value))

    strategy = campaign.get('strategy', '')
    concurent_bet = campaign.get('concurent_bet', '')
    my_bet = campaign.get('my_bet', '')
    sr_click = campaign.get('sr_click', '')
    orders = campaign.get('orders', campaign.get('offers', 0))
    to_cart = campaign.get('to_cart', 0)
    drr = campaign.get('drr', '0')
    ctp = campaign.get('ctp', '0')
    views = campaign.get('views', 0)
    clicks = campaign.get('clicks', 0)
    budget = campaign.get('camping_budget', 0)
    product_price = campaign.get('product_price', '')

    drr_value = clean_number(drr)
    ctr_value = clean_number(ctp)
    sr_click_value = clean_number(sr_click)
    budget_value = clean_number(budget)
    product_price_value = clean_number(product_price)

    return [
        strategy if strategy else '',
        str(concurent_bet) if concurent_bet else '',
        str(my_bet) if my_bet else '',
        round(sr_click_value, 2) if sr_click_value else 0,
        clean_int(orders),
        clean_int(to_cart),
        round(drr_value, 2),
        round(ctr_value, 2),
        clean_int(views),
        clean_int(clicks),
        round(budget_value, 2),
        round(product_price_value, 2)
    ]


def format_cpo_campaigns(campaigns):
    """Форматирует данные для кампаний с оплатой за заказ"""
    if not campaigns:
        return [""] * 6

    if len(campaigns) == 1:
        return format_single_cpo_campaign(campaigns[0])

    result = []
    for field_idx in range(6):
        values = []
        for camp in campaigns:
            formatted = format_single_cpo_campaign(camp)
            val = formatted[field_idx]
            if val is not None and str(val) != "" and str(val) != "0" and str(val) != "—":
                values.append(str(val))
        result.append(", ".join(values) if values else "")

    return result


def format_single_cpo_campaign(campaign):
    """Форматирует одну кампанию с оплатой за заказ"""

    def clean_number(value):
        if value is None or value == '' or value == '—':
            return 0
        if isinstance(value, (int, float)):
            return float(value) if not isinstance(value, bool) else 0
        if isinstance(value, str):
            value = value.replace('\u202f', '').replace(' ', '').replace(',', '.').strip()
            value = value.replace('%', '')
            value = value.replace('₽', '')
            if value == '' or value == '—':
                return 0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0

    def clean_int(value):
        return int(clean_number(value))

    bet_amount = campaign.get('bet_amount', '')
    bet_percent = campaign.get('bet_percent', '')
    product_price = campaign.get('product_price', '')
    index_view = campaign.get('index_view', '')
    product_buy_pay = campaign.get('product_buy_pay', 0)
    product_buy_combo = campaign.get('product_buy_combo_model', 0)
    drr = campaign.get('drr', '—')

    bet_amount_value = clean_number(bet_amount)
    product_price_value = clean_number(product_price)
    drr_value = clean_number(drr)

    bet_display = f"{round(bet_amount_value, 2)} [{bet_percent}%]" if bet_percent and bet_amount_value else ""

    return [
        bet_display,
        round(product_price_value, 2),
        str(index_view) if index_view else '',
        clean_int(product_buy_pay),
        clean_int(product_buy_combo),
        round(drr_value, 2) if drr_value else '—'
    ]


def write_error_to_sheet(error_message):
    creds = Credentials.from_service_account_file(
        "google_sheets.json",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(
        spread_id
    )
    """Просто создает лист ERROR и пишет туда текст ошибки"""
    try:
        try:
            sheet = spreadsheet.worksheet("ERROR")
            sheet.clear()
        except:
            sheet = spreadsheet.add_worksheet(title="ERROR", rows=100, cols=5)

        sheet.update("A1", "ОШИБКА АВТОРИЗАЦИИ")
        sheet.update("A2", error_message)

        print("✅ Ошибка записана в лист ERROR")
    except:
        print("❌ Не удалось создать лист ERROR")