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


# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================
def execute_with_exponential_backoff(func, *args, max_retries=10, **kwargs):
    """
    Выполняет функцию с экспоненциальной задержкой при ошибках 429
    """
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if '429' in str(e) or 'Quota exceeded' in str(e):
                wait_time = min(30 * (2 ** attempt), 600)
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
    """Вставляет строки пакетами с обработкой лимитов"""
    total_rows = len(rows)
    print(f"  📝 Вставка {total_rows} строк пакетами по {batch_size}...")

    for i in range(0, total_rows, batch_size):
        batch = rows[i:i + batch_size]
        for row_idx, row in enumerate(batch):
            try:
                execute_with_exponential_backoff(sheet.insert_row, row, index=start_row + i + row_idx)
                time.sleep(1)
            except Exception as e:
                print(f"  ❌ Ошибка при вставке строки {i + row_idx + 1}: {e}")
                raise

        print(f"  ✅ Вставлено {min(i + batch_size, total_rows)}/{total_rows} строк")
        if i + batch_size < total_rows:
            time.sleep(3)


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

        try:
            sheet = spreadsheet.worksheet("ERROR_PARS")
            sheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title="ERROR_PARS", rows=100, cols=5)

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        full_error = f"[{timestamp}] {error_message}"
        sheet.update("A1", full_error)

        print(f"✅ Ошибка записана в лист ERROR_PARS: {error_message[:100]}...")

    except Exception as e:
        print(f"❌ Не удалось записать ошибку в ERROR_PARS: {e}")


def write_error_to_sheet(error_message):
    """Записывает ошибки авторизации в лист ERROR"""
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

        try:
            sheet = spreadsheet.worksheet("ERROR")
            sheet.clear()
        except:
            sheet = spreadsheet.add_worksheet(title="ERROR", rows=100, cols=5)

        sheet.update("A1", "ОШИБКА АВТОРИЗАЦИИ")
        sheet.update("A2", error_message)

        print("✅ Ошибка записана в лист ERROR")
    except Exception as e:
        print(f"❌ Не удалось создать лист ERROR: {e}")


# ================= ОСНОВНАЯ ФУНКЦИЯ =================
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
    spreadsheet = client.open_by_key(spread_id)

    # Устанавливаем часовой пояс Москвы и формат даты
    moscow_tz = pytz.timezone('Europe/Moscow')
    now_moscow = datetime.now(moscow_tz)
    current_date_str = now_moscow.strftime("%d.%m.%Y")

    # =========================================================
    # 📊 DASHBOARD
    # =========================================================
    def get_or_create_sheet(spreadsheet, title, rows=1000, cols=30):
        try:
            return execute_with_exponential_backoff(spreadsheet.worksheet, title)
        except gspread.exceptions.WorksheetNotFound:
            return execute_with_exponential_backoff(spreadsheet.add_worksheet, title=title, rows=rows, cols=cols)

    print("📊 Работа с листом DASHBOARD...")
    dashboard = execute_with_exponential_backoff(get_or_create_sheet, spreadsheet, "DASHBOARD")

    print("🗑️ Очистка листа DASHBOARD...")
    try:
        all_vals = execute_with_retry(dashboard.get_all_values)
        total_rows = len(all_vals)
        if total_rows > 0:
            execute_with_retry(dashboard.delete_rows, 1, total_rows)
            print(f"  ✅ Удалено {total_rows} строк")
            time.sleep(2)
        execute_with_retry(dashboard.add_rows, 1)
        time.sleep(1)
    except Exception as e:
        print(f"  ⚠️ Ошибка при очистке: {e}")
        try:
            execute_with_retry(dashboard.batch_clear, ["A1:Z1000"])
            time.sleep(2)
        except:
            pass

    dashboard_headers = [
        "Артикул товара",
        "Сумма продаж за день на текущий момент",
        "Количество продаж за день",
        "ДРР на текущий момент (%)"
    ]

    execute_with_exponential_backoff(dashboard.append_row, dashboard_headers)
    time.sleep(1)

    execute_with_exponential_backoff(format_cell_range, dashboard, "A1:D1",
                                     CellFormat(
                                         textFormat=TextFormat(bold=True, fontSize=11),
                                         backgroundColor=Color(0.9, 1, 0.9)
                                     ))
    execute_with_exponential_backoff(set_frozen, dashboard, rows=1)
    time.sleep(1)

    for col, width in [('A', 250), ('B', 250), ('C', 220), ('D', 220)]:
        execute_with_exponential_backoff(set_column_width, dashboard, col, width)
    time.sleep(1)

    # Собираем данные для DASHBOARD
    dashboard_rows = []
    total_revenue = 0
    total_orders = 0
    total_expenses = 0  # ← восстановлена переменная

    for item in all_items_dict.values():
        offer_id = item.get("offer_id")
        total_revenue_item = item.get("total_revenue", 0)
        total_ordered_units = item.get("total_ordered_units", 0)

        offer_campaigns = campaigns_data.get(offer_id, []) if campaigns_data else []

        # ========= НОВЫЙ РАСЧЕТ DRR ИЗ РЕКЛАМНЫХ АНАЛИТИК =========
        drr_values = []  # список для сбора всех DRR > 0

        for camp in offer_campaigns:
            camping_type = camp.get('camping_type', '')
            drr_raw = camp.get('drr', None)

            # Проверяем, есть ли DRR в этой кампании
            if drr_raw is not None and drr_raw != '' and drr_raw != '—':
                try:
                    # Очищаем строку от символов и пробелов
                    drr_str = str(drr_raw).replace('\u202f', '').replace(' ', '').replace(',', '.').strip()
                    drr_str = drr_str.replace('%', '')

                    if drr_str and drr_str != '' and drr_str != '—':
                        drr_value = float(drr_str)
                        if drr_value > 0:  # учитываем только положительные значения
                            drr_values.append(drr_value)
                            print(f"    📊 DRR для {offer_id} ({camping_type}): {drr_value}%")
                except (ValueError, TypeError):
                    pass

        # Усредняем DRR по всем кампаниям, где есть значение
        if drr_values:
            drr_value = round(sum(drr_values) / len(drr_values), 2)
        else:
            # Если DRR нигде нет, считаем по старой формуле (расходы / выручка)
            total_expenses_item = 0
            for camp in offer_campaigns:
                camping_type = camp.get('camping_type', '')

                if camping_type in ['Поиск', 'Поиск и рекомендации']:
                    expense = camp.get('expense', 0)
                    if expense and expense != '—' and expense != '':
                        try:
                            expense_clean = float(str(expense).replace(',', '.').strip()) if isinstance(expense,
                                                                                                        str) else float(
                                expense)
                            total_expenses_item += expense_clean
                        except (ValueError, TypeError):
                            pass

                elif camping_type == 'Оплата за заказ':
                    expense = camp.get('expense', 0)
                    if expense and expense != '—' and expense != '':
                        try:
                            expense_clean = float(str(expense).replace(',', '.').strip()) if isinstance(expense,
                                                                                                        str) else float(
                                expense)
                            total_expenses_item += expense_clean
                        except (ValueError, TypeError):
                            pass

                    expense_model = camp.get('expense_model', 0)
                    if expense_model and expense_model != '—' and expense_model != '':
                        try:
                            expense_model_clean = float(str(expense_model).replace(',', '.').strip()) if isinstance(
                                expense_model, str) else float(expense_model)
                            total_expenses_item += expense_model_clean
                        except (ValueError, TypeError):
                            pass

            drr_value = round((total_expenses_item / total_revenue_item) * 100, 2) if total_revenue_item > 0 else 0
            total_expenses += total_expenses_item  # ← добавляем в общую сумму расходов

        dashboard_rows.append({
            'offer_id': offer_id,
            'revenue': total_revenue_item,
            'orders': total_ordered_units,
            'drr': drr_value
        })

        total_revenue += total_revenue_item
        total_orders += total_ordered_units
        # total_expenses уже обновляется внутри else

    # Сортируем по количеству продаж
    dashboard_rows.sort(key=lambda x: x['orders'], reverse=True)

    dashboard_data = []
    for row in dashboard_rows:
        dashboard_data.append([row['offer_id'], row['revenue'], row['orders'], row['drr']])

    dashboard_data.append(["", "", "", ""])

    # Итоговый DRR - среднее арифметическое DRR всех товаров
    if dashboard_rows:
        avg_drr_sum = sum([row['drr'] for row in dashboard_rows])
        total_avg_drr = round(avg_drr_sum / len(dashboard_rows), 2)
    else:
        total_avg_drr = 0

    dashboard_data.append(["ИТОГО", total_revenue, total_orders, total_avg_drr])

    if dashboard_data:
        print(f"  📝 Вставка {len(dashboard_data)} строк...")
        execute_with_retry(dashboard.update, "A1", [dashboard_headers])
        time.sleep(2)

        # Вставляем данные одной операцией
        start_row = 2
        end_row = start_row + len(dashboard_data) - 1
        range_all = f"A{start_row}:D{end_row}"
        execute_with_retry(dashboard.update, range_all, dashboard_data)
        print(f"  ✅ Вставлено {len(dashboard_data)} строк за раз")

        last_row_with_data = len(dashboard_data) + 1

        # ========= ДОБАВЛЯЕМ ФИЛЬТРЫ ПОСЛЕ ВСТАВКИ ДАННЫХ (ТОЛЬКО НА ДАННЫЕ, БЕЗ ИТОГО) =========
        try:
            # Получаем ID листа
            dashboard_id = dashboard.id

            # Количество строк с данными (без строки ИТОГО)
            # dashboard_data содержит: все товары + пустая строка + строка ИТОГО
            # Нужно отфильтровать только строки с товарами (от строки 2 до last_row_with_data - 2)
            data_end_row = last_row_with_data - 2  # исключаем пустую строку и ИТОГО

            if data_end_row > 1:  # если есть хотя бы одна строка с данными
                body = {
                    "requests": [
                        {
                            "setBasicFilter": {
                                "filter": {
                                    "range": {
                                        "sheetId": dashboard_id,
                                        "startRowIndex": 0,  # с первой строки (заголовки)
                                        "endRowIndex": data_end_row,  # только до последней строки с данными
                                        "startColumnIndex": 0,  # с колонки A
                                        "endColumnIndex": 4  # до колонки D
                                    }
                                }
                            }
                        }
                    ]
                }

                spreadsheet.batch_update(body)
                print(f"  ✅ Фильтры добавлены на строки 1-{data_end_row} (ИТОГО не фильтруется)")
            time.sleep(1)
        except Exception as e:
            print(f"  ⚠️ Не удалось добавить фильтры: {e}")

        # Продолжаем форматирование...
        execute_with_retry(format_cell_range, dashboard, f"A{last_row_with_data}:D{last_row_with_data}",
                           CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.95, 0.95, 0.95)))

        time.sleep(2)

        if len(dashboard_data) > 0:
            execute_with_retry(format_cell_range, dashboard, f"A2:A{last_row_with_data}",
                               CellFormat(textFormat=TextFormat(bold=True)))
            time.sleep(2)

        border_range = f"A1:D{last_row_with_data}"
        borders = Borders(
            top=Border('SOLID', Color(0, 0, 0)),
            bottom=Border('SOLID', Color(0, 0, 0)),
            left=Border('SOLID', Color(0, 0, 0)),
            right=Border('SOLID', Color(0, 0, 0))
        )
        execute_with_retry(format_cell_range, dashboard, border_range, CellFormat(borders=borders))
        time.sleep(2)

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

        skus_list = item.get("skus", [])
        print(f"Обработка товара {idx + 1}/{len(all_items_dict.values())}: {offer_id} (SKU: {', '.join(skus_list)})")

        offer_campaigns = campaigns_data.get(offer_id, []) if campaigns_data else []

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

        position_category = position_value if position_value is not None else total_metrics.get("position_category", 0)

        search_campaigns = [camp for camp in offer_campaigns if camp.get('camping_type') == 'Поиск']
        rec_campaigns = [camp for camp in offer_campaigns if camp.get('camping_type') == 'Поиск и рекомендации']
        cpo_campaigns = [camp for camp in offer_campaigns if camp.get('camping_type') == 'Оплата за заказ']

        try:
            sheet = execute_with_exponential_backoff(spreadsheet.worksheet, offer_id)
            need_setup = False
        except:
            sheet = execute_with_exponential_backoff(spreadsheet.add_worksheet, title=offer_id, rows=2000, cols=60)
            need_setup = True
            time.sleep(3)

        if need_setup:
            print(f"  🆕 Создание нового листа {offer_id}...")

            execute_with_exponential_backoff(sheet.update, range_name="A1", values=[["Артикул", offer_id]])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="A2", values=[["SKU", ", ".join(skus_list)]])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="A4", values=[[""]])
            time.sleep(1)

            # Заголовки блоков (строка 5)
            execute_with_exponential_backoff(sheet.update, range_name="A5",
                                             values=[["АНАЛИТИКА"]])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="K5", values=[["РЕКЛАМА — ПОИСК"]])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="Y5", values=[["РЕКЛАМА — ПОИСК И РЕКОМЕНДАЦИИ"]])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="AM5", values=[["РЕКЛАМА — ОПЛАТА ЗА ЗАКАЗ"]])
            time.sleep(1)

            # Заголовки колонок (строка 6)
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
                "Бюджет (₽)", "Цена товара (₽)", "Расходы (₽)"
            ]

            cpo_headers = [
                "Ставка (₽) [%]", "Цена товара (₽)", "Индекс видимости",
                "Заказы (Оплата за заказ)", "Заказы (Комбо-модель)", "ДРР (%)",
                "Расходы (Оплата за заказ) (₽)", "Расходы (Комбо-модель) (₽)"
            ]

            execute_with_exponential_backoff(sheet.update, range_name="A6", values=[analytics_headers])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="K6", values=[campaign_headers])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="Y6", values=[campaign_headers])
            time.sleep(1)
            execute_with_exponential_backoff(sheet.update, range_name="AM6", values=[cpo_headers])
            time.sleep(2)

            # Форматирование заголовков блоков
            execute_with_exponential_backoff(format_cell_range, sheet, "A5",
                                             CellFormat(textFormat=TextFormat(bold=True, fontSize=12),
                                                        backgroundColor=Color(0.9, 1, 0.9)))
            time.sleep(1)
            execute_with_exponential_backoff(format_cell_range, sheet, "K5",
                                             CellFormat(textFormat=TextFormat(bold=True, fontSize=12),
                                                        backgroundColor=Color(0.85, 0.92, 1)))
            time.sleep(1)
            execute_with_exponential_backoff(format_cell_range, sheet, "Y5",
                                             CellFormat(textFormat=TextFormat(bold=True, fontSize=12),
                                                        backgroundColor=Color(0.95, 0.9, 1)))
            time.sleep(1)
            execute_with_exponential_backoff(format_cell_range, sheet, "AM5",
                                             CellFormat(textFormat=TextFormat(bold=True, fontSize=12),
                                                        backgroundColor=Color(1, 0.95, 0.8)))
            time.sleep(1)

            # Форматирование заголовков колонок
            execute_with_exponential_backoff(format_cell_range, sheet, "A6:I6",
                                             CellFormat(textFormat=TextFormat(bold=True),
                                                        backgroundColor=Color(0.9, 1, 0.9)))
            time.sleep(1)
            execute_with_exponential_backoff(format_cell_range, sheet, "K6:W6",
                                             CellFormat(textFormat=TextFormat(bold=True),
                                                        backgroundColor=Color(0.85, 0.92, 1)))
            time.sleep(1)
            execute_with_exponential_backoff(format_cell_range, sheet, "Y6:AK6",
                                             CellFormat(textFormat=TextFormat(bold=True),
                                                        backgroundColor=Color(0.95, 0.9, 1)))
            time.sleep(1)
            execute_with_exponential_backoff(format_cell_range, sheet, "AM6:AU6",
                                             CellFormat(textFormat=TextFormat(bold=True),
                                                        backgroundColor=Color(1, 0.95, 0.8)))
            time.sleep(1)

            execute_with_exponential_backoff(set_frozen, sheet, rows=6)
            time.sleep(1)

            # Устанавливаем ширину колонок
            for col in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']:
                execute_with_exponential_backoff(set_column_width, sheet, col, 75)
                time.sleep(0.5)

            for col in ['K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W']:
                execute_with_exponential_backoff(set_column_width, sheet, col, 75)
                time.sleep(0.5)

            for col in ['X', 'Y', 'Z', 'AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI']:
                execute_with_exponential_backoff(set_column_width, sheet, col, 75)
                time.sleep(0.5)

            for col in ['AK', 'AL', 'AM', 'AN', 'AO', 'AP', 'AQ', 'AR', 'AS', 'AT']:
                execute_with_exponential_backoff(set_column_width, sheet, col, 75)
                time.sleep(0.5)

            time.sleep(3)

        # Формируем строку данных
        analytics_row = [
            current_date_str,
            total_metrics.get("revenue", 0),
            total_metrics.get("ordered_units", 0),
            round(position_category, 0),
            total_metrics.get("hits_view", 0),
            total_metrics.get("hits_view_pdp", 0),
            total_metrics.get("conversion_search_to_pdp", 0),
            total_metrics.get("conv_tocart_search", 0),
            total_metrics.get("conv_tocart", 0)
        ]

        search_data = format_search_campaigns(search_campaigns)
        rec_data = format_search_campaigns(rec_campaigns)
        cpo_data = format_cpo_campaigns(cpo_campaigns)

        full_row = analytics_row + [""] + search_data + [""] + rec_data + [""] + cpo_data

        # Ищем существующую строку
        existing_row_index = None
        all_data = execute_with_retry(sheet.get_all_values)

        for i, row in enumerate(all_data[6:], start=7):
            if len(row) > 0 and row[0] == current_date_str:
                existing_row_index = i
                break

        if existing_row_index:
            range_label = f"A{existing_row_index}"
            print(f"  🔄 Обновление строки {existing_row_index}, начиная с {range_label}")
            execute_with_retry(sheet.update, range_label, [full_row], value_input_option='USER_ENTERED')
            print(f"  ✅ Обновлена строка за {current_date_str}")
        else:
            print(f"  📝 Добавление новой строки за {current_date_str}")
            execute_with_exponential_backoff(sheet.insert_row, full_row, index=7)
            print(f"  📝 Добавлена новая строка за {current_date_str}")

        time.sleep(2)

        current_rows = len(execute_with_exponential_backoff(sheet.get_all_values))
        if current_rows > 500:
            try:
                execute_with_exponential_backoff(sheet.delete_rows, 501, current_rows)
                print(f"  ✅ Удалены старые строки в листе {offer_id}, осталось 500")
                time.sleep(2)
            except Exception as e:
                print(f"  ⚠️ Не удалось удалить строки в листе {offer_id}: {e}")

        time.sleep(5)

        if (idx + 1) % 3 == 0:
            print(f"Обработано {idx + 1} товаров, пауза 10 секунд для соблюдения лимитов...")
            time.sleep(10)


# ================= ФУНКЦИИ ФОРМАТИРОВАНИЯ =================
def format_search_campaigns(campaigns):
    """Форматирует данные для поиска и поиска+рекомендаций"""
    if not campaigns:
        return [""] * 13

    if len(campaigns) == 1:
        return format_single_search_campaign(campaigns[0])

    result = []
    for field_idx in range(13):
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
    expense = campaign.get('expense', 0)

    drr_value = clean_number(drr)
    ctr_value = clean_number(ctp)
    sr_click_value = clean_number(sr_click)
    budget_value = clean_number(budget)
    product_price_value = clean_number(product_price)
    expense_value = clean_number(expense)

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
        round(product_price_value, 2),
        round(expense_value, 2)
    ]


def format_cpo_campaigns(campaigns):
    """Форматирует данные для кампаний с оплатой за заказ"""
    if not campaigns:
        return [""] * 8

    if len(campaigns) == 1:
        return format_single_cpo_campaign(campaigns[0])

    result = []
    for field_idx in range(8):
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
    expense = campaign.get('expense', 0)
    expense_model = campaign.get('expense_model', 0)

    bet_amount_value = clean_number(bet_amount)
    product_price_value = clean_number(product_price)
    drr_value = clean_number(drr)
    expense_value = clean_number(expense)
    expense_model_value = clean_number(expense_model)

    bet_display = f"{round(bet_amount_value, 2)} [{bet_percent}%]" if bet_percent and bet_amount_value else ""

    return [
        bet_display,
        round(product_price_value, 2),
        str(index_view) if index_view else '',
        clean_int(product_buy_pay),
        clean_int(product_buy_combo),
        round(drr_value, 2) if drr_value else '—',
        round(expense_value, 2) if expense_value else '—',
        round(expense_model_value, 2) if expense_model_value else '—'
    ]


def test():
    all_dict = {'ross-gradient': {'offer_id': 'ross-gradient', 'product_name': 'Вибратор для женщин INTRIGUE ROSS c подогревом / Двусторонний вибратор микрофон / Голубой, 22 см', 'skus': ['2303299653'], 'total_revenue': 31581, 'total_ordered_units': 9, 'total_hits_view': 5274, 'total_hits_view_search': 4161, 'total_hits_view_pdp': 197, 'total_session_view_pdp': 136, 'total_session_view_search': 3990, 'total_hits_tocart': 20, 'avg_position_category': 44.0, 'conversion_search_to_pdp': 4.73, 'avg_conversion_search_to_pdp': 4.73, 'conversion_view_to_order': 0.17, 'avg_conv_tocart_search': 0.07, 'avg_conv_tocart': 0.38}, 'vibgr-ross': {'offer_id': 'vibgr-ross', 'product_name': 'Вибратор для женщин INTRIGUE ROSS c подогревом / Двусторонний вибратор микрофон / Розовый, 22 см', 'skus': ['1138910513'], 'total_revenue': 26068, 'total_ordered_units': 7, 'total_hits_view': 5365, 'total_hits_view_search': 4233, 'total_hits_view_pdp': 190, 'total_session_view_pdp': 130, 'total_session_view_search': 3796, 'total_hits_tocart': 21, 'avg_position_category': 42.0, 'conversion_search_to_pdp': 4.49, 'avg_conversion_search_to_pdp': 4.49, 'conversion_view_to_order': 0.13, 'avg_conv_tocart_search': 0.21, 'avg_conv_tocart': 0.39}, 'stek-60': {'offer_id': 'stek-60', 'product_name': 'Стек БДСМ / Плетка 60 см / Кнут для порки для ролевых игр из экокожи', 'skus': ['1576598131'], 'total_revenue': 18256, 'total_ordered_units': 16, 'total_hits_view': 5080, 'total_hits_view_search': 2417, 'total_hits_view_pdp': 241, 'total_session_view_pdp': 191, 'total_session_view_search': 3308, 'total_hits_tocart': 19, 'avg_position_category': 13.0, 'conversion_search_to_pdp': 9.97, 'avg_conversion_search_to_pdp': 9.97, 'conversion_view_to_order': 0.31, 'avg_conv_tocart_search': 0.24, 'avg_conv_tocart': 0.37}, 'ross-black': {'offer_id': 'ross-black', 'product_name': 'Вибратор для женщин INTRIGUE ROSS c подогревом / Двусторонний вибратор микрофон / Черный, 22 см', 'skus': ['2303288208'], 'total_revenue': 15028, 'total_ordered_units': 4, 'total_hits_view': 3721, 'total_hits_view_search': 3049, 'total_hits_view_pdp': 121, 'total_session_view_pdp': 89, 'total_session_view_search': 2773, 'total_hits_tocart': 12, 'avg_position_category': 56.0, 'conversion_search_to_pdp': 3.97, 'avg_conversion_search_to_pdp': 3.97, 'conversion_view_to_order': 0.11, 'avg_conv_tocart_search': 0.14, 'avg_conv_tocart': 0.32}, 'Бандаж_4х': {'offer_id': 'Бандаж_4х', 'product_name': 'Фиксатор для рук и ног / Бандаж БДСМ / БДСМ набор для кровати', 'skus': ['3550081206'], 'total_revenue': 14701, 'total_ordered_units': 6, 'total_hits_view': 2765, 'total_hits_view_search': 1197, 'total_hits_view_pdp': 196, 'total_session_view_pdp': 155, 'total_session_view_search': 1155, 'total_hits_tocart': 17, 'avg_position_category': 44.0, 'conversion_search_to_pdp': 16.37, 'avg_conversion_search_to_pdp': 16.37, 'conversion_view_to_order': 0.22, 'avg_conv_tocart_search': 0.51, 'avg_conv_tocart': 0.61}, 'чокер XL': {'offer_id': 'чокер XL', 'product_name': 'Ошейник БДСМ / чокер для ролевых игр / Поводок бдсм', 'skus': ['1577564202'], 'total_revenue': 12201, 'total_ordered_units': 7, 'total_hits_view': 3063, 'total_hits_view_search': 1970, 'total_hits_view_pdp': 162, 'total_session_view_pdp': 118, 'total_session_view_search': 1940, 'total_hits_tocart': 7, 'avg_position_category': 25.0, 'conversion_search_to_pdp': 8.22, 'avg_conversion_search_to_pdp': 8.22, 'conversion_view_to_order': 0.23, 'avg_conv_tocart_search': 0.15, 'avg_conv_tocart': 0.23}, 'pletka_2': {'offer_id': 'pletka_2', 'product_name': 'Плетка-флоггер для БДСМ / Плетка для ролевых игр, 44 см / Кнут для порки для ролевых игр из экокожи', 'skus': ['1577425915'], 'total_revenue': 9876, 'total_ordered_units': 6, 'total_hits_view': 5285, 'total_hits_view_search': 1206, 'total_hits_view_pdp': 121, 'total_session_view_pdp': 94, 'total_session_view_search': 1592, 'total_hits_tocart': 10, 'avg_position_category': 40.0, 'conversion_search_to_pdp': 10.03, 'avg_conversion_search_to_pdp': 10.03, 'conversion_view_to_order': 0.11, 'avg_conv_tocart_search': 0.25, 'avg_conv_tocart': 0.19}, 'кляп-black': {'offer_id': 'кляп-black', 'product_name': 'Кляп БДСМ в рот INTRIGUE черный / для взрослых / эротических игр, секс-игрушка.', 'skus': ['1001060362'], 'total_revenue': 9226, 'total_ordered_units': 7, 'total_hits_view': 2526, 'total_hits_view_search': 1464, 'total_hits_view_pdp': 136, 'total_session_view_pdp': 92, 'total_session_view_search': 1732, 'total_hits_tocart': 9, 'avg_position_category': 37.0, 'conversion_search_to_pdp': 9.29, 'avg_conversion_search_to_pdp': 9.29, 'conversion_view_to_order': 0.28, 'avg_conv_tocart_search': 0.23, 'avg_conv_tocart': 0.36}, 'Наручник-STEEL': {'offer_id': 'Наручник-STEEL', 'product_name': 'Наручники металлические / наручники для ролевых игр, для секса, бдсм', 'skus': ['2378759430'], 'total_revenue': 7510, 'total_ordered_units': 2, 'total_hits_view': 1065, 'total_hits_view_search': 796, 'total_hits_view_pdp': 55, 'total_session_view_pdp': 40, 'total_session_view_search': 692, 'total_hits_tocart': 6, 'avg_position_category': 46.0, 'conversion_search_to_pdp': 6.91, 'avg_conversion_search_to_pdp': 6.91, 'conversion_view_to_order': 0.19, 'avg_conv_tocart_search': 0.14, 'avg_conv_tocart': 0.56}, 'Чокер_наручники_bdsm': {'offer_id': 'Чокер_наручники_bdsm', 'product_name': 'Ошейник БДСМ / чокер для ролевых игр / Наручники бдсм', 'skus': ['2379415440'], 'total_revenue': 7197, 'total_ordered_units': 3, 'total_hits_view': 4845, 'total_hits_view_search': 2799, 'total_hits_view_pdp': 238, 'total_session_view_pdp': 186, 'total_session_view_search': 3566, 'total_hits_tocart': 14, 'avg_position_category': 20.0, 'conversion_search_to_pdp': 8.5, 'avg_conversion_search_to_pdp': 8.5, 'conversion_view_to_order': 0.06, 'avg_conv_tocart_search': 0.22, 'avg_conv_tocart': 0.29}, 'кляп-10см': {'offer_id': 'кляп-10см', 'product_name': 'Кляп с фаллоимитатором 10 см и замком / Страпон БДСМ, секс игрушка/ INTRIGUE', 'skus': ['1244287965'], 'total_revenue': 6928, 'total_ordered_units': 4, 'total_hits_view': 1945, 'total_hits_view_search': 1090, 'total_hits_view_pdp': 131, 'total_session_view_pdp': 94, 'total_session_view_search': 1200, 'total_hits_tocart': 7, 'avg_position_category': 54.0, 'conversion_search_to_pdp': 12.02, 'avg_conversion_search_to_pdp': 12.02, 'conversion_view_to_order': 0.21, 'avg_conv_tocart_search': 0.25, 'avg_conv_tocart': 0.36}, 'Бандаж-BDSM': {'offer_id': 'Бандаж-BDSM', 'product_name': 'Бандаж БДСМ / БДСМ набор для кровати / Фиксатор для ног', 'skus': ['2379245975'], 'total_revenue': 6424, 'total_ordered_units': 3, 'total_hits_view': 2596, 'total_hits_view_search': 1631, 'total_hits_view_pdp': 165, 'total_session_view_pdp': 146, 'total_session_view_search': 1651, 'total_hits_tocart': 17, 'avg_position_category': 34.0, 'conversion_search_to_pdp': 10.12, 'avg_conversion_search_to_pdp': 10.12, 'conversion_view_to_order': 0.12, 'avg_conv_tocart_search': 0.54, 'avg_conv_tocart': 0.65}, 'кляп-страпон-х2': {'offer_id': 'кляп-страпон-х2', 'product_name': 'Кляп БДСМ / Кляп страпон / Фаллоимитаторы 10 см и 6 см', 'skus': ['1245593058'], 'total_revenue': 6342, 'total_ordered_units': 3, 'total_hits_view': 1370, 'total_hits_view_search': 924, 'total_hits_view_pdp': 76, 'total_session_view_pdp': 61, 'total_session_view_search': 947, 'total_hits_tocart': 4, 'avg_position_category': 49.0, 'conversion_search_to_pdp': 8.23, 'avg_conversion_search_to_pdp': 8.23, 'conversion_view_to_order': 0.22, 'avg_conv_tocart_search': 0.31, 'avg_conv_tocart': 0.29}, 'Простынь_бдсм': {'offer_id': 'Простынь_бдсм', 'product_name': 'Простынь для секса 200х210 / Непромокаемая виниловая БДСМ простынь 18+', 'skus': ['3550419443'], 'total_revenue': 5718, 'total_ordered_units': 6, 'total_hits_view': 3511, 'total_hits_view_search': 2224, 'total_hits_view_pdp': 155, 'total_session_view_pdp': 124, 'total_session_view_search': 2763, 'total_hits_tocart': 9, 'avg_position_category': 41.0, 'conversion_search_to_pdp': 6.97, 'avg_conversion_search_to_pdp': 6.97, 'conversion_view_to_order': 0.17, 'avg_conv_tocart_search': 0.14, 'avg_conv_tocart': 0.26}, 'dildo_1': {'offer_id': 'dildo_1', 'product_name': 'Фаллоимитатор реалистичный / Вибратор для женщин / Дилдо с подогревом', 'skus': ['1590350185'], 'total_revenue': 5498, 'total_ordered_units': 2, 'total_hits_view': 3488, 'total_hits_view_search': 3025, 'total_hits_view_pdp': 128, 'total_session_view_pdp': 99, 'total_session_view_search': 2756, 'total_hits_tocart': 7, 'avg_position_category': 55.0, 'conversion_search_to_pdp': 4.23, 'avg_conversion_search_to_pdp': 4.23, 'conversion_view_to_order': 0.06, 'avg_conv_tocart_search': 0.14, 'avg_conv_tocart': 0.2}, 'pletka_3': {'offer_id': 'pletka_3', 'product_name': 'Плетка БДСМ / Плетка 64 см для ролевых игр / Секс-игрушка для взрослых 18+', 'skus': ['1577400316'], 'total_revenue': 4578, 'total_ordered_units': 2, 'total_hits_view': 1059, 'total_hits_view_search': 639, 'total_hits_view_pdp': 35, 'total_session_view_pdp': 28, 'total_session_view_search': 672, 'total_hits_tocart': 3, 'avg_position_category': 101.0, 'conversion_search_to_pdp': 5.48, 'avg_conversion_search_to_pdp': 5.48, 'conversion_view_to_order': 0.19, 'avg_conv_tocart_search': 0.29, 'avg_conv_tocart': 0.28}, 'кляп-дырка-4.5см': {'offer_id': 'кляп-дырка-4.5см', 'product_name': 'Кляп БДСМ в рот INTRIGUE черный / для взрослых / эротических игр, секс-игрушка.', 'skus': ['2379169166'], 'total_revenue': 4575, 'total_ordered_units': 3, 'total_hits_view': 2368, 'total_hits_view_search': 1494, 'total_hits_view_pdp': 69, 'total_session_view_pdp': 63, 'total_session_view_search': 1813, 'total_hits_tocart': 3, 'avg_position_category': 32.0, 'conversion_search_to_pdp': 4.62, 'avg_conversion_search_to_pdp': 4.62, 'conversion_view_to_order': 0.13, 'avg_conv_tocart_search': 0.05, 'avg_conv_tocart': 0.13}, 'Наручники_МЕХ': {'offer_id': 'Наручники_МЕХ', 'product_name': 'Наручники для ролевых игр с мехом / Наручники БДСМ для секса', 'skus': ['3550630567'], 'total_revenue': 4344, 'total_ordered_units': 6, 'total_hits_view': 3961, 'total_hits_view_search': 1993, 'total_hits_view_pdp': 176, 'total_session_view_pdp': 123, 'total_session_view_search': 2505, 'total_hits_tocart': 10, 'avg_position_category': 31.0, 'conversion_search_to_pdp': 8.83, 'avg_conversion_search_to_pdp': 8.83, 'conversion_view_to_order': 0.15, 'avg_conv_tocart_search': 0.11, 'avg_conv_tocart': 0.25}, 'ross-pink': {'offer_id': 'ross-pink', 'product_name': 'Вибратор для женщин INTRIGUE ROSS c подогревом / Двусторонний вибратор микрофон / светло-розовый, 22 см', 'skus': ['3083873907'], 'total_revenue': 3890, 'total_ordered_units': 1, 'total_hits_view': 996, 'total_hits_view_search': 686, 'total_hits_view_pdp': 61, 'total_session_view_pdp': 41, 'total_session_view_search': 619, 'total_hits_tocart': 6, 'avg_position_category': 121.0, 'conversion_search_to_pdp': 8.89, 'avg_conversion_search_to_pdp': 8.89, 'conversion_view_to_order': 0.1, 'avg_conv_tocart_search': 0.0, 'avg_conv_tocart': 0.6}, 'Бандаж-BDSM_2_наруч': {'offer_id': 'Бандаж-BDSM_2_наруч', 'product_name': 'Бандаж БДСМ / БДСМ набор для кровати / Фиксатор для ног', 'skus': ['3550264049'], 'total_revenue': 3690, 'total_ordered_units': 2, 'total_hits_view': 1818, 'total_hits_view_search': 737, 'total_hits_view_pdp': 80, 'total_session_view_pdp': 62, 'total_session_view_search': 813, 'total_hits_tocart': 3, 'avg_position_category': 74.0, 'conversion_search_to_pdp': 10.85, 'avg_conversion_search_to_pdp': 10.85, 'conversion_view_to_order': 0.11, 'avg_conv_tocart_search': 0.12, 'avg_conv_tocart': 0.17}, 'Наручники-XL': {'offer_id': 'Наручники-XL', 'product_name': 'Наручники БДСМ / Наручники для секса / Фиксатор БДСМ', 'skus': ['2378759471'], 'total_revenue': 3420, 'total_ordered_units': 2, 'total_hits_view': 3206, 'total_hits_view_search': 1922, 'total_hits_view_pdp': 87, 'total_session_view_pdp': 62, 'total_session_view_search': 1895, 'total_hits_tocart': 3, 'avg_position_category': 34.0, 'conversion_search_to_pdp': 4.53, 'avg_conversion_search_to_pdp': 4.53, 'conversion_view_to_order': 0.06, 'avg_conv_tocart_search': 0.05, 'avg_conv_tocart': 0.09}, 'pletka_4_eco': {'offer_id': 'pletka_4_eco', 'product_name': 'Плетка-флоггер для БДСМ / Плетка для ролевых игр, 49 см / Кнут для порки для ролевых игр из экокожи', 'skus': ['1002710297'], 'total_revenue': 3312, 'total_ordered_units': 4, 'total_hits_view': 2191, 'total_hits_view_search': 1188, 'total_hits_view_pdp': 49, 'total_session_view_pdp': 28, 'total_session_view_search': 1740, 'total_hits_tocart': 7, 'avg_position_category': 36.0, 'conversion_search_to_pdp': 4.12, 'avg_conversion_search_to_pdp': 4.12, 'conversion_view_to_order': 0.18, 'avg_conv_tocart_search': 0.28, 'avg_conv_tocart': 0.32}, 'Падл_31см': {'offer_id': 'Падл_31см', 'product_name': 'Шлепалка БДСМ (Паддл / Пэддл) Кожаный стек бдсм для ролевых игр / Флоггер и кнут плетка для секса', 'skus': ['3550480083'], 'total_revenue': 3236, 'total_ordered_units': 4, 'total_hits_view': 2532, 'total_hits_view_search': 1267, 'total_hits_view_pdp': 66, 'total_session_view_pdp': 52, 'total_session_view_search': 1852, 'total_hits_tocart': 4, 'avg_position_category': 37.0, 'conversion_search_to_pdp': 5.21, 'avg_conversion_search_to_pdp': 5.21, 'conversion_view_to_order': 0.16, 'avg_conv_tocart_search': 0.1, 'avg_conv_tocart': 0.16}, 'кляп-дырка-white-4.5см': {'offer_id': 'кляп-дырка-white-4.5см', 'product_name': 'Кляп БДСМ в рот INTRIGUE белый / для взрослых / эротических игр, секс-игрушка.', 'skus': ['3550525248'], 'total_revenue': 2800, 'total_ordered_units': 2, 'total_hits_view': 1352, 'total_hits_view_search': 903, 'total_hits_view_pdp': 64, 'total_session_view_pdp': 42, 'total_session_view_search': 866, 'total_hits_tocart': 3, 'avg_position_category': 54.0, 'conversion_search_to_pdp': 7.09, 'avg_conversion_search_to_pdp': 7.09, 'conversion_view_to_order': 0.15, 'avg_conv_tocart_search': 0.11, 'avg_conv_tocart': 0.22}, 'кляп-красный': {'offer_id': 'кляп-красный', 'product_name': 'Кляп БДСМ в рот INTRIGUE красный / для взрослых / силиконовый 4 см', 'skus': ['1002707016'], 'total_revenue': 2636, 'total_ordered_units': 2, 'total_hits_view': 1263, 'total_hits_view_search': 741, 'total_hits_view_pdp': 50, 'total_session_view_pdp': 36, 'total_session_view_search': 752, 'total_hits_tocart': 5, 'avg_position_category': 59.0, 'conversion_search_to_pdp': 6.75, 'avg_conversion_search_to_pdp': 6.75, 'conversion_view_to_order': 0.16, 'avg_conv_tocart_search': 0.39, 'avg_conv_tocart': 0.4}, 'кляп-5см': {'offer_id': 'кляп-5см', 'product_name': 'Кляп БДСМ с замком / Страпон для взрослых 5 см / Кляп фаллоимитатор, INTRIGUE', 'skus': ['1244553469'], 'total_revenue': 1638, 'total_ordered_units': 1, 'total_hits_view': 911, 'total_hits_view_search': 387, 'total_hits_view_pdp': 12, 'total_session_view_pdp': 11, 'total_session_view_search': 351, 'total_hits_tocart': 1, 'avg_position_category': 83.0, 'conversion_search_to_pdp': 3.1, 'avg_conversion_search_to_pdp': 3.1, 'conversion_view_to_order': 0.11, 'avg_conv_tocart_search': 0.28, 'avg_conv_tocart': 0.11}, 'губы_black': {'offer_id': 'губы_black', 'product_name': 'Кляп БДСМ в рот / Кляп губы / Расширитель рта, секс-игрушка', 'skus': ['3550497708'], 'total_revenue': 1522, 'total_ordered_units': 2, 'total_hits_view': 2476, 'total_hits_view_search': 866, 'total_hits_view_pdp': 82, 'total_session_view_pdp': 55, 'total_session_view_search': 777, 'total_hits_tocart': 6, 'avg_position_category': 61.0, 'conversion_search_to_pdp': 9.47, 'avg_conversion_search_to_pdp': 9.47, 'conversion_view_to_order': 0.08, 'avg_conv_tocart_search': 0.12, 'avg_conv_tocart': 0.24}, 'губы_красные': {'offer_id': 'губы_красные', 'product_name': 'Кляп БДСМ в рот / Кляп губы / Расширитель рта, секс-игрушка', 'skus': ['3550501398'], 'total_revenue': 1522, 'total_ordered_units': 2, 'total_hits_view': 3910, 'total_hits_view_search': 1763, 'total_hits_view_pdp': 225, 'total_session_view_pdp': 176, 'total_session_view_search': 2002, 'total_hits_tocart': 8, 'avg_position_category': 31.0, 'conversion_search_to_pdp': 12.76, 'avg_conversion_search_to_pdp': 12.76, 'conversion_view_to_order': 0.05, 'avg_conv_tocart_search': 0.09, 'avg_conv_tocart': 0.2}, 'Чокер_ремень_наручники': {'offer_id': 'Чокер_ремень_наручники', 'product_name': 'Ошейник БДСМ / чокер для ролевых игр / Наручники бдсм', 'skus': ['3549963080'], 'total_revenue': 1047, 'total_ordered_units': 1, 'total_hits_view': 3909, 'total_hits_view_search': 688, 'total_hits_view_pdp': 157, 'total_session_view_pdp': 135, 'total_session_view_search': 653, 'total_hits_tocart': 2, 'avg_position_category': 69.0, 'conversion_search_to_pdp': 22.82, 'avg_conversion_search_to_pdp': 22.82, 'conversion_view_to_order': 0.03, 'avg_conv_tocart_search': 0.15, 'avg_conv_tocart': 0.05}, 'кляп-rose': {'offer_id': 'кляп-rose', 'product_name': 'Кляп БДСМ в рот INTRIGUE розовый / для взрослых / эротических игр, секс-игрушка.', 'skus': ['1115648448'], 'total_revenue': 0, 'total_ordered_units': 0, 'total_hits_view': 667, 'total_hits_view_search': 278, 'total_hits_view_pdp': 23, 'total_session_view_pdp': 12, 'total_session_view_search': 293, 'total_hits_tocart': 0, 'avg_position_category': 102.0, 'conversion_search_to_pdp': 8.27, 'avg_conversion_search_to_pdp': 8.27, 'conversion_view_to_order': 0.0, 'avg_conv_tocart_search': 0.0, 'avg_conv_tocart': 0.0}, 'кляп-фиолетовый': {'offer_id': 'кляп-фиолетовый', 'product_name': 'Кляп БДСМ в рот INTRIGUE фиолетовый / для взрослых / эротических игр, секс-игрушка.', 'skus': ['1115653152'], 'total_revenue': 0, 'total_ordered_units': 0, 'total_hits_view': 610, 'total_hits_view_search': 302, 'total_hits_view_pdp': 20, 'total_session_view_pdp': 10, 'total_session_view_search': 291, 'total_hits_tocart': 1, 'avg_position_category': 94.0, 'conversion_search_to_pdp': 6.62, 'avg_conversion_search_to_pdp': 6.62, 'conversion_view_to_order': 0.0, 'avg_conv_tocart_search': 0.34, 'avg_conv_tocart': 0.16}, 'intrigue-garter': {'offer_id': 'intrigue-garter', 'product_name': 'Портупея женская / Гартеры с наручниками / INTRIGUE', 'skus': ['1196814571'], 'total_revenue': 0, 'total_ordered_units': 0, 'total_hits_view': 49, 'total_hits_view_search': 3, 'total_hits_view_pdp': 1, 'total_session_view_pdp': 1, 'total_session_view_search': 3, 'total_hits_tocart': 0, 'avg_position_category': 30.0, 'conversion_search_to_pdp': 33.33, 'avg_conversion_search_to_pdp': 33.33, 'conversion_view_to_order': 0.0, 'avg_conv_tocart_search': 0.0, 'avg_conv_tocart': 0.0}, 'чекер-bdsm': {'offer_id': 'чекер-bdsm', 'product_name': 'Ошейник БДСМ с поводком / чокер с наручниками / бдсм набор 18+', 'skus': ['1197517973'], 'total_revenue': 0, 'total_ordered_units': 0, 'total_hits_view': 1531, 'total_hits_view_search': 654, 'total_hits_view_pdp': 43, 'total_session_view_pdp': 39, 'total_session_view_search': 671, 'total_hits_tocart': 4, 'avg_position_category': 93.0, 'conversion_search_to_pdp': 6.57, 'avg_conversion_search_to_pdp': 6.57, 'conversion_view_to_order': 0.0, 'avg_conv_tocart_search': 0.29, 'avg_conv_tocart': 0.26}, 'pletka_1': {'offer_id': 'pletka_1', 'product_name': 'Плетка БДСМ / Плетка для ролевых игр / Секс-игрушка для взрослых 18+', 'skus': ['1577362116'], 'total_revenue': 0, 'total_ordered_units': 0, 'total_hits_view': 298, 'total_hits_view_search': 167, 'total_hits_view_pdp': 6, 'total_session_view_pdp': 5, 'total_session_view_search': 179, 'total_hits_tocart': 0, 'avg_position_category': 171.0, 'conversion_search_to_pdp': 3.59, 'avg_conversion_search_to_pdp': 3.59, 'conversion_view_to_order': 0.0, 'avg_conv_tocart_search': 0.0, 'avg_conv_tocart': 0.0}, 'intrigue_port_3sm': {'offer_id': 'intrigue_port_3sm', 'product_name': 'Портупея женская / Гартеры с наручниками / INTRIGUE', 'skus': ['1611577310'], 'total_revenue': 0, 'total_ordered_units': 0, 'total_hits_view': 96, 'total_hits_view_search': 28, 'total_hits_view_pdp': 6, 'total_session_view_pdp': 3, 'total_session_view_search': 42, 'total_hits_tocart': 0, 'avg_position_category': 338.0, 'conversion_search_to_pdp': 21.43, 'avg_conversion_search_to_pdp': 21.43, 'conversion_view_to_order': 0.0, 'avg_conv_tocart_search': 0.0, 'avg_conv_tocart': 0.0}, 'ross-fiolet': {'offer_id': 'ross-fiolet', 'product_name': 'Вибратор для женщин INTRIGUE ROSS c подогревом / Двусторонний вибратор микрофон / Фиолетовый, 22 см', 'skus': ['2303276196'], 'total_revenue': 0, 'total_ordered_units': 0, 'total_hits_view': 795, 'total_hits_view_search': 528, 'total_hits_view_pdp': 43, 'total_session_view_pdp': 22, 'total_session_view_search': 537, 'total_hits_tocart': 2, 'avg_position_category': 154.0, 'conversion_search_to_pdp': 8.14, 'avg_conversion_search_to_pdp': 8.14, 'conversion_view_to_order': 0.0, 'avg_conv_tocart_search': 0.18, 'avg_conv_tocart': 0.25}, 'Упряга-XL': {'offer_id': 'Упряга-XL', 'product_name': 'Наручники для ролевых игр / Наручники БДСМ / Фиксатор', 'skus': ['2378759304'], 'total_revenue': 0, 'total_ordered_units': 0, 'total_hits_view': 993, 'total_hits_view_search': 537, 'total_hits_view_pdp': 34, 'total_session_view_pdp': 27, 'total_session_view_search': 502, 'total_hits_tocart': 3, 'avg_position_category': 69.0, 'conversion_search_to_pdp': 6.33, 'avg_conversion_search_to_pdp': 6.33, 'conversion_view_to_order': 0.0, 'avg_conv_tocart_search': 0.59, 'avg_conv_tocart': 0.3}, 'Оковы-XL': {'offer_id': 'Оковы-XL', 'product_name': 'Оковы БДСМ / Наручники для секса / Фиксатор БДСМ', 'skus': ['2436329222'], 'total_revenue': 0, 'total_ordered_units': 0, 'total_hits_view': 616, 'total_hits_view_search': 323, 'total_hits_view_pdp': 24, 'total_session_view_pdp': 16, 'total_session_view_search': 307, 'total_hits_tocart': 0, 'avg_position_category': 91.0, 'conversion_search_to_pdp': 7.43, 'avg_conversion_search_to_pdp': 7.43, 'conversion_view_to_order': 0.0, 'avg_conv_tocart_search': 0.0, 'avg_conv_tocart': 0.0}, 'ошейник_карабины': {'offer_id': 'ошейник_карабины', 'product_name': 'Ошейник БДСМ / чокер для ролевых игр / Наручники бдсм', 'skus': ['3550058602'], 'total_revenue': 0, 'total_ordered_units': 0, 'total_hits_view': 484, 'total_hits_view_search': 356, 'total_hits_view_pdp': 21, 'total_session_view_pdp': 12, 'total_session_view_search': 339, 'total_hits_tocart': 0, 'avg_position_category': 98.0, 'conversion_search_to_pdp': 5.9, 'avg_conversion_search_to_pdp': 5.9, 'conversion_view_to_order': 0.0, 'avg_conv_tocart_search': 0.0, 'avg_conv_tocart': 0.0}, 'Наручники_Gold': {'offer_id': 'Наручники_Gold', 'product_name': 'Наручники для ролевых игр / Наручники БДСМ для секса', 'skus': ['3550616647'], 'total_revenue': 0, 'total_ordered_units': 0, 'total_hits_view': 977, 'total_hits_view_search': 828, 'total_hits_view_pdp': 11, 'total_session_view_pdp': 6, 'total_session_view_search': 794, 'total_hits_tocart': 1, 'avg_position_category': 69.0, 'conversion_search_to_pdp': 1.33, 'avg_conversion_search_to_pdp': 1.33, 'conversion_view_to_order': 0.0, 'avg_conv_tocart_search': 0.0, 'avg_conv_tocart': 0.1}}
    s_dict = {'vibgr-ross': [{'offer_id': 'vibgr-ross', 'sku': '1138910513', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 38257.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '43', 'my_bet': '21', 'sr_click': '20,54\u202f', 'offers': '1', 'to_cart': '4', 'drr': '28,1%', 'ctp': '3,49%', 'views': '1\u202f462', 'clicks': '51', 'product_price': '3\u202f724', 'expense': '1\u202f047.38'}, {'offer_id': 'vibgr-ross', 'sku': '1138910513', 'camping_type': 'Поиск', 'camping_budget': 45397.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '39', 'my_bet': '26', 'sr_click': '24,81\u202f', 'offers': '4', 'to_cart': '17', 'drr': '20,1%', 'ctp': '3,03%', 'views': '3\u202f791', 'clicks': '115', 'product_price': '3\u202f724', 'expense': '2\u202f852.88'}, {'offer_id': 'vibgr-ross', 'sku': '1138910513', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '372,4', 'product_price': '3724', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'dildo_1': [{'offer_id': 'dildo_1', 'sku': '1590350185', 'camping_type': 'Поиск', 'camping_budget': 30000.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '36', 'my_bet': '41', 'sr_click': '16,99\u202f', 'offers': '1', 'to_cart': '5', 'drr': '71,1%', 'ctp': '3,34%', 'views': '3\u202f446', 'clicks': '115', 'product_price': '2\u202f749', 'expense': '1\u202f953.45'}, {'offer_id': 'dildo_1', 'sku': '1590350185', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '274,9', 'product_price': '2749', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'pletka_2': [{'offer_id': 'pletka_2', 'sku': '1577425915', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 14800.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '32', 'my_bet': '7', 'sr_click': '6,83\u202f', 'offers': '4', 'to_cart': '12', 'drr': '13,1%', 'ctp': '2,44%', 'views': '5\u202f160', 'clicks': '126', 'product_price': '1\u202f646', 'expense': '861.12'}, {'offer_id': 'pletka_2', 'sku': '1577425915', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '164,6', 'product_price': '1646', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'ross-fiolet': [{'offer_id': 'ross-fiolet', 'sku': '2303276196', 'camping_type': 'Поиск', 'camping_budget': 14000.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '96', 'my_bet': '20', 'sr_click': '24,85\u202f', 'offers': '1', 'to_cart': '2', 'drr': '1,9%', 'ctp': '0,53%', 'views': '571', 'clicks': '3', 'product_price': '3\u202f599', 'expense': '74.54'}, {'offer_id': 'ross-fiolet', 'sku': '2303276196', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 21600.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '57', 'my_bet': '19', 'sr_click': '18,46\u202f', 'offers': '0', 'to_cart': '0', 'drr': '0,0%', 'ctp': '0,88%', 'views': '113', 'clicks': '1', 'product_price': '3\u202f599', 'expense': '18.46'}, {'offer_id': 'ross-fiolet', 'sku': '2303276196', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '359,9', 'product_price': '3599', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'ross-gradient': [{'offer_id': 'ross-gradient', 'sku': '2303299653', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 29000.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '40', 'my_bet': '22.5', 'sr_click': '20,39\u202f', 'offers': '1', 'to_cart': '7', 'drr': '48,8%', 'ctp': '3,08%', 'views': '2\u202f729', 'clicks': '84', 'product_price': '3\u202f509', 'expense': '1\u202f712.69'}, {'offer_id': 'ross-gradient', 'sku': '2303299653', 'camping_type': 'Поиск', 'camping_budget': 32000.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '37', 'my_bet': '21.6', 'sr_click': '19,91\u202f', 'offers': '2', 'to_cart': '8', 'drr': '22,1%', 'ctp': '3,22%', 'views': '2\u202f425', 'clicks': '78', 'product_price': '3\u202f509', 'expense': '1\u202f553.08'}, {'offer_id': 'ross-gradient', 'sku': '2303299653', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '350,9', 'product_price': '3509', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'stek-60': [{'offer_id': 'stek-60', 'sku': '1576598131', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 16000.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '10', 'my_bet': '8', 'sr_click': '6,78\u202f', 'offers': '4', 'to_cart': '5', 'drr': '23,2%', 'ctp': '4,47%', 'views': '3\u202f487', 'clicks': '156', 'product_price': '1\u202f141', 'expense': '1\u202f058.29'}, {'offer_id': 'stek-60', 'sku': '1576598131', 'camping_type': 'Поиск', 'camping_budget': 14600.0, 'strategy': 'Автостратегия', 'concurent_bet': '9,13', 'my_bet': '-', 'sr_click': '2\u202f282\u202f', 'offers': '693,52\u202f₽', 'to_cart': 'Плетки и стеки', 'drr': '3,8%', 'ctp': '1\u202f141\u202f₽', 'views': '12', 'clicks': '6,20%', 'product_price': '31.03.2026', 'expense': '18\u202f256'}, {'offer_id': 'stek-60', 'sku': '1576598131', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '114,1', 'product_price': '1141', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'ross-pink': [{'offer_id': 'ross-pink', 'sku': '3083873907', 'camping_type': 'Поиск', 'camping_budget': 8000.0, 'strategy': 'Автостратегия', 'concurent_bet': '26,63', 'my_bet': '-', 'sr_click': '0\u202f', 'offers': '505,98\u202f₽', 'to_cart': 'Вибраторы', 'drr': '13,0%', 'ctp': '3\u202f890\u202f₽', 'views': '4', 'clicks': '2,79%', 'product_price': '25.11.2025', 'expense': '3\u202f890'}], 'Наручник-STEEL': [{'offer_id': 'Наручник-STEEL', 'sku': '2378759430', 'camping_type': 'Поиск', 'camping_budget': 8500.0, 'strategy': 'Автостратегия', 'concurent_bet': '9,16', 'my_bet': '-', 'sr_click': '3\u202f755\u202f', 'offers': '476,2\u202f₽', 'to_cart': 'Наручники и фиксаторы', 'drr': '6,3%', 'ctp': '3\u202f755\u202f₽', 'views': '3', 'clicks': '5,97%', 'product_price': '01.12.2025', 'expense': '7\u202f510'}, {'offer_id': 'Наручник-STEEL', 'sku': '2378759430', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '375,5', 'product_price': '3755', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'ross-black': [{'offer_id': 'ross-black', 'sku': '2303288208', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 21200.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '47', 'my_bet': '21.6', 'sr_click': '21,37\u202f', 'offers': '5', 'to_cart': '7', 'drr': '6,7%', 'ctp': '2,58%', 'views': '2\u202f247', 'clicks': '58', 'product_price': '3\u202f757', 'expense': '1\u202f239.57'}, {'offer_id': 'ross-black', 'sku': '2303288208', 'camping_type': 'Поиск', 'camping_budget': 43527.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '44', 'my_bet': '19.7', 'sr_click': '19,11\u202f', 'offers': '0', 'to_cart': '1', 'drr': '0,0%', 'ctp': '2,43%', 'views': '1\u202f398', 'clicks': '34', 'product_price': '3\u202f757', 'expense': '649.76'}, {'offer_id': 'ross-black', 'sku': '2303288208', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '375,7', 'product_price': '3757', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'кляп-black': [{'offer_id': 'кляп-black', 'sku': '1001060362', 'camping_type': 'Поиск', 'camping_budget': 10500.0, 'strategy': 'Автостратегия', 'concurent_bet': '3,01', 'my_bet': '-', 'sr_click': '10\u202f580\u202f', 'offers': '349,47\u202f₽', 'to_cart': 'Аксессуары', 'drr': '3,8%', 'ctp': '1\u202f318\u202f₽', 'views': '9', 'clicks': '5,45%', 'product_price': '05.01.2026', 'expense': '9\u202f226'}, {'offer_id': 'кляп-black', 'sku': '1001060362', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '131,8', 'product_price': '1318', 'index_view': '2', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'кляп-красный': [{'offer_id': 'кляп-красный', 'sku': '1002707016', 'camping_type': 'Поиск', 'camping_budget': 10500.0, 'strategy': 'Автостратегия', 'concurent_bet': '4,07', 'my_bet': '-', 'sr_click': '2\u202f636\u202f', 'offers': '179,09\u202f₽', 'to_cart': 'Аксессуары', 'drr': '6,8%', 'ctp': '1\u202f318\u202f₽', 'views': '5', 'clicks': '5,05%', 'product_price': '05.01.2026', 'expense': '2\u202f636'}, {'offer_id': 'кляп-красный', 'sku': '1002707016', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '131,8', 'product_price': '1318', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'Наручники-XL': [{'offer_id': 'Наручники-XL', 'sku': '2378759471', 'camping_type': 'Поиск', 'camping_budget': 14800.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '22', 'my_bet': '15', 'sr_click': '12,93\u202f', 'offers': '1', 'to_cart': '2', 'drr': '49,9%', 'ctp': '3,31%', 'views': '1\u202f995', 'clicks': '66', 'product_price': '1\u202f710', 'expense': '853.62'}, {'offer_id': 'Наручники-XL', 'sku': '2378759471', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 14600.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '25', 'my_bet': '6', 'sr_click': '6,00\u202f', 'offers': '1', 'to_cart': '1', 'drr': '7,0%', 'ctp': '1,89%', 'views': '1\u202f060', 'clicks': '20', 'product_price': '1\u202f710', 'expense': '120.05'}, {'offer_id': 'Наручники-XL', 'sku': '2378759471', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '171,0', 'product_price': '1710', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '2', 'drr': '—', 'expense': '0', 'expense_combo': '342'}], 'чокер XL': [{'offer_id': 'чокер XL', 'sku': '1577564202', 'camping_type': 'Поиск', 'camping_budget': 19000.0, 'strategy': 'Автостратегия', 'concurent_bet': '10,81', 'my_bet': '-', 'sr_click': '12\u202f557\u202f', 'offers': '1\u202f459,04\u202f₽', 'to_cart': 'Аксессуары', 'drr': '12,0%', 'ctp': '1\u202f743\u202f₽', 'views': '6', 'clicks': '5,59%', 'product_price': '17.02.2026', 'expense': '12\u202f201'}, {'offer_id': 'чокер XL', 'sku': '1577564202', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '174,3', 'product_price': '1743', 'index_view': '1', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'кляп-10см': [{'offer_id': 'кляп-10см', 'sku': '1244287965', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 4000.0, 'strategy': 'Автостратегия', 'concurent_bet': '1,31', 'my_bet': '-', 'sr_click': '6\u202f928\u202f', 'offers': '161,18\u202f₽', 'to_cart': 'Аксессуары', 'drr': '2,3%', 'ctp': '1\u202f732\u202f₽', 'views': '8', 'clicks': '7,36%', 'product_price': '18.02.2026', 'expense': '6\u202f928'}, {'offer_id': 'кляп-10см', 'sku': '1244287965', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '173,2', 'product_price': '1732', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '3', 'drr': '—', 'expense': '0', 'expense_combo': '519.6'}], 'Бандаж_4х': [{'offer_id': 'Бандаж_4х', 'sku': '3550081206', 'camping_type': 'Поиск', 'camping_budget': 2000.0, 'strategy': 'Автостратегия', 'concurent_bet': '1,09', 'my_bet': '-', 'sr_click': '4\u202f918\u202f', 'offers': '124,59\u202f₽', 'to_cart': 'Наручники и фиксаторы', 'drr': '0,8%', 'ctp': '2\u202f459\u202f₽', 'views': '11', 'clicks': '13,54%', 'product_price': '07.03.2026', 'expense': '14\u202f701'}, {'offer_id': 'Бандаж_4х', 'sku': '3550081206', 'camping_type': 'Оплата за заказ', 'bet_percent': '11', 'bet_amount': '270,5', 'product_price': '2459', 'index_view': '10+', 'product_buy_pay': '2', 'product_buy_combo_model': '2', 'drr': '11,0%', 'expense': '540.98', 'expense_combo': '540.98'}], 'Простынь_бдсм': [{'offer_id': 'Простынь_бдсм', 'sku': '3550419443', 'camping_type': 'Поиск', 'camping_budget': 6000.0, 'strategy': 'Автостратегия', 'concurent_bet': '3,44', 'my_bet': '-', 'sr_click': '1\u202f906\u202f', 'offers': '282,22\u202f₽', 'to_cart': 'Мебель и подушки для секса', 'drr': '4,9%', 'ctp': '953\u202f₽', 'views': '4', 'clicks': '7,25%', 'product_price': '07.03.2026', 'expense': '5\u202f718'}, {'offer_id': 'Простынь_бдсм', 'sku': '3550419443', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 4366.0, 'strategy': 'Автостратегия', 'concurent_bet': '1,78', 'my_bet': '-', 'sr_click': '2\u202f268\u202f', 'offers': '229,03\u202f₽', 'to_cart': 'Мебель и подушки для секса', 'drr': '4,0%', 'ctp': '953\u202f₽', 'views': '4', 'clicks': '5,63%', 'product_price': '07.03.2026', 'expense': '5\u202f718'}, {'offer_id': 'Простынь_бдсм', 'sku': '3550419443', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '95,3', 'product_price': '953', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '3', 'drr': '—', 'expense': '0', 'expense_combo': '354.31'}], 'губы_красные': [{'offer_id': 'губы_красные', 'sku': '3550501398', 'camping_type': 'Поиск', 'camping_budget': 5000.0, 'strategy': 'Автостратегия', 'concurent_bet': '1,45', 'my_bet': '-', 'sr_click': '761\u202f', 'offers': '241,49\u202f₽', 'to_cart': 'Аксессуары', 'drr': '15,9%', 'ctp': '761\u202f₽', 'views': '7', 'clicks': '7,35%', 'product_price': '07.03.2026', 'expense': '1\u202f522'}, {'offer_id': 'губы_красные', 'sku': '3550501398', 'camping_type': 'Оплата за заказ', 'bet_percent': '11', 'bet_amount': '83,7', 'product_price': '761', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '1', 'drr': '—', 'expense': '0', 'expense_combo': '83.71'}], 'Наручники_Gold': [{'offer_id': 'Наручники_Gold', 'sku': '3550616647', 'camping_type': 'Поиск', 'camping_budget': 14600.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '31', 'my_bet': '13', 'sr_click': '13,34\u202f', 'offers': '0', 'to_cart': '1', 'drr': '0,0%', 'ctp': '1,04%', 'views': '961', 'clicks': '10', 'product_price': '1\u202f650', 'expense': '133.4'}, {'offer_id': 'Наручники_Gold', 'sku': '3550616647', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '165,0', 'product_price': '1650', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '1', 'drr': '—', 'expense': '0', 'expense_combo': '165'}], 'Чокер_ремень_наручники': [{'offer_id': 'Чокер_ремень_наручники', 'sku': '3549963080', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 8000.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '21', 'my_bet': '6', 'sr_click': '2,99\u202f', 'offers': '1', 'to_cart': '3', 'drr': '44,8%', 'ctp': '4,15%', 'views': '3\u202f780', 'clicks': '157', 'product_price': '1\u202f047', 'expense': '469'}, {'offer_id': 'Чокер_ремень_наручники', 'sku': '3549963080', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '104,7', 'product_price': '1047', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '1', 'drr': '—', 'expense': '0', 'expense_combo': '115.17'}], 'Бандаж-BDSM_2_наруч': [{'offer_id': 'Бандаж-BDSM_2_наруч', 'sku': '3550264049', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 2500.0, 'strategy': 'Автостратегия', 'concurent_bet': '2,27', 'my_bet': '-', 'sr_click': '1\u202f845\u202f', 'offers': '126,85\u202f₽', 'to_cart': 'Наручники и фиксаторы', 'drr': '3,4%', 'ctp': '1\u202f845\u202f₽', 'views': '2', 'clicks': '4,91%', 'product_price': '07.03.2026', 'expense': '3\u202f690'}, {'offer_id': 'Бандаж-BDSM_2_наруч', 'sku': '3550264049', 'camping_type': 'Оплата за заказ', 'bet_percent': '11', 'bet_amount': '203,0', 'product_price': '1845', 'index_view': '3', 'product_buy_pay': '1', 'product_buy_combo_model': '1', 'drr': '11,0%', 'expense': '202.95', 'expense_combo': '202.95'}], 'Падл_31см': [{'offer_id': 'Падл_31см', 'sku': '3550480083', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 5000.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '17', 'my_bet': '3', 'sr_click': '2,72\u202f', 'offers': '1', 'to_cart': '4', 'drr': '29,2%', 'ctp': '3,85%', 'views': '2\u202f261', 'clicks': '87', 'product_price': '809', 'expense': '236.39'}, {'offer_id': 'Падл_31см', 'sku': '3550480083', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '80,9', 'product_price': '809', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '1', 'drr': '—', 'expense': '0', 'expense_combo': '80.9'}], 'губы_black': [{'offer_id': 'губы_black', 'sku': '3550497708', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 2500.0, 'strategy': 'Автостратегия', 'concurent_bet': '1,70', 'my_bet': '-', 'sr_click': '761\u202f', 'offers': '131,04\u202f₽', 'to_cart': 'Аксессуары', 'drr': '8,6%', 'ctp': '761\u202f₽', 'views': '4', 'clicks': '3,58%', 'product_price': '07.03.2026', 'expense': '1\u202f522'}, {'offer_id': 'губы_black', 'sku': '3550497708', 'camping_type': 'Оплата за заказ', 'bet_percent': '11', 'bet_amount': '83,7', 'product_price': '761', 'index_view': '2', 'product_buy_pay': '0', 'product_buy_combo_model': '1', 'drr': '—', 'expense': '0', 'expense_combo': '83.71'}], 'Наручники_МЕХ': [{'offer_id': 'Наручники_МЕХ', 'sku': '3550630567', 'camping_type': 'Поиск и рекомендации', 'camping_budget': 8000.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '17', 'my_bet': '5', 'sr_click': '2,80\u202f', 'offers': '6', 'to_cart': '11', 'drr': '11,9%', 'ctp': '5,13%', 'views': '3\u202f584', 'clicks': '184', 'product_price': '724', 'expense': '515.57'}, {'offer_id': 'Наручники_МЕХ', 'sku': '3550630567', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '72,4', 'product_price': '724', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '1', 'drr': '—', 'expense': '0', 'expense_combo': '79.64'}], 'кляп-дырка-4.5см': [{'offer_id': 'кляп-дырка-4.5см', 'sku': '2379169166', 'camping_type': 'Поиск', 'camping_budget': 6239.0, 'strategy': 'Автостратегия', 'concurent_bet': '3,00', 'my_bet': '-', 'sr_click': '1\u202f525\u202f', 'offers': '243,36\u202f₽', 'to_cart': 'Аксессуары', 'drr': '5,3%', 'ctp': '1\u202f525\u202f₽', 'views': '3', 'clicks': '3,64%', 'product_price': '15.03.2026', 'expense': '4\u202f575'}, {'offer_id': 'кляп-дырка-4.5см', 'sku': '2379169166', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '152,5', 'product_price': '1525', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '1', 'drr': '—', 'expense': '0', 'expense_combo': '152.5'}], 'кляп-дырка-white-4.5см': [{'offer_id': 'кляп-дырка-white-4.5см', 'sku': '3550525248', 'camping_type': 'Поиск', 'camping_budget': 4000.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '12', 'my_bet': '12', 'sr_click': '7,21\u202f', 'offers': '0', 'to_cart': '2', 'drr': '0,0%', 'ctp': '4,16%', 'views': '937', 'clicks': '39', 'product_price': '1\u202f400', 'expense': '281.14'}, {'offer_id': 'кляп-дырка-white-4.5см', 'sku': '3550525248', 'camping_type': 'Оплата за заказ', 'bet_percent': '11', 'bet_amount': '154,0', 'product_price': '1400', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'pletka_4_eco': [{'offer_id': 'pletka_4_eco', 'sku': '1002710297', 'camping_type': 'Поиск', 'camping_budget': 10000.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '26', 'my_bet': '37', 'sr_click': '20,49\u202f', 'offers': '2', 'to_cart': '7', 'drr': '42,1%', 'ctp': '1,85%', 'views': '1\u202f836', 'clicks': '34', 'product_price': '828', 'expense': '696.7'}, {'offer_id': 'pletka_4_eco', 'sku': '1002710297', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '82,8', 'product_price': '828', 'index_view': '2', 'product_buy_pay': '0', 'product_buy_combo_model': '1', 'drr': '—', 'expense': '0', 'expense_combo': '82.8'}], 'Чокер_наручники_bdsm': [{'offer_id': 'Чокер_наручники_bdsm', 'sku': '2379415440', 'camping_type': 'Поиск', 'camping_budget': 23000.0, 'strategy': 'Средняя стоимость клика', 'concurent_bet': '9', 'my_bet': '12', 'sr_click': '6,33\u202f', 'offers': '2', 'to_cart': '13', 'drr': '33,5%', 'ctp': '5,81%', 'views': '4\u202f374', 'clicks': '254', 'product_price': '2\u202f399', 'expense': '1\u202f606.69'}, {'offer_id': 'Чокер_наручники_bdsm', 'sku': '2379415440', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '239,9', 'product_price': '2399', 'index_view': '1', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'Бандаж-BDSM': [{'offer_id': 'Бандаж-BDSM', 'sku': '2379245975', 'camping_type': 'Поиск', 'camping_budget': 2000.0, 'strategy': 'Автостратегия', 'concurent_bet': '1,25', 'my_bet': '-', 'sr_click': '2\u202f157\u202f', 'offers': '190,77\u202f₽', 'to_cart': 'Наручники и фиксаторы', 'drr': '3,0%', 'ctp': '2\u202f157\u202f₽', 'views': '15', 'clicks': '10,51%', 'product_price': '03.04.2026', 'expense': '6\u202f424'}, {'offer_id': 'Бандаж-BDSM', 'sku': '2379245975', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '215,7', 'product_price': '2157', 'index_view': '2', 'product_buy_pay': '0', 'product_buy_combo_model': '1', 'drr': '—', 'expense': '0', 'expense_combo': '215.7'}], 'кляп-страпон-х2': [{'offer_id': 'кляп-страпон-х2', 'sku': '1245593058', 'camping_type': 'Поиск', 'camping_budget': 2000.0, 'strategy': 'Автостратегия', 'concurent_bet': '1,22', 'my_bet': '-', 'sr_click': '4\u202f228\u202f', 'offers': '75,69\u202f₽', 'to_cart': 'Аксессуары', 'drr': '1,2%', 'ctp': '2\u202f114\u202f₽', 'views': '4', 'clicks': '6,16%', 'product_price': '08.04.2026', 'expense': '6\u202f342'}, {'offer_id': 'кляп-страпон-х2', 'sku': '1245593058', 'camping_type': 'Оплата за заказ', 'bet_percent': '10', 'bet_amount': '211,4', 'product_price': '2114', 'index_view': '10+', 'product_buy_pay': '1', 'product_buy_combo_model': '2', 'drr': '23,0%', 'expense': '486.22', 'expense_combo': '422.8'}], 'ошейник_карабины': [{'offer_id': 'ошейник_карабины', 'sku': '3550058602', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '233,7', 'product_price': '1016', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'Оковы-XL': [{'offer_id': 'Оковы-XL', 'sku': '2436329222', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '513,4', 'product_price': '2232', 'index_view': '4+1', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'Упряга-XL': [{'offer_id': 'Упряга-XL', 'sku': '2378759304', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '479,1', 'product_price': '2083', 'index_view': '3', 'product_buy_pay': '2', 'product_buy_combo_model': '0', 'drr': '23,0%', 'expense': '958.18', 'expense_combo': '0'}], 'pletka_3': [{'offer_id': 'pletka_3', 'sku': '1577400316', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '526,5', 'product_price': '2289', 'index_view': '4+1', 'product_buy_pay': '1', 'product_buy_combo_model': '0', 'drr': '23,0%', 'expense': '526.47', 'expense_combo': '0'}], 'кляп-rose': [{'offer_id': 'кляп-rose', 'sku': '1115648448', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '308,0', 'product_price': '1339', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'кляп-фиолетовый': [{'offer_id': 'кляп-фиолетовый', 'sku': '1115653152', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '303,1', 'product_price': '1318', 'index_view': '3', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'чекер-bdsm': [{'offer_id': 'чекер-bdsm', 'sku': '1197517973', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '406,6', 'product_price': '1768', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'кляп-5см': [{'offer_id': 'кляп-5см', 'sku': '1244553469', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '376,7', 'product_price': '1638', 'index_view': '4', 'product_buy_pay': '1', 'product_buy_combo_model': '0', 'drr': '23,0%', 'expense': '376.74', 'expense_combo': '0'}], 'pletka_1': [{'offer_id': 'pletka_1', 'sku': '1577362116', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '367,5', 'product_price': '1598', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'intrigue-top-naruchniki': [{'offer_id': 'intrigue-top-naruchniki', 'sku': '1197643043', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '505,8', 'product_price': '2199', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'lilo_micro': [{'offer_id': 'lilo_micro', 'sku': '1709682683', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '758,8', 'product_price': '3299', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'vibro_wand': [{'offer_id': 'vibro_wand', 'sku': '1676507590', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '1078,7', 'product_price': '4690', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'garter-bant': [{'offer_id': 'garter-bant', 'sku': '1611579754', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '554,5', 'product_price': '2411', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'intrigue_port_3sm': [{'offer_id': 'intrigue_port_3sm', 'sku': '1611577310', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '919,8', 'product_price': '3999', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'intrigue-garter': [{'offer_id': 'intrigue-garter', 'sku': '1196814571', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '461,6', 'product_price': '2007', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}], 'vacuum_qq': [{'offer_id': 'vacuum_qq', 'sku': '1591552692', 'camping_type': 'Оплата за заказ', 'bet_percent': '23', 'bet_amount': '459,8', 'product_price': '1999', 'index_view': '10+', 'product_buy_pay': '0', 'product_buy_combo_model': '0', 'drr': '—', 'expense': '0', 'expense_combo': '0'}]}
    l_dict = {'1676507590': '-', '1138910513': '18', '2303299653': '51', '2303288208': '109', '1590350185': '200', '2303276196': '335', '3083873907': '545', '1576598131': '2', '1577425915': '3', '1002710297': '5', '3550480083': '9', '1577400316': '29', '1577362116': '140', '1577564202': '2', '3549963080': '21', '2379415440': '25', '1197517973': '135', '3550058602': '170'}
    upload_to_google_sheets(all_dict, s_dict, l_dict)


if __name__ == "__main__":
    test()
