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

    MAX_DASHBOARD_ROWS = 500  # Максимальное количество строк данных
    HEADER_ROWS = 1  # Количество строк заголовков

    # Получаем текущие данные
    current_data = execute_with_retry(dashboard.get_all_values)
    current_total_rows = len(current_data)  # Общее количество строк в листе (включая пустые)
    current_data_rows = current_total_rows  # Строк с данными (включая заголовки)

    print(f"  📊 Текущее количество строк в листе DASHBOARD: {current_total_rows}")
    print(f"  📊 Строк с данными: {current_data_rows}")

    # Подготавливаем данные для вставки
    dashboard_headers = [
        "Артикул товара",
        "Сумма продаж за день на текущий момент",
        "Количество продаж за день",
        "ДРР (поиск/поиск и рекомендации) %",
        "ДРР (оплата за заказ) %",
        "ДРР (общий) %"
    ]

    # Проверяем и настраиваем заголовки
    if len(current_data) == 0 or (len(current_data) > 0 and current_data[0] != dashboard_headers):
        print("  📝 Настройка заголовков...")
        execute_with_exponential_backoff(dashboard.clear)
        time.sleep(1)
        execute_with_exponential_backoff(dashboard.append_row, dashboard_headers)
        time.sleep(1)

        execute_with_exponential_backoff(format_cell_range, dashboard, "A1:F1",
                                         CellFormat(
                                             textFormat=TextFormat(bold=True, fontSize=11),
                                             backgroundColor=Color(0.9, 1, 0.9)
                                         ))
        execute_with_exponential_backoff(set_frozen, dashboard, rows=1)
        time.sleep(1)

        for col, width in [('A', 250), ('B', 250), ('C', 220), ('D', 220), ('E', 220), ('F', 220)]:
            execute_with_exponential_backoff(set_column_width, dashboard, col, width)
        time.sleep(1)

        current_total_rows = 1  # Теперь только заголовок
        current_data_rows = 1
    else:
        # Очищаем старые данные, но НЕ удаляем строки (только содержимое)
        if current_data_rows > 1:
            print("  🗑️ Очищаем старые данные...")
            try:
                # Очищаем только содержимое, без удаления строк
                execute_with_retry(dashboard.batch_clear, [f"A2:F{current_total_rows}"])
                print(f"  ✅ Очищено содержимое строк 2-{current_total_rows}")
                time.sleep(2)
            except Exception as e:
                print(f"  ⚠️ Ошибка при очистке: {e}")

    # Собираем данные для DASHBOARD
    dashboard_rows = []
    total_revenue = 0
    total_orders = 0

    # Общие суммы для итогов
    total_expenses_search = 0  # расходы по поиску и поиск+рекомендации
    total_selled_search = 0  # продажи по поиску и поиск+рекомендации
    total_expenses_cpo = 0  # расходы по оплате за заказ
    total_selled_cpo = 0  # продажи по оплате за заказ
    total_expenses_all = 0  # все расходы
    total_selled_all = 0  # все продажи

    for item in all_items_dict.values():
        offer_id = item.get("offer_id")
        total_revenue_item = item.get("total_revenue", 0)  # Заказано на сумму
        total_ordered_units = item.get("total_ordered_units", 0)

        offer_campaigns = campaigns_data.get(offer_id, []) if campaigns_data else []

        # Инициализируем переменные для разных типов кампаний
        total_expenses_item_search = 0  # расходы по поиску и поиск+рекомендации
        total_selled_item_search = 0  # продажи по поиску и поиск+рекомендации
        total_expenses_item_cpo = 0  # расходы по оплате за заказ
        total_selled_item_cpo = 0  # продажи по оплате за заказ

        for camp in offer_campaigns:
            camping_type = camp.get('camping_type', '')

            # Получаем продажи (selled)
            selled = camp.get('selled', 0)
            selled_clean = 0
            if selled and selled != '—' and selled != '':
                try:
                    selled_clean = float(str(selled).replace('\u202f', '').replace(' ', '').replace(',', '.').strip())
                except (ValueError, TypeError):
                    pass

            # Расходы по кампании
            expense = camp.get('expense', 0)
            expense_clean = 0
            if expense and expense != '—' and expense != '':
                try:
                    expense_clean = float(str(expense).replace('\u202f', '').replace(' ', '').replace(',', '.').strip())
                except (ValueError, TypeError):
                    pass

            # Для типа "Оплата за заказ" добавляем также expense_model
            expense_model_clean = 0
            if camping_type == 'Оплата за заказ':
                expense_model = camp.get('expense_model', 0)
                if expense_model and expense_model != '—' and expense_model != '':
                    try:
                        expense_model_clean = float(
                            str(expense_model).replace('\u202f', '').replace(' ', '').replace(',', '.').strip())
                    except (ValueError, TypeError):
                        pass

            # Распределяем по типам кампаний
            if camping_type in ['Поиск', 'Поиск и рекомендации']:
                total_expenses_item_search += expense_clean
                total_selled_item_search += selled_clean
                print(f"    📊 Поисковая кампания {offer_id}: расходы={expense_clean}, продажи={selled_clean}")

            elif camping_type == 'Оплата за заказ':
                total_expenses_item_cpo += expense_clean + expense_model_clean
                total_selled_item_cpo += selled_clean
                print(
                    f"    📊 CPO кампания {offer_id}: расходы={expense_clean + expense_model_clean}, продажи={selled_clean}")

        # ========= РАСЧЕТ ДРР (поиск/поиск и рекомендации) =========
        # Формула: расходы на поисковые кампании / продажи по поисковым кампаниям * 100
        if total_selled_item_search > 0:
            drr_search = round((total_expenses_item_search / total_selled_item_search) * 100, 2)
        else:
            drr_search = 0

        # ========= РАСЧЕТ ДРР (оплата за заказ) =========
        # Формула: расходы на CPO / продажи по CPO * 100
        if total_selled_item_cpo > 0:
            drr_cpo = round((total_expenses_item_cpo / total_selled_item_cpo) * 100, 2)
        else:
            drr_cpo = 0

        # ========= РАСЧЕТ ДРР (общий) =========
        total_expenses_item_all = total_expenses_item_search + total_expenses_item_cpo
        total_selled_item_all = total_selled_item_search + total_selled_item_cpo

        if total_selled_item_all > 0:
            drr_total = round((total_expenses_item_all / total_selled_item_all) * 100, 2)
        else:
            drr_total = 0

        print(f"\n  📊 {offer_id}:")
        print(f"     Продажи по поиску: {total_selled_item_search} руб., расходы: {total_expenses_item_search} руб.")
        print(f"     Продажи по CPO: {total_selled_item_cpo} руб., расходы: {total_expenses_item_cpo} руб.")
        print(f"     ДРР поиск: {drr_search}%")
        print(f"     ДРР CPO: {drr_cpo}%")
        print(f"     ДРР общий: {drr_total}%")

        dashboard_rows.append({
            'offer_id': offer_id,
            'revenue': total_revenue_item,
            'orders': total_ordered_units,
            'drr_search': drr_search,
            'drr_cpo': drr_cpo,
            'drr_total': drr_total,
            'expenses_search': total_expenses_item_search,
            'selled_search': total_selled_item_search,
            'expenses_cpo': total_expenses_item_cpo,
            'selled_cpo': total_selled_item_cpo,
        })

        total_revenue += total_revenue_item
        total_orders += total_ordered_units
        total_expenses_search += total_expenses_item_search
        total_selled_search += total_selled_item_search
        total_expenses_cpo += total_expenses_item_cpo
        total_selled_cpo += total_selled_item_cpo
        total_expenses_all += total_expenses_item_all
        total_selled_all += total_selled_item_all

    # Сортируем по количеству продаж
    dashboard_rows.sort(key=lambda x: x['orders'], reverse=True)

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

    dashboard_data.append(["", "", "", "", "", ""])  # Пустая строка-разделитель

    # Итоговые строки
    if dashboard_rows:
        # ИТОГОВЫЙ ДРР (поиск/поиск и рекомендации)
        if total_selled_search > 0:
            total_drr_search = round((total_expenses_search / total_selled_search) * 100, 2)
        else:
            total_drr_search = 0

        # ИТОГОВЫЙ ДРР (оплата за заказ)
        if total_selled_cpo > 0:
            total_drr_cpo = round((total_expenses_cpo / total_selled_cpo) * 100, 2)
        else:
            total_drr_cpo = 0

        # ИТОГОВЫЙ ДРР (общий)
        if total_selled_all > 0:
            total_drr_total = round((total_expenses_all / total_selled_all) * 100, 2)
        else:
            total_drr_total = 0

        print(f"\n  {'=' * 50}")
        print(f"  📊 ИТОГО по всем товарам:")
        print(f"     Общая выручка (из статистики): {total_revenue:,.2f} руб.")
        print(f"     Общие заказы: {total_orders}")
        print(f"     Продажи по поиску: {total_selled_search:,.2f} руб., расходы: {total_expenses_search:,.2f} руб.")
        print(f"     Продажи по CPO: {total_selled_cpo:,.2f} руб., расходы: {total_expenses_cpo:,.2f} руб.")
        print(
            f"     Всего продаж (по рекламе): {total_selled_all:,.2f} руб., всего расходов: {total_expenses_all:,.2f} руб.")
        print(f"     Итоговый ДРР (поиск): {total_drr_search}%")
        print(f"     Итоговый ДРР (CPO): {total_drr_cpo}%")
        print(f"     Итоговый ДРР (общий): {total_drr_total}%")
        print(f"  {'=' * 50}\n")
    else:
        total_drr_search = 0
        total_drr_cpo = 0
        total_drr_total = 0

    dashboard_data.append(["ИТОГО", total_revenue, total_orders, total_drr_search, total_drr_cpo, total_drr_total])

    # Проверяем и добавляем строки при необходимости
    rows_needed = len(dashboard_data) + HEADER_ROWS
    BUFFER_ROWS = 10

    print(f"  📊 Текущее количество строк в листе: {current_total_rows}")
    print(f"  📊 Необходимо строк для вставки: {rows_needed}")
    print(f"  📊 Нужно добавить строк: {max(0, rows_needed + BUFFER_ROWS - current_total_rows)}")

    # Если строк не хватает, добавляем новые
    if current_total_rows < rows_needed + BUFFER_ROWS:
        rows_to_add = (rows_needed + BUFFER_ROWS) - current_total_rows
        print(f"  ➕ Добавляем {rows_to_add} новых строк в лист...")
        try:
            dashboard.add_rows(rows_to_add)
            print(f"  ✅ Добавлено {rows_to_add} строк. Теперь строк: {current_total_rows + rows_to_add}")
            time.sleep(2)
        except Exception as e:
            print(f"  ⚠️ Ошибка при добавлении строк: {e}")
            try:
                body = {
                    "requests": [{
                        "insertRange": {
                            "range": {
                                "sheetId": dashboard.id,
                                "startRowIndex": current_total_rows,
                                "endRowIndex": current_total_rows + rows_to_add
                            },
                            "shiftDimension": "ROWS"
                        }
                    }]
                }
                spreadsheet.batch_update(body)
                print(f"  ✅ Добавлено {rows_to_add} строк через batch_update")
                time.sleep(2)
            except Exception as e2:
                print(f"  ❌ Не удалось добавить строки: {e2}")

    # Обновляем заголовки и вставляем данные
    print(f"  📝 Вставка {len(dashboard_data)} строк данных...")
    execute_with_retry(dashboard.update, "A1", [dashboard_headers])
    time.sleep(2)

    # Вставляем данные одной операцией
    start_row = 2
    end_row = start_row + len(dashboard_data) - 1
    range_all = f"A{start_row}:F{end_row}"
    execute_with_retry(dashboard.update, range_all, dashboard_data)
    print(f"  ✅ Вставлено {len(dashboard_data)} строк данных")
    time.sleep(2)

    last_row_with_data = len(dashboard_data) + 1

    # Добавляем фильтры
    try:
        dashboard_id = dashboard.id
        data_end_row = last_row_with_data - 2

        if data_end_row > 1:
            body = {
                "requests": [
                    {
                        "setBasicFilter": {
                            "filter": {
                                "range": {
                                    "sheetId": dashboard_id,
                                    "startRowIndex": 0,
                                    "endRowIndex": data_end_row,
                                    "startColumnIndex": 0,
                                    "endColumnIndex": 6
                                }
                            }
                        }
                    }
                ]
            }
            spreadsheet.batch_update(body)
            print(f"  ✅ Фильтры добавлены на строки 1-{data_end_row}")
        time.sleep(1)
    except Exception as e:
        print(f"  ⚠️ Не удалось добавить фильтры: {e}")

    # Форматирование
    execute_with_retry(format_cell_range, dashboard, f"A{last_row_with_data}:F{last_row_with_data}",
                       CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(0.95, 0.95, 0.95)))
    time.sleep(2)

    if len(dashboard_data) > 0:
        execute_with_retry(format_cell_range, dashboard, f"A2:A{last_row_with_data}",
                           CellFormat(textFormat=TextFormat(bold=True)))
        time.sleep(2)

    # Добавляем границы
    border_range = f"A1:F{last_row_with_data}"
    borders = Borders(
        top=Border('SOLID', Color(0, 0, 0)),
        bottom=Border('SOLID', Color(0, 0, 0)),
        left=Border('SOLID', Color(0, 0, 0)),
        right=Border('SOLID', Color(0, 0, 0))
    )
    execute_with_retry(format_cell_range, dashboard, border_range, CellFormat(borders=borders))
    time.sleep(2)

    print("  ✅ DASHBOARD обновлен")
    time.sleep(5)

    # =========================================================
    # 📄 PRODUCT SHEETS (оставляем без изменений)
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

            for col in ['Y', 'Z', 'AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK']:
                execute_with_exponential_backoff(set_column_width, sheet, col, 75)
                time.sleep(0.5)

            for col in ['AM', 'AN', 'AO', 'AP', 'AQ', 'AR', 'AS', 'AT', 'AU']:
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

        # Контроль размера листа товара
        MAX_PRODUCT_ROWS = 500
        current_rows = len(execute_with_exponential_backoff(sheet.get_all_values))

        if current_rows > MAX_PRODUCT_ROWS:
            rows_to_delete = current_rows - MAX_PRODUCT_ROWS
            try:
                execute_with_exponential_backoff(sheet.delete_rows, 7, rows_to_delete)
                print(
                    f"  ✅ Удалены старые строки в листе {offer_id}, удалено {rows_to_delete} строк, осталось {MAX_PRODUCT_ROWS}")
                time.sleep(2)
            except Exception as e:
                print(f"  ⚠️ Не удалось удалить строки в листе {offer_id}: {e}")
        else:
            print(f"  ℹ️ В листе {offer_id} {current_rows} строк, лимит не превышен")

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
    with open('all_items_dict.json', 'r', encoding='utf-8') as f:
        all_dict = json.load(f)
    with open('advert_analytic.json', 'r', encoding='utf-8') as f:
        s_dict = json.load(f)
    with open('position_analytic.json', 'r', encoding='utf-8') as f:
        l_dict = json.load(f)
    upload_to_google_sheets(all_dict, s_dict, l_dict)


if __name__ == "__main__":
    test()
