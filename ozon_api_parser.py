import json
import time
import math
import traceback
import inspect
import pytz
import logging

import requests
from datetime import datetime, timedelta, timezone
from ozon_perfomance_api import OzonPerfomanceAPI
from config import OZON_PERFORMANCE_TOKEN
from config import ozon_api_key, ozon_client_id

from data_to_google_sheets import upload_to_google_sheets, write_parser_error_to_sheet

# Настройка логирования
logger = logging.getLogger(__name__)


class OzonSellerParse:
    def __init__(self):
        self.headers = {
            "Client-Id": ozon_client_id,
            "Api-Key": ozon_api_key,
            "Content-Type": "application/json"
        }

        self.headers_perfomance = {
            "Authorization": f"Bearer {OZON_PERFORMANCE_TOKEN}",
            "Content-Type": "application/json"
        }
        self.update_token()

    def update_token(self, max_retries: int = 3):
        """Обновление токена с повторными попытками"""
        for attempt in range(max_retries):
            try:
                logger.info(f"   🔑 Обновление токена (попытка {attempt + 1}/{max_retries})")
                token = OzonPerfomanceAPI()._get_access_token()
                self.headers_perfomance = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }
                logger.info("   ✅ Токен успешно обновлен")
                time.sleep(1)
                return True
            except Exception as e:
                logger.error(f"   ❌ Ошибка при обновлении токена (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(3)
                    continue
                else:
                    error_msg = f"Не удалось обновить токен после {max_retries} попыток: {e}"
                    logger.error(f"   ❌ {error_msg}")
                    write_parser_error_to_sheet(error_msg)
                    return False
        return False

    def _make_request(self, url: str, method: str = 'POST', payload: dict = None, headers: dict = None,
                      max_retries: int = 3, backoff_factor: int = 2):
        """Универсальный метод для выполнения запросов с повторными попытками"""
        if headers is None:
            headers = self.headers

        for attempt in range(max_retries):
            try:
                if method.upper() == 'POST':
                    response = requests.post(url, json=payload, headers=headers)
                elif method.upper() == 'GET':
                    response = requests.get(url, headers=headers, params=payload)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Too Many Requests
                    wait_time = backoff_factor ** attempt * 2
                    logger.warning(
                        f"   ⚠️ Превышен лимит запросов (429). Пауза {wait_time} сек... (попытка {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"   ❌ Ошибка API: статус {response.status_code}, текст: {response.text[:200]}")
                    if attempt < max_retries - 1:
                        time.sleep(backoff_factor)
                        continue
                    else:
                        return None

            except requests.exceptions.RequestException as e:
                logger.error(f"   ❌ Ошибка запроса (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    wait_time = backoff_factor ** attempt
                    time.sleep(wait_time)
                    continue
                else:
                    return None
            except Exception as e:
                logger.error(f"   ❌ Неожиданная ошибка (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(backoff_factor)
                    continue
                else:
                    return None

        return None

    def get_all_products_in_sale(self, max_retries: int = 3):
        """Получение всех товаров в продаже с повторными попытками"""
        url = "https://api-seller.ozon.ru/v3/product/list"
        all_items = []
        all_product_id = {}
        last_id = ''
        old_len = 0

        logger.info("   📦 Начинаем получение списка товаров в продаже...")

        while True:
            payload = {
                "filter": {
                    "visibility": "IN_SALE"
                },
                "last_id": f"{last_id}",
                "limit": 1000
            }

            response = self._make_request(url, 'POST', payload, self.headers, max_retries)

            if response and response.status_code == 200:
                data = response.json()
                products = data.get("result", {}).get("items", [])

                logger.info(f"   📄 Получено {len(products)} товаров (last_id: {last_id or 'начало'})")

                for p in products:
                    product_id = p.get("product_id")
                    offer_id = p.get("offer_id")

                    if offer_id not in all_items:
                        all_items.append(offer_id)
                    if product_id not in all_product_id:
                        all_product_id[offer_id] = product_id

                last_id = data.get("result", {}).get("last_id")
                if old_len == len(all_items):
                    logger.info(f"   ✅ Всего получено {len(all_items)} товаров в продаже")
                    return all_items, all_product_id
                else:
                    old_len = len(all_items)
            else:
                logger.error(f"   ❌ Ошибка при получении списка товаров")
                return all_items, all_product_id

    def get_items_info_for_product_id(self, offer_ids: list, max_retries: int = 3):
        """Получение информации о товарах по offer_id с повторными попытками"""
        all_skus = []
        all_items_dict = {}
        api_url = "https://api-seller.ozon.ru/v3/product/info/list"

        # Разбиваем offer_ids на части по 100, чтобы не превысить лимит
        chunk_size = 100
        total_chunks = math.ceil(len(offer_ids) / chunk_size)
        logger.info(f"   📝 Получение информации для {len(offer_ids)} товаров ({total_chunks} частей по {chunk_size})")

        for chunk_idx, i in enumerate(range(0, len(offer_ids), chunk_size)):
            chunk = offer_ids[i:i + chunk_size]
            logger.info(f"   🔄 Обработка части {chunk_idx + 1}/{total_chunks} ({len(chunk)} товаров)")

            for attempt in range(max_retries):
                try:
                    response = requests.post(api_url, headers=self.headers, json={'offer_id': chunk})

                    if response.status_code == 200:
                        data_json = response.json()
                        items_count = 0
                        for item in data_json.get("items", []):
                            item_name = item.get("name")
                            item_product_id = str(item.get("id"))
                            item_offer_id = str(item.get("offer_id"))
                            all_item_skus = []
                            items_skus = item.get("sources")
                            for item_sku in items_skus:
                                all_item_skus.append(str(item_sku["sku"]))
                                all_skus.append(str(item_sku["sku"]))

                            ready_json = {
                                "name": item_name,
                                "offer_id": item_offer_id,
                                "product_id": item_product_id,
                                "skus": all_item_skus,
                                "skus_metrics": [],
                            }
                            all_items_dict[item_offer_id] = ready_json
                            items_count += 1

                        logger.info(f"   ✅ Получена информация для {items_count} товаров")
                        time.sleep(0.5)
                        break  # Успешно, выходим из цикла повторных попыток

                    elif response.status_code == 429:  # Too Many Requests
                        wait_time = 2 ** attempt * 2
                        logger.warning(
                            f"   ⚠️ Превышен лимит (429). Пауза {wait_time} сек... (попытка {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"   ❌ Ошибка API: статус {response.status_code}, текст: {response.text[:200]}")
                        if attempt < max_retries - 1:
                            time.sleep(2)
                            continue
                        else:
                            break

                except Exception as e:
                    logger.error(f"   ❌ Ошибка при получении информации о товарах (попытка {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    else:
                        break

        logger.info(f"   ✅ Всего получена информация для {len(all_items_dict)} товаров, найдено {len(all_skus)} SKU")
        return all_items_dict, all_skus

    def stat_product_to_pay(self, max_retries: int = 3):
        """Получение статистики оплаты за товар с повторными попытками"""
        dict_ = {}

        # Используем московское время
        moscow_tz = pytz.timezone('Europe/Moscow')
        now_moscow = datetime.now(moscow_tz)

        # Запрашиваем статистику за последние 24 часа
        date_from = (now_moscow - timedelta(days=1)).strftime("%Y-%m-%d")
        date_to = now_moscow.strftime("%Y-%m-%d")

        api_url = f"https://api-performance.ozon.ru:443/api/client/statistics/campaign/media/json?dateFrom={date_from}&dateTo={date_to}"

        logger.info(f"   📊 Запрос статистики медийных кампаний за {date_from} - {date_to}")

        for attempt in range(max_retries):
            try:
                response = requests.get(api_url, headers=self.headers_perfomance)

                if response.status_code == 200:
                    logger.info("   ✅ Статистика медийных кампаний получена")
                    data_json = response.json()
                    for row in data_json.get('rows', []):
                        item_status = row.get("status")
                        if item_status == 'running':
                            item_id = row.get('id')
                            dict_[item_id] = row
                    logger.info(f"   📊 Найдено {len(dict_)} активных медийных кампаний")
                    return dict_
                elif response.status_code == 429:
                    wait_time = 2 ** attempt * 2
                    logger.warning(
                        f"   ⚠️ Превышен лимит (429). Пауза {wait_time} сек... (попытка {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"   ❌ Ошибка API: статус {response.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    else:
                        return {}

            except Exception as e:
                logger.error(f"   ❌ Ошибка при получении статистики (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    return {}

    def stat_pay_to_click(self, max_retries: int = 3):
        """Получение статистики оплаты за клик с повторными попытками"""
        dict_ = {}

        # Используем московское время
        moscow_tz = pytz.timezone('Europe/Moscow')
        now_moscow = datetime.now(moscow_tz)

        # Запрашиваем статистику за последние 24 часа
        date_from = (now_moscow - timedelta(days=1)).strftime("%Y-%m-%d")
        date_to = now_moscow.strftime("%Y-%m-%d")

        logger.info(f"   📊 Запрос статистики кликов за {date_from} - {date_to}")
        api_url = f'https://api-performance.ozon.ru:443/api/client/statistics/campaign/product/json?dateFrom={date_from}&dateTo={date_to}'

        for attempt in range(max_retries):
            try:
                response = requests.get(api_url, headers=self.headers_perfomance)

                if response.status_code == 200:
                    logger.info("   ✅ Статистика кликов получена")
                    data_json = response.json()
                    for row in data_json.get('rows', []):
                        item_status = row.get("status")
                        if item_status == 'running':
                            item_id = row.get('id')
                            dict_[item_id] = row
                    logger.info(f"   📊 Найдено {len(dict_)} активных кампаний")
                    return dict_
                elif response.status_code == 429:
                    wait_time = 2 ** attempt * 2
                    logger.warning(
                        f"   ⚠️ Превышен лимит (429). Пауза {wait_time} сек... (попытка {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"   ❌ Ошибка API: статус {response.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    else:
                        return {}

            except Exception as e:
                logger.error(f"   ❌ Ошибка при получении статистики (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    return {}

    def get_analytic(self, max_retries: int = 3):
        """Получение аналитики с повторными попытками"""
        api_url = f"https://api-seller.ozon.ru/v1/analytics/data"
        moscow_tz = pytz.timezone('Europe/Moscow')
        now_moscow = datetime.now(moscow_tz)

        date_yesterday = now_moscow.strftime("%Y-%m-%d")

        logger.info(f"   📊 Запрос аналитики за {date_yesterday}")

        params = {
            "date_from": date_yesterday,
            "date_to": date_yesterday,
            "limit": 1000,
            "metrics": [
                'revenue',
                'ordered_units',
                'hits_view',
                'hits_view_search',
                'hits_view_pdp',
                'session_view_pdp',
                'session_view_search',
                'position_category',
                'hits_tocart',
                'hits_tocart_search',
                'hits_tocart_pdp',
                'conv_tocart',
                'conv_tocart_search',
                'conv_tocart_pdp'
            ],
            "dimension": ["sku"],
        }

        for attempt in range(max_retries):
            try:
                response = requests.post(api_url, headers=self.headers, json=params)

                if response.status_code == 200:
                    logger.info("   ✅ Аналитика успешно получена")
                    return response.json()
                elif response.status_code == 429:
                    wait_time = 2 ** attempt * 2
                    logger.warning(
                        f"   ⚠️ Превышен лимит (429). Пауза {wait_time} сек... (попытка {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"   ❌ Ошибка API: статус {response.status_code}, текст: {response.text[:200]}")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    else:
                        return {"result": {"data": []}}

            except Exception as e:
                logger.error(f"   ❌ Ошибка при получении аналитики (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    return {"result": {"data": []}}

        return {"result": {"data": []}}

    def match_metrics_with_skus(self, products_data, analytics_data):
        """Сопоставляет метрики из аналитики с SKU товаров"""
        logger.info("   🔗 Сопоставление метрик с SKU товаров...")

        sku_to_offer = {}
        for offer_id, product_info in products_data.items():
            for sku in product_info.get('skus', []):
                sku_to_offer[sku] = {
                    'offer_id': offer_id,
                    'product_id': product_info.get('product_id'),
                    'name': product_info.get('name'),
                }

        result = {}
        items_processed = 0

        for item in analytics_data.get('result', {}).get('data', []):
            dimensions = item.get('dimensions', [])
            metrics = item.get('metrics', [])

            if dimensions and len(metrics) >= 14:
                sku_id = dimensions[0].get('id')

                if sku_id in sku_to_offer:
                    offer_data = sku_to_offer[sku_id]
                    offer_id = offer_data['offer_id']
                    items_processed += 1

                    revenue = metrics[0]
                    ordered_units = metrics[1]
                    hits_view = metrics[2]
                    hits_view_search = metrics[3]
                    hits_view_pdp = metrics[4]
                    session_view_pdp = metrics[5]
                    session_view_search = metrics[6]
                    position_category = metrics[7]
                    hits_tocart = metrics[8]
                    hits_tocart_search = metrics[9]
                    hits_tocart_pdp = metrics[10]
                    conv_tocart = metrics[11]
                    conv_tocart_search = metrics[12]
                    conv_tocart_pdp = metrics[13]

                    conversion_search_to_pdp = (hits_view_pdp / hits_view_search * 100) if hits_view_search > 0 else 0

                    if offer_id not in result:
                        result[offer_id] = {
                            'offer_id': offer_id,
                            'product_name': offer_data['name'],
                            'skus': [],
                            'total_revenue': 0,
                            'total_ordered_units': 0,
                            'total_hits_view': 0,
                            'total_hits_view_search': 0,
                            'total_hits_view_pdp': 0,
                            'total_session_view_pdp': 0,
                            'total_session_view_search': 0,
                            'total_hits_tocart': 0,
                            'positions_list': [],
                            'conv_tocart_search_weighted': 0,
                            'conv_tocart_weighted': 0,
                            'total_weight': 0
                        }

                    if sku_id not in result[offer_id]['skus']:
                        result[offer_id]['skus'].append(sku_id)

                    result[offer_id]['total_revenue'] += revenue
                    result[offer_id]['total_ordered_units'] += ordered_units
                    result[offer_id]['total_hits_view'] += hits_view
                    result[offer_id]['total_hits_view_search'] += hits_view_search
                    result[offer_id]['total_hits_view_pdp'] += hits_view_pdp
                    result[offer_id]['total_session_view_pdp'] += session_view_pdp
                    result[offer_id]['total_session_view_search'] += session_view_search
                    result[offer_id]['total_hits_tocart'] += hits_tocart

                    if position_category > 0:
                        result[offer_id]['positions_list'].append(position_category)

                    if hits_view > 0:
                        result[offer_id]['conv_tocart_search_weighted'] += conv_tocart_search * hits_view
                        result[offer_id]['conv_tocart_weighted'] += conv_tocart * hits_view
                        result[offer_id]['total_weight'] += hits_view

        logger.info(f"   🔗 Обработано {items_processed} SKU, сформировано {len(result)} товаров")

        for offer_id, data in result.items():
            if data['positions_list']:
                data['avg_position_category'] = round(sum(data['positions_list']) / len(data['positions_list']), 0)
            else:
                data['avg_position_category'] = 0

            data['conversion_search_to_pdp'] = round(
                (data['total_hits_view_pdp'] / data['total_hits_view_search'] * 100), 2) if data[
                                                                                                'total_hits_view_search'] > 0 else 0

            # Добавить для совместимости
            data['avg_conversion_search_to_pdp'] = data['conversion_search_to_pdp']

            data['conversion_view_to_order'] = round((data['total_ordered_units'] / data['total_hits_view'] * 100),
                                                     2) if data['total_hits_view'] > 0 else 0

            if data['total_weight'] > 0:
                data['avg_conv_tocart_search'] = round(data['conv_tocart_search_weighted'] / data['total_weight'], 2)
                data['avg_conv_tocart'] = round((data['total_hits_tocart'] / data['total_hits_view'] * 100), 2) if data[
                                                                                                                       'total_hits_view'] > 0 else 0
            else:
                data['avg_conv_tocart_search'] = 0
                data['avg_conv_tocart'] = 0

            data['total_revenue'] = round(data['total_revenue'], 2)

            del data['positions_list']
            del data['conv_tocart_search_weighted']
            del data['conv_tocart_weighted']
            del data['total_weight']

        return result

    def main(self):
        """Основной метод с общей обработкой ошибок"""
        try:
            logger.info("=" * 60)
            logger.info("🚀 НАЧАЛО РАБОТЫ OZON API PARSER")
            logger.info("=" * 60)

            # Обновляем токен
            if not self.update_token():
                logger.error("❌ Не удалось обновить токен, работа API парсера остановлена")
                return {}

            # Получаем все offer_id, которые в продаже
            all_items_offer_id, all_product_id = self.get_all_products_in_sale()
            if not all_items_offer_id:
                logger.warning("⚠️ Не найдено товаров в продаже")
                return {}

            logger.info(f"✅ Получено {len(all_items_offer_id)} offer_id товаров в продаже")
            time.sleep(1)

            # Получаем все SKU по offer_id
            all_skus_for_offer_id, all_sku = self.get_items_info_for_product_id(all_items_offer_id)
            if not all_skus_for_offer_id:
                logger.warning("⚠️ Не удалось получить информацию о товарах")
                return {}

            # Получаем аналитику
            analytics_data = self.get_analytic()
            if not analytics_data or not analytics_data.get('result', {}).get('data'):
                logger.warning("⚠️ Не удалось получить аналитику")
                return {}

            matched_data = self.match_metrics_with_skus(all_skus_for_offer_id, analytics_data)

            logger.info("=" * 60)
            logger.info(f"✅ API ПАРСЕР УСПЕШНО ЗАВЕРШИЛ РАБОТУ")
            logger.info(f"   Обработано товаров: {len(matched_data)}")
            logger.info("=" * 60)

            return matched_data

        except Exception as e:
            logger.error(f"❌ Критическая ошибка в API парсере: {e}")
            logger.error(traceback.format_exc())
            write_parser_error_to_sheet(f"Критическая ошибка в API парсере: {str(e)}")
            return {}


if __name__ == "__main__":
    parse = OzonSellerParse()
    res = parse.main()
    print(f"\nРезультат: {len(res)} товаров")
    upload_to_google_sheets(res)