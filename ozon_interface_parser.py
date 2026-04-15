import os
import time
import random
import logging
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime
from data_to_google_sheets import write_error_to_sheet, write_parser_error_to_sheet
import json

# Настройка логирования для парсера
logger = logging.getLogger(__name__)


class InterfaceParser:
    def __init__(self, profile_name: str = "selenium_profile"):
        self.project_root = os.getcwd()
        self.profile_path = os.path.join(self.project_root, "selenium_profile")

        self.random_sleep_from = 1
        self.random_sleep_to = 2

        # Создаем папку профиля, если её нет
        os.makedirs(self.profile_path, exist_ok=True)

        self.driver = None

    def _get_options(self):
        options = uc.ChromeOptions()

        # Дополнительные настройки (по желанию)
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-blink-features=AutomationControlled")

        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")

        return options

    def start_browser(self, headless: bool = False, max_retries: int = 3):
        """Запуск браузера с повторными попытками"""
        for attempt in range(max_retries):
            try:
                logger.info(f"   🌐 Попытка запуска браузера {attempt + 1}/{max_retries}")
                options = self._get_options()

                if headless:
                    options.add_argument("--headless=new")

                self.driver = uc.Chrome(
                    options=options,
                    use_subprocess=True,
                    user_data_dir=self.profile_path,
                    driver_executable_path="chromedriver.exe",
                )

                logger.info("   ✅ Браузер успешно запущен")
                time.sleep(2)
                return True

            except Exception as e:
                logger.error(f"   ❌ Ошибка при запуске браузера (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                else:
                    error_msg = f"Не удалось запустить браузер после {max_retries} попыток: {e}"
                    logger.error(f"   ❌ {error_msg}")
                    write_parser_error_to_sheet(error_msg)
                    return False

        return False

    def open_page(self, url: str = "https://www.google.com"):
        if not self.driver:
            raise Exception("Сначала запустите браузер через start_browser()")

        self.driver.get(url)
        print(f"Открыта страница: {url}")

    def wait(self, seconds: int = 999999):
        print(f"Ожидание {seconds} секунд...")
        try:
            time.sleep(seconds)
        except KeyboardInterrupt:
            print("Ожидание прервано")

    def close(self):
        if self.driver:
            self.driver.quit()
            print("Браузер закрыт")

    def random_sleep(self, count: int = 1):
        for _ in range(count):
            time.sleep(random.uniform(self.random_sleep_from, self.random_sleep_to))

    def random_scroll(self, min_pause=0.2, max_pause=0.5, steps=2):
        """
        Плавный рандомный скролл вниз/вверх
        """
        driver = self.driver
        last_height = driver.execute_script("return document.body.scrollHeight")

        for _ in range(steps):
            # случайное направление
            direction = random.choice([-1, 1])

            # случайная дистанция
            scroll_amount = random.randint(150, 300) * direction

            driver.execute_script(f"window.scrollBy(0, {scroll_amount});")

            time.sleep(random.uniform(min_pause, max_pause))

            # иногда доскролливаем до конца
            if random.random() < 0.2:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(0.2, 0.8))

    def scroll_to_element_center(self, element):
        self.driver.execute_script("""
            arguments[0].scrollIntoView({
                behavior: 'smooth',
                block: 'center',
                inline: 'center'
            });
        """, element)

    def check_auth_in_ozon(self):
        driver = self.driver
        driver.get("https://seller.ozon.ru/app/advertisement/product/cpc")
        self.random_sleep()
        try:
            h1 = driver.find_element(By.XPATH, "//h1[starts-with(@class,'sxRegistration_l4 heading-500')]").text
            if "Вход и регистрация" in h1:
                print(f"Слетела авторизация")
                write_error_to_sheet(f"Слетела авторизация")
            else:
                write_error_to_sheet(h1)
                print(f"Неизвестная ошибка в h1")

            return False
        except:
            return True

    def auth(self):
        driver = self.driver
        driver.get('https://seller.ozon.ru/app/advertisement/product/cpc')
        input("Нажмите Enter после авторизации и выбора магазина")

    def get_advert_analytic_pay_to_click(self, max_retries: int = 3):
        """Получение аналитики оплаты за клик с повторными попытками"""
        driver = self.driver
        analytic_advert_dict = {}

        for attempt in range(max_retries):
            try:
                logger.info(f"   📊 Попытка {attempt + 1}/{max_retries} получения аналитики CPC")

                driver.get('https://seller.ozon.ru/app/advertisement/product/cpc')
                result_date = self.get_ozon_date_today()

                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[(@type='button')]")))

                self.random_sleep(2)

                btn_status = None

                """ Находим кнопку с календарем и выбираем текущую дату """
                all_buttons_with_type = driver.find_elements(By.XPATH, ".//button[starts-with(@type, 'button')]")
                for button_calendar in all_buttons_with_type:
                    btn_text = button_calendar.text
                    if 'Стать Premium Pro' in btn_text or 'Кампании' in btn_text or 'Архив' in btn_text:
                        continue

                    button_calendar.click()
                    time.sleep(1.5)
                    try:
                        button = driver.find_element(By.XPATH, "//button[@type='button' and text()='Сегодня']")
                        button.click()
                        logger.info(f"   ✅ Успешно нажали на дату Сегодня")
                        try:
                            btn_status = all_buttons_with_type[all_buttons_with_type.index(button_calendar) + 1]
                        except:
                            pass
                        break
                    except:
                        continue

                self.random_sleep()
                if btn_status and 'Статус: Активна' in btn_status.text:
                    pass
                else:
                    if btn_status:
                        btn_status.click()
                    self.random_sleep()
                    tippy_content = driver.find_element(By.XPATH, "//div[(@class='tippy-content')]")
                    button = tippy_content.find_element(By.XPATH, ".//div[text()='Активна']")
                    button.click()

                all_pages_with_active_status = driver.find_element(By.XPATH,
                                                                   "//div[starts-with(@class,'_wrapper_lftsu')]")
                self.scroll_to_element_center(all_pages_with_active_status)
                self.random_sleep()
                all_pages_with_active_status = all_pages_with_active_status.find_element(By.XPATH,
                                                                                         ".//ul").find_elements(
                    By.XPATH, ".//li")
                self.random_sleep()
                logger.info(f"   📄 Всего страниц - {len(all_pages_with_active_status)}")

                for page in all_pages_with_active_status:
                    for page_attempt in range(3):
                        try:
                            self.scroll_to_element_center(page)
                            page.click()
                            self.random_sleep(2)
                            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//tbody")))
                            tbody = driver.find_element(By.XPATH, "//tbody")
                            tbody_elements = tbody.find_elements(By.XPATH, ".//tr")
                            for row in tbody_elements:
                                camping_id = str(row.find_elements(By.XPATH, ".//td")[1].text)
                                camping_url = f'https://seller.ozon.ru/app/advertisement/product/cpc/{camping_id}'
                                camping_strategy = row.find_elements(By.XPATH, ".//td")[4].text
                                camping_type = row.find_elements(By.XPATH, ".//td")[5].text
                                camping_budget = row.find_elements(By.XPATH, ".//td")[6].text.replace('\u202f', '')
                                if '₽' in camping_budget:
                                    camping_budget = float(camping_budget.split('₽')[0])
                                analytic_advert_dict[camping_id] = {"camping_url": camping_url,
                                                                    "camping_type": camping_type,
                                                                    "camping_strategy": camping_strategy,
                                                                    "camping_budget": camping_budget}
                            break
                        except Exception as e:
                            logger.warning(f"   ⚠️ Ошибка при обработке страницы: {e}")
                            continue

                res = self.parser_advert_dict(analytic_advert_dict)
                if res:
                    logger.info(f"   ✅ Аналитика CPC успешно получена")
                    return res
                else:
                    raise Exception("Результат парсинга пустой")

            except Exception as e:
                logger.error(f"   ❌ Ошибка в get_advert_analytic_pay_to_click (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    logger.info(f"   🔄 Перезагрузка страницы и повторная попытка...")
                    driver.refresh()
                    time.sleep(3)
                    continue
                else:
                    error_msg = f"Не удалось получить аналитику CPC после {max_retries} попыток: {e}"
                    logger.error(f"   ❌ {error_msg}")
                    write_parser_error_to_sheet(error_msg)
                    return {}

        return {}

    def get_ozon_date_today(self):
        months = [
            "янв", "фев", "мар", "апр", "май", "июн",
            "июл", "авг", "сен", "окт", "ноя", "дек"
        ]
        now = datetime.now()
        result_date = f"{now.day} {months[now.month - 1]}"
        return result_date

    def parser_advert_dict(self, advert_dict, max_retries: int = 3):
        driver = self.driver
        result_advert_dict = {}

        result_date = self.get_ozon_date_today()

        count_items = len(advert_dict.keys())
        logger.info(f"   📊 Обработка {count_items} рекламных кампаний")
        count = 0

        for camping_id in advert_dict:
            count += 1
            logger.info(f"   📝 {count}/{count_items} Обработка кампании {camping_id}")

            camping_json = advert_dict[camping_id]
            camping_url = camping_json["camping_url"]
            camping_type = camping_json["camping_type"]
            camping_strategy = camping_json["camping_strategy"]
            camping_budget = camping_json["camping_budget"]

            for attempt in range(max_retries):
                try:
                    driver.get(camping_url)
                    self.random_sleep(2)

                    all_buttons_with_type = driver.find_elements(By.XPATH, ".//button[starts-with(@type, 'button')]")
                    for button_calendar in all_buttons_with_type:
                        btn_text = button_calendar.text
                        if 'Стать Premium Pro' in btn_text or 'Кампании' in btn_text or 'Архив' in btn_text:
                            continue
                        if result_date == btn_text:
                            break
                        button_calendar.click()
                        time.sleep(1.5)
                        try:
                            button = driver.find_element(By.XPATH, "//button[@type='button' and text()='Сегодня']")
                            button.click()
                            self.random_sleep(1)
                            break
                        except:
                            continue

                    table = None
                    table_attempt = 0
                    while table_attempt < 20:
                        table = driver.find_element(By.XPATH, "//tbody")
                        if len(table.text) == 0:
                            time.sleep(0.5)
                            table_attempt += 1
                        else:
                            break

                    self.random_sleep(1)

                    all_row = table.find_elements(By.XPATH, f".//tr")
                    for row in all_row:
                        all_td = row.find_elements(By.XPATH, f".//td")
                        sku_and_offer_id = all_td[1].text
                        sku_split = sku_and_offer_id.split('\n')
                        sku = sku_split[0].replace('\n', '').strip()
                        offer_id = str(sku_split[1].replace('\n', '').strip())

                        try:
                            concurent_bet = all_td[4].text.replace('₽', '').strip().replace('\n', '').strip()
                        except:
                            concurent_bet = '-'

                        try:
                            my_bet = row.find_element(By.XPATH, f".//input[(@data-testid='InputCount')]").get_attribute(
                                'value')
                        except:
                            my_bet = '-'

                        sr_click = all_td[6].text.replace('\n', '').strip().replace('₽', '')
                        count_offers = all_td[7].text.replace('\n', '').strip().replace('\n', '').strip()
                        to_cart = all_td[15].text.replace('\n', '').strip()
                        drr = all_td[10].text.replace('\n', '').strip()
                        ctp = all_td[16].text.replace('\n', '').strip()
                        views = all_td[13].text.replace('\n', '').strip()
                        clicks = all_td[14].text.replace('\n', '').strip()
                        product_price = all_td[17].text.replace('₽', '').replace('\n', '').strip()

                        offer_dict = {
                            'offer_id': offer_id,
                            'sku': sku,
                            'camping_type': camping_type,
                            'camping_budget': camping_budget,
                            'strategy': camping_strategy,
                            'concurent_bet': concurent_bet,
                            'my_bet': my_bet,
                            'sr_click': sr_click,
                            'offers': count_offers,
                            'to_cart': to_cart,
                            'drr': drr,
                            'ctp': ctp,
                            'views': views,
                            'clicks': clicks,
                            'product_price': product_price,
                        }
                        if offer_id in result_advert_dict.keys():
                            result_advert_dict[offer_id].append(offer_dict)
                        else:
                            result_advert_dict[offer_id] = [offer_dict]
                    break

                except Exception as e:
                    logger.warning(f"   ⚠️ Ошибка при обработке кампании {camping_id} (попытка {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        driver.refresh()
                        time.sleep(3)
                        continue
                    else:
                        logger.error(f"   ❌ Не удалось обработать кампанию {camping_id}")
                        continue

        return result_advert_dict

    def get_advert_analytics_pay_to_buy(self, analytic_advert_dict, max_retries: int = 3):
        """Получение аналитики оплаты за заказ с повторными попытками"""
        analytic_advert_dict = analytic_advert_dict.copy() if analytic_advert_dict else {}
        driver = self.driver

        result_date = self.get_ozon_date_today()

        for attempt in range(max_retries):
            try:
                logger.info(f"   📊 Попытка {attempt + 1}/{max_retries} получения аналитики CPO")

                driver.get('https://seller.ozon.ru/app/advertisement/product/cpo/selected')
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[(@type='button')]")))

                self.random_sleep(2)

                """ Находим кнопку с календарем и выбираем текущую дату """
                all_buttons_with_type = driver.find_elements(By.XPATH, ".//button[starts-with(@type, 'button')]")
                for button_calendar in all_buttons_with_type:
                    btn_text = button_calendar.text
                    if 'Стать Premium Pro' in btn_text or 'Кампании' in btn_text or 'Архив' in btn_text:
                        continue
                    if result_date == btn_text:
                        break
                    button_calendar.click()
                    time.sleep(1.5)
                    try:
                        button = driver.find_element(By.XPATH, "//button[@type='button' and text()='Сегодня']")
                        button.click()
                        logger.info(f"   ✅ Успешно нажали на дату Сегодня")
                        self.random_sleep()
                        break
                    except:
                        continue

                all_pages_with_active_status = driver.find_element(By.XPATH,
                                                                   "//div[starts-with(@class,'_wrapper_lftsu')]")
                self.scroll_to_element_center(all_pages_with_active_status)
                self.random_sleep()
                all_pages_with_active_status = all_pages_with_active_status.find_element(By.XPATH,
                                                                                         ".//ul").find_elements(
                    By.XPATH, ".//li")
                self.random_sleep()
                logger.info(f"   📄 Всего страниц - {len(all_pages_with_active_status)}")

                for page in all_pages_with_active_status:
                    self.scroll_to_element_center(page)
                    page.click()
                    self.random_sleep(2)
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//tbody")))
                    time.sleep(2)
                    tbody = driver.find_element(By.XPATH, "//tbody")
                    tbody_elements = tbody.find_elements(By.XPATH, ".//tr")
                    for row in tbody_elements:
                        try:
                            all_td = row.find_elements(By.XPATH, ".//td")
                            sku_and_offer_id = all_td[2].text
                            sku_split = sku_and_offer_id.split("\n")

                            sku = sku_split[0].strip().replace("\n", "").strip().replace('\u202f', '')
                            offer_id = sku_split[1].strip().replace("\n", "").strip().replace('\u202f', '')

                            bet_data = all_td[5].text.split('\n')
                            if len(bet_data) == 2:
                                bet_percent = bet_data[0].replace('\n', '').strip().replace('%', '').strip().replace(
                                    '\u202f', '')
                                bet_amount = bet_data[1].replace('\n', '').strip().replace('₽', '').strip().replace(
                                    '\u202f', '')
                            else:
                                bet_percent = bet_data[0].replace('\n', '').strip().replace('%', '').strip().replace(
                                    '\u202f', '')
                                bet_amount = bet_data[2].replace('\n', '').strip().replace('₽', '').strip().replace(
                                    '\u202f', '')

                            product_price = all_td[7].text.replace('\n', '').strip().replace(' ₽', '').strip().replace(
                                '₽', '').strip().replace('\u202f', '')
                            index_view = all_td[8].text.replace('\n', '').strip().replace('\u202f', '')

                            product_buy_pay = all_td[13].text.replace('\n', '').strip().replace('\u202f', '')
                            product_buy_combo_model = all_td[14].text.replace('\n', '').strip().replace('\u202f', '')

                            drr = all_td[15].text.replace('\n', '').strip().replace('\u202f', '')

                            offer_dict = {
                                'offer_id': offer_id,
                                'sku': sku,
                                'camping_type': "Оплата за заказ",
                                'bet_percent': bet_percent,
                                'bet_amount': bet_amount,
                                'product_price': product_price,
                                'index_view': index_view,
                                'product_buy_pay': product_buy_pay,
                                'product_buy_combo_model': product_buy_combo_model,
                                'drr': drr,
                            }

                            if offer_id in analytic_advert_dict.keys():
                                analytic_advert_dict[offer_id].append(offer_dict)
                            else:
                                analytic_advert_dict[offer_id] = [offer_dict]
                        except Exception as e:
                            logger.warning(f"   ⚠️ Ошибка при обработке строки CPO: {e}")
                            continue

                logger.info(f"   ✅ Аналитика CPO успешно получена")
                return analytic_advert_dict

            except Exception as e:
                logger.error(f"   ❌ Ошибка в get_advert_analytics_pay_to_buy (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    logger.info(f"   🔄 Перезагрузка страницы и повторная попытка...")
                    driver.refresh()
                    time.sleep(3)
                    continue
                else:
                    error_msg = f"Не удалось получить аналитику CPO после {max_retries} попыток: {e}"
                    logger.error(f"   ❌ {error_msg}")
                    write_parser_error_to_sheet(error_msg)
                    return analytic_advert_dict if analytic_advert_dict else {}

        return analytic_advert_dict if analytic_advert_dict else {}

    def get_position_product_from_sku(self, max_retries: int = 3):
        """Получение позиций товаров с повторными попытками"""
        driver = self.driver
        position_dict = {}

        if not self.check_auth_in_ozon():
            error_msg = "Авторизация не пройдена, невозможно получить позиции товаров"
            logger.error(f"   ❌ {error_msg}")
            write_parser_error_to_sheet(error_msg)
            return {}

        for attempt in range(max_retries):
            try:
                logger.info(f"   📍 Попытка {attempt + 1}/{max_retries} получения позиций товаров")

                driver.get('https://seller.ozon.ru/app/analytics-search/search-results/validator')
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[@class='analyticsSearchApp']")))

                time.sleep(random.uniform(self.random_sleep_from, self.random_sleep_to))

                all_templates = driver.find_element(By.XPATH, "//div[@class='analyticsSearchApp']").find_element(
                    By.XPATH, "./div/div/div[1]/div[2]/div/div[1]").find_elements(By.XPATH, './div')

                logger.info(f"   📄 Найдено {len(all_templates)} шаблонов поиска")

                # Проходим по всем шаблонам по одному разу
                for template_idx, template in enumerate(all_templates):
                    logger.info(f"   🔍 Обработка шаблона {template_idx + 1}/{len(all_templates)}")

                    try:
                        # Кликаем по шаблону
                        self.scroll_to_element_center(template)
                        time.sleep(random.uniform(0.5, 1))
                        template.click()
                        self.random_scroll()

                        # Ждем загрузки таблицы
                        self.random_sleep(2)
                        try:
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.XPATH, "//tbody")))
                        except:
                            logger.warning(f"   ⚠️ Шаблон {template_idx + 1}: таблица не загрузилась, пропускаем")
                            continue

                        table = driver.find_element(By.XPATH, "//tbody")
                        all_items = table.find_elements(By.XPATH, ".//tr")

                        if not all_items:
                            logger.warning(f"   ⚠️ Шаблон {template_idx + 1}: таблица пуста, пропускаем")
                            continue

                        # Парсим данные из текущего шаблона
                        items_count = 0
                        for item in all_items:
                            try:
                                td_elements = item.find_elements(By.XPATH, ".//td")
                                if len(td_elements) < 3:
                                    continue

                                sku_text = td_elements[1].text
                                if 'SKU' not in sku_text:
                                    continue

                                item_sku = sku_text.split('SKU')[1].split('\n')[0].strip()
                                item_position = td_elements[2].find_element(By.XPATH, './/div').text

                                if item_sku and item_position:
                                    try:
                                        item_sku = str(int(item_sku))
                                        # Добавляем или обновляем позицию
                                        position_dict[item_sku] = str(item_position)
                                        items_count += 1
                                    except (ValueError, TypeError):
                                        continue
                            except Exception as e:
                                continue

                        logger.info(f"   ✅ Шаблон {template_idx + 1}: получено {items_count} позиций")

                        # Небольшая пауза между шаблонами
                        time.sleep(random.uniform(1, 2))

                    except Exception as e:
                        logger.warning(f"   ⚠️ Ошибка при обработке шаблона {template_idx + 1}: {e}")
                        continue

                if position_dict:
                    logger.info(
                        f"   ✅ Всего получены позиции для {len(position_dict)} SKU из {len(all_templates)} шаблонов")
                    return position_dict
                else:
                    # Если не нашли данные ни в одном шаблоне
                    if attempt < max_retries - 1:
                        logger.warning(f"   ⚠️ Данные не найдены ни в одном шаблоне, перезагрузка страницы...")
                        driver.refresh()
                        time.sleep(3)
                        continue
                    else:
                        raise Exception("Не удалось получить позиции товаров ни в одном шаблоне")

            except Exception as e:
                logger.error(f"   ❌ Ошибка в get_position_product_from_sku (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    logger.info(f"   🔄 Перезагрузка страницы и повторная попытка...")
                    try:
                        driver.refresh()
                    except:
                        pass
                    time.sleep(3)
                    continue
                else:
                    error_msg = f"Не удалось получить позиции товаров после {max_retries} попыток: {e}"
                    logger.error(f"   ❌ {error_msg}")
                    write_parser_error_to_sheet(error_msg)
                    return {}

        return {}

    def get_all_advert_analytic(self, max_retries: int = 3):
        """Получение всей рекламной аналитики с обработкой ошибок"""
        if not self.check_auth_in_ozon():
            error_msg = "Авторизация не пройдена, невозможно получить рекламную аналитику"
            logger.error(f"   ❌ {error_msg}")
            write_parser_error_to_sheet(error_msg)
            return {}

        try:
            logger.info("   📊 Начинаем сбор рекламной аналитики...")
            res_dict = self.get_advert_analytic_pay_to_click(max_retries)
            if res_dict:
                res_dict = self.get_advert_analytics_pay_to_buy(res_dict, max_retries)
                logger.info(f"   ✅ Рекламная аналитика успешно собрана для {len(res_dict)} товаров")
                return res_dict
            else:
                logger.warning("   ⚠️ Не удалось получить аналитику CPC")
                return {}
        except Exception as e:
            error_msg = f"Критическая ошибка при сборе рекламной аналитики: {e}"
            logger.error(f"   ❌ {error_msg}")
            write_parser_error_to_sheet(error_msg)
            return {}


if __name__ == "__main__":
    parser = InterfaceParser()

    parser.start_browser(headless=False)
    parser.auth()
    time.sleep(2)
    parser.close()