import os
import time
import random
import logging
import traceback
import inspect

import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime
from data_to_google_sheets import write_error_to_sheet, write_parser_error_to_sheet
import json
import shutil

from config import profile_name

# Настройка логирования для парсера
logger = logging.getLogger(__name__)


class InterfaceParser:
    def __init__(self):
        self.project_root = os.getcwd()
        self.profile_path = os.path.join(self.project_root, profile_name)

        self.random_sleep_from = 1
        self.random_sleep_to = 2

        # Создаем папку профиля, если её нет
        os.makedirs(self.profile_path, exist_ok=True)

        self.driver = None

    def clean_profile_cache(self):
        """Очищает кэш и временные файлы профиля, сохраняя авторизацию"""
        logger.info(f"Init func - {inspect.currentframe()}")
        try:
            # Файлы и папки, которые можно безопасно удалить
            items_to_remove = [
                "Cache",
                "Code Cache",
                "Service Worker",
                "File System",
                "IndexedDB",
                "Local Storage",
                "Session Storage",
                "Web Data",
                "Web Data-journal",
                "Cookies-journal",  # Не удаляем сами Cookies
                "History",
                "History-journal",
                "Visited Links",
                "Top Sites",
                "Top Sites-journal",
                "QuotaManager",
                "QuotaManager-journal",
                "GPUCache",
                "DawnCache",
                "ShaderCache",
                "GrShaderCache",
                "graphite_dawn_cache",
            ]

            for item in items_to_remove:
                item_path = os.path.join(self.profile_path, item)
                if os.path.exists(item_path):
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                        logger.info(f"   🗑️ Удалена папка: {item}")
                    else:
                        os.remove(item_path)
                        logger.info(f"   🗑️ Удалён файл: {item}")

            logger.info("   ✅ Профиль очищен от кэша")
            return True
        except Exception as e:
            logger.warning(f"   ⚠️ Ошибка при очистке профиля: {e}")
            return False

    def _get_options(self):
        logger.info(f"Init func - {inspect.currentframe()}")
        options = uc.ChromeOptions()

        # Дополнительные настройки (по желанию)
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-blink-features=AutomationControlled")

        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")

        # ОТКЛЮЧАЕМ КЭШ, но сохраняем авторизацию
        options.add_argument("--disable-cache")
        options.add_argument("--disable-application-cache")
        options.add_argument("--disable-offline-load-strict-mode")
        options.add_argument("--disk-cache-size=0")
        options.add_argument("--media-cache-size=0")

        # Ограничиваем размер профиля
        options.add_argument("--disable-session-crashed-bubble")
        options.add_argument("--disable-component-update")

        # Не сохраняем историю и данные форм
        options.add_argument("--disable-save-password-bubble")
        options.add_argument("--disable-autofill")

        # Очищаем кэш при запуске (дополнительно)
        prefs = {
            "profile.block_third_party_cookies": False,
            "profile.default_content_setting_values.images": 1,
            "profile.default_content_setting_values.javascript": 1,
            "profile.managed_default_content_settings.stylesheets": 1,
            # Отключаем сохранение кэша
            "disk-cache-size": 0,
            "media-cache-size": 0,
            # Не сохраняем пароли
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        }
        options.add_experimental_option("prefs", prefs)

        # Удаляем ненужные аргументы командной строки, которые создают лишние файлы
        options.add_argument("--disable-logging")
        options.add_argument("--log-level=3")
        options.add_argument("--silent")

        return options

    def start_browser(self, headless: bool = False, max_retries: int = 3, clean_cache: bool = True):
        logger.info(f"Init func - {inspect.currentframe()}")
        """Запуск браузера с повторными попытками"""

        # Очищаем кэш перед запуском (опционально)
        if clean_cache:
            self.clean_profile_cache()

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

                self.driver.maximize_window()
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
        logger.info(f"Init func - {inspect.currentframe()}")
        if not self.driver:
            raise Exception("Сначала запустите браузер через start_browser()")

        self.driver.get(url)
        print(f"Открыта страница: {url}")

    def wait(self, seconds: int = 999999):
        logger.info(f"Init func - {inspect.currentframe()}")
        print(f"Ожидание {seconds} секунд...")
        try:
            time.sleep(seconds)
        except KeyboardInterrupt:
            print("Ожидание прервано")

    def close(self):
        logger.info(f"Init func - {inspect.currentframe()}")
        if self.driver:
            try:
                self.driver.close()
            except Exception as e:
                pass
            time.sleep(7)
            print("Браузер закрыт")

    def random_sleep(self, count: int = 1):
        logger.info(f"Init func - {inspect.currentframe()}")
        for _ in range(count):
            time.sleep(random.uniform(self.random_sleep_from, self.random_sleep_to))

    def random_scroll(self, min_pause=0.2, max_pause=0.5, steps=2):
        logger.info(f"Init func - {inspect.currentframe()}")
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
        logger.info(f"Init func - {inspect.currentframe()}")
        self.driver.execute_script("""
            arguments[0].scrollIntoView({
                behavior: 'smooth',
                block: 'center',
                inline: 'center'
            });
        """, element)

    def auth_online(self):
        logger.info(f"Init func - {inspect.currentframe()}")
        input(f"Авторизуйтесь")
        return False

    def check_auth_in_ozon(self):
        logger.info(f"Init func - {inspect.currentframe()}")
        driver = self.driver
        self.random_sleep()
        try:
            h1 = driver.find_element(By.XPATH, "//h1[starts-with(@class,'sxRegistration_l4 heading-500')]").text
            if "Вход и регистрация" in h1:
                print(f"Слетела авторизация")
                write_error_to_sheet(f"Слетела авторизация")
            else:
                write_error_to_sheet(h1)
                print(f"Неизвестная ошибка в h1")

            if self.auth_online():
                return True
            else:
                return False
        except:
            return True

    def auth(self):
        logger.info(f"Init func - {inspect.currentframe()}")
        driver = self.driver
        driver.get('https://seller.ozon.ru/app/advertisement/product/cpc')
        input("Нажмите Enter после авторизации и выбора магазина")

    def get_advert_analytic_pay_to_click(self, max_retries: int = 3):
        logger.info(f"Init func - {inspect.currentframe()}")
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

                try:
                    all_pages_with_active_status = driver.find_element(By.XPATH,
                                                                       "//div[starts-with(@class,'_wrapper_lftsu')]")
                    self.scroll_to_element_center(all_pages_with_active_status)
                    self.random_sleep()
                    all_pages_with_active_status = all_pages_with_active_status.find_element(By.XPATH,
                                                                                             ".//ul").find_elements(
                        By.XPATH, ".//li")
                except:
                    all_pages_with_active_status = []

                self.random_sleep()
                logger.info(f"   📄 Всего страниц - {len(all_pages_with_active_status)}")

                if all_pages_with_active_status:
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
                else:
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
        logger.info(f"Init func - {inspect.currentframe()}")
        months = [
            "янв", "фев", "мар", "апр", "май", "июн",
            "июл", "авг", "сен", "окт", "ноя", "дек"
        ]
        now = datetime.now()
        result_date = f"{now.day} {months[now.month - 1]}"
        return result_date

    def pars_table_advert(self, driver, row, all_td: list, camping_type):
        logger.info(f"Init func - {inspect.currentframe()}")
        print(camping_type)

        concurent_bet = '-'
        my_bet = "-"
        sr_click = "-"
        count_offers = "-"
        selled = "-"
        expense = "-"
        drr = "-"
        views = "-"
        clicks = "-"
        to_cart = "-"
        ctp = "-"
        product_price = "-"

        concurent_bet_index = False
        my_bet_index = False
        sr_click_index = False
        count_offers_index = False
        selled_index = False
        expense_index = False
        drr_index = False
        views_index = False
        clicks_index = False
        to_cart_index = False
        ctp_index = False
        product_price_index = False

        try:
            thead_table = driver.find_element(By.XPATH, "//thead")
            all_th = thead_table.find_elements(By.XPATH, ".//th")

            for th in all_th:
                if "Конкурентная ставка" == th.text:
                    concurent_bet_index = all_th.index(th)
                elif "Ваша ставка" == th.text:
                    my_bet_index = all_th.index(th)
                elif "Средняя стоимость клика" == th.text:
                    sr_click_index = all_th.index(th)
                elif "Заказы" == th.text:
                    count_offers_index = all_th.index(th)
                elif "Продажи" == th.text:
                    selled_index = all_th.index(th)
                elif "Расход" == th.text:
                    expense_index = all_th.index(th)
                elif "ДРР" == th.text:
                    drr_index = all_th.index(th)
                elif "Показы" == th.text:
                    views_index = all_th.index(th)
                elif "Клики" == th.text:
                    clicks_index = all_th.index(th)
                elif "В корзину" == th.text:
                    to_cart_index = all_th.index(th)
                elif "CTR" == th.text:
                    ctp_index = all_th.index(th)
                elif "Ваша цена" == th.text:
                    product_price_index = all_th.index(th)

            print(f"concurent_bet_index: {concurent_bet_index}", f"my_bet_index: {my_bet_index}",
                  f"sr_click_index: {sr_click_index}", f"count_offers_index: {count_offers_index}",
                  f"selled_index: {selled_index}", f"expense_index: {expense_index}", f"drr_index: {drr_index}",
                  f"views_index: {views_index}", f"clicks_index: {clicks_index}", f"to_cart_index: {to_cart_index}",
                  f"ctp_index: {ctp_index}", f"product_price_index: {product_price_index}", sep='\n', end='\n\n')

            if concurent_bet_index:
                concurent_bet = all_td[concurent_bet_index].text.replace('₽', '').strip().replace('\n', '').strip()
            if my_bet_index:
                my_bet = all_td[my_bet_index].find_element(By.XPATH,
                                                           f".//input[(@data-testid='InputCount')]").get_attribute(
                    'value')
            if sr_click_index:
                sr_click = all_td[sr_click_index].text.replace('\n', '').strip().replace('₽', '')
            if count_offers_index:
                count_offers = all_td[count_offers_index].text.replace('\n', '').strip().replace('\n', '').strip()
            if selled_index:
                selled = all_td[selled_index].text.replace('\n', '').strip().replace('₽', '').replace(',', '.').strip()
            if expense_index:
                expense = all_td[expense_index].text.replace('\n', '').strip().replace('₽', '').replace(',',
                                                                                                        '.').strip()
            if drr_index:
                drr = all_td[drr_index].text.replace('\n', '').strip()
            if views_index:
                views = all_td[views_index].text.replace('\n', '').strip()
            if clicks_index:
                clicks = all_td[clicks_index].text.replace('\n', '').strip()
            if to_cart_index:
                to_cart = all_td[to_cart_index].text.replace('\n', '').strip()
            if ctp_index:
                ctp = all_td[ctp_index].text.replace('\n', '').strip()
            if product_price_index:
                product_price = all_td[product_price_index].text.replace('₽', '').replace('\n', '').strip()

            print(f"Моя ставка - {my_bet}", f"Конкурентная ставка - {concurent_bet}", f"Средняя стоимость клика - {sr_click}",
                  f"Заказы - {count_offers}", to_cart, f"DRR - {drr}", ctp, views, clicks,
                  f"Цена товара - {product_price}", expense, sep='\n', end='\n\n')
            return my_bet, concurent_bet, sr_click, count_offers, to_cart, drr, ctp, views, clicks, product_price, expense, selled
        except Exception as e:
            print(traceback.format_exc())

            if str(camping_type) == "Поиск и рекомендации":
                if len(all_td) == 19:
                    concurent_bet = '-'
                    my_bet = '-'
                    target_drr = all_td[4].text.replace('\n', '').strip().replace('₽', '')
                    sr_click = all_td[5].text.replace('\n', '').strip().replace('₽', '')
                    count_offers = all_td[6].text.replace('\n', '').strip().replace('\n', '').strip()
                    selled = all_td[7].text.replace('\n', '').strip().replace('₽', '').replace(',', '.').strip()
                    expense = all_td[8].text.replace('\n', '').strip().replace('₽', '').replace(',', '.').strip()
                    drr = all_td[9].text.replace('\n', '').strip()
                    views = all_td[12].text.replace('\n', '').strip()
                    clicks = all_td[13].text.replace('\n', '').strip()
                    to_cart = all_td[14].text.replace('\n', '').strip()
                    ctp = all_td[15].text.replace('\n', '').strip()
                    product_price = all_td[16].text.replace('₽', '').replace('\n', '').strip()
                else:
                    try:
                        concurent_bet = all_td[4].text.replace('₽', '').strip().replace('\n', '').strip()
                    except:
                        concurent_bet = '-'
                    try:
                        my_bet = row.find_element(By.XPATH,
                                                  f".//input[(@data-testid='InputCount')]").get_attribute('value')
                    except:
                        my_bet = '-'
                    sr_click = all_td[6].text.replace('\n', '').strip().replace('₽', '')
                    count_offers = all_td[7].text.replace('\n', '').strip().replace('\n', '').strip()
                    selled = all_td[8].text.replace('\n', '').strip().replace('₽', '').replace(',', '.').strip()
                    expense = all_td[9].text.replace('\n', '').strip().replace('₽', '').replace(',', '.').strip()
                    drr = all_td[10].text.replace('\n', '').strip()
                    views = all_td[13].text.replace('\n', '').strip()
                    clicks = all_td[14].text.replace('\n', '').strip()
                    to_cart = all_td[15].text.replace('\n', '').strip()
                    ctp = all_td[16].text.replace('\n', '').strip()
                    product_price = all_td[17].text.replace('₽', '').replace('\n', '').strip()
            else:
                if len(all_td) == 19:
                    my_bet = '-'
                    concurent_bet = '-'
                    sr_click = all_td[4].text.replace('\n', '').strip().replace('₽', '')
                    count_offers = all_td[5].text.replace('\n', '').strip().replace('\n', '').strip()
                    selled = all_td[6].text.replace('\n', '').strip().replace('₽', '').replace(',', '.').strip()
                    expense = all_td[7].text.replace('\n', '').strip().replace('₽', '').replace(',', '.').strip()
                    drr = all_td[8].text.replace('\n', '').strip()
                    views = all_td[11].text.replace('\n', '').strip()
                    clicks = all_td[12].text.replace('\n', '').strip()
                    to_cart = all_td[13].text.replace('\n', '').strip()
                    ctp = all_td[14].text.replace('\n', '').strip()
                    product_price = all_td[16].text.replace('₽', '').replace('\n', '').strip()
                else:
                    try:
                        concurent_bet = all_td[4].text.replace('₽', '').strip().replace('\n', '').strip()
                    except:
                        concurent_bet = '-'
                    try:
                        my_bet = row.find_element(By.XPATH,
                                                  f".//input[(@data-testid='InputCount')]").get_attribute(
                            'value')
                    except:
                        my_bet = '-'
                    sr_click = all_td[6].text.replace('\n', '').strip().replace('₽', '')
                    count_offers = all_td[7].text.replace('\n', '').strip().replace('\n', '').strip()
                    selled = all_td[8].text.replace('\n', '').strip().replace('₽', '').replace(',', '.').strip()
                    expense = all_td[9].text.replace('\n', '').strip().replace('₽', '').replace(',', '.').strip()
                    drr = all_td[10].text.replace('\n', '').strip()
                    views = all_td[13].text.replace('\n', '').strip()
                    clicks = all_td[14].text.replace('\n', '').strip()
                    to_cart = all_td[15].text.replace('\n', '').strip()
                    ctp = all_td[16].text.replace('\n', '').strip()
                    product_price = all_td[17].text.replace('₽', '').replace('\n', '').strip()

            print(my_bet, concurent_bet, sr_click, count_offers, to_cart, drr, ctp, views, clicks,
                  f"Цена товара - {product_price}", expense, sep='\n', end='\n\n')
            return my_bet, concurent_bet, sr_click, count_offers, to_cart, drr, ctp, views, clicks, product_price, expense, selled

    def parser_advert_dict(self, advert_dict, max_retries: int = 3):
        logger.info(f"Init func - {inspect.currentframe()}")
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

                    self.random_sleep(2)

                    all_row = table.find_elements(By.XPATH, f".//tr")
                    for row in all_row:
                        all_td = row.find_elements(By.XPATH, f".//td")
                        sku_and_offer_id = all_td[1].text
                        sku_split = sku_and_offer_id.split('\n')
                        sku = sku_split[0].replace('\n', '').strip()
                        offer_id = str(sku_split[1].replace('\n', '').strip())

                        print(f"Длинна all td - {len(all_td)}")
                        my_bet, concurent_bet, sr_click, count_offers, to_cart, drr, ctp, views, clicks, product_price, expense, selled = self.pars_table_advert(driver, row, all_td, camping_type)

                        logger.info(
                            f"""
                                offer id - {offer_id}\n
                                sku - {sku}\n
                                стратегия - {camping_strategy}\n
                                конкурентная ставка - {concurent_bet}\n
                                ваша ставка - {my_bet}\n
                                Средняя стоимость клика - {sr_click}\n
                                Заказы - {count_offers}\n
                                В корзину(шт) - {to_cart}\n
                                ДРР - {drr}\n
                                Показы - {views}\n
                                Клики - {clicks}\n
                                Бюджет - {camping_budget}
                                Цена товара - {product_price}\n
                                Расходы - {expense}\n
                                Продажи - {selled}\n                 

                            """)

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
                            'expense': expense,
                            'selled': selled,
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
        logger.info(f"Init func - {inspect.currentframe()}")
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

                try:
                    all_pages_with_active_status = driver.find_element(By.XPATH,
                                                                       "//div[starts-with(@class,'_wrapper_lftsu')]")
                    self.scroll_to_element_center(all_pages_with_active_status)
                    self.random_sleep()
                    all_pages_with_active_status = all_pages_with_active_status.find_element(By.XPATH,
                                                                                             ".//ul").find_elements(
                        By.XPATH, ".//li")
                except:
                    all_pages_with_active_status = []

                self.random_sleep()
                logger.info(f"   📄 Всего страниц - {len(all_pages_with_active_status)}")

                if all_pages_with_active_status:
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
                                    bet_percent = bet_data[0].replace('\n', '').strip().replace('%',
                                                                                                '').strip().replace(
                                        '\u202f', '')
                                    bet_amount = bet_data[1].replace('\n', '').strip().replace('₽', '').strip().replace(
                                        '\u202f', '')
                                else:
                                    bet_percent = bet_data[0].replace('\n', '').strip().replace('%',
                                                                                                '').strip().replace(
                                        '\u202f', '')
                                    bet_amount = bet_data[2].replace('\n', '').strip().replace('₽', '').strip().replace(
                                        '\u202f', '')

                                product_price = all_td[7].text.replace('\n', '').strip().replace(' ₽',
                                                                                                 '').strip().replace(
                                    '₽', '').strip().replace('\u202f', '')
                                index_view = all_td[8].text.replace('\n', '').strip().replace('\u202f', '')

                                product_buy_pay = all_td[13].text.replace('\n', '').strip().replace('\u202f', '')
                                product_buy_combo_model = all_td[14].text.replace('\n', '').strip().replace('\u202f',
                                                                                                            '')

                                combo_sell = all_td[10].text.replace('\n', '').strip().replace('₽', '').replace(',',
                                                                                                                '.').strip()

                                drr = all_td[15].text.replace('\n', '').strip().replace('\u202f', '')
                                if 'Выключено' in drr or 'Включено' in drr:
                                    drr = all_td[14].text.replace('\n', '').strip().replace('\u202f', '')

                                expense = all_td[9].text.replace('\n', '').strip().replace('₽', '').replace(',',
                                                                                                            '.').strip()
                                expense_combo = all_td[10].text.replace('\n', '').strip().replace('₽', '').replace(',',
                                                                                                                   '.').strip()
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
                                    'expense': expense,
                                    'expense_combo': expense_combo,
                                    'selled': combo_sell,
                                }

                                if offer_id in analytic_advert_dict.keys():
                                    analytic_advert_dict[offer_id].append(offer_dict)
                                else:
                                    analytic_advert_dict[offer_id] = [offer_dict]
                            except Exception as e:
                                logger.warning(f"   ⚠️ Ошибка при обработке строки CPO: {e}")
                                continue
                else:
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

                            expense = all_td[9].text.replace('\n', '').strip().replace('₽', '').replace(',',
                                                                                                        '.').strip()
                            expense_combo = all_td[10].text.replace('\n', '').strip().replace('₽', '').replace(',',
                                                                                                               '.').strip()

                            selled = all_td[10].text.replace('\n', '').strip().replace('₽', '').replace(',',
                                                                                                        '.').strip()
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
                                'expense': expense,
                                'expense_combo': expense_combo,
                                'selled': selled,
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
        logger.info(f"Init func - {inspect.currentframe()}")
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

                        self.random_sleep(2)
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

                                sku_text = td_elements[1].text

                                item_sku = sku_text.split('SKU')[1].split('\n')[0].strip()
                                item_position = td_elements[2].find_element(By.XPATH, './/div').text
                                print(f"{item_sku}: {item_position}")
                                position_dict[item_sku] = str(item_position)

                                items_count += 1
                            except Exception as e:
                                print(traceback.format_exc())
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

    def get_actual_prices_offer_id(self):
        logger.info(f"Init func - {inspect.currentframe()}")
        driver = self.driver
        prices_dict = {}
        for attempt in range(3):
            try:
                logger.info(f"Попытка №{attempt} получить цены товаров")

                driver.get('https://seller.ozon.ru/app/prices/control')
                WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH, "//tbody")))

                self.random_sleep()
                self.random_sleep()

                if not self.check_auth_in_ozon():
                    error_msg = "Авторизация не пройдена, невозможно получить позиции товаров"
                    logger.error(f"   ❌ {error_msg}")
                    write_parser_error_to_sheet(error_msg)
                    return {}

                try:
                    all_pages_with_active_status = driver.find_elements(By.XPATH, "//ul")[-1]
                    self.scroll_to_element_center(all_pages_with_active_status)
                    self.random_sleep()
                    all_pages_with_active_status = all_pages_with_active_status.find_elements(By.XPATH, ".//li")
                except:
                    all_pages_with_active_status = []

                logger.info(f"Всего страниц с ценой - {len(all_pages_with_active_status)}")
                if all_pages_with_active_status:
                    for page in all_pages_with_active_status:
                        print(f"Текст кнопки - {page.text}")
                        self.scroll_to_element_center(page)
                        self.random_sleep()
                        page.click()
                        self.random_sleep()
                        self.random_sleep()

                        pages_widget = driver.find_element(By.XPATH, "//article")
                        self.scroll_to_element_center(pages_widget)
                        time.sleep(3)
                        input_element = driver.find_element(By.XPATH, "//input[starts-with(@id, 'baseInput')]")
                        self.scroll_to_element_center(input_element)
                        time.sleep(3)

                        for _ in range(6):
                            try:
                                elem = driver.find_elements(By.XPATH, "//ul")[-1]
                                self.scroll_to_element_center(elem)
                                self.random_sleep(1)
                            except:
                                pass

                        actual_table = driver.find_element(By.XPATH, "//tbody")
                        all_tr = actual_table.find_elements(By.XPATH, ".//tr")
                        for row in all_tr:
                            for _ in range(3):
                                try:
                                    all_td = row.find_elements(By.XPATH, ".//td")
                                    self.scroll_to_element_center(all_td[1])
                                    all_td = row.find_elements(By.XPATH, ".//td")
                                    time.sleep(2.5)
                                    item_name = all_td[2].text
                                    if 'Название и артикул' in item_name:
                                        continue

                                    print(item_name.split('\n'))
                                    item_offer_id = item_name.split('\n')[1].replace('₽', '').strip().replace('\n',
                                                                                                              '').strip()
                                    print(all_td[5].text.split('\n'))
                                    item_price = all_td[5].text
                                    print(f'Item price - {item_price}')
                                    if '\n' in item_price:
                                        item_price = item_price.split('\n')[1].replace('₽', '').strip().replace('\n','').strip()
                                    else:
                                        item_price = item_price.replace('₽', '').strip().replace('\n','').strip()

                                    if item_price == '':
                                        time.sleep(3)
                                        all_td = row.find_elements(By.XPATH, ".//td")
                                        self.scroll_to_element_center(all_td[5])
                                        time.sleep(3)
                                        item_price = all_td[5].text
                                        print(f'Item price - {item_price}')
                                        if '\n' in item_price:
                                            item_price = item_price.split('\n')[1].replace('₽', '').strip().replace(
                                                '\n', '').strip()
                                        else:
                                            item_price = item_price.replace('₽', '').strip().replace('\n', '').strip()

                                    item_price_before = all_td[4].text
                                    if '\n' in item_price_before:
                                        item_price_before = item_price_before.split('\n')[0].replace('₽', '').strip().replace('\n', '').strip()
                                    else:
                                        item_price_before = item_price_before.replace('₽', '').strip().replace('\n', '').strip()

                                    self.scroll_to_element_center(all_td[19])
                                    time.sleep(2)
                                    cost_price = all_td[19].text
                                    if '\n' in cost_price:
                                        cost_price = cost_price.split('\n')[0]
                                    cost_price = cost_price.replace('₽', '').strip().replace('\n', '').strip()

                                    commission_fbo = all_td[20].text.replace('₽', '').strip().replace('\n', '').strip()
                                    stock_balance = all_td[17].text.replace('₽', '').strip().replace('\n', '').strip()

                                    print(item_offer_id, f"Цена после скидки - {item_price}",
                                          f"Цена ДО скидки - {item_price_before}", f"Себестоимсоть - {cost_price}",
                                          f"Коммисия FBO - {commission_fbo}", f"Остатки товара - {stock_balance}",
                                          sep='\n', end='\n\n')
                                    if item_offer_id not in prices_dict.keys():
                                        prices_dict[item_offer_id] = {'price': item_price,
                                                                      'price_before': item_price_before,
                                                                      'cost_price': cost_price,
                                                                      'commission_fbo': commission_fbo,
                                                                      'stock_balance': stock_balance,
                                                                      }
                                    break
                                except:
                                    print(traceback.format_exc())
                                    time.sleep(1)
                else:
                    actual_table = driver.find_element(By.XPATH, "//tbody")
                    all_tr = actual_table.find_elements(By.XPATH, ".//tr")
                    for row in all_tr:
                        for _ in range(3):
                            try:
                                all_td = row.find_elements(By.XPATH, ".//td")
                                self.scroll_to_element_center(all_td[1])
                                all_td = row.find_elements(By.XPATH, ".//td")
                                time.sleep(2.5)
                                item_name = all_td[2].text
                                if 'Название и артикул' in item_name:
                                    continue

                                print(item_name.split('\n'))
                                item_offer_id = item_name.split('\n')[1].replace('₽', '').strip().replace('\n',
                                                                                                          '').strip()
                                print(all_td[5].text.split('\n'))
                                item_price = all_td[5].text
                                print(f'Item price - {item_price}')
                                if '\n' in item_price:
                                    item_price = item_price.split('\n')[1].replace('₽', '').strip().replace('\n',
                                                                                                            '').strip()
                                else:
                                    item_price = item_price.replace('₽', '').strip().replace('\n', '').strip()

                                if item_price == '':
                                    time.sleep(3)
                                    all_td = row.find_elements(By.XPATH, ".//td")
                                    self.scroll_to_element_center(all_td[5])
                                    time.sleep(3)
                                    item_price = all_td[5].text
                                    print(f'Item price - {item_price}')
                                    if '\n' in item_price:
                                        item_price = item_price.split('\n')[1].replace('₽', '').strip().replace(
                                            '\n', '').strip()
                                    else:
                                        item_price = item_price.replace('₽', '').strip().replace('\n', '').strip()

                                item_price_before = all_td[4].text
                                if '\n' in item_price_before:
                                    item_price_before = item_price_before.split('\n')[0].replace('₽',
                                                                                                 '').strip().replace(
                                        '\n', '').strip()
                                else:
                                    item_price_before = item_price_before.replace('₽', '').strip().replace('\n',
                                                                                                           '').strip()

                                self.scroll_to_element_center(all_td[19])
                                time.sleep(2)
                                cost_price = all_td[19].text
                                if '\n' in cost_price:
                                    cost_price = cost_price.split('\n')[0]
                                cost_price = cost_price.replace('₽', '').strip().replace('\n', '').strip()

                                commission_fbo = all_td[20].text.replace('₽', '').strip().replace('\n', '').strip()
                                stock_balance = all_td[17].text.replace('₽', '').strip().replace('\n', '').strip()

                                print(item_offer_id, f"Цена после скидки - {item_price}",
                                      f"Цена ДО скидки - {item_price_before}", f"Себестоимсоть - {cost_price}",
                                      f"Коммисия FBO - {commission_fbo}", f"Остатки товара - {stock_balance}",
                                      sep='\n', end='\n\n')
                                if item_offer_id not in prices_dict.keys():
                                    prices_dict[item_offer_id] = {'price': item_price,
                                                                  'price_before': item_price_before,
                                                                  'cost_price': cost_price,
                                                                  'commission_fbo': commission_fbo,
                                                                  'stock_balance': stock_balance,
                                                                  }
                                break
                            except:
                                print(traceback.format_exc())
                                time.sleep(1)
                return prices_dict
            except:
                print(traceback.format_exc())
                continue

    def get_analytic_money_spent(self):
        logger.info(f"Init func - {inspect.currentframe()}")
        driver = self.driver
        time.sleep(2)

        def open_menu_product():
            logger.info(f"Init func - {inspect.currentframe()}")
            menu_with_items_btn = driver.find_element(By.XPATH,
                                                      "//span[text()='По категории, товару или кампании' or text()='По товарам: 1']")
            self.scroll_to_element_center(menu_with_items_btn)
            time.sleep(0.5)
            menu_with_items_btn.click()

            time.sleep(1)

            WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.XPATH, "//div[text()='Товары']")))

            tippy_content = driver.find_element(By.XPATH, "//div[@class='tippy-content']")
            menu_next_step_btn = tippy_content.find_element(By.XPATH, ".//div[text()='Товары']")
            menu_next_step_btn.click()

            time.sleep(2)

        def clear_old_data():
            logger.info(f"Init func - {inspect.currentframe()}")
            try:
                open_menu_product()
                WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Сбросить']")))
                btn_reset = driver.find_element(By.XPATH, "//button[text()='Сбросить']")
                btn_reset.click()
            except:
                print(traceback.format_exc())

        for attempt in range(3):
            money_spent_dict = {}
            try:
                driver.get('https://seller.ozon.ru/app/advertisement/product/overview')
                self.check_auth_in_ozon()

                WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH, "//button[(@type='button')]")))
                time.sleep(3)

                all_btns = driver.find_elements(By.XPATH, "//button[(@type='button')]")
                btn_calendar = all_btns[2]
                btn_calendar.click()
                logger.info("   ✅ Успешно нажали на кнопку календаря")

                WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[@type='button' and text()='Сегодня']")))
                time.sleep(0.5)
                button = driver.find_element(By.XPATH, "//button[@type='button' and text()='Сегодня']")
                button.click()
                logger.info(f"   ✅ Успешно нажали на дату Сегодня")
                time.sleep(1)

                WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[@title='Товар']")))

                btn_product = driver.find_element(By.XPATH, "//button[@title='Товар']")
                btn_product.click()
                logger.info("   ✅ Успешно нажали на кнопку Товар")
                time.sleep(1)

                btn_only_choiced = driver.find_element(By.XPATH, "//span[text()='Только выбранные']")
                self.scroll_to_element_center(btn_only_choiced)
                time.sleep(0.5)
                btn_only_choiced.click()
                logger.info("   ✅ Успешно нажали на кнопку Только выбранные")
                time.sleep(1)

                open_menu_product()

                time.sleep(0.5)
                try:
                    wrapper = driver.find_elements(By.XPATH, "//ul")[-1]
                    li_elements = wrapper.find_elements(By.XPATH, ".//li")
                    count_pages = len(li_elements)
                except:
                    count_pages = 1

                for page in range(1, count_pages + 1):
                    if count_pages != 1:
                        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//ul")))
                        wrapper = driver.find_elements(By.XPATH, "//ul")[-1]
                        li_elements = wrapper.find_elements(By.XPATH, ".//li")
                        self.scroll_to_element_center(li_elements[page - 1])
                        time.sleep(1)
                        li_elements[page - 1].click()
                        time.sleep(2)

                    table_div = driver.find_element(By.XPATH, "//div[starts-with(@class, '_laputaContainer')]")
                    table = table_div.find_element(By.XPATH, ".//tbody")
                    all_tr = table.find_elements(By.XPATH, ".//tr")
                    print(f"Всего товаров - {len(all_tr)}, страница - {page}")
                    for num_product in range(len(all_tr)):
                        try:
                            time.sleep(0.5)
                            if count_pages != 1:
                                WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//ul")))
                                wrapper = driver.find_elements(By.XPATH, "//ul")[-1]
                                li_elements = wrapper.find_elements(By.XPATH, ".//li")
                                self.scroll_to_element_center(li_elements[page - 1])
                                time.sleep(1)
                                li_elements[page - 1].click()
                                time.sleep(2)

                            table_div = driver.find_element(By.XPATH, "//div[starts-with(@class, '_laputaContainer')]")
                            table = table_div.find_element(By.XPATH, ".//tbody")
                            all_tr = table.find_elements(By.XPATH, ".//tr")

                            product_tr = all_tr[num_product]

                            item_offer_id = product_tr.find_elements(By.XPATH, ".//td")[1].text.split("\n")[1].strip()

                            product_input = product_tr.find_element(By.XPATH, ".//input[@type='checkbox']")
                            self.scroll_to_element_center(product_input)
                            time.sleep(0.5)
                            product_input.click()

                            time.sleep(1)

                            btn_submit = driver.find_element(By.XPATH, "//span[text()='Выбрать']")
                            btn_submit.click()
                            time.sleep(2)

                            money_spent_index = 3
                            drr_index = 6

                            headers = driver.find_element(By.XPATH, ".//thead")
                            self.scroll_to_element_center(headers)
                            time.sleep(1)
                            all_th = headers.find_elements(By.XPATH, ".//th")
                            for th in all_th:
                                if 'Расход' == th.text:
                                    money_spent_index = all_th.index(th)
                                    logger.info(f"Найден INDEX Расхода - {money_spent_index}")
                                elif 'ДРР' == th.text:
                                    drr_index = all_th.index(th)
                                    logger.info(f"Найден INDEX ДРР - {drr_index}")

                            time.sleep(1)
                            data = driver.find_element(By.XPATH, "//tbody").find_element(By.XPATH, ".//tr")
                            all_td = data.find_elements(By.XPATH, ".//td")
                            money_spent = all_td[money_spent_index].text.replace('%', '').replace(',', '.').replace('₽','').strip()
                            drr = all_td[drr_index].text.replace('%', '').replace(',', '.').replace('₽', '').strip()

                            if '\n' in money_spent:
                                money_spent = money_spent.split('\n')[0]

                            if '\n' in drr:
                                drr = drr.split('\n')[0]

                            logger.info(f"Get data - {item_offer_id} Расход = {money_spent}")
                            print(f"Get data - {item_offer_id} Расход = {money_spent}")
                            money_spent_dict[item_offer_id] = {'money_spent': money_spent, 'drr': drr}

                            clear_old_data()
                        except Exception as e:
                            print(traceback.format_exc())

                try:
                    with open('money_spent_advert_dict.json', 'w', encoding='utf-8') as f:
                        json.dump(money_spent_dict, f, ensure_ascii=False, indent=4)
                except Exception as e:
                    pass
                return money_spent_dict
            except:
                logger.error(f"Ошибка при сборе DRR: {traceback.format_exc()}")

        return {}


    # Получение объема товара
    def get_volume_product(self):
        logger.info(f"Init func - {inspect.currentframe()}")
        driver = self.driver
        volume_dict = {}
        for attempt in range(3):
            try:
                driver.get('https://seller.ozon.ru/app/products?filter=in_sale')

                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//thead")))
                self.random_sleep(1)

                offer_id_index = False
                volume_index = False
                headers = driver.find_element(By.XPATH, ".//thead").find_element(By.XPATH, './/tr').find_elements(By.XPATH, './/th')
                for th in headers:
                    if 'Артикул' == th.text.strip():
                        offer_id_index = headers.index(th)
                    elif 'Объем товара, л' in th.text:
                        volume_index = headers.index(th)

                logger.info(f'Offer id index = {offer_id_index}\nVolume index = {volume_index}\n\n')

                pagination = driver.find_element(By.XPATH, "//div[@id='pagination']")
                pages = pagination.find_element(By.XPATH, ".//ul").find_elements(By.XPATH, './/li')

                for page in pages:
                    page_btn = page.find_element(By.XPATH, ".//button")
                    self.scroll_to_element_center(page_btn)
                    self.random_sleep(2)
                    page_btn.click()

                    self.random_sleep(2)
                    all_items = driver.find_element(By.XPATH, ".//tbody").find_elements(By.XPATH, './/tr')
                    self.scroll_to_element_center(all_items[-1])
                    self.random_sleep(2)

                    for item in all_items:
                        try:
                            item_tds = item.find_elements(By.XPATH, ".//td")
                            if offer_id_index:
                                offer_id = item_tds[offer_id_index].text.strip()
                            else:
                                offer_id = item_tds[3].text.strip()

                            if '\n' in offer_id:
                                offer_id = offer_id.split('\n')[0].strip()

                            if volume_index:
                                item_volume = item_tds[volume_index].text.strip()
                            else:
                                item_volume = item_tds[14].text.strip()

                            item_volume_l = item_volume.split('\n')[0].strip()
                            item_volume_kg = item_volume.split('\n')[1].strip()

                            print(offer_id, item_volume_l, item_volume_kg, sep='\n', end='\n\n')
                            volume_dict[offer_id] = {'item_volume_l': item_volume_l, 'item_volume_kg': item_volume_kg}
                        except:
                            print(traceback.format_exc())
                            pass

                return volume_dict
            except:
                logger.error(f"Ошибка при получении объема товара: {traceback.format_exc()}")
                self.random_sleep(1)
                continue

    def get_all_advert_analytic(self, max_retries: int = 3):
        logger.info(f"Init func - {inspect.currentframe()}")
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

                try:
                    volume_dict = self.get_volume_product()
                    for offer_id in volume_dict:
                        item_volume_l = volume_dict[offer_id]['item_volume_l']
                        item_volume_kg = volume_dict[offer_id]['item_volume_kg']
                        if offer_id in res_dict:
                            for item_dict in res_dict[offer_id]:
                                item_dict['item_volume_l'] = item_volume_l
                                item_dict['item_volume_kg'] = item_volume_kg

                except Exception as e:
                    logger.warning(f"Ошибка сопоставления Объема товара - {str(e)}")
                    pass

                try:
                    price_dict = self.get_actual_prices_offer_id()
                    # Исправление: обрабатываем список товаров для каждого offer_id
                    for offer_id in price_dict:
                        actual_price = price_dict[offer_id]['price']
                        price_before = price_dict[offer_id]['price_before']
                        cost_price = price_dict[offer_id]['cost_price']
                        commission_fbo = price_dict[offer_id]['commission_fbo']
                        stock_balance = price_dict[offer_id]['stock_balance']

                        if offer_id in res_dict:
                            for item_dict in res_dict[offer_id]:
                                item_dict['product_price'] = actual_price
                                item_dict['product_price_before'] = price_before
                                item_dict['cost_price'] = cost_price
                                item_dict['commission_fbo'] = commission_fbo
                                item_dict['stock_balance'] = stock_balance
                        else:
                            logger.warning(f"   ⚠️ offer_id {offer_id} не найден в рекламной аналитике")

                    logger.info(f"   ✅ Актуальные цены добавлены для {len(price_dict)} товаров")
                    return res_dict

                except Exception as e:
                    logger.error(f'   ❌ Ошибка сопоставления актуальных цен - {str(e)}')
                    import traceback
                    logger.error(traceback.format_exc())
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
    #parser.get_analytic_money_spent()
    #input('test')

    #parser.auth()
    time.sleep(2)
    parser.close()