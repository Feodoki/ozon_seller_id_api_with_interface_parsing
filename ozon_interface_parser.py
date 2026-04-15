import json
import os
import time
import traceback
import random
import undetected_chromedriver as uc
from selenium.common import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime
from data_to_google_sheets import write_error_to_sheet


class InterfaceParser:
    def __init__(self, profile_name: str = "chrome_profile"):
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

    def start_browser(self, headless: bool = False):
        options = self._get_options()

        if headless:
            options.add_argument("--headless=new")

        self.driver = uc.Chrome(
            options=options,
            use_subprocess=True,
            user_data_dir=self.profile_path,
            driver_executable_path="chromedriver.exe",
        )

        print("Браузер запущен")
        time.sleep(2)

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


    def get_advert_analytic_pay_to_click(self):
        driver = self.driver
        analytic_advert_dict = {}

        for _ in range(3):
            driver.get('https://seller.ozon.ru/app/advertisement/product/cpc')
            result_date = self.get_ozon_date_today()

            try:
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
                        print(f"Успешно нажали на дату Сегодня")
                        try:
                            btn_status = all_buttons_with_type[all_buttons_with_type.index(button_calendar)+1]
                        except:
                            print(traceback.format_exc())
                        break
                    except:
                        continue

                self.random_sleep()
                if 'Статус: Активна' in btn_status.text:
                    pass
                else:
                    if btn_status:
                        btn_status.click()
                    self.random_sleep()
                    tippy_content = driver.find_element(By.XPATH, "//div[(@class='tippy-content')]")
                    button = tippy_content.find_element(By.XPATH, ".//div[text()='Активна']")
                    button.click()

                all_pages_with_active_status = driver.find_element(By.XPATH, "//div[starts-with(@class,'_wrapper_lftsu')]")
                self.scroll_to_element_center(all_pages_with_active_status)
                self.random_sleep()
                all_pages_with_active_status = all_pages_with_active_status.find_element(By.XPATH, ".//ul").find_elements(By.XPATH, ".//li")
                self.random_sleep()
                print(f"Всего страниц - {len(all_pages_with_active_status)}")

                for page in all_pages_with_active_status:
                    for _ in range(3):
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
                                analytic_advert_dict[camping_id] = {"camping_url": camping_url, "camping_type": camping_type,"camping_strategy": camping_strategy, "camping_budget": camping_budget}
                            break
                        except:
                            print(traceback.format_exc())

                res = self.parser_advert_dict(analytic_advert_dict)
                return res
            except:
                print(traceback.format_exc())

    def get_ozon_date_today(self):
        months = [
            "янв", "фев", "мар", "апр", "май", "июн",
            "июл", "авг", "сен", "окт", "ноя", "дек"
        ]
        now = datetime.now()
        result_date = f"{now.day} {months[now.month - 1]}"
        return result_date


    def parser_advert_dict(self, advert_dict):
        driver = self.driver
        result_advert_dict = {}

        result_date = self.get_ozon_date_today()

        count_items = len(advert_dict.keys())
        print(count_items)
        count = 0
        for camping_id in advert_dict:
            count += 1
            print(f"{count}/{count_items}")
            camping_json = advert_dict[camping_id]
            camping_url = camping_json["camping_url"]
            camping_type = camping_json["camping_type"]
            camping_strategy = camping_json["camping_strategy"]
            camping_budget = camping_json["camping_budget"]
            print(f"{camping_url}, {camping_id}, {camping_strategy}", sep='\n', end='\n')

            driver.get(camping_url)
            for _ in range(3):
                try:
                    self.random_sleep(2)

                    all_buttons_with_type = driver.find_elements(By.XPATH, ".//button[starts-with(@type, 'button')]")
                    for button_calendar in all_buttons_with_type:
                        btn_text = button_calendar.text
                        if 'Стать Premium Pro' in btn_text or 'Кампании' in btn_text or 'Архив' in btn_text:
                            continue
                        if result_date == btn_text:
                            print(f"Дата {result_date} сегодняшняя")
                            break
                        button_calendar.click()
                        time.sleep(1.5)
                        try:
                            button = driver.find_element(By.XPATH, "//button[@type='button' and text()='Сегодня']")
                            button.click()
                            print(f"Успешно нажали на дату Сегодня")
                            self.random_sleep(1)
                            break
                        except:
                            continue

                    table = None
                    attempt = 0
                    while attempt < 20:
                        table = driver.find_element(By.XPATH, "//tbody")
                        if len(table.text) == 0:
                            time.sleep(0.5)
                            attempt += 1
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
                            my_bet = row.find_element(By.XPATH, f".//input[(@data-testid='InputCount')]").get_attribute('value')
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
                        print(offer_id, sku, concurent_bet, my_bet, sr_click, to_cart, drr, ctp, views, clicks, product_price, sep='\n', end='\n\n')
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
                except:
                    print(traceback.format_exc())
                    time.sleep(1)
                    try:
                        span = driver.find_element(By.XPATH, "//span[text()='обновить']")
                        span.click()
                    except:
                        time.sleep(10)
                        driver.refresh()
        return result_advert_dict


    def get_advert_analytics_pay_to_buy(self, analytic_advert_dict):
        analytic_advert_dict = analytic_advert_dict.copy()
        driver = self.driver

        result_date = self.get_ozon_date_today()

        for _ in range(3):
            try:
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
                        print(f"Дата {result_date} сегодняшняя")
                        break
                    button_calendar.click()
                    time.sleep(1.5)
                    try:
                        button = driver.find_element(By.XPATH, "//button[@type='button' and text()='Сегодня']")
                        button.click()
                        print(f"Успешно нажали на дату Сегодня")
                        self.random_sleep()
                        break
                    except:
                        continue

                all_pages_with_active_status = driver.find_element(By.XPATH, "//div[starts-with(@class,'_wrapper_lftsu')]")
                self.scroll_to_element_center(all_pages_with_active_status)
                self.random_sleep()
                all_pages_with_active_status = all_pages_with_active_status.find_element(By.XPATH, ".//ul").find_elements(
                    By.XPATH, ".//li")
                self.random_sleep()
                print(f"Всего страниц - {len(all_pages_with_active_status)}")

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
                                bet_percent = bet_data[0].replace('\n', '').strip().replace('%', '').strip().replace('\u202f', '')
                                bet_amount = bet_data[1].replace('\n', '').strip().replace('₽', '').strip().replace('\u202f', '')
                            else:
                                bet_percent = bet_data[0].replace('\n', '').strip().replace('%', '').strip().replace('\u202f', '')
                                bet_amount = bet_data[2].replace('\n', '').strip().replace('₽', '').strip().replace('\u202f', '')

                            product_price = all_td[7].text.replace('\n', '').strip().replace(' ₽', '').strip().replace('₽', '').strip().replace('\u202f', '')
                            index_view = all_td[8].text.replace('\n', '').strip().replace('\u202f', '')

                            product_buy_pay = all_td[13].text.replace('\n', '').strip().replace('\u202f', '')
                            product_buy_combo_model = all_td[14].text.replace('\n', '').strip().replace('\u202f', '')

                            drr = all_td[15].text.replace('\n', '').strip().replace('\u202f', '')

                            print(sku, offer_id, bet_percent, bet_amount, product_price, index_view, product_buy_pay, product_buy_combo_model, drr, sep='\n', end='\n\n')

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
                        except:
                            print(traceback.format_exc())

                return analytic_advert_dict
            except:
                print(traceback.format_exc())

    def get_position_product_from_sku(self):
        driver = self.driver
        position_dict = {}
        if self.check_auth_in_ozon():
            pass
        else:
            return {}

        for _ in range(3):
            try:
                driver.get('https://seller.ozon.ru/app/analytics-search/search-results/validator')
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f"//div[(@class='analyticsSearchApp')]")))

                time.sleep(random.uniform(self.random_sleep_from, self.random_sleep_to))


                all_templates = driver.find_element(By.XPATH, f"//div[(@class='analyticsSearchApp')]").find_element(By.XPATH, f"./div/div/div[1]/div[2]/div/div[1]").find_elements(By.XPATH, './div')

                old_table = None
                for template in all_templates:
                    for _ in range(3):
                        try:
                            self.scroll_to_element_center(template)
                            time.sleep(random.uniform(0.5, 1))
                            template.click()
                            self.random_scroll()

                            while True:
                                self.random_sleep(2)
                                WebDriverWait(driver, 5).until(
                                    EC.element_to_be_clickable((By.XPATH, f"//tbody")))
                                table = driver.find_element(By.XPATH, f"//tbody")
                                if old_table != table:
                                    all_items = table.find_elements(By.XPATH, f".//tr")
                                    for item in all_items:
                                        item_sku = item.find_elements(By.XPATH, f".//td")[1].text.split('SKU')[1].split('\n')[0].strip()
                                        item_position = item.find_elements(By.XPATH, f".//td")[2].find_element(By.XPATH, './/div').text
                                        print(item_sku, item_position, sep='\n', end='\n\n')
                                        try:
                                            item_sku = int(item_sku)
                                            item_sku = str(item_sku)
                                            position_dict[item_sku] = str(item_position)
                                        except:
                                            print(traceback.format_exc())

                                    self.random_sleep(1)
                                    break
                        except:
                            print(traceback.format_exc())

                return position_dict
            except:
                print(traceback.format_exc())


    def get_all_advert_analytic(self):
        if self.check_auth_in_ozon():
            res_dict = self.get_advert_analytic_pay_to_click()
            res_dict = self.get_advert_analytics_pay_to_buy(res_dict)
            return res_dict
        else:
            return {}

if __name__ == "__main__":
    parser = InterfaceParser()

    parser.start_browser(headless=False)
    parser.auth()
    time.sleep(2)
    parser.close()
