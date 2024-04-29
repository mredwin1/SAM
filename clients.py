import gspread
import json
import logging
import random
import requests
import time
import traceback

from appium import webdriver
from appium.options.android import UiAutomator2Options
from appium.webdriver.common.appiumby import AppiumBy
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from oauth2client.service_account import ServiceAccountCredentials


class AppiumClient:
    def __init__(
        self,
        uuid: str,
        capabilities: dict,
        logger: logging.Logger,
        appium_url: str,
        system_port: int = None,
        mjpeg_server_port: int = None
    ):
        self.driver = None
        self.logger = logger
        self.files_sent = []

        capabilities["udid"] = uuid
        capabilities["adbExecTimeout"] = 50000

        if system_port:
            capabilities["systemPort"] = system_port

        if mjpeg_server_port:
            capabilities["mjpegServerPort"] = mjpeg_server_port

        self.appium_url = appium_url
        self.capabilities = capabilities
        self.capabilities_options = UiAutomator2Options().load_capabilities(
            capabilities
        )

    def __enter__(self):
        self.open()

        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def open(self):
        """Used to open the appium web driver session"""
        self.driver = webdriver.Remote(
            self.appium_url, options=self.capabilities_options
        )

    def close(self):
        """Closes the appium driver session"""
        if self.driver:
            self.driver.quit()
            self.logger.debug("Driver was quit")

    def locate(self, by, locator, location_type=None):
        """Locates the first elements with the given By"""
        wait = WebDriverWait(self.driver, 10)
        if location_type:
            if location_type == "visibility":
                return wait.until(
                    expected_conditions.visibility_of_element_located((by, locator))
                )
            elif location_type == "clickable":
                return wait.until(
                    expected_conditions.element_to_be_clickable((by, locator))
                )
            else:
                return None
        else:
            return wait.until(
                expected_conditions.presence_of_element_located((by, locator))
            )

    def locate_all(self, by, locator, location_type=None):
        """Locates all web elements with the given By and returns a list of them"""
        wait = WebDriverWait(self.driver, 5)
        if location_type:
            if location_type == "visibility":
                return wait.until(
                    expected_conditions.visibility_of_all_elements_located(
                        (by, locator)
                    )
                )
            else:
                return None
        else:
            return wait.until(
                expected_conditions.presence_of_all_elements_located((by, locator))
            )

    def is_present(self, by, locator):
        """Checks if a web element is present"""
        try:
            wait = WebDriverWait(self.driver, 1)
            wait.until(expected_conditions.presence_of_element_located((by, locator)))
        except (NoSuchElementException, TimeoutException):
            return False
        return True

    def sleep(self, lower, upper=None):
        """Will simply sleep and log the amount that is sleeping for, can also be randomized amount of time if given the
        upper value"""
        seconds = random.randint(lower, upper) if upper else lower

        if seconds:
            if seconds > 60:
                duration = seconds / 60
                word = "minutes"
            else:
                duration = seconds
                word = "second" if seconds == 1 else "seconds"

            self.logger.debug(f"Sleeping for {round(duration, 2)} {word}")
            time.sleep(seconds)

    def click(self, element=None, x=None, y=None):
        if element:
            width = element.size["width"]
            height = element.size["height"]
            center_x = width / 2
            center_y = height / 2

            xoffset = int(center_x) - random.randint(
                int(center_x * 0.2), int(center_x * 0.8)
            )
            yoffset = int(center_y) - random.randint(
                int(center_y * 0.2), int(center_y * 0.8)
            )
            action = (
                ActionChains(self.driver)
                .move_to_element_with_offset(element, xoffset, yoffset)
                .click()
            )
            action.perform()
        else:
            self.driver.execute_script("mobile: clickGesture", {"x": x, "y": y})

    def long_click(self, element, duration=1000):
        width = element.size["width"]
        height = element.size["height"]

        xoffset = random.randint(int(width * 0.2), int(width * 0.8))
        yoffset = random.randint(int(height * 0.2), int(height * 0.8))

        self.driver.execute_script(
            "mobile: longClickGesture",
            {"x": xoffset, "y": yoffset, "elementId": element.id, "duration": duration},
        )

    def swipe(self, direction, scroll_amount, speed=1200):
        window_size = self.driver.get_window_size()
        if direction in ("up", "down"):
            bounding_box_xpercent_min = 0.5
            bounding_box_xpercent_max = 0.9
            bounding_box_ypercent_min = 0.2
            bounding_box_ypercent_max = 0.7
        else:
            bounding_box_xpercent_min = 0.1
            bounding_box_xpercent_max = 0.9
            bounding_box_ypercent_min = 0.7
            bounding_box_ypercent_max = 0.8

        scroll_x = bounding_box_xpercent_min * window_size["width"]
        scroll_y = bounding_box_ypercent_min * window_size["height"]
        scroll_width = (window_size["width"] * bounding_box_xpercent_max) - (
            window_size["width"] * bounding_box_xpercent_min
        )
        scroll_height = (window_size["height"] * bounding_box_ypercent_max) - (
            window_size["height"] * bounding_box_ypercent_min
        )

        if direction in ("up", "down"):
            full_scrolls = int(scroll_amount / scroll_height)
            last_scroll = round(scroll_amount / scroll_height % 1, 2)
        else:
            full_scrolls = int(scroll_amount / scroll_width)
            last_scroll = round(scroll_amount / scroll_width % 1, 2)
        scroll_percents = [1.0] * full_scrolls + [last_scroll]

        total_scroll = 0
        for index, scroll_percent in enumerate(scroll_percents):
            total_scroll += scroll_height * scroll_percent
            self.driver.execute_script(
                "mobile: swipeGesture",
                {
                    "left": scroll_x,
                    "top": scroll_y,
                    "width": scroll_width,
                    "height": scroll_height,
                    "direction": direction,
                    "percent": scroll_percent,
                    "speed": speed,
                },
            )
            if index + 1 != len(scroll_percents):
                self.sleep(0.5)

    def send_keys(self, element, text):
        char = None
        self.click(element)
        for index, line in enumerate(text.split("\n")):
            action = ActionChains(self.driver)

            if index != 0:
                self.driver.press_keycode(66)

            words = line.split(" ")
            for inner_index, word in enumerate(words):
                word = word.strip()
                try:
                    for char in word:
                        action.send_keys(char).pause(random.uniform(0.1, 0.2))

                    if len(words) > 1 and inner_index != len(words) - 1:
                        action.send_keys(" ")

                    action.perform()
                except Exception:
                    self.logger.warning(
                        f'Could not parse the following character "{char}" in the word "{word}"'
                    )
                    self.logger.warning(traceback.format_exc())

        self.driver.back()

    def launch_app(self, app_name, retries=0):
        self.logger.info(f"Launching app {app_name}. Attempt # {retries + 1}")

        self.logger.info("Going to home screen")

        self.driver.press_keycode(3)

        self.sleep(1)

        self.swipe("up", 1000)

        self.logger.info("Searching for app")

        search = self.locate(
            AppiumBy.ID, "com.google.android.apps.nexuslauncher:id/input"
        )
        _index = app_name.find("_")

        if _index != -1:
            search.send_keys(app_name[:_index])
        else:
            search.send_keys(app_name)

        if not self.is_present(AppiumBy.ACCESSIBILITY_ID, app_name):
            return False

        app = self.locate(AppiumBy.ACCESSIBILITY_ID, app_name)
        app.click()

        self.logger.info("App launched")

        self.sleep(2)

        return True


class HushedClient(AppiumClient):
    def __init__(self, uuid: str, logger: logging.Logger, appium_url: str):
        capabilities = dict(
            platformName="Android",
            automationName="uiautomator2",
            appPackage="com.hushed.release",
            appActivity="com.hushed.base.landing.SplashActivity",
            language="en",
            locale="US",
            noReset=True,
            forceAppLaunch=True,
        )
        super(HushedClient, self).__init__(uuid, capabilities, logger)

    def go_to_messages_screen(self, number: str):
        number_xpath = f'//android.widget.TextView[@resource-id="com.hushed.release:id/drawer_number_subtitle" and @text="{number}"]'
        if self.is_present(AppiumBy.XPATH, number_xpath):
            number_btn = self.locate(AppiumBy.XPATH, number_xpath)
            self.click(number_btn)
            return

        on_message_screen = self.is_present(AppiumBy.XPATH, "//android.widget.LinearLayout[@resource-id='com.hushed.release:id/collapsibleHeader']//android.widget.TextView[@text='Messages']")
        number_selected = self.is_present(AppiumBy.XPATH, f'//android.widget.TextView[@resource-id="com.hushed.release:id/tvTitle" and @text="{number}"]')
        if not (on_message_screen and number_selected):
            back_btn_id = "com.hushed.release:id/headerButtonLeft"
            tries = 0
            while self.is_present(AppiumBy.ID, back_btn_id) and tries < 3:
                back_btn = self.locate(AppiumBy.ID, back_btn_id)
                self.click(back_btn)
                tries += 1

            if not self.is_present(AppiumBy.XPATH, number_xpath):
                hamburger_menu = self.locate(AppiumBy.ID, "com.hushed.release:id/btnHamburger")
                self.click(hamburger_menu)

                number_btn = self.locate(AppiumBy.XPATH, number_xpath)
                self.click(number_btn)

    def send_sms(self, sender_number: str, recipient: str, message: str):
        self.go_to_messages_screen(sender_number)

        new_message_btn = self.locate(AppiumBy.ID, 'com.hushed.release:id/btnMessageCompose')
        self.click(new_message_btn)

        got_it_id = "com.hushed.release:id/btnAgree"
        if self.is_present(AppiumBy.ID, got_it_id):
            got_it_btn = self.locate(AppiumBy.ID, got_it_id)
            self.click(got_it_btn)

            deny_button_id = "com.android.packageinstaller:id/permission_deny_button"
            if self.is_present(AppiumBy.ID, deny_button_id):
                deny_btn = self.locate(AppiumBy.ID, deny_button_id)
                self.click(deny_btn)

        number_search = self.locate(AppiumBy.ID, "com.hushed.release:id/etSearch")
        self.send_keys(number_search, recipient)

        no_match_tile = self.locate(AppiumBy.ID, "com.hushed.release:id/noMatchNumber")
        self.click(no_match_tile)

        message_input = self.locate(AppiumBy.ID, "com.hushed.release:id/message_content")
        self.send_keys(message_input, message)

        send_btn = self.locate(AppiumBy.ID, "com.hushed.release:id/btnSend")
        self.click(send_btn)


class GoogleSheetClient:
    def __init__(self, credentials_file, spreadsheet_name):
        self.credentials_file = credentials_file
        self.spreadsheet_name = spreadsheet_name
        self.client = None
        self.sheet = None

        # Define the scope of the application
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

        # Load credentials from the service account JSON file
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_file, scope)

        # Authenticate and create a client
        self.client = gspread.authorize(credentials)

    def open_sheet(self, sheet_name):
        self.sheet = self.client.open(self.spreadsheet_name).worksheet(sheet_name)

    def read_records(self):
        # Read all records from the sheet
        if self.sheet is not None:
            return self.sheet.get_all_records()
        else:
            raise Exception("Sheet not opened. Please call open_sheet() method first.")

    def get_column_index(self, column_name):
        """
        Retrieve the column index for a given column name based on the first row.

        :param column_name: The name of the column to find.
        :return: The 1-based index of the column.
        """
        row_values = self.sheet.row_values(1)  # assuming the first row contains headers
        return row_values.index(column_name) + 1

    def get_last_row(self):
        return len(self.sheet.get_all_records()) + 2

    def update_cell(self, row_number, column_index, value):
        self.sheet.update_cell(row_number, column_index, value)


class BatchDataClient:
    BASE_URL = "https://api.batchdata.com/api/v1"

    def __init__(self, logger, config_file_name: str = "config.json"):
        self.config_file_name = config_file_name
        self.logger = logger

    def __enter__(self):
        # Load the config file on context entry
        with open(self.config_file_name, 'rb') as config_file:
            self.config = json.load(config_file)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Handle resource cleanup or specific operations when context is exited
        # This could be a good place to handle errors, if needed
        if exc_type:
            self.logger.error(f"An error occurred: {exc_value}")

    def _get_headers(self):
        return {
            "Authorization": f"Bearer {self.config['batch_data_api_key']}"
        }

    def skip_trace(self, first_name: str, last_name: str, city: str, street: str, state: str, zipcode: str, num_phones: int = 3):
        headers = self._get_headers()
        body = {
            "requests": [
                {
                    "name": {
                        "first": first_name,
                        "last": last_name
                    },
                    "propertyAddress": {
                        "city": city,
                        "street": street,
                        "state": state,
                        "zip": zipcode
                    }
                }
            ]
        }

        phone_numbers = []

        try:
            response = requests.post(f"{self.BASE_URL}/property/skip-trace", json=body, headers=headers).json()
            print(response)
            people = response.get("results", {}).get("persons", [])

            for person in people:
                if not person.get("death", {}).get("deceased", False):
                    phones_list = person.get("phoneNumbers", [])
                    mobile_numbers = [num["number"] for num in phones_list if num["type"] == "Mobile"]
                    phone_numbers.extend(mobile_numbers[:num_phones])

        except (KeyError, IndexError) as e:
            self.logger.error(e, exc_info=True)

        return phone_numbers
