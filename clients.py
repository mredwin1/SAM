import asyncio
import gspread
import json
import logging
import os
import pyppeteer
import random
import re
import requests
import time
import traceback
import usaddress

from appium import webdriver
from appium.options.android import UiAutomator2Options
from appium.webdriver.common.appiumby import AppiumBy
from datetime import datetime, timedelta
from pyppeteer.browser import Browser
from pyppeteer.page import Page
from pyppeteer.element_handle import ElementHandle
from pyppeteer.us_keyboard_layout import keyDefinitions
from pyppeteer.errors import TimeoutError
from python_ghost_cursor.pyppeteer import create_cursor
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from oauth2client.service_account import ServiceAccountCredentials

from typing import Union, List, Dict, Tuple


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


class BasePuppeteerClient:
    def __init__(self, executable_path: str, logger: logging.Logger, width: int = 800, height: int = 600):
        """
        Initializes the BasePuppeteerClient.

        :param width: The width of the viewport for the Puppeteer browser.
        :param height: The height of the viewport for the Puppeteer browser.
        :param logger: Optional logger for logging messages.
        """
        self.width = width
        self.height = height
        self.logger = logger
        self.browser: Union[Browser, None] = None
        self.page: Union[Page, None] = None
        self.cursor = None
        self.recovery_attempted = False
        self.executable_path = executable_path

    async def __aenter__(self) -> "BasePuppeteerClient":
        """Enters the context, starting the Puppeteer browser."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self.close()
        finally:
            if exc_type is not None:
                raise exc_val

    async def start(self):
        """Asynchronously starts the Puppeteer browser and sets the page."""
        self.browser = await pyppeteer.launch(
            headless=True,
            executablePath=self.executable_path,
            defaultViewport={
                "width": self.width,
                "height": self.height
            },
            ignoreHTTPSErrors=True,
        )

        # Get the list of all open pages
        pages = await self.browser.pages()

        # Choose the page you want to work with (e.g., the first page in the list)
        if pages:
            self.page = pages[0]
        else:
            # If no pages are open, create a new one
            self.page = await self.browser.newPage()

        self.cursor = create_cursor(self.page)

        self.page.setDefaultNavigationTimeout(60000)

    async def close(self):
        if self.browser:
            # Close the browser
            try:
                await self.browser.close()
            except Exception as e:
                self.logger.error(f"Error while closing browser: {e}")
        else:
            self.logger.info("Browser already disconnected.")

    @staticmethod
    def cleanse_selector(selector):
        """
        Cleanses a string to be used as a CSS selector by removing characters
        that are not valid in CSS identifiers.

        This function keeps letters (a-z, A-Z), digits (0-9), hyphens (-),
        and underscores (_), and removes all other characters. It does not handle
        cases where the selector starts with a digit or two hyphens, which are
        technically invalid in CSS.

        Parameters:
        selector (str): The string to be cleansed for use as a CSS selector.

        Returns:
        str: A cleansed string with only valid CSS identifier characters.
        """
        return re.sub(r"[^a-zA-Z0-9-_]", "", selector)

    @staticmethod
    def random_coordinates_within_box(
            x: float, y: float, width: float, height: float
    ) -> Tuple[float, float]:
        # Find the minimum and maximum x, y values within the box
        min_x, max_x = x, x + width
        min_y, max_y = y, y + height

        # Generate random x, y coordinates within the box
        random_x = random.uniform(min_x, max_x)
        random_y = random.uniform(min_y, max_y)

        return random_x, random_y

    @staticmethod
    async def sleep(lower: float, upper: float = None) -> None:
        if not upper:
            upper = lower

        total_sleep = random.uniform(lower, upper)

        await asyncio.sleep(total_sleep)

    async def save_screenshot(self, directory: str, filename: str) -> None:
        screenshots_dir = os.path.join(os.getcwd(), directory)
        screenshot_path = os.path.join(screenshots_dir, filename)
        try:
            os.makedirs(screenshots_dir, exist_ok=True)
            await self.page.screenshot({"path": screenshot_path})
            self.logger.info(f"Screenshot saved to {screenshot_path}")
        except Exception:
            self.logger.warning(f"Could not save screenshot to {screenshot_path}")

    async def is_present(self, selector: str) -> bool:
        try:
            await self.find(selector, options={"visible": True, "timeout": 2000})

            return True
        except TimeoutError:
            return False

    async def find(self, selector: str, options: Dict = None) -> ElementHandle:
        if options is None:
            options = {"visible": True, "timeout": 5000}

        if "//" in selector:
            return await self.page.waitForXPath(selector, options)
        else:
            return await self.page.waitForSelector(selector, options)

    async def find_all(self, selector: str) -> List[ElementHandle]:
        options = {"visible": True, "timeout": 5000}

        if "//" in selector:
            await self.page.waitForXPath(selector, options)
            return await self.page.xpath(selector)
        else:
            await self.page.waitForSelector(selector, options)
            return await self.page.querySelectorAll(selector)

    async def scroll_to_element(self, selector: Union[ElementHandle, str]):
        if isinstance(selector, str):
            element = await self.find(selector)
        else:
            element = selector

        # Check if the element is in the viewport
        is_in_viewport = await self.page.evaluate(
            """
            async element => {
                // Use IntersectionObserver to check if the element is fully within the viewport
                const visibleRatio = await new Promise(resolve => {
                    const observer = new IntersectionObserver(entries => {
                        resolve(entries[0].intersectionRatio);
                        observer.disconnect();
                    }, {threshold: 1.0});
                    observer.observe(element);
                });

                // Return true if the element is fully visible
                return visibleRatio === 1.0;
            }
            """,
            element,
        )

        # If the element is not in view, perform smooth scrolling
        if not is_in_viewport:
            try:
                await asyncio.wait_for(
                    self.page.evaluate(
                        """
                    element => {
                        const smoothScroll = (element) => {
                            const rect = element.getBoundingClientRect();
                            const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
                            const viewHeight = Math.max(document.documentElement.clientHeight, window.innerHeight);
                            const finalY = rect.top + scrollTop - (viewHeight / 2) + (rect.height / 2);
                            const maxScroll = document.documentElement.scrollHeight - window.innerHeight;

                            let step = Math.round(Math.random() * 20) + 5; // Initial step size
                            let interval = Math.round(Math.random() * 20) + 10; // Initial interval

                            return new Promise(resolve => {
                                const scrollInterval = setInterval(() => {
                                    // Randomly change step size and interval to simulate natural human behavior
                                    if (Math.random() < 0.1) { // 10% chance to change step and interval
                                        step = Math.round(Math.random() * 20) + 5;
                                        interval = Math.round(Math.random() * 20) + 10;
                                    }

                                    step = Math.min(step, maxScroll - window.scrollY)                                   

                                    if (Math.abs(finalY - window.pageYOffset) > step) {
                                        window.scrollBy(0, step);
                                    } else {
                                        window.scrollTo(0, finalY); // directly jump to final position if within one step
                                        clearInterval(scrollInterval);
                                        resolve();
                                    }
                                }, interval);
                            });
                        };
                        return smoothScroll(element);
                    }
                    """,
                        element,
                    ),
                    timeout=10,
                )
            except asyncio.TimeoutError:
                self.logger.warning("Timeout while scrolling")
            except Exception:
                self.logger.warning("Unhandled error while scrolling")

    async def click(
            self,
            selector: Union[ElementHandle, str],
            navigation: bool = False,
            navigation_options: Dict = None,
    ):
        if navigation and navigation_options is None:
            navigation_options = {"timeout": 30000}

        if isinstance(selector, str) and "//" in selector:
            selector = await self.find(selector)

        await self.scroll_to_element(selector)

        # Get the bounding box of the element
        if navigation:
            completed, _ = await asyncio.wait(
                [
                    self.cursor.click(
                        selector,
                        wait_for_click=random.randint(100, 200),
                        wait_for_selector=5000,
                    ),
                    self.page.waitForNavigation(navigation_options),
                ],
            )

            for task in completed:
                if task.exception():
                    if isinstance(task.exception(), TimeoutError):
                        self.logger.warning(
                            "Timed out waiting for navigation after click"
                        )
                    else:
                        self.logger.warning(
                            "Some other exception occurred while performing click"
                        )

        else:
            await self.cursor.click(
                selector,
                wait_for_click=random.randint(100, 200),
                wait_for_selector=5000,
            )

        await self.sleep(0.1)

    async def type(
            self,
            selector: Union[ElementHandle, str],
            text: str,
            wpm: int = 100,
            mistakes: bool = True,
    ) -> None:
        # Check current text before proceeding
        if isinstance(selector, str):
            element = await self.find(selector)
        else:
            element = selector
        current_text = await self.page.evaluate("(element) => element.value", element)

        await self.click(selector)
        if text == current_text:
            return
        elif current_text != "":
            await self.page.keyboard.press("End")
            await self.sleep(0.04, 0.14)
            for _ in current_text:
                await self.page.keyboard.press("Backspace")
                await self.sleep(0.04, 0.14)

        # Calculate average pause between chars
        total_duration = len(text) / (wpm * 4.5)

        # Calculate time to wait between sending each character
        avg_pause = (total_duration * 60) / len(text)
        punctuation = [".", "!", "?"]

        words = 0
        last_char = ""
        mistake_made = False
        mistake_index = 0
        mistake_rate = 0.04
        lines = text.splitlines()
        for index, line in enumerate(lines):
            for char_index, char in enumerate(line):
                delay = random.uniform(avg_pause * 0.9, avg_pause * 1.2)

                if mistakes and random.random() < mistake_rate and not mistake_made:
                    selected_char = random.choice("abcdefghijklmnopqrstuvwxyz")
                    mistake_made = True
                    mistake_index = char_index
                else:
                    selected_char = char

                if selected_char in keyDefinitions:
                    await self.page.keyboard.press(selected_char)
                else:
                    await self.page.keyboard.sendCharacter(selected_char)

                if selected_char == " ":
                    words += 1

                if (
                        selected_char in punctuation
                        and last_char not in punctuation
                        and words > 3
                        and random.random() < 0.1
                ):
                    delay += random.uniform(0.2, 0.75)
                    words = 0

                if mistake_made and (
                        char_index - mistake_index > random.randint(1, 5)
                        or char_index == len(line) - 1
                ):
                    await self.sleep(0.5, 0.8)
                    # Moving back to the mistake
                    for _ in range(char_index - mistake_index + 1):
                        await self.page.keyboard.press("Backspace")
                        await self.sleep(random.uniform(avg_pause * 0.8, avg_pause * 1))
                    # Retyping the correct text from the mistake point
                    for correct_char in line[mistake_index: char_index + 1]:
                        await self.page.keyboard.sendCharacter(correct_char)
                        await self.sleep(avg_pause * 0.9, avg_pause * 1.2)
                    mistake_made = False

                last_char = char
                await self.sleep(delay)
            if index != len(lines) - 1:
                await self.page.keyboard.press("Enter")
                if random.random() < 0.1:
                    random.uniform(0.2, 0.75)

        await self.sleep(random.random() * 2)

    async def upload_file(
            self, selector: Union[ElementHandle, str], *file_paths: str
    ) -> None:
        if isinstance(selector, str):
            element = await self.find(selector)
        else:
            element = selector

        await element.uploadFile(*file_paths)

    async def click_random(self, selector: str, count: int = None) -> None:
        elements = await self.find_all(selector)

        if count is None:
            count = random.randint(0, int(len(elements) * 0.65))

        selected_elements: List[ElementHandle] = random.choices(elements, k=count)

        for element in selected_elements:
            await self.click(element)

            await self.sleep(0.5, 0.84)

    async def check_fingerprint(self):
        await self.page.goto("https://iphey.com/")
        await self.sleep(10)

        await self.save_screenshot("screenshots", "fingerprint_check.png")

        title_text = await self.page.querySelectorEval(
            ".fw-500", "(element) => element.textContent"
        )

        self.logger.info(f"-------->{title_text}<--------")

    async def solve_challenge(self):
        import time

        await self.page.goto("https://bot.incolumitas.com/")
        # Handle the dialog
        self.page.on("dialog", lambda dialog: time.sleep(2))
        await self.sleep(random.random())

        # Wait for the form to appear on the page
        while not await self.is_present("#formStuff"):
            await self.sleep(1)

        # Overwrite the existing text in the 'userName' field
        await self.type('input[name="userName"]', "bot3000")

        # Overwrite the existing text in the 'eMail' field
        await self.type('input[name="eMail"]', "bot3000@gmail.com")

        # Select an option from a dropdown
        await self.click('select[name="cookies"]')
        await self.sleep(1)
        await self.page.select('[name="cookies"]', "I want all the Cookies")

        await self.click('input[name="terms"]')

        # Click buttons
        await self.click("#smolCat")
        await self.click("#bigCat")

        # Submit the form
        await self.click("#submit")
        await self.sleep(0.2)

        # Wait for results to appear
        await self.find("#tableStuff tbody tr .url")
        await self.sleep(0.1)  # Sleep for 100 ms

        # Update prices
        await self.find("#updatePrice0")
        await self.click("#updatePrice0")
        await self.page.waitForFunction(
            'document.getElementById("price0").getAttribute("data-last-update")'
        )

        await self.find("#updatePrice1")
        await self.click("#updatePrice1")
        await self.page.waitForFunction(
            'document.getElementById("price1").getAttribute("data-last-update")'
        )

        # Scrape the response
        data = await self.page.evaluate(
            """() => {
            let results = [];
            document.querySelectorAll('#tableStuff tbody tr').forEach((row) => {
                results.push({
                    name: row.querySelector('.name').innerText,
                    price: row.querySelector('.price').innerText,
                    url: row.querySelector('.url').innerText,
                })
            });
            return results;
        }"""
        )
        print(data)
        await self.sleep(20)

    async def check_bot(self):
        await self.page.goto("https://antoinevastel.com/bots/datadome")
        await self.sleep(2)

        await self.save_screenshot("screenshots", "bot_detection.png")
        await self.sleep(2)

        await self.page.goto("https://arh.antoinevastel.com/bots/areyouheadless")
        await self.sleep(2)

        await self.save_screenshot("screenshots", "headless_detection.png")

        await self.page.goto("https://pixelscan.net/")
        await self.sleep(6)

        await self.save_screenshot("screenshots", "pixelscan.png")


class WYANGovClient(BasePuppeteerClient):
    STREET_LABELS = [
        "AddressNumberPrefix",
        "AddressNumber",
        "AddressNumberSuffix",
        "StreetNamePreModifier",
        "StreetNamePreDirectional",
        "StreetNamePreType",
        "StreetName",
        "StreetNamePostType",
        "StreetNamePostDirectional",
        "SubaddressType",
        "SubaddressIdentifier",
        "BuildingName",
        "OccupancyType",
        "OccupancyIdentifier",
        "CornerOf",
        "LandmarkName"
    ]

    IGNORED_CASE_PREFIX = [
        "TOW",
        "GRA"
    ]

    def parse_address(self, address_str: str):
        street_str = ""
        parsed_address = usaddress.parse(address_str)

        address_dict = {}
        for value, key in parsed_address:
            address_dict[key] = address_dict[key] + f" {value}" if address_dict.get(key) else value

        for label in self.STREET_LABELS:
            try:
                street_str += f"{address_dict[label].title()} "
            except KeyError:
                pass

        parsed_address_dict = {
            "street_name": street_str.strip().replace(",", "").title(),
            "city": address_dict.get("PlaceName").title(),
            "state": address_dict.get("StateName"),
            "zipcode": address_dict.get("ZipCode").title()
        }

        return parsed_address_dict

    def check_case_number(self, case_number: str):
        for case_prefix in self.IGNORED_CASE_PREFIX:
            if case_number.startswith(case_prefix):
                return True

    async def get_code_violations(self):
        yesterday = datetime.today() - timedelta(days=1)
        await self.page.goto("https://mauwi.wycokck.org/CitizenAccess/Welcome.aspx")
        await self.sleep(5)

        await self.click('a[title="Property Maintenance"]')
        await self.sleep(5)

        start_date_elem = await self.find('input[name="ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate"]')
        end_date_elem = await self.find('input[name="ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate"]')
        await self.page.evaluate('(element, value) => { element.value = value; }', start_date_elem,
                                   yesterday.strftime("%m/%d/%Y"))
        await self.page.evaluate('(element, value) => { element.value = value; }', end_date_elem,
                                 yesterday.strftime("%m/%d/%Y"))

        await self.click('a[id="ctl00_PlaceHolderMain_btnNewSearch"]')
        await self.sleep(4)

        addresses = []
        page_elements = await self.find_all('.aca_pagination_td')
        for _ in range(len(page_elements[2:-1])):
            await self.page.keyboard.press("PageDown")
            await self.page.keyboard.press("PageDown")
            await self.page.keyboard.press("PageDown")

            code_violations = await self.find_all(".ACA_TabRow_Odd, .ACA_TabRow_Even")

            for code_violation in code_violations:
                case_number_element = await code_violation.querySelector('[id$="PermitNumber"]')
                case_number = await self.page.evaluate('(element) => element.textContent', case_number_element)

                if not self.check_case_number(case_number):
                    address_element = await code_violation.querySelector('[id$="_lblAddress"]')
                    address = await self.page.evaluate('(element) => element.textContent', address_element)
                    addresses.append(self.parse_address(address))

            pagination_buttons = await self.find_all('.aca_pagination_PrevNext')
            await pagination_buttons[-1].click()

            await self.sleep(2, 4)

        return addresses


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
        super(HushedClient, self).__init__(uuid, capabilities, logger, appium_url)

    def go_to_messages_screen(self, number: str):
        number_xpath = f'//android.widget.TextView[@resource-id="com.hushed.release:id/drawer_number_subtitle" and @text="{number}"]'
        selected_number_xpath = f'//android.widget.TextView[@resource-id="com.hushed.release:id/tvTitle" and @text="{number}"]'
        if self.is_present(AppiumBy.XPATH, number_xpath):
            number_btn = self.locate(AppiumBy.XPATH, number_xpath)
            self.click(number_btn)
            return

        on_message_screen = self.is_present(AppiumBy.XPATH, "//android.widget.LinearLayout[@resource-id='com.hushed.release:id/collapsibleHeader']//android.widget.TextView[@text='Messages']")
        number_selected = self.is_present(AppiumBy.XPATH, selected_number_xpath)
        if not (on_message_screen and number_selected):
            back_btn_id = "com.hushed.release:id/headerButtonLeft"
            tries = 0
            while self.is_present(AppiumBy.ID, back_btn_id) and not number_selected and tries < 3:
                back_btn = self.locate(AppiumBy.ID, back_btn_id)
                self.click(back_btn)
                number_selected = self.is_present(AppiumBy.XPATH, selected_number_xpath)
                tries += 1

            if not self.is_present(AppiumBy.XPATH, number_xpath) and not number_selected:
                hamburger_menu = self.locate(AppiumBy.ID, "com.hushed.release:id/btnHamburger")
                self.click(hamburger_menu)

            if not number_selected:
                number_btn = self.locate(AppiumBy.XPATH, number_xpath)
                self.click(number_btn)

    def send_sms(self, sender_number: str, recipient: str, message: str):
        self.go_to_messages_screen(sender_number)

        self.sleep(2)

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
    def __init__(self, credentials_file, spreadsheet_name, logger: logging.Logger):
        self.credentials_file = credentials_file
        self.spreadsheet_name = spreadsheet_name
        self.client: Union[None, gspread.Client] = None
        self.sheet: Union[None, gspread.Worksheet] = None
        self.logger = logger

        # Define the scope of the application
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

        # Load credentials from the service account JSON file
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_file, scope)

        # Authenticate and create a client
        self.client = gspread.authorize(credentials)

    # Function to perform API requests with exponential backoff
    def _perform_request_with_backoff(self, func, *args, **kwargs):
        max_retries = 5
        for retry in range(max_retries):
            try:
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as e:
                if "Rate Limit Exceeded" in str(e):
                    wait_time = (2 ** retry) * 0.5  # Exponential backoff
                    self.logger.warning(f"Rate limit exceeded. Waiting for {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                else:
                    raise e  # Re-raise if it's not a rate limit error
        raise Exception("Max retries exceeded")

    def open_sheet(self, sheet_name):
        self.sheet = self.client.open(self.spreadsheet_name).worksheet(sheet_name)

    def read_records(self):
        # Read all records from the sheet
        if self.sheet is not None:
            return self._perform_request_with_backoff(self.sheet.get_all_records)
        else:
            raise Exception("Sheet not opened. Please call open_sheet() method first.")

    def get_column_index(self, column_name):
        """
        Retrieve the column index for a given column name based on the first row.

        :param column_name: The name of the column to find.
        :return: The 1-based index of the column.
        """
        row_values = self._perform_request_with_backoff(self.sheet.row_values, 1)  # assuming the first row contains headers
        return row_values.index(column_name) + 1

    def get_last_row(self):
        return len(self._perform_request_with_backoff(self.sheet.get_all_records)) + 2

    def update_cell(self, row_number, column_index, value):
        self._perform_request_with_backoff(self.sheet.update_cell, row_number, column_index, value)


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

    def skip_trace(self, city: str, street: str, state: str, zipcode: str, first_name: str = "", last_name: str = "", num_phones: int = 3):
        headers = self._get_headers()
        body = {
            "requests": [
                {
                    "propertyAddress": {
                        "city": city,
                        "street": street,
                        "state": state,
                        "zip": zipcode
                    }
                }
            ]
        }

        if first_name and last_name:
            body["name"] = {
                "first": first_name,
                "last": last_name
            }

        phone_numbers = []
        try:
            response = requests.post(f"{self.BASE_URL}/property/skip-trace", json=body, headers=headers).json()
            people = response.get("results", {}).get("persons", [])

            for person in people:
                if not person.get("death", {}).get("deceased", False):
                    phones_list = person.get("phoneNumbers", [])
                    mobile_numbers = [num["number"] for num in phones_list if num["type"] == "Mobile"]
                    phone_numbers.extend(mobile_numbers[:num_phones])

        except (KeyError, IndexError) as e:
            self.logger.error(e, exc_info=True)

        return phone_numbers


async def test():
    async with WYANGovClient(logging.getLogger(__name__), width=1920, height=1920) as client:
        print(await client.get_code_violations())


if __name__ == "__main__":
    asyncio.run(test())
