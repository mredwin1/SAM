import datetime
import json
import logging
import os
import random

from clients import GoogleSheetClient, HushedClient

script_dir = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger("leads_manager_logger")
file_handler = logging.FileHandler("leads_manager.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)


def calculate_messages_to_send(messages_left: int, current_time: datetime.datetime, total_sent_this_hour: int,
                               max_per_hour: int, send_probability: int, interval=5):
    minutes_to_hour = 60 - current_time.minute
    intervals_left = (minutes_to_hour + interval - 1) // interval
    max_allowed_this_hour = max_per_hour - total_sent_this_hour
    if intervals_left > 1 and random.randint(0, 100) >= send_probability:
        return 0

    num_messages_to_send = messages_left // ((60 / interval) - current_time.minute // interval)

    num_messages_to_send = min(num_messages_to_send, max_allowed_this_hour, max_allowed_this_hour // intervals_left)
    return num_messages_to_send


def send_messages(sheet_client: GoogleSheetClient, config: dict):
    sheet_client.open_sheet("Message Queue")
    messages = sheet_client.read_records()
    time_sent_column_number = sheet_client.get_column_index("DateTimeSent")
    sender_number_column_number = sheet_client.get_column_index("SenderNumber")
    messages_to_send = []
    execution_time = datetime.datetime.now()

    numbers = {number: 0 for number in config["numbers_for_send"]}

    for index, message in enumerate(messages):
        row_num = index + 2
        datetime_sent_str = message["DateTimeSent"]
        message_str = str(message["Message"])
        recipient_str = str(message["Recipient"])
        sender_number = str(message["SenderNumber"])
        try:
            if not datetime_sent_str and message_str and recipient_str:
                messages_to_send.append({
                    "row_num": row_num,
                    "recipient": recipient_str,
                    "message": message_str
                })
            elif message["DateTimeSent"] and sender_number:
                try:
                    datetime_sent = datetime.datetime.strptime(datetime_sent_str, "%m/%d/%Y %H:%M:%S")

                    if datetime_sent.year == execution_time.year and datetime_sent.month == execution_time.month and datetime_sent.day == execution_time.day and datetime_sent.hour == execution_time.hour:
                        numbers[sender_number] += 1
                except KeyError:
                    pass
                except ValueError:
                    logger.error(f"Could not parse DateTimeSent in row number {row_num}")
        except KeyError:
            logger.warning(f"Skipping processing row {row_num} due to missing columns")

    with HushedClient(config["phone_uuid"], logger, config["appium_url"]) as client:
        available_numbers = [key for key, value in numbers.items() if value < config["max_number_of_messages_to_send"]]
        if available_numbers:
            run_interval = config["leads_manager_run_interval"]
            max_messages_per_hour = config["max_number_of_messages_to_send"] * len(config["numbers_for_send"])
            chance_to_send_messages = config["chance_to_send"]
            num_messages_to_send = calculate_messages_to_send(len(messages_to_send), execution_time, sum(numbers.values()), max_messages_per_hour, chance_to_send_messages, run_interval)
            logger.info(f"{len(messages_to_send)} messages in queue and chose to send {num_messages_to_send} right now")
            for x in range(num_messages_to_send):
                try:
                    message_to_send = messages_to_send.pop()
                    number_for_sending = random.choice(available_numbers)

                    client.send_sms(number_for_sending, message_to_send["recipient"], message_to_send["message"])
                    now = datetime.datetime.now()
                    sheet_client.update_cell(message_to_send["row_num"], time_sent_column_number, now.strftime("%m/%d/%Y %H:%M:%S"))
                    sheet_client.update_cell(message_to_send["row_num"], sender_number_column_number, number_for_sending)
                    logger.info(f"Sent \"{message_to_send['message']}\" to {message_to_send['recipient']} from {number_for_sending}")

                    numbers[number_for_sending] += 1
                    available_numbers = [key for key, value in numbers.items() if value < config["max_number_of_messages_to_send"]]
                except IndexError:
                    pass


if __name__ == "__main__":
    with open(os.path.join(script_dir, 'config.json'), 'rb') as config_file:
        master_config = json.load(config_file)

    google_sheet_client = GoogleSheetClient(os.path.join(script_dir, "credentials-file.json"), "SAM", logger)

    send_messages(google_sheet_client, master_config)
