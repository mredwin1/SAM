import datetime
import json
import logging
import os
import random

from clients import GoogleSheetClient, BatchDataClient, HushedClient

script_dir = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger("leads_manager_logger")
file_handler = logging.FileHandler("leads_manager.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)


def validate_lead(lead: dict):
    try:
        if lead["SkipTraceSuccess"] == "" and lead["ContactCity"] and lead["ContactStreet"] and lead["ContactState"] and lead["ContactZip"] and not (lead["ContactPhone1"] and lead["ContactPhone2"] and lead["ContactPhone3"]):
            return True
    except KeyError as e:
        logger.error(e, exc_info=True)

    return False


def skip_trace(sheet_client: GoogleSheetClient):
    sheet_client.open_sheet("Leads Master")

    leads = sheet_client.read_records()

    with BatchDataClient(logger, os.path.join(script_dir, "config.json")) as client:
        for index, lead in enumerate(leads):
            row_num = index + 2
            if validate_lead(lead):
                try:
                    traced_phone_numbers = client.skip_trace(
                        str(lead["ContactCity"]),
                        str(lead["ContactStreet"]),
                        str(lead["ContactState"]),
                        str(lead["ContactZip"]),
                        first_name=str(lead["ContactFirstName"]),
                        last_name=str(lead["ContactLastName"])
                    )

                    if traced_phone_numbers:
                        sheet_client.update_cell(row_num, 1, "TRUE")
                        try:
                            sheet_client.update_cell(row_num, 12, traced_phone_numbers[0])
                            sheet_client.update_cell(row_num, 14, traced_phone_numbers[1])
                            sheet_client.update_cell(row_num, 16, traced_phone_numbers[2])
                        except IndexError:
                            pass
                    else:
                        logger.warning(f"No phone numbers found for lead: {lead}")
                        sheet_client.update_cell(row_num, 1, "FALSE")
                except Exception as e:
                    logger.error(e, exc_info=True)
                    sheet_client.update_cell(row_num, 1, "FALSE")


def validate_phones_lead(lead: dict):
    try:
        if (lead["SMS1QueuedDateTime"] == "" or lead["SMS2QueuedDateTime"] == "" or lead["SMS3QueuedDateTime"] == "") and lead["TargetStreet"]:
            return True
    except KeyError as e:
        logger.error(e, exc_info=True)

    return False


def not_queued_and_phone_present(lead, phone_key, queued_key):
    return lead[queued_key] == "" and lead[phone_key] != ""


def is_delay_met_for_phone(lead: dict, message_index: int, config: dict):
    if message_index > 1:
        previous_queued_key = f"SMS{message_index - 1}QueuedDateTime"
        if lead[previous_queued_key] != "":
            previous_queued_time = datetime.datetime.strptime(lead[previous_queued_key], "%m/%d/%Y %H:%M:%S")
            return previous_queued_time + datetime.timedelta(days=config["delay_between_messages"]) < datetime.datetime.now()
    return True


def queue_message(sheet_client: GoogleSheetClient, row_num: int, message: str, recipient: str):
    message_col_num = sheet_client.get_column_index("Message")
    recipient_col_num = sheet_client.get_column_index("Recipient")
    time_queued_col_num = sheet_client.get_column_index("DateTimeQueued")

    now = datetime.datetime.now()
    time_queued_str = now.strftime("%m/%d/%Y %H:%M:%S")
    sheet_client.update_cell(row_num, message_col_num, message)
    sheet_client.update_cell(row_num, recipient_col_num, recipient)
    sheet_client.update_cell(row_num, time_queued_col_num, time_queued_str)

    return time_queued_str


def queue_messages(sheet_client: GoogleSheetClient):
    with open(os.path.join(script_dir, 'config.json'), 'rb') as config_file:
        config = json.load(config_file)

    sheet_client.open_sheet("Message Templates")
    messages = sheet_client.read_records()
    messages = [message["Message"] for message in messages]

    sheet_client.open_sheet("Leads Master")

    leads = sheet_client.read_records()

    queue_message_sheet_client = GoogleSheetClient(os.path.join(script_dir, "credentials-file.json"), "SAM", logger)
    queue_message_sheet_client.open_sheet("Message Queue")
    queue_last_row = queue_message_sheet_client.get_last_row()

    for index, lead in enumerate(leads):
        row_num = index + 2
        if validate_phones_lead(lead):
            for message_index in range(1, 4):
                phone_key = f"ContactPhone{message_index}"
                queued_key = f"SMS{message_index}QueuedDateTime"
                if not_queued_and_phone_present(lead, phone_key, queued_key) and is_delay_met_for_phone(lead, message_index, config):
                    message = random.choice(messages).replace("{TargetStreet}", lead["TargetStreet"])
                    time_queued = queue_message(queue_message_sheet_client, queue_last_row, message, lead[phone_key])
                    sheet_client.update_cell(row_num, sheet_client.get_column_index(queued_key), time_queued)
                    queue_last_row += 1


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

    skip_trace(google_sheet_client)
    queue_messages(google_sheet_client)
    send_messages(google_sheet_client, master_config)
