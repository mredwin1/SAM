import datetime
import json
import logging
import os
import random

from clients import GoogleSheetClient

script_dir = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger("sms_queuer_logger")
file_handler = logging.FileHandler("sms_queuer.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)


def validate_lead(lead: dict):
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

    queue_message_sheet_client = GoogleSheetClient(os.path.join(script_dir, "credentials-file.json"), "SAM")
    queue_message_sheet_client.open_sheet("Message Queue")
    queue_last_row = queue_message_sheet_client.get_last_row()

    for index, lead in enumerate(leads):
        row_num = index + 2
        if validate_lead(lead):
            for message_index in range(1, 4):
                phone_key = f"ContactPhone{message_index}"
                queued_key = f"SMS{message_index}QueuedDateTime"
                if not_queued_and_phone_present(lead, phone_key, queued_key) and is_delay_met_for_phone(lead, message_index, config):
                    message = random.choice(messages).replace("{TargetStreet}", lead["TargetStreet"])
                    time_queued = queue_message(queue_message_sheet_client, queue_last_row, message, lead[phone_key])
                    sheet_client.update_cell(row_num, sheet_client.get_column_index(queued_key), time_queued)
                    queue_last_row += 1


if __name__ == "__main__":
    google_sheet_client = GoogleSheetClient(os.path.join(script_dir, "credentials-file.json"), "SAM")
    queue_messages(google_sheet_client)
